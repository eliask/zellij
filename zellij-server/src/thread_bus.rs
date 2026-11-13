//! Definitions and helpers for sending and receiving messages between threads.

use std::sync::Arc;

use crate::pty_ingress::PtyIngressBudget;
use crate::{
    background_jobs::BackgroundJob, os_input_output::ServerOsApi, plugins::PluginInstruction,
    pty::PtyInstruction, pty_writer::AdmittedWrite, pty_writer::PtyWriteInstruction,
    screen::ScreenInstruction, ServerInstruction,
};
use zellij_utils::errors::prelude::*;
use zellij_utils::{channels, channels::SenderWithContext, errors::ErrorContext};

/// Transport and admission budget for the pty-writer inbox.
///
/// Every `PtyWriteInstruction::Write` goes through [`PtyWriteSender::send`],
/// which reserves before the send and releases that reservation if the send
/// fails. Control instructions use the same transport and are not metered.
#[derive(Clone)]
pub struct PtyWriteSender {
    transport: SenderWithContext<PtyWriteInstruction>,
    budget: Option<Arc<PtyIngressBudget>>,
}

impl PtyWriteSender {
    pub fn new(
        transport: SenderWithContext<PtyWriteInstruction>,
        budget: Option<Arc<PtyIngressBudget>>,
    ) -> Self {
        PtyWriteSender { transport, budget }
    }

    pub fn budget(&self) -> Option<Arc<PtyIngressBudget>> {
        self.budget.clone()
    }

    pub fn set_budget(&mut self, budget: Arc<PtyIngressBudget>) {
        self.budget = Some(budget);
    }

    pub fn send(&self, instruction: PtyWriteInstruction) -> Result<()> {
        if matches!(instruction, PtyWriteInstruction::Write(_)) {
            self.admit_write(instruction)
        } else {
            self.send_control(instruction)
        }
    }

    fn send_control(&self, instruction: PtyWriteInstruction) -> Result<()> {
        self.transport
            .send(instruction)
            .to_anyhow()
            .context("failed to send message to pty writer")
    }

    /// Send a write that was not reserved. Used only for silent test buses
    /// that never installed a budget.
    fn send_unmetered(&self, instruction: PtyWriteInstruction) -> Result<()> {
        self.transport
            .send(instruction)
            .to_anyhow()
            .context("failed to send message to pty writer")
    }

    /// Reserve, then send. A missing budget or a failed send does not leave
    /// an unmetered write on the inbox.
    fn admit_write(&self, instruction: PtyWriteInstruction) -> Result<()> {
        let Some(budget) = self.budget.as_ref() else {
            return reject_unadmitted(
                instruction,
                "pty write was not admitted because no ingress budget is installed",
            );
        };
        let instruction = reserve_pty_write(budget, instruction)?;
        match self.transport.send(instruction) {
            Ok(()) => Ok(()),
            Err(error) => {
                let (failed, _) = error.into_inner();
                fail_unsent_pty_write(failed);
                Err(anyhow::anyhow!(
                    "the pty writer is gone and the write was not performed"
                ))
                .context("failed to send message to pty writer")
            },
        }
    }
}

/// A container for senders to the different threads in zellij on the server side
#[derive(Default, Clone)]
pub struct ThreadSenders {
    pub to_screen: Option<SenderWithContext<ScreenInstruction>>,
    pub to_screen_priority: Option<SenderWithContext<ScreenInstruction>>,
    pub to_pty: Option<SenderWithContext<PtyInstruction>>,
    pub to_plugin: Option<SenderWithContext<PluginInstruction>>,
    pub to_server: Option<SenderWithContext<ServerInstruction>>,
    pub to_pty_writer: Option<PtyWriteSender>,
    pub to_background_jobs: Option<SenderWithContext<BackgroundJob>>,
    // this is a convenience for the unit tests
    // it's not advisable to set it to true in production code
    pub should_silently_fail: bool,
}

impl ThreadSenders {
    pub fn send_to_screen_priority(&self, instruction: ScreenInstruction) -> Result<()> {
        match self.to_screen_priority.as_ref() {
            Some(sender) => {
                if self.should_silently_fail {
                    let _ = sender.send(instruction);
                    Ok(())
                } else {
                    sender
                        .send(instruction)
                        .to_anyhow()
                        .context("failed to send message to screen")
                }
            },
            None => self.send_to_screen(instruction),
        }
    }

    pub fn send_to_screen(&self, instruction: ScreenInstruction) -> Result<()> {
        if self.should_silently_fail {
            let _ = self
                .to_screen
                .as_ref()
                .map(|sender| sender.send(instruction))
                .unwrap_or_else(|| Ok(()));
            Ok(())
        } else {
            self.to_screen
                .as_ref()
                .context("failed to get screen sender")?
                .send(instruction)
                .to_anyhow()
                .context("failed to send message to screen")
        }
    }

    pub fn send_to_pty(&self, instruction: PtyInstruction) -> Result<()> {
        if self.should_silently_fail {
            let _ = self
                .to_pty
                .as_ref()
                .map(|sender| sender.send(instruction))
                .unwrap_or_else(|| Ok(()));
            Ok(())
        } else {
            self.to_pty
                .as_ref()
                .context("failed to get pty sender")?
                .send(instruction)
                .to_anyhow()
                .context("failed to send message to pty")
        }
    }

    pub fn send_to_plugin(&self, instruction: PluginInstruction) -> Result<()> {
        if self.should_silently_fail {
            let _ = self
                .to_plugin
                .as_ref()
                .map(|sender| sender.send(instruction))
                .unwrap_or_else(|| Ok(()));
            Ok(())
        } else {
            self.to_plugin
                .as_ref()
                .context("failed to get plugin sender")?
                .send(instruction)
                .to_anyhow()
                .context("failed to send message to plugin")
        }
    }

    pub fn send_to_server(&self, instruction: ServerInstruction) -> Result<()> {
        if self.should_silently_fail {
            let _ = self
                .to_server
                .as_ref()
                .map(|sender| sender.send(instruction))
                .unwrap_or_else(|| Ok(()));
            Ok(())
        } else {
            self.to_server
                .as_ref()
                .context("failed to get server sender")?
                .send(instruction)
                .to_anyhow()
                .context("failed to send message to server")
        }
    }
    pub fn send_to_pty_writer(&self, instruction: PtyWriteInstruction) -> Result<()> {
        let write = matches!(instruction, PtyWriteInstruction::Write(_));
        let Some(sender) = self.to_pty_writer.as_ref() else {
            if write {
                if self.should_silently_fail {
                    // Test buses often have no writer. Nothing is queued, so
                    // this is not an unmetered admission. Drop settles the
                    // completion the way a writer that shuts down would.
                    drop(instruction);
                    return Ok(());
                }
                return reject_unadmitted(
                    instruction,
                    "pty write was not admitted because the pty writer sender is missing",
                );
            }
            if self.should_silently_fail {
                return Ok(());
            }
            return Err(anyhow::anyhow!("failed to get pty writer sender"))
                .context("failed to send message to pty writer");
        };
        if write {
            // A budget, when installed, always meters the write. Silent test
            // buses that never installed one keep the old unmetered send so
            // existing fixtures still enqueue.
            if sender.budget().is_none() && self.should_silently_fail {
                let _ = sender.send_unmetered(instruction);
                return Ok(());
            }
            return sender.send(instruction);
        }
        if self.should_silently_fail {
            let _ = sender.send(instruction);
            return Ok(());
        }
        sender.send(instruction)
    }
    pub fn send_to_background_jobs(&self, background_job: BackgroundJob) -> Result<()> {
        if self.should_silently_fail {
            let _ = self
                .to_background_jobs
                .as_ref()
                .map(|sender| sender.send(background_job))
                .unwrap_or_else(|| Ok(()));
            Ok(())
        } else {
            self.to_background_jobs
                .as_ref()
                .context("failed to get background jobs sender")?
                .send(background_job)
                .to_anyhow()
                .context("failed to send message to background jobs")
        }
    }

    #[allow(unused)]
    pub fn silently_fail_on_send(mut self) -> Self {
        // this is mostly used for the tests, see struct
        self.should_silently_fail = true;
        self
    }
    #[allow(unused)]
    pub fn replace_to_pty_writer(
        &mut self,
        new_pty_writer: SenderWithContext<PtyWriteInstruction>,
    ) {
        // this is mostly used for the tests, see struct
        let budget = self
            .to_pty_writer
            .as_ref()
            .and_then(|sender| sender.budget());
        self.to_pty_writer
            .replace(PtyWriteSender::new(new_pty_writer, budget));
    }

    pub fn set_pty_write_budget(&mut self, budget: Arc<PtyIngressBudget>) {
        if let Some(sender) = self.to_pty_writer.as_mut() {
            sender.set_budget(budget);
        }
    }
    #[allow(unused)]
    pub fn replace_to_pty(&mut self, new_pty: SenderWithContext<PtyInstruction>) {
        // this is mostly used for the tests, see struct
        self.to_pty.replace(new_pty);
    }

    #[allow(unused)]
    pub fn replace_to_plugin(&mut self, new_to_plugin: SenderWithContext<PluginInstruction>) {
        // this is mostly used for the tests, see struct
        self.to_plugin.replace(new_to_plugin);
    }
}

/// A container for a receiver, OS input and the senders to a given thread
#[derive(Default)]
pub(crate) struct Bus<T> {
    receivers: Vec<channels::Receiver<(T, ErrorContext)>>,
    priority_receiver: Option<channels::Receiver<(T, ErrorContext)>>,
    pub senders: ThreadSenders,
    pub os_input: Option<Box<dyn ServerOsApi>>,
}

impl<T> Bus<T> {
    pub fn new(
        receivers: Vec<channels::Receiver<(T, ErrorContext)>>,
        to_screen: Option<&SenderWithContext<ScreenInstruction>>,
        to_pty: Option<&SenderWithContext<PtyInstruction>>,
        to_plugin: Option<&SenderWithContext<PluginInstruction>>,
        to_server: Option<&SenderWithContext<ServerInstruction>>,
        to_pty_writer: Option<&SenderWithContext<PtyWriteInstruction>>,
        to_background_jobs: Option<&SenderWithContext<BackgroundJob>>,
        os_input: Option<Box<dyn ServerOsApi>>,
    ) -> Self {
        Bus {
            receivers,
            priority_receiver: None,
            senders: ThreadSenders {
                to_screen: to_screen.cloned(),
                to_screen_priority: None,
                to_pty: to_pty.cloned(),
                to_plugin: to_plugin.cloned(),
                to_server: to_server.cloned(),
                to_pty_writer: to_pty_writer
                    .cloned()
                    .map(|transport| PtyWriteSender::new(transport, None)),
                to_background_jobs: to_background_jobs.cloned(),
                should_silently_fail: false,
            },
            os_input: os_input.clone(),
        }
    }
    #[allow(unused)]
    pub fn should_silently_fail(mut self) -> Self {
        // this is mostly used for the tests
        self.senders.should_silently_fail = true;
        self
    }
    #[allow(unused)]
    pub fn empty() -> Self {
        // this is mostly used for the tests
        Bus {
            receivers: vec![],
            priority_receiver: None,
            senders: ThreadSenders {
                to_screen: None,
                to_screen_priority: None,
                to_pty: None,
                to_plugin: None,
                to_server: None,
                to_pty_writer: None,
                to_background_jobs: None,
                should_silently_fail: true,
            },
            os_input: None,
        }
    }

    pub fn with_priority(
        mut self,
        priority_receiver: channels::Receiver<(T, ErrorContext)>,
        priority_sender: &SenderWithContext<ScreenInstruction>,
    ) -> Self {
        self.priority_receiver = Some(priority_receiver);
        self.senders.to_screen_priority = Some(priority_sender.clone());
        self
    }

    pub fn recv(&self) -> Result<(T, ErrorContext), channels::RecvError> {
        let Some(priority_receiver) = self.priority_receiver.as_ref() else {
            let mut selector = channels::Select::new();
            self.receivers.iter().for_each(|r| {
                selector.recv(r);
            });
            let oper = selector.select();
            let idx = oper.index();
            return oper.recv(&self.receivers[idx]);
        };
        if let Ok(message) = priority_receiver.try_recv() {
            return Ok(message);
        }
        let mut selector = channels::Select::new();
        selector.recv(priority_receiver);
        self.receivers.iter().for_each(|r| {
            selector.recv(r);
        });
        let oper = selector.select();
        let idx = oper.index();
        match idx {
            0 => oper.recv(priority_receiver),
            _ => oper.recv(&self.receivers[idx - 1]),
        }
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(T, ErrorContext), channels::RecvTimeoutError> {
        let mut selector = channels::Select::new();
        self.receivers.iter().for_each(|r| {
            selector.recv(r);
        });
        match selector.select_timeout(timeout) {
            Ok(oper) => {
                let idx = oper.index();
                oper.recv(&self.receivers[idx])
                    .map_err(|_| channels::RecvTimeoutError::Disconnected)
            },
            Err(_) => Err(channels::RecvTimeoutError::Timeout),
        }
    }

    /// Non-blocking drain of every queue this bus owns. Stops when each
    /// receiver is empty or disconnected; it does not wait for a new message.
    pub fn drain_available(&self) -> Vec<T> {
        let mut drained = Vec::new();
        for receiver in &self.receivers {
            while let Ok((message, _)) = receiver.try_recv() {
                drained.push(message);
            }
        }
        if let Some(priority_receiver) = &self.priority_receiver {
            while let Ok((message, _)) = priority_receiver.try_recv() {
                drained.push(message);
            }
        }
        drained
    }
}

fn reserve_pty_write(
    budget: &Arc<PtyIngressBudget>,
    instruction: PtyWriteInstruction,
) -> Result<PtyWriteInstruction> {
    let PtyWriteInstruction::Write(admitted) = instruction else {
        return Ok(instruction);
    };
    // Disarm the caller's guard before try_reserve. Rejection must not build
    // another one: nothing was reserved, so nothing may be released.
    let (bytes, terminal_id, mut completion, _caller_budget) = admitted.into_parts();
    if let Err(message) = budget.try_reserve(terminal_id, bytes.len()) {
        fail_write_completion(&mut completion, message.clone());
        return Err(anyhow::anyhow!(message)).context("failed to send message to pty writer");
    }
    Ok(PtyWriteInstruction::Write(AdmittedWrite::new(
        bytes,
        terminal_id,
        completion,
        Some(Arc::clone(budget)),
    )))
}

fn reject_unadmitted(instruction: PtyWriteInstruction, message: &str) -> Result<()> {
    if let PtyWriteInstruction::Write(admitted) = instruction {
        let (_bytes, _terminal_id, mut completion, caller_budget) = admitted.into_parts();
        drop(caller_budget);
        fail_write_completion(&mut completion, message.to_string());
    }
    Err(anyhow::anyhow!(message.to_string())).context("failed to send message to pty writer")
}

/// Admission rejected the bytes. The waiter must see a finished failure, not
/// an empty drop (which is success).
fn fail_write_completion(completion: &mut Option<crate::route::NotificationEnd>, message: String) {
    if let Some(mut completion) = completion.take() {
        completion.set_exit_status(1);
        completion.set_error_message(message);
    }
}

/// The send failed after admission. Dropping a `Write` releases its reservation
/// and fails the completion. A control instruction is not a `Write`.
fn fail_unsent_pty_write(instruction: PtyWriteInstruction) {
    drop(instruction);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pty_ingress::PER_TERMINAL_LIMIT;
    use crate::route::NotificationEnd;

    fn budgeted_senders(
        budget: Arc<PtyIngressBudget>,
    ) -> (
        ThreadSenders,
        channels::Receiver<(PtyWriteInstruction, ErrorContext)>,
    ) {
        let (tx, rx) = channels::unbounded();
        let mut senders = ThreadSenders::default();
        senders.to_pty_writer = Some(PtyWriteSender::new(
            SenderWithContext::new(tx),
            Some(budget),
        ));
        (senders, rx)
    }

    #[test]
    fn admission_over_budget_does_not_send_or_stick() {
        let budget = Arc::new(PtyIngressBudget::new());
        budget.try_reserve(1, PER_TERMINAL_LIMIT).unwrap();
        let before = budget.reserved_bytes();
        let (senders, rx) = budgeted_senders(Arc::clone(&budget));
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let rejected = senders.send_to_pty_writer(PtyWriteInstruction::Write(AdmittedWrite::new(
            vec![0xAB],
            1,
            Some(NotificationEnd::new(completion_tx)),
            None,
        )));
        assert!(
            rejected.is_err(),
            "a write past the budget must not be sent"
        );
        assert!(
            rx.try_recv().is_err(),
            "rejection must not enqueue the write"
        );
        assert_eq!(budget.reserved_bytes(), before);
        assert_eq!(budget.terminal_reserved(1), PER_TERMINAL_LIMIT);
        let message = completion_rx
            .blocking_recv()
            .expect("rejected write resolves")
            .error_message
            .expect("rejection is not success");
        assert!(
            message.contains("rejected before it was queued"),
            "{message}"
        );
    }

    #[test]
    fn admitted_write_instruction_carries_the_reservation() {
        let budget = Arc::new(PtyIngressBudget::new());
        let (senders, rx) = budgeted_senders(Arc::clone(&budget));
        let payload = b"metered".to_vec();
        senders
            .send_to_pty_writer(PtyWriteInstruction::Write(AdmittedWrite::new(
                payload.clone(),
                4,
                None,
                None,
            )))
            .expect("the write is under the budget");
        assert_eq!(budget.reserved_bytes(), payload.len());
        let (instruction, _) = rx.try_recv().expect("the admitted write was sent");
        let PtyWriteInstruction::Write(write) = instruction else {
            panic!("admission must send a Write");
        };
        assert_eq!(write.bytes(), payload.as_slice());
        assert_eq!(write.terminal_id(), 4);
        drop(write);
        assert_eq!(
            budget.reserved_bytes(),
            0,
            "dropping the admitted instruction releases the reservation it carried"
        );
    }

    #[test]
    fn silent_missing_sender_discards_the_write_and_settles_completion() {
        let senders = ThreadSenders::default().silently_fail_on_send();
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let discarded = senders.send_to_pty_writer(PtyWriteInstruction::Write(AdmittedWrite::new(
            b"nowhere".to_vec(),
            1,
            Some(NotificationEnd::new(completion_tx)),
            None,
        )));
        assert!(
            discarded.is_ok(),
            "a test bus with no writer discards the write instead of failing the action"
        );
        let outcome = completion_rx
            .blocking_recv()
            .expect("the discarded write still settles");
        assert!(
            outcome.error_message.is_some(),
            "discard is not a successful write"
        );
        assert_eq!(outcome.exit_status, Some(1));
    }

    #[test]
    fn silent_missing_budget_sends_the_write_unmetered() {
        let (tx, rx) = channels::unbounded();
        let mut senders = ThreadSenders::default().silently_fail_on_send();
        senders.to_pty_writer = Some(PtyWriteSender::new(SenderWithContext::new(tx), None));
        senders
            .send_to_pty_writer(PtyWriteInstruction::write(b"legacy".to_vec(), 1, None))
            .expect("a silent bus without a budget still enqueues");
        let (instruction, _) = rx.try_recv().expect("the unmetered write was sent");
        let PtyWriteInstruction::Write(write) = instruction else {
            panic!("expected a Write");
        };
        assert_eq!(write.bytes(), b"legacy");
    }
}
