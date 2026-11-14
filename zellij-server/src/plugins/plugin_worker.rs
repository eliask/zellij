use crate::plugins::plugin_map::PluginEnv;
use crate::plugins::zellij_exports::wasi_write_object;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use wasmi::{Instance, Store};

use prost::Message;
use tokio::sync::mpsc::{channel, error::TrySendError, Receiver, Sender};
use zellij_utils::errors::prelude::*;
use zellij_utils::plugin_api::message::ProtobufMessage;

/// Worker callbacks can queue at most this many messages before back-pressure rejects new work.
/// A plugin that floods its worker faster than the worker runs must not grow host memory without
/// bound.
pub(crate) const WORKER_CHANNEL_CAPACITY: usize = 64;

pub struct RunningWorker {
    pub instance: Instance,
    pub name: String,
    pub store: Store<PluginEnv>,
}

impl RunningWorker {
    pub fn new(store: Store<PluginEnv>, instance: Instance, name: &str) -> Self {
        RunningWorker {
            store,
            instance,
            name: name.into(),
        }
    }
    pub fn send_message(&mut self, message: String, payload: String) -> Result<()> {
        let err_context = || format!("Failed to send message to worker");
        let protobuf_message = ProtobufMessage {
            name: message,
            payload,
            ..Default::default()
        };
        let protobuf_bytes = protobuf_message.encode_to_vec();
        let work_function = self
            .instance
            .get_typed_func::<(), ()>(&mut self.store, &self.name)
            .with_context(err_context)?;
        wasi_write_object(self.store.data(), &protobuf_bytes).with_context(err_context)?;
        crate::plugins::shared::refuel_for_callback(&mut self.store);
        work_function
            .call(&mut self.store, ())
            .with_context(err_context)?;
        Ok(())
    }
}

pub enum MessageToWorker {
    Message(String, String), // message, payload
    Exit,
}

/// Handle for one worker task. Retirement is a flag the task observes even when `Exit` cannot
/// be queued, so a full channel cannot keep the guest running after unload.
#[derive(Clone)]
pub struct PluginWorkerHandle {
    sender: Sender<MessageToWorker>,
    stop: Arc<AtomicBool>,
}

impl PluginWorkerHandle {
    /// Queue one message. A full queue or a retired worker rejects it.
    pub fn try_post(&self, message: String, payload: String) -> bool {
        if !worker_should_run(&self.stop) {
            return false;
        }
        try_send_to_worker(&self.sender, MessageToWorker::Message(message, payload))
    }

    /// Stop the worker. The flag is set before the `Exit` send so a full queue still retires it.
    pub fn retire(&self) {
        self.stop.store(true, Ordering::Release);
        let _ = try_send_to_worker(&self.sender, MessageToWorker::Exit);
    }
}

fn worker_should_run(stop: &AtomicBool) -> bool {
    !stop.load(Ordering::Acquire)
}

/// Queue a message for a worker, rejecting it when the bounded queue is full.
pub(crate) fn try_send_to_worker(
    sender: &Sender<MessageToWorker>,
    message: MessageToWorker,
) -> bool {
    match sender.try_send(message) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            log::warn!("Plugin worker message queue is full; rejecting message");
            false
        },
        Err(TrySendError::Closed(_)) => false,
    }
}

pub fn plugin_worker(mut worker: RunningWorker) -> PluginWorkerHandle {
    let (sender, mut receiver): (Sender<MessageToWorker>, Receiver<MessageToWorker>) =
        channel(WORKER_CHANNEL_CAPACITY);
    let stop = Arc::new(AtomicBool::new(false));
    let task_stop = Arc::clone(&stop);
    crate::global_async_runtime::get_tokio_runtime().spawn({
        async move {
            loop {
                if !worker_should_run(&task_stop) {
                    break;
                }
                match receiver.recv().await {
                    Some(MessageToWorker::Message(message, payload)) => {
                        // Unload may have retired this worker while it was waiting. Do not enter
                        // the guest once that has happened; messages already queued are discarded.
                        // The guest itself runs on the blocking pool. This runtime has four
                        // worker threads, and a wasmi call on one of them stalls the rest.
                        if !worker_should_run(&task_stop) {
                            break;
                        }
                        let call_stop = Arc::clone(&task_stop);
                        let call = tokio::task::spawn_blocking(move || {
                            // Retirement can land after this message was accepted and before a
                            // blocking thread starts it. Check again on that thread, with no
                            // await between the check and the guest call.
                            if !worker_should_run(&call_stop) {
                                return (worker, None);
                            }
                            let result = worker.send_message(message, payload);
                            (worker, Some(result))
                        });
                        match call.await {
                            Ok((returned, Some(Ok(())))) => {
                                worker = returned;
                            },
                            Ok((_returned, None)) => {
                                break;
                            },
                            Ok((returned, Some(Err(e)))) => {
                                log::error!("Failed to send message to worker: {:?}", e);
                                // Other failures leave the plugin running, as they always have. A
                                // worker that exhausts its fuel is what a runaway plugin looks like,
                                // so it is treated like the plugin's own callbacks doing so.
                                if crate::plugins::shared::is_out_of_fuel(&e) {
                                    let data = returned.store.data();
                                    crate::plugins::wasm_bridge::handle_plugin_crash(
                                        data.plugin_id,
                                        format!("{:?}", e).replace('\n', "\n\r"),
                                        data.senders.clone(),
                                    );
                                    break;
                                }
                                worker = returned;
                            },
                            Err(error) => {
                                log::error!("Plugin worker panicked: {}", error);
                                break;
                            },
                        }
                    },
                    Some(MessageToWorker::Exit) => {
                        break;
                    },
                    None => {
                        log::error!("Failed to receive worker message on channel");
                        break;
                    },
                }
            }
        }
    });
    PluginWorkerHandle { sender, stop }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_message_queues_are_bounded() {
        let (sender, _receiver) = channel(WORKER_CHANNEL_CAPACITY);
        let handle = PluginWorkerHandle {
            sender,
            stop: Arc::new(AtomicBool::new(false)),
        };
        for index in 0..WORKER_CHANNEL_CAPACITY {
            assert!(handle.try_post(format!("message-{index}"), "payload".to_owned()));
        }
        assert!(
            !handle.try_post("overflow".to_owned(), "payload".to_owned()),
            "a full worker queue must reject the message instead of growing"
        );
    }

    #[test]
    fn a_retired_worker_rejects_new_messages() {
        let (sender, _receiver) = channel(WORKER_CHANNEL_CAPACITY);
        let handle = PluginWorkerHandle {
            sender,
            stop: Arc::new(AtomicBool::new(false)),
        };
        assert!(worker_should_run(&handle.stop));
        handle.retire();
        assert!(
            !worker_should_run(&handle.stop),
            "a callback queued before retirement must not be dispatched"
        );
        assert!(
            !handle.try_post("after-retire".to_owned(), "payload".to_owned()),
            "retirement must reject work even when the queue has room"
        );
    }

    #[test]
    fn retirement_sticks_when_the_queue_cannot_take_exit() {
        let (sender, _receiver) = channel(WORKER_CHANNEL_CAPACITY);
        let handle = PluginWorkerHandle {
            sender,
            stop: Arc::new(AtomicBool::new(false)),
        };
        for index in 0..WORKER_CHANNEL_CAPACITY {
            assert!(handle.try_post(format!("message-{index}"), "payload".to_owned()));
        }
        handle.retire();
        assert!(!worker_should_run(&handle.stop));
        assert!(!handle.try_post("still-going".to_owned(), "payload".to_owned()));
    }
}
