use super::*;
use crate::os_input_output::ServerOsApi;
use crate::plugins::PluginInstruction;
use crate::pty_writer::PtyWriteInstruction;
use crate::screen::ScreenInstruction;
use crate::thread_bus::Bus;
use interprocess::local_socket::Stream as LocalSocketStream;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use zellij_utils::channels::{self, SenderWithContext};
use zellij_utils::data::{Event, Palette};
use zellij_utils::errors::{ErrorContext, ZellijError};
use zellij_utils::input::command::RunCommand;
use zellij_utils::input::layout::{Layout, Run, SplitDirection, TiledPaneLayout};
use zellij_utils::ipc::{ClientToServerMsg, IpcReceiverWithContext, ServerToClientMsg};

#[derive(Clone)]
enum PlannedSpawn {
    Success { terminal_id: u32, child_pid: u32 },
    Error(String),
    CommandNotFound { terminal_id: u32, command: String },
}

#[derive(Clone)]
struct MockOsApi {
    cwds: Arc<Mutex<HashMap<u32, PathBuf>>>,
    cmds: Arc<Mutex<HashMap<u32, Vec<String>>>>,
    cmds_by_ppid: Arc<Mutex<HashMap<String, Vec<String>>>>,
    plans: Arc<Mutex<VecDeque<PlannedSpawn>>>,
    killed: Arc<Mutex<Vec<u32>>>,
    cleared: Arc<Mutex<Vec<u32>>>,
}

impl MockOsApi {
    fn new() -> Self {
        MockOsApi {
            cwds: Arc::new(Mutex::new(HashMap::new())),
            cmds: Arc::new(Mutex::new(HashMap::new())),
            cmds_by_ppid: Arc::new(Mutex::new(HashMap::new())),
            plans: Arc::new(Mutex::new(VecDeque::new())),
            killed: Arc::new(Mutex::new(Vec::new())),
            cleared: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn plan(&self, spawn: PlannedSpawn) {
        self.plans.lock().unwrap().push_back(spawn);
    }
    fn killed_pids(&self) -> Vec<u32> {
        self.killed.lock().unwrap().clone()
    }
    fn cleared_ids(&self) -> Vec<u32> {
        self.cleared.lock().unwrap().clone()
    }
    fn next_plan(&self) -> anyhow::Result<PlannedSpawn> {
        self.plans
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("no planned spawn"))
    }
    fn set_cwd(&self, pid: u32, path: PathBuf) {
        self.cwds.lock().unwrap().insert(pid, path);
    }
    fn set_cmd(&self, pid: u32, cmd: Vec<String>) {
        self.cmds.lock().unwrap().insert(pid, cmd);
    }
    fn set_foreground_cmd(&self, ppid: u32, cmd: Vec<String>) {
        self.cmds_by_ppid
            .lock()
            .unwrap()
            .insert(ppid.to_string(), cmd);
    }
    fn clear_foreground_cmd(&self, ppid: u32) {
        self.cmds_by_ppid.lock().unwrap().remove(&ppid.to_string());
    }
}

impl ServerOsApi for MockOsApi {
    fn set_terminal_size_using_terminal_id(
        &self,
        _: u32,
        _: u16,
        _: u16,
        _: Option<u16>,
        _: Option<u16>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn spawn_terminal(
        &self,
        _: TerminalAction,
        _: Box<dyn Fn(PaneId, Option<i32>, RunCommand) + Send>,
        _: Option<PathBuf>,
        _pane_env: &crate::os_input_output::PaneEnv,
    ) -> anyhow::Result<(u32, Box<dyn AsyncReader>, Option<u32>)> {
        match self.next_plan()? {
            PlannedSpawn::Success {
                terminal_id,
                child_pid,
            } => {
                if !crate::screen::try_admit_uninstalled_terminal(terminal_id) {
                    return Err(anyhow::anyhow!("too many panes are still opening"));
                }
                Ok((terminal_id, Box::new(NullAsyncReader), Some(child_pid)))
            },
            PlannedSpawn::Error(message) => Err(anyhow::anyhow!(message)),
            PlannedSpawn::CommandNotFound {
                terminal_id,
                command,
            } => {
                if !crate::screen::try_admit_uninstalled_terminal(terminal_id) {
                    return Err(anyhow::anyhow!("too many panes are still opening"));
                }
                Err(anyhow::Error::from(ZellijError::CommandNotFound {
                    terminal_id,
                    command,
                }))
                .context("command was not found")
            },
        }
    }
    fn reserve_terminal_id(&self) -> anyhow::Result<u32> {
        match self.next_plan()? {
            PlannedSpawn::Success { terminal_id, .. }
            | PlannedSpawn::CommandNotFound { terminal_id, .. } => {
                if !crate::screen::try_admit_uninstalled_terminal(terminal_id) {
                    return Err(anyhow::anyhow!("too many panes are still opening"));
                }
                Ok(terminal_id)
            },
            PlannedSpawn::Error(message) => Err(anyhow::anyhow!(message)),
        }
    }
    fn write_to_tty_stdin(&self, _: u32, buf: &[u8]) -> anyhow::Result<usize> {
        Ok(buf.len())
    }
    fn tcdrain(&self, _: u32) -> anyhow::Result<()> {
        Ok(())
    }
    fn kill(&self, pid: u32) -> anyhow::Result<()> {
        self.killed.lock().unwrap().push(pid);
        Ok(())
    }
    fn force_kill(&self, _: u32) -> anyhow::Result<()> {
        Ok(())
    }
    fn send_sigint(&self, _: u32) -> anyhow::Result<()> {
        Ok(())
    }
    fn box_clone(&self) -> Box<dyn ServerOsApi> {
        Box::new((*self).clone())
    }
    fn send_to_client(&self, _: ClientId, _: ServerToClientMsg) -> anyhow::Result<()> {
        Ok(())
    }
    fn register_client(
        &mut self,
        _: ClientId,
        _: &IpcReceiverWithContext<ClientToServerMsg>,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
    fn register_client_with_reply(
        &mut self,
        _: ClientId,
        _: LocalSocketStream,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
    fn remove_client(&mut self, _: ClientId) -> anyhow::Result<()> {
        Ok(())
    }
    fn load_palette(&self) -> Palette {
        Palette::default()
    }
    fn get_cwd(&self, pid: u32) -> Option<PathBuf> {
        self.cwds.lock().unwrap().get(&pid).cloned()
    }
    fn get_cwds(&self, pids: Vec<u32>) -> (HashMap<u32, PathBuf>, HashMap<u32, Vec<String>>) {
        let cwds_lock = self.cwds.lock().unwrap();
        let cmds_lock = self.cmds.lock().unwrap();
        let cwds = pids
            .iter()
            .filter_map(|pid| cwds_lock.get(pid).map(|cwd| (*pid, cwd.clone())))
            .collect();
        let cmds = pids
            .iter()
            .filter_map(|pid| cmds_lock.get(pid).map(|cmd| (*pid, cmd.clone())))
            .collect();
        (cwds, cmds)
    }
    fn get_all_cmds_by_ppid(&self, _: &Option<String>) -> HashMap<String, Vec<String>> {
        self.cmds_by_ppid.lock().unwrap().clone()
    }
    fn get_foreground_cmds(
        &self,
        panes: &[(u32, u32)],
        _: &Option<String>,
    ) -> HashMap<u32, Vec<String>> {
        let cmds_by_ppid = self.cmds_by_ppid.lock().unwrap();
        panes
            .iter()
            .filter_map(|(terminal_id, shell_pid)| {
                cmds_by_ppid
                    .get(&shell_pid.to_string())
                    .map(|cmd| (*terminal_id, cmd.clone()))
            })
            .collect()
    }
    fn write_to_file(&mut self, _: String, _: Option<String>) -> anyhow::Result<()> {
        Ok(())
    }
    fn re_run_command_in_terminal(
        &self,
        _: u32,
        _: RunCommand,
        _: Box<dyn Fn(PaneId, Option<i32>, RunCommand) + Send>,
        _pane_env: &crate::os_input_output::PaneEnv,
    ) -> anyhow::Result<(Box<dyn AsyncReader>, Option<u32>)> {
        unimplemented!()
    }
    fn clear_terminal_id(&self, terminal_id: u32) -> anyhow::Result<()> {
        self.cleared.lock().unwrap().push(terminal_id);
        Ok(())
    }
}

fn make_pty_with_plugin_receiver(
    mock: MockOsApi,
) -> (Pty, channels::Receiver<(PluginInstruction, ErrorContext)>) {
    let (plugin_tx, plugin_rx) = channels::unbounded();
    let plugin_sender = SenderWithContext::new(plugin_tx);
    let mut bus: Bus<PtyInstruction> = Bus::empty().should_silently_fail();
    bus.os_input = Some(Box::new(mock));
    bus.senders.to_plugin = Some(plugin_sender);
    let pty = Pty::new(bus, false, None, None, Default::default());
    (pty, plugin_rx)
}

fn set_active_terminal(pty: &mut Pty, terminal_id: u32, child_pid: u32) {
    let flag = Arc::new(AtomicBool::new(true));
    pty.id_to_child_pid.insert(terminal_id, child_pid);
    pty.pane_activity_flags.insert(terminal_id, flag);
}

fn collect_cwd_changed_events(
    rx: &channels::Receiver<(PluginInstruction, ErrorContext)>,
) -> Vec<(PaneId, PathBuf)> {
    let mut events = Vec::new();
    while let Ok((instruction, _)) = rx.try_recv() {
        if let PluginInstruction::Update(updates) = instruction {
            for (_, _, event) in updates {
                if let Event::CwdChanged(pane_id, cwd, _) = event {
                    events.push((pane_id.into(), cwd));
                }
            }
        }
    }
    events
}

fn collect_command_changed_events(
    rx: &channels::Receiver<(PluginInstruction, ErrorContext)>,
) -> Vec<(PaneId, Vec<String>, bool)> {
    let mut events = Vec::new();
    while let Ok((instruction, _)) = rx.try_recv() {
        if let PluginInstruction::Update(updates) = instruction {
            for (_, _, event) in updates {
                if let Event::CommandChanged(pane_id, cmd, is_foreground, _) = event {
                    events.push((pane_id.into(), cmd, is_foreground));
                }
            }
        }
    }
    events
}

#[test]
fn foreground_command_emitted_with_is_foreground_true() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["vim".into(), "file.rs".into()]);
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.update_and_report_cwds();

    let events = collect_command_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, PaneId::Terminal(1));
    assert_eq!(events[0].1, vec!["vim", "file.rs"]);
    assert!(events[0].2, "expected is_foreground=true");
}

#[test]
fn empty_foreground_falls_back_to_shell_command() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_cmd(child_pid, vec!["/bin/bash".into()]);
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.update_and_report_cwds();

    let events = collect_command_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, PaneId::Terminal(1));
    assert_eq!(events[0].1, vec!["/bin/bash"]);
    assert!(!events[0].2, "expected is_foreground=false");
}

#[test]
fn foreground_clearing_emits_shell_fallback() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_cmd(child_pid, vec!["/bin/zsh".into()]);
    mock.set_foreground_cmd(child_pid, vec!["cargo".into(), "build".into()]);
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock.clone());
    set_active_terminal(&mut pty, 1, child_pid);

    pty.update_and_report_cwds();
    let events = collect_command_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert!(events[0].2, "first event should be foreground");
    assert_eq!(events[0].1, vec!["cargo", "build"]);

    mock.clear_foreground_cmd(child_pid);
    pty.pane_activity_flags
        .get(&1)
        .unwrap()
        .store(true, Ordering::Relaxed);

    pty.update_and_report_cwds();
    let events = collect_command_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1, vec!["/bin/zsh"]);
    assert!(
        !events[0].2,
        "after clearing foreground, should fall back to shell"
    );
}

#[test]
fn no_event_when_foreground_unchanged() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["htop".into()]);
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.update_and_report_cwds();
    let _ = collect_command_changed_events(&rx);

    pty.pane_activity_flags
        .get(&1)
        .unwrap()
        .store(true, Ordering::Relaxed);
    pty.update_and_report_cwds();
    let events = collect_command_changed_events(&rx);
    assert!(
        events.is_empty(),
        "no event expected when command unchanged"
    );
}

#[test]
fn no_event_for_inactive_terminal() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["vim".into()]);
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);
    pty.pane_activity_flags
        .get(&1)
        .unwrap()
        .store(false, Ordering::Relaxed);

    pty.update_and_report_cwds();
    let events = collect_command_changed_events(&rx);
    assert!(
        events.is_empty(),
        "inactive terminal should produce no events"
    );
}

#[test]
fn foreground_change_between_two_commands() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["vim".into()]);
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock.clone());
    set_active_terminal(&mut pty, 1, child_pid);

    pty.update_and_report_cwds();
    let events = collect_command_changed_events(&rx);
    assert_eq!(events[0].1, vec!["vim"]);
    assert!(events[0].2);

    mock.set_foreground_cmd(child_pid, vec!["cargo".into(), "test".into()]);
    pty.pane_activity_flags
        .get(&1)
        .unwrap()
        .store(true, Ordering::Relaxed);

    pty.update_and_report_cwds();
    let events = collect_command_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1, vec!["cargo", "test"]);
    assert!(events[0].2);
}

// --- Activity flag gating ---

#[test]
fn activity_flag_reset_after_poll() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);
    assert!(pty
        .pane_activity_flags
        .get(&1)
        .unwrap()
        .load(Ordering::Relaxed));

    pty.update_and_report_cwds();

    assert!(
        !pty.pane_activity_flags
            .get(&1)
            .unwrap()
            .load(Ordering::Relaxed),
        "activity flag should be reset to false after poll"
    );
}

#[test]
fn multiple_terminals_only_active_ones_polled() {
    let mock = MockOsApi::new();
    let pid_active = 100;
    let pid_inactive = 200;
    mock.set_cwd(pid_active, PathBuf::from("/active"));
    mock.set_cwd(pid_inactive, PathBuf::from("/inactive"));
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, pid_active);
    set_active_terminal(&mut pty, 2, pid_inactive);
    pty.pane_activity_flags
        .get(&2)
        .unwrap()
        .store(false, Ordering::Relaxed);

    pty.update_and_report_cwds();

    let events = collect_cwd_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, PaneId::Terminal(1));
    assert_eq!(events[0].1, PathBuf::from("/active"));
}

// --- CWD change events ---

#[test]
fn cwd_changed_event_emitted_on_change() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_cwd(child_pid, PathBuf::from("/home/user"));
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.update_and_report_cwds();

    let events = collect_cwd_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, PaneId::Terminal(1));
    assert_eq!(events[0].1, PathBuf::from("/home/user"));
}

#[test]
fn no_cwd_event_when_unchanged() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_cwd(child_pid, PathBuf::from("/home/user"));
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);
    pty.terminal_cwds.insert(1, PathBuf::from("/home/user"));

    pty.update_and_report_cwds();

    let events = collect_cwd_changed_events(&rx);
    assert!(events.is_empty(), "no event expected when cwd unchanged");
}

// --- OSC7 CWD notification ---

#[test]
fn osc7_emits_cwd_changed() {
    let mock = MockOsApi::new();
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    pty.id_to_child_pid.insert(1, 100);

    pty.notify_cwd_from_osc7(1, PathBuf::from("/tmp/new"));

    let events = collect_cwd_changed_events(&rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, PaneId::Terminal(1));
    assert_eq!(events[0].1, PathBuf::from("/tmp/new"));
    assert_eq!(
        pty.terminal_cwds.get(&1),
        Some(&PathBuf::from("/tmp/new")),
        "cache should be updated"
    );
}

#[test]
fn osc7_no_event_when_unchanged() {
    let mock = MockOsApi::new();
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    pty.id_to_child_pid.insert(1, 100);
    pty.terminal_cwds.insert(1, PathBuf::from("/same"));

    pty.notify_cwd_from_osc7(1, PathBuf::from("/same"));

    let events = collect_cwd_changed_events(&rx);
    assert!(events.is_empty(), "no event when osc7 path matches cache");
}

#[test]
fn osc7_clears_activity_flag() {
    let mock = MockOsApi::new();
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock);
    let flag = Arc::new(AtomicBool::new(true));
    pty.id_to_child_pid.insert(1, 100);
    pty.pane_activity_flags.insert(1, flag.clone());

    pty.notify_cwd_from_osc7(1, PathBuf::from("/new"));

    assert!(
        !flag.load(Ordering::Relaxed),
        "osc7 should clear the activity flag"
    );
}

#[test]
fn osc7_then_poll_skips_terminal() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_cwd(child_pid, PathBuf::from("/from-proc"));
    mock.set_foreground_cmd(child_pid, vec!["vim".into()]);
    let (mut pty, rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.notify_cwd_from_osc7(1, PathBuf::from("/from-osc7"));
    let osc7_events = collect_cwd_changed_events(&rx);
    assert_eq!(osc7_events.len(), 1);

    pty.update_and_report_cwds();
    let cwd_events = collect_cwd_changed_events(&rx);
    let cmd_events = collect_command_changed_events(&rx);
    assert!(
        cwd_events.is_empty() && cmd_events.is_empty(),
        "poll after osc7 should skip terminal since flag was cleared"
    );
}

fn session_with_terminal_pane(terminal_id: u32) -> SessionLayoutMetadata {
    let mut meta = SessionLayoutMetadata::default();
    let pane = crate::session_layout_metadata::PaneLayoutMetadata::new(
        PaneId::Terminal(terminal_id),
        zellij_utils::pane_size::PaneGeom::default(),
        false,
        None,
        None,
        false,
        None,
        vec![],
        None,
        None,
        Default::default(),
    );
    meta.add_tab("tab1".to_string(), true, false, vec![pane], vec![]);
    meta
}

fn serialized_run_and_cwd(pty: &mut Pty, terminal_id: u32) -> (Option<Run>, Option<PathBuf>) {
    let mut meta = session_with_terminal_pane(terminal_id);
    pty.populate_session_layout_metadata(&mut meta);
    let manifest: session_serialization::GlobalLayoutManifest = meta.into();
    let global_cwd = manifest.global_cwd;
    let pane = manifest
        .tabs
        .into_iter()
        .next()
        .unwrap()
        .1
        .tiled_panes
        .into_iter()
        .next()
        .unwrap();
    let cwd = match (global_cwd, pane.cwd) {
        (Some(global_cwd), Some(cwd)) => Some(global_cwd.join(cwd)),
        (None, cwd) => cwd,
        (global_cwd, None) => global_cwd,
    };
    (pane.run, cwd)
}

fn run_command(command: &str, args: Vec<&str>) -> Option<Run> {
    let mut run_command = RunCommand::new(PathBuf::from(command));
    run_command.args = args.into_iter().map(|a| a.to_string()).collect();
    Some(Run::Command(run_command))
}

fn resurrect_command(command: &str, args: Vec<&str>, cwd: Option<&str>) -> ResurrectCommand {
    ResurrectCommand {
        command: PathBuf::from(command),
        args: args.into_iter().map(|a| a.to_string()).collect(),
        cwd: cwd.map(PathBuf::from),
    }
}

#[test]
fn resurrect_command_overrides_detected_command_and_cwd() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["htop".into()]);
    mock.set_cwd(child_pid, PathBuf::from("/tmp/detected"));
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.set_resurrect_command(
        1,
        Some(resurrect_command(
            "opencode",
            vec!["-s", "ses_abc"],
            Some("/tmp/resurrected"),
        )),
    )
    .unwrap();

    let (run, cwd) = serialized_run_and_cwd(&mut pty, 1);
    assert_eq!(run, run_command("opencode", vec!["-s", "ses_abc"]));
    assert_eq!(cwd, Some(PathBuf::from("/tmp/resurrected")));
}

#[test]
fn resurrect_command_without_cwd_keeps_detected_cwd() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["htop".into()]);
    mock.set_cwd(child_pid, PathBuf::from("/tmp/detected"));
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.set_resurrect_command(1, Some(resurrect_command("opencode", vec![], None)))
        .unwrap();

    let (run, cwd) = serialized_run_and_cwd(&mut pty, 1);
    assert_eq!(run, run_command("opencode", vec![]));
    assert_eq!(cwd, Some(PathBuf::from("/tmp/detected")));
}

#[test]
fn cleared_resurrect_command_falls_back_to_detected_command() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["htop".into()]);
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.set_resurrect_command(1, Some(resurrect_command("opencode", vec![], None)))
        .unwrap();
    pty.set_resurrect_command(1, None).unwrap();

    let (run, _cwd) = serialized_run_and_cwd(&mut pty, 1);
    assert_eq!(run, run_command("htop", vec![]));
}

#[test]
fn resurrect_command_persists_after_foreground_command_changes() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    mock.set_foreground_cmd(child_pid, vec!["htop".into()]);
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock.clone());
    set_active_terminal(&mut pty, 1, child_pid);

    pty.set_resurrect_command(1, Some(resurrect_command("opencode", vec![], None)))
        .unwrap();
    mock.set_foreground_cmd(child_pid, vec!["top".into()]);

    let (run, _cwd) = serialized_run_and_cwd(&mut pty, 1);
    assert_eq!(run, run_command("opencode", vec![]));
}

#[test]
fn closing_pane_removes_resurrect_command() {
    let mock = MockOsApi::new();
    let child_pid = 100;
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock);
    set_active_terminal(&mut pty, 1, child_pid);

    pty.set_resurrect_command(1, Some(resurrect_command("opencode", vec![], None)))
        .unwrap();
    pty.close_pane(PaneId::Terminal(1)).unwrap();

    assert!(!pty.resurrect_commands.contains_key(&1));
}

#[test]
fn resurrect_command_for_unknown_terminal_is_rejected() {
    let mock = MockOsApi::new();
    let (mut pty, _rx) = make_pty_with_plugin_receiver(mock);

    let result = pty.set_resurrect_command(7, Some(resurrect_command("opencode", vec![], None)));

    assert!(result.is_err());
    assert!(pty.resurrect_commands.is_empty());
}

fn command_layout(commands: Vec<RunCommand>) -> TiledPaneLayout {
    let mut layout = TiledPaneLayout::default();
    layout.children_split_direction = SplitDirection::Vertical;
    layout.children = commands
        .into_iter()
        .map(|command| {
            let mut child = TiledPaneLayout::default();
            child.run = Some(Run::Command(command));
            child
        })
        .collect();
    layout
}

fn bare_layout() -> Layout {
    Layout {
        tabs: vec![],
        focused_tab_index: None,
        template: Some((TiledPaneLayout::default(), vec![])),
        swap_layouts: vec![],
        swap_tiled_layouts: vec![],
        swap_floating_layouts: vec![],
        session_attention_muted: false,
    }
}

fn drain<T>(receiver: &channels::Receiver<(T, ErrorContext)>) -> Vec<T> {
    let mut events = Vec::new();
    while let Ok((event, _)) = receiver.try_recv() {
        events.push(event);
    }
    events
}

struct LayoutEnds {
    screen_rx: channels::Receiver<(ScreenInstruction, ErrorContext)>,
    writer_rx: channels::Receiver<(PtyWriteInstruction, ErrorContext)>,
}

fn pty_for_layout(mock: MockOsApi, connected_screen: bool) -> (Pty, LayoutEnds) {
    let (screen_tx, screen_rx) = channels::unbounded();
    let screen_sender = SenderWithContext::new(screen_tx);
    let (writer_tx, writer_rx) = channels::unbounded();
    let writer_sender = SenderWithContext::new(writer_tx);
    let screen_rx = if connected_screen {
        screen_rx
    } else {
        drop(screen_rx);
        channels::unbounded().1
    };
    let bus = Bus::new(
        vec![],
        Some(&screen_sender),
        None,
        None,
        None,
        Some(&writer_sender),
        None,
        Some(Box::new(mock)),
    );
    let pty = Pty::new(bus, false, None, None, Default::default());
    (
        pty,
        LayoutEnds {
            screen_rx,
            writer_rx,
        },
    )
}

fn spawn_two_commands(pty: &mut Pty, commands: Vec<RunCommand>, tab_index: usize) -> Result<()> {
    pty.spawn_terminals_for_layout(
        None,
        command_layout(commands),
        vec![],
        None,
        HashMap::new(),
        None,
        tab_index,
        false,
        false,
        (1, false),
        None,
    )
}

#[test]
fn a_mid_layout_spawn_failure_rolls_everything_back_without_stopping_the_pty_thread() {
    let _admission = crate::screen::lock_admission_for_test();
    crate::screen::clear_admitted_uninstalled_terminals();
    let mock = MockOsApi::new();
    mock.plan(PlannedSpawn::Success {
        terminal_id: 7,
        child_pid: 4242,
    });
    mock.plan(PlannedSpawn::Error(
        "simulated layout spawn failure".to_owned(),
    ));

    let (screen_tx, screen_rx) = channels::unbounded();
    let screen_sender = SenderWithContext::new(screen_tx);
    let (writer_tx, writer_rx) = channels::unbounded();
    let writer_sender = SenderWithContext::new(writer_tx);
    let (pty_tx, pty_rx) = channels::unbounded();
    let pty_sender = SenderWithContext::new(pty_tx);
    let bus = Bus::new(
        vec![pty_rx],
        Some(&screen_sender),
        Some(&pty_sender),
        None,
        None,
        Some(&writer_sender),
        None,
        Some(Box::new(mock.clone())),
    );
    let pty = Pty::new(bus, false, None, None, Default::default());
    let thread =
        std::thread::spawn(move || pty_thread_main(pty, Box::new(bare_layout()), "r04".to_owned()));

    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    pty_sender
        .send(PtyInstruction::NewTab(
            None,
            None,
            Some(command_layout(vec![
                RunCommand::new(PathBuf::from("echo")),
                RunCommand::new(PathBuf::from("echo")),
            ])),
            vec![],
            3,
            HashMap::new(),
            None,
            false,
            false,
            (1, false),
            Some(crate::route::NotificationEnd::new(completion_tx)),
        ))
        .unwrap();

    let result = completion_rx.blocking_recv().unwrap();
    let message = result.error_message.clone().unwrap_or_default();
    assert!(
        message.contains("simulated layout spawn failure"),
        "completion was {result:?}"
    );
    assert_eq!(result.exit_status, Some(1));
    assert_eq!(mock.killed_pids(), vec![4242]);
    assert_eq!(mock.cleared_ids(), vec![7]);
    let screen = drain(&screen_rx);
    assert!(screen
        .iter()
        .any(|event| matches!(event, ScreenInstruction::LayoutSpawnFailed(3))));
    assert!(!screen
        .iter()
        .any(|event| matches!(event, ScreenInstruction::ApplyLayout(..))));
    let writer = drain(&writer_rx);
    assert!(writer
        .iter()
        .any(|event| matches!(event, PtyWriteInstruction::DropTerminal(7))));

    pty_sender.send(PtyInstruction::Exit).unwrap();
    thread.join().unwrap().unwrap();
    crate::screen::clear_admitted_uninstalled_terminals();
}

#[test]
fn an_admission_refusal_rolls_the_partial_layout_back() {
    let _admission = crate::screen::lock_admission_for_test();
    crate::screen::clear_admitted_uninstalled_terminals();
    for terminal_id in 1_000_000..1_000_000 + crate::screen::MAX_UNINSTALLED_TERMINALS - 1 {
        assert!(crate::screen::try_admit_uninstalled_terminal(
            terminal_id as u32
        ));
    }
    let mock = MockOsApi::new();
    mock.plan(PlannedSpawn::Success {
        terminal_id: 7,
        child_pid: 11,
    });
    mock.plan(PlannedSpawn::Success {
        terminal_id: 8,
        child_pid: 12,
    });
    let (mut pty, ends) = pty_for_layout(mock.clone(), true);
    let err = spawn_two_commands(
        &mut pty,
        vec![
            RunCommand::new(PathBuf::from("echo")),
            RunCommand::new(PathBuf::from("echo")),
        ],
        4,
    )
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("too many panes are still opening"),
        "{err:#}"
    );
    assert_eq!(mock.killed_pids(), vec![11]);
    assert_eq!(mock.cleared_ids(), vec![7]);
    let screen = drain(&ends.screen_rx);
    assert!(screen
        .iter()
        .any(|event| matches!(event, ScreenInstruction::LayoutSpawnFailed(4))));
    assert!(!screen
        .iter()
        .any(|event| matches!(event, ScreenInstruction::ApplyLayout(..))));
    crate::screen::clear_admitted_uninstalled_terminals();
}

#[test]
fn a_failed_layout_publication_rolls_staged_panes_back() {
    let _admission = crate::screen::lock_admission_for_test();
    crate::screen::clear_admitted_uninstalled_terminals();
    let mock = MockOsApi::new();
    mock.plan(PlannedSpawn::Success {
        terminal_id: 7,
        child_pid: 4242,
    });
    mock.plan(PlannedSpawn::Success {
        terminal_id: 8,
        child_pid: 4243,
    });
    let (mut pty, _ends) = pty_for_layout(mock.clone(), false);
    let err = spawn_two_commands(
        &mut pty,
        vec![
            RunCommand::new(PathBuf::from("echo")),
            RunCommand::new(PathBuf::from("echo")),
        ],
        5,
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("failed to send"), "{err:#}");
    assert_eq!(mock.killed_pids(), vec![4242, 4243]);
    assert_eq!(mock.cleared_ids(), vec![7, 8]);
    crate::screen::clear_admitted_uninstalled_terminals();
}

#[test]
fn a_successful_layout_publishes_every_staged_pane() {
    let _admission = crate::screen::lock_admission_for_test();
    crate::screen::clear_admitted_uninstalled_terminals();
    let mock = MockOsApi::new();
    mock.plan(PlannedSpawn::Success {
        terminal_id: 7,
        child_pid: 100,
    });
    mock.plan(PlannedSpawn::Success {
        terminal_id: 8,
        child_pid: 101,
    });
    let (mut pty, ends) = pty_for_layout(mock.clone(), true);
    // Committed readers hit EOF at once. Swallow the follow-up screen send so
    // dropping the receiver at the end of this test does not abort the process.
    pty.bus.senders.should_silently_fail = true;
    spawn_two_commands(
        &mut pty,
        vec![
            RunCommand::new(PathBuf::from("echo")),
            RunCommand::new(PathBuf::from("echo")),
        ],
        6,
    )
    .unwrap();
    assert!(mock.killed_pids().is_empty());
    assert!(mock.cleared_ids().is_empty());
    let screen = drain(&ends.screen_rx);
    let published = screen.iter().any(|event| {
        matches!(
            event,
            ScreenInstruction::ApplyLayout(_, _, panes, floating, _, 6, _, _, _, _)
                if panes.iter().map(|(id, _)| *id).collect::<Vec<_>>() == vec![7, 8]
                    && floating.is_empty()
        )
    });
    assert!(published, "screen events did not publish both panes");
    assert!(!screen
        .iter()
        .any(|event| matches!(event, ScreenInstruction::LayoutSpawnFailed(_))));
    crate::screen::clear_admitted_uninstalled_terminals();
}

#[test]
fn command_not_found_layout_panes_stay_in_the_committed_layout() {
    let _admission = crate::screen::lock_admission_for_test();
    crate::screen::clear_admitted_uninstalled_terminals();
    let mock = MockOsApi::new();
    mock.plan(PlannedSpawn::CommandNotFound {
        terminal_id: 7,
        command: "missing-held".to_owned(),
    });
    mock.plan(PlannedSpawn::CommandNotFound {
        terminal_id: 8,
        command: "missing-close".to_owned(),
    });
    let mut held = RunCommand::new(PathBuf::from("missing-held"));
    held.hold_on_close = true;
    let closing = RunCommand::new(PathBuf::from("missing-close"));
    let (mut pty, ends) = pty_for_layout(mock.clone(), true);
    spawn_two_commands(&mut pty, vec![held, closing], 9).unwrap();

    assert!(mock.killed_pids().is_empty());
    assert_eq!(mock.cleared_ids(), vec![8]);
    let screen = drain(&ends.screen_rx);
    assert!(screen.iter().any(|event| {
        matches!(
            event,
            ScreenInstruction::ApplyLayout(_, _, panes, _, _, 9, _, _, _, _)
                if panes.iter().map(|(id, _)| *id).collect::<Vec<_>>() == vec![7, 8]
        )
    }));
    assert!(screen
        .iter()
        .any(|event| matches!(event, ScreenInstruction::PtyBytes(7, _))));
    assert!(screen.iter().any(|event| matches!(
        event,
        ScreenInstruction::HoldPane(PaneId::Terminal(7), Some(2), _)
    )));
    assert!(screen.iter().any(|event| matches!(
        event,
        ScreenInstruction::ClosePane(PaneId::Terminal(8), None, None, Some(2))
    )));
    assert!(!screen
        .iter()
        .any(|event| matches!(event, ScreenInstruction::LayoutSpawnFailed(_))));
    let writer = drain(&ends.writer_rx);
    assert!(writer
        .iter()
        .any(|event| matches!(event, PtyWriteInstruction::DropTerminal(8))));
    assert!(!writer
        .iter()
        .any(|event| matches!(event, PtyWriteInstruction::DropTerminal(7))));
    crate::screen::clear_admitted_uninstalled_terminals();
}
