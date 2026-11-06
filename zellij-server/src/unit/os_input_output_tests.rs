use super::*;
use zellij_utils::input::command::RunCommand;

fn make_server() -> ServerOsInputOutput {
    get_server_os_input().expect("failed to create server os input")
}

// --- Cross-platform command helpers ---

#[allow(dead_code)]
#[cfg(not(windows))]
fn long_running_cmd() -> Command {
    let mut cmd = Command::new("sleep");
    cmd.arg("60");
    cmd
}

#[allow(dead_code)]
#[cfg(windows)]
fn long_running_cmd() -> Command {
    use std::os::windows::process::CommandExt;
    let mut cmd = Command::new("timeout");
    cmd.args(&["/T", "60"]);
    cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    cmd
}

#[allow(dead_code)]
#[cfg(not(windows))]
fn echo_cmd(msg: &str) -> Command {
    let mut cmd = Command::new("echo");
    cmd.arg(msg);
    cmd
}

#[allow(dead_code)]
#[cfg(windows)]
fn echo_cmd(msg: &str) -> Command {
    use std::os::windows::process::CommandExt;
    let mut cmd = Command::new("cmd");
    cmd.args(&["/C", "echo", msg]);
    cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    cmd
}

#[allow(dead_code)]
#[cfg(not(windows))]
fn stdin_reader_cmd() -> Command {
    let mut cmd = Command::new("cat");
    cmd.stdin(std::process::Stdio::piped());
    cmd
}

#[allow(dead_code)]
#[cfg(windows)]
fn stdin_reader_cmd() -> Command {
    use std::os::windows::process::CommandExt;
    let mut cmd = Command::new("findstr");
    cmd.arg("/R").arg(".*");
    cmd.stdin(std::process::Stdio::piped());
    cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    cmd
}

#[test]
fn get_cwd() {
    let server = make_server();

    let pid = std::process::id();
    assert!(
        server.get_cwd(pid).is_some(),
        "Get current working directory from PID {}",
        pid
    );
}

// --- Signal delivery tests ---

#[cfg(not(windows))]
#[test]
fn kill_sends_sighup_to_process() {
    let child = long_running_cmd()
        .spawn()
        .expect("failed to spawn long-running process");
    let pid = child.id();

    let server = make_server();

    server.kill(pid).expect("kill should succeed");

    // Give the signal time to be delivered
    std::thread::sleep(std::time::Duration::from_millis(100));
}

#[cfg(not(windows))]
#[test]
fn force_kill_sends_sigkill_to_process() {
    let child = long_running_cmd()
        .spawn()
        .expect("failed to spawn long-running process");
    let pid = child.id();

    let server = make_server();

    server.force_kill(pid).expect("force_kill should succeed");

    std::thread::sleep(std::time::Duration::from_millis(100));
}

#[cfg(not(windows))]
#[test]
fn send_sigint_to_process() {
    let child = stdin_reader_cmd()
        .spawn()
        .expect("failed to spawn stdin-reader process");
    let pid = child.id();

    let server = make_server();

    server.send_sigint(pid).expect("send_sigint should succeed");

    std::thread::sleep(std::time::Duration::from_millis(100));
}

#[test]
fn spawn_and_read_output() {
    use crate::panes::PaneId;
    use zellij_utils::input::command::TerminalAction;

    let server = make_server();
    let test_message = "hello_zellij_test";

    #[cfg(not(windows))]
    let cmd = RunCommand {
        command: PathBuf::from("echo"),
        args: vec![test_message.to_string()],
        ..Default::default()
    };
    #[cfg(windows)]
    let cmd = RunCommand {
        command: PathBuf::from("cmd"),
        args: vec![
            "/K".to_string(),
            "echo".to_string(),
            test_message.to_string(),
        ],
        ..Default::default()
    };

    let action = TerminalAction::RunCommand(cmd);
    let quit_cb: Box<dyn Fn(PaneId, Option<i32>, RunCommand) + Send> =
        Box::new(|_pane_id, _exit_status, _run_command| {});

    let (_terminal_id, mut reader, _child_pid) = server
        .spawn_terminal(action, quit_cb, None, &Default::default())
        .expect("spawn_terminal should succeed");

    // Read output from the spawned terminal
    let mut output = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        loop {
            if std::time::Instant::now() > deadline {
                break;
            }
            match tokio::time::timeout(
                std::time::Duration::from_millis(500),
                reader.read_chunk(4096),
            )
            .await
            {
                Ok(Ok(bytes)) if bytes.is_empty() => break,
                Ok(Ok(bytes)) => {
                    output.extend_from_slice(&bytes);
                    let s = String::from_utf8_lossy(&output);
                    if s.contains(test_message) {
                        break;
                    }
                },
                Ok(Err(_)) => break,
                Err(_) => {
                    // timeout — check if we already have enough
                    let s = String::from_utf8_lossy(&output);
                    if s.contains(test_message) {
                        break;
                    }
                },
            }
        }
    });

    let output_str = String::from_utf8_lossy(&output);
    assert!(
        output_str.contains(test_message),
        "expected output to contain '{}', got: '{}'",
        test_message,
        output_str
    );
}

// Run with this process's stdin redirected to NUL by the test below, the way a server started
// by the zellij-terminal launcher runs: a pane's shell must still read the pseudo console.
#[cfg(windows)]
#[test]
#[ignore = "run by a_pane_shell_reads_the_pseudo_console_even_when_the_server_input_is_redirected"]
fn an_interactive_shell_in_a_pane_waits_for_input() {
    use crate::panes::PaneId;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use zellij_utils::input::command::TerminalAction;

    let server = make_server();
    let exited = Arc::new(AtomicBool::new(false));
    let quit_cb: Box<dyn Fn(PaneId, Option<i32>, RunCommand) + Send> = Box::new({
        let exited = exited.clone();
        move |_pane_id, _exit_status, _run_command| exited.store(true, Ordering::SeqCst)
    });
    let cmd = RunCommand {
        command: PathBuf::from("cmd"),
        ..Default::default()
    };
    let (_terminal_id, _reader, child_pid) = server
        .spawn_terminal(
            TerminalAction::RunCommand(cmd),
            quit_cb,
            None,
            &Default::default(),
        )
        .expect("spawn_terminal should succeed");

    std::thread::sleep(std::time::Duration::from_secs(2));
    let still_running = !exited.load(Ordering::SeqCst);
    if let Some(child_pid) = child_pid {
        let _ = server.force_kill(child_pid);
    }
    assert!(
        still_running,
        "the shell exited at once: it read the server's own stdin instead of the pseudo console"
    );
}

#[cfg(windows)]
#[test]
fn a_pane_shell_reads_the_pseudo_console_even_when_the_server_input_is_redirected() {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_crate, path)| path);
    let output = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            &format!("{}::an_interactive_shell_in_a_pane_waits_for_input", module),
            "--ignored",
            "--nocapture",
        ])
        .stdin(std::process::Stdio::null())
        .output()
        .expect("failed to run the test binary with its stdin on NUL");
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
#[test]
fn tcgetpgrp_returns_foreground_group() {
    use crate::panes::PaneId;
    use zellij_utils::input::command::TerminalAction;

    let server = make_server();

    // `login_tty` in the child makes the spawned command the controlling terminal's
    // foreground process group, so tcgetpgrp(master) should return its pid
    let cmd = RunCommand {
        command: PathBuf::from("sleep"),
        args: vec!["60".to_string()],
        ..Default::default()
    };
    let quit_cb: Box<dyn Fn(PaneId, Option<i32>, RunCommand) + Send> = Box::new(|_, _, _| {});
    let (terminal_id, _reader, child_pid) = server
        .spawn_terminal(
            TerminalAction::RunCommand(cmd),
            quit_cb,
            None,
            &Default::default(),
        )
        .expect("spawn_terminal should succeed");

    // poll (bounded to ~2s) for the child to finish setting up its controlling terminal
    let mut fpgid = None;
    for _ in 0..100 {
        fpgid = server.pty_backend.tcgetpgrp(terminal_id);
        let ready = match (fpgid, child_pid) {
            (Some(p), Some(child_pid)) => p > 0 && p as u32 == child_pid,
            (Some(p), None) => p > 0,
            _ => false,
        };
        if ready {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        matches!(fpgid, Some(p) if p > 0),
        "tcgetpgrp should return the foreground process group, got {:?}",
        fpgid
    );
    if let (Some(fpgid), Some(child_pid)) = (fpgid, child_pid) {
        assert_eq!(
            fpgid as u32, child_pid,
            "the spawned command leads its own foreground group"
        );
    }

    // An unknown terminal id has no fd and must not panic.
    assert_eq!(server.pty_backend.tcgetpgrp(u32::MAX), None);

    if let Some(child_pid) = child_pid {
        let _ = server.force_kill(child_pid);
    }
}

#[test]
fn client_buffer_refuses_messages_when_limit_is_reached() {
    let (buffer, receiver) = client_buffer(CLIENT_BUFFER_LIMIT);
    let msg = || ServerToClientMsg::Exit {
        exit_reason: ExitReason::Normal,
    };
    for _ in 0..CLIENT_BUFFER_LIMIT {
        assert!(buffer.try_send(msg()).is_ok());
    }
    assert!(matches!(buffer.try_send(msg()), Err(TrySendError::Full(_))));
    assert!(matches!(buffer.try_send(msg()), Err(TrySendError::Full(_))));
    assert!(receiver.recv().is_some());
    assert!(buffer.try_send(msg()).is_ok());
    assert!(matches!(buffer.try_send(msg()), Err(TrySendError::Full(_))));
}

#[test]
fn client_buffer_reports_disconnect_when_receiver_is_gone() {
    let (buffer, receiver) = client_buffer(CLIENT_BUFFER_LIMIT);
    drop(receiver);
    let result = buffer.try_send(ServerToClientMsg::Exit {
        exit_reason: ExitReason::Normal,
    });
    assert!(matches!(result, Err(TrySendError::Disconnected(_))));
    assert_eq!(buffer.queued.load(Ordering::Acquire), 0);
}

#[cfg(unix)]
#[test]
fn the_pane_environment_is_added_to_and_removed_from_the_child_environment() {
    use crate::os_input_output::PaneEnv;
    use crate::panes::PaneId;
    use zellij_utils::input::command::TerminalAction;

    let server = make_server();
    let cmd = RunCommand {
        command: PathBuf::from("sh"),
        args: vec![
            "-c".to_string(),
            "echo \"start:${ZELLIJ_PANE_ENV_TEST}:${HOME-unset}:end\"".to_string(),
        ],
        ..Default::default()
    };
    let mut pane_env = PaneEnv::new();
    pane_env.insert(
        "ZELLIJ_PANE_ENV_TEST".to_owned(),
        Some("from the config".to_owned()),
    );
    pane_env.insert("HOME".to_owned(), None);
    let quit_cb: Box<dyn Fn(PaneId, Option<i32>, RunCommand) + Send> = Box::new(|_, _, _| {});
    let (_terminal_id, mut reader, _child_pid) = server
        .spawn_terminal(TerminalAction::RunCommand(cmd), quit_cb, None, &pane_env)
        .expect("spawn_terminal should succeed");
    let expected = "start:from the config:unset:end";
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let output = rt.block_on(async {
        let mut output = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline
            && !String::from_utf8_lossy(&output).contains(expected)
        {
            match tokio::time::timeout(
                std::time::Duration::from_millis(500),
                reader.read_chunk(4096),
            )
            .await
            {
                Ok(Ok(bytes)) if bytes.is_empty() => break,
                Ok(Ok(bytes)) => output.extend_from_slice(&bytes),
                Ok(Err(_)) => break,
                Err(_) => {},
            }
        }
        String::from_utf8_lossy(&output).to_string()
    });
    assert!(output.contains(expected), "got: '{}'", output);
    assert!(std::env::var("ZELLIJ_PANE_ENV_TEST").is_err());
}

#[cfg(not(windows))]
mod bounded_command_tests {
    use super::super::{run_bounded_with, BoundedOutput};
    use std::process::Command;
    use std::time::{Duration, Instant};

    // Every test gets its own reader thread budget so that the threads one
    // test leaves behind cannot make another one refuse to run.
    fn run_bounded(
        command: Command,
        timeout: Duration,
        max_output: usize,
    ) -> std::io::Result<BoundedOutput> {
        run_bounded_with(
            command,
            timeout,
            max_output,
            leaked_counter(0),
            &real_spawner,
        )
    }

    fn sh(script: &str) -> Command {
        let mut command = Command::new("sh");
        command.arg("-c").arg(script);
        command
    }

    #[test]
    fn output_and_status_are_returned() {
        let output = run_bounded(
            sh("printf out; printf err >&2; exit 3"),
            Duration::from_secs(5),
            1024,
        )
        .expect("completes");
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"out");
        assert_eq!(output.stderr, b"err");
    }

    #[test]
    fn retained_output_is_capped_while_the_child_runs_to_completion() {
        let output = run_bounded(
            sh("head -c 1000000 /dev/zero; printf done >&2"),
            Duration::from_secs(10),
            4096,
        )
        .expect("completes");
        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 4096);
        assert_eq!(output.stderr, b"done");
    }

    #[test]
    fn a_hanging_child_is_killed_at_the_deadline() {
        let started = Instant::now();
        let error = run_bounded(sh("sleep 30"), Duration::from_millis(200), 1024)
            .err()
            .expect("times out");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn a_background_process_holding_the_pipes_cannot_outlast_the_deadline() {
        // The hook exits at once, but a background process inherits its
        // stdout and keeps it open; collecting output must still give up at
        // the deadline instead of blocking until the background job ends.
        let started = Instant::now();
        let error = run_bounded(
            sh("sleep 3 & printf early"),
            Duration::from_millis(300),
            1024,
        )
        .err()
        .expect("times out");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn processes_left_behind_by_a_timed_out_command_are_killed() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("still-running");
        let script = format!("(sleep 1; touch '{}') & printf early", marker.display());
        let error = run_bounded(sh(&script), Duration::from_millis(300), 1024)
            .err()
            .expect("times out");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        std::thread::sleep(Duration::from_secs(2));
        assert!(
            !marker.exists(),
            "the background process must be killed with the command's group"
        );
    }

    fn leaked_counter(initial: usize) -> &'static std::sync::atomic::AtomicUsize {
        Box::leak(Box::new(std::sync::atomic::AtomicUsize::new(initial)))
    }

    fn real_spawner(name: &str, work: Box<dyn FnOnce() + Send + 'static>) -> std::io::Result<()> {
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(work)
            .map(|_| ())
    }

    fn wait_for_count(counter: &std::sync::atomic::AtomicUsize, expected: usize) {
        let started = Instant::now();
        while counter.load(std::sync::atomic::Ordering::Acquire) != expected {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "reader threads were not released"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn reader_threads_are_released_when_a_run_completes() {
        let counter = leaked_counter(0);
        let output = run_bounded_with(
            sh("printf out"),
            Duration::from_secs(5),
            1024,
            counter,
            &real_spawner,
        )
        .expect("completes");
        assert_eq!(output.stdout, b"out");
        wait_for_count(counter, 0);
    }

    #[test]
    fn a_run_is_refused_without_starting_the_command_when_the_reader_cap_is_reached() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ran");
        let at_cap = super::super::MAX_OUTSTANDING_HOOK_READER_THREADS - 1;
        let counter = leaked_counter(at_cap);
        let script = format!("touch '{}'", marker.display());
        let error = run_bounded_with(
            sh(&script),
            Duration::from_secs(5),
            1024,
            counter,
            &real_spawner,
        )
        .err()
        .expect("refused");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert!(!marker.exists(), "the command must not have been started");
        assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), at_cap);
    }

    #[test]
    fn leaked_readers_of_a_process_outside_the_group_are_capped() {
        // `setsid` puts the background process out of reach of the group
        // kill, so it keeps both pipes (and both reader threads) open.
        if Command::new("setsid").arg("--version").output().is_err() {
            return;
        }
        let max = super::super::MAX_OUTSTANDING_HOOK_READER_THREADS;
        let counter = leaked_counter(0);
        let mut refused = 0;
        let mut timed_out = 0;
        for _ in 0..(max / 2 + 2) {
            match run_bounded_with(
                sh("setsid sleep 4 & printf early"),
                Duration::from_millis(200),
                1024,
                counter,
                &real_spawner,
            ) {
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => timed_out += 1,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => refused += 1,
                other => panic!("unexpected result: {:?}", other.map(|o| o.stdout)),
            }
            assert!(counter.load(std::sync::atomic::Ordering::Acquire) <= max);
        }
        assert_eq!(timed_out, max / 2);
        assert_eq!(refused, 2);
        // The stragglers end with the background processes.
        wait_for_count(counter, 0);
    }

    #[test]
    fn a_reader_thread_that_cannot_be_started_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("still-running");
        for fail_on_call in [0usize, 1] {
            let counter = leaked_counter(0);
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let spawner = |name: &str, work: Box<dyn FnOnce() + Send + 'static>| {
                if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == fail_on_call {
                    // Dropping `work` releases its slot, as a failed
                    // Builder::spawn does.
                    Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "no threads",
                    ))
                } else {
                    real_spawner(name, work)
                }
            };
            let script = format!("(sleep 1; touch '{}') & sleep 30", marker.display());
            let started = Instant::now();
            let error = run_bounded_with(
                sh(&script),
                Duration::from_secs(20),
                1024,
                counter,
                &spawner,
            )
            .err()
            .expect("fails");
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
            assert!(started.elapsed() < Duration::from_secs(10));
            // The command is killed with its group, and every slot comes back.
            wait_for_count(counter, 0);
        }
        std::thread::sleep(Duration::from_secs(2));
        assert!(!marker.exists(), "the command's group must be killed");
    }
}
