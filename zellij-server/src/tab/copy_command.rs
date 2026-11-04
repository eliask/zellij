use std::io::prelude::*;
use std::process::{Child, Command, Stdio};
use std::thread::JoinHandle;

use anyhow::{bail, Context, Result};

pub struct CopyCommand {
    command: String,
    args: Vec<String>,
}

impl CopyCommand {
    pub fn new(command: String) -> Self {
        let mut command_with_args = shell_words::split(&command)
            .ok()
            .filter(|parts| !parts.is_empty())
            .unwrap_or_else(|| command.split_ascii_whitespace().map(String::from).collect())
            .into_iter();

        Self {
            command: command_with_args.next().unwrap_or_default(),
            args: command_with_args.collect(),
        }
    }
    pub fn set(&self, value: String) -> Result<()> {
        if self.command.is_empty() {
            bail!("copy_command is empty");
        }
        let mut process = Command::new(self.command.clone())
            .args(self.args.clone())
            .stdin(Stdio::piped())
            .spawn()
            .with_context(|| format!("couldn't spawn {}", self.command))?;
        let stdin = process.stdin.take();
        reap(process);
        stdin
            .context("could not get stdin")?
            .write_all(value.as_bytes())
            .with_context(|| format!("couldn't write to {} stdin", self.command))?;

        Ok(())
    }
}

/// Wait for the copy command's process in the background, so that it does not linger as a zombie
/// once it exits.
///
/// The process is deliberately not killed if it outlives the copy operation: X11 and Wayland
/// clipboard helpers (eg. `xsel`, `xclip` or `wl-copy`) keep running for as long as they own the
/// selection, and killing them clears the clipboard. Copy commands that do exit by themselves (eg.
/// `pbcopy`) are reaped as soon as they do.
fn reap(mut process: Child) -> JoinHandle<()> {
    std::thread::spawn(move || {
        if let Err(e) = process.wait() {
            log::error!("Clipboard failure: {}", e);
        }
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use nix::errno::Errno;
    use nix::sys::wait::{waitpid, WaitPidFlag};
    use nix::unistd::Pid;
    use std::path::Path;
    use std::time::{Duration, Instant};

    // stands in for an x11/wayland clipboard helper: consumes stdin and then keeps running the way
    // `xsel` or `xclip` do while they own the selection
    fn fake_clipboard_helper(dir: &Path, received: &Path, still_alive: &Path) -> String {
        let script = dir.join("fake-clipboard-helper");
        std::fs::write(
            &script,
            format!(
                "cat > '{}'\nsleep 2\ntouch '{}'\n",
                received.display(),
                still_alive.display()
            ),
        )
        .unwrap();
        // run it through `sh` rather than making it executable, so that a concurrent fork/exec
        // elsewhere in the test binary can't make this spawn fail with ETXTBSY
        format!("sh {}", script.display())
    }

    #[test]
    fn copy_command_preserves_quoted_and_empty_arguments() {
        let copy_command =
            CopyCommand::new("sh '/tmp/path with spaces' --flag '' trailing\\ value".to_owned());

        assert_eq!(copy_command.command, "sh");
        assert_eq!(
            copy_command.args,
            vec![
                "/tmp/path with spaces".to_owned(),
                "--flag".to_owned(),
                String::new(),
                "trailing value".to_owned(),
            ]
        );
    }

    #[test]
    fn copy_command_falls_back_for_malformed_quotes() {
        let copy_command = CopyCommand::new("helper 'unterminated".to_owned());

        assert_eq!(copy_command.command, "helper");
        assert_eq!(copy_command.args, vec!["'unterminated".to_owned()]);
    }

    #[test]
    fn copy_command_collapses_repeated_whitespace_without_creating_arguments() {
        let copy_command = CopyCommand::new("helper   --flag\t\tvalue".to_owned());

        assert_eq!(copy_command.command, "helper");
        assert_eq!(
            copy_command.args,
            vec!["--flag".to_owned(), "value".to_owned()]
        );
    }

    #[test]
    fn an_empty_copy_command_is_an_error_not_a_panic() {
        let copy_command = CopyCommand::new("   ".to_owned());

        assert!(copy_command.set("text".to_owned()).is_err());
    }

    #[test]
    fn copy_command_is_not_killed_when_it_outlives_the_copy() {
        let dir = tempfile::tempdir().unwrap();
        let received = dir.path().join("received");
        let still_alive = dir.path().join("still-alive");
        let copy_command = fake_clipboard_helper(dir.path(), &received, &still_alive);

        CopyCommand::new(copy_command)
            .set("some copied text".to_owned())
            .unwrap();

        // poll (bounded to ~15s) for the helper to outlive the copy operation
        let deadline = Instant::now() + Duration::from_secs(15);
        while !still_alive.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }

        assert_eq!(
            std::fs::read_to_string(&received).unwrap(),
            "some copied text",
            "the copy command should be fed the copied text on its stdin"
        );
        assert!(
            still_alive.exists(),
            "the copy command should not be killed for outliving the copy operation"
        );
    }

    #[test]
    fn copy_command_is_reaped_once_it_exits() {
        let process = Command::new("sh")
            .args(["-c", "exit 0"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let pid = Pid::from_raw(process.id() as i32);

        reap(process).join().unwrap();

        // having been waited for, the process is gone from the process table rather than left
        // behind as a zombie
        assert_eq!(
            waitpid(pid, Some(WaitPidFlag::WNOHANG)).err(),
            Some(Errno::ECHILD),
            "the copy command's process should have been reaped"
        );
    }
}
