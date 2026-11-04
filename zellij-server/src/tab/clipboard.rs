use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::engine::Engine as _;
use std::sync::mpsc::{sync_channel, SyncSender};
use zellij_utils::{data::CopyDestination, input::options::Clipboard};

use super::copy_command::CopyCommand;

const CLIPBOARD_QUEUE_CAPACITY: usize = 16;

pub(crate) struct CommandClipboard {
    sender: SyncSender<String>,
}

impl CommandClipboard {
    fn new(command: CopyCommand) -> Self {
        let (sender, receiver) = sync_channel(CLIPBOARD_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("clipboard-helper".to_owned())
            .spawn(move || {
                while let Ok(content) = receiver.recv() {
                    if let Err(error) = command.set(content) {
                        log::error!("Clipboard helper failed: {:?}", error);
                    }
                }
            })
            .expect("failed to start clipboard helper thread");
        Self { sender }
    }

    fn set_content(&self, content: &str) -> Result<()> {
        self.sender
            .try_send(content.to_owned())
            .context("clipboard helper queue is full")
    }
}

pub(crate) enum ClipboardProvider {
    Command(CommandClipboard),
    Osc52(Clipboard),
}

impl ClipboardProvider {
    pub(crate) fn command(command: CopyCommand) -> Self {
        Self::Command(CommandClipboard::new(command))
    }

    pub(crate) fn set_content(&self, content: &str) -> Result<Option<String>> {
        match &self {
            ClipboardProvider::Command(command) => {
                command.set_content(content)?;
                Ok(None)
            },
            ClipboardProvider::Osc52(clipboard) => {
                let dest = match clipboard {
                    #[cfg(not(target_os = "macos"))]
                    Clipboard::Primary => 'p',
                    #[cfg(target_os = "macos")] // primary selection does not exist on macos
                    Clipboard::Primary => 'c',
                    Clipboard::System => 'c',
                };
                Ok(Some(format!(
                    "\u{1b}]52;{};{}\u{1b}\\",
                    dest,
                    BASE64_STANDARD.encode(content)
                )))
            },
        }
    }

    pub(crate) fn as_copy_destination(&self) -> CopyDestination {
        match self {
            ClipboardProvider::Command(_) => CopyDestination::Command,
            ClipboardProvider::Osc52(clipboard) => match clipboard {
                Clipboard::Primary => CopyDestination::Primary,
                Clipboard::System => CopyDestination::System,
            },
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::tab::copy_command::CopyCommand;
    use std::time::{Duration, Instant};

    #[test]
    fn command_clipboard_transfer_does_not_block_the_caller() {
        let dir = tempfile::tempdir().unwrap();
        let received = dir.path().join("received");
        let command = format!("sh -c 'sleep 2; cat > {}'", received.display());
        let provider = ClipboardProvider::command(CopyCommand::new(command));

        let started = Instant::now();
        assert!(provider.set_content("copied text").unwrap().is_none());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the helper transfer must run off the caller thread"
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        while std::fs::read_to_string(&received).ok().as_deref() != Some("copied text")
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(std::fs::read_to_string(received).unwrap(), "copied text");
    }
}
