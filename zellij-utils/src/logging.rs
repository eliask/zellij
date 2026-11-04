//! Zellij logging utility functions.

use std::{
    fs,
    io::{self, prelude::*},
    path::{Path, PathBuf},
};

use log::LevelFilter;

use log4rs::append::rolling_file::{
    policy::compound::{
        roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
    },
    RollingFileAppender,
};
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;

use crate::consts::{ZELLIJ_TMP_DIR, ZELLIJ_TMP_LOG_DIR, ZELLIJ_TMP_LOG_FILE};
use crate::shared::set_permissions;

const LOG_MAX_BYTES: u64 = 1024 * 1024 * 16; // 16 MiB per log

pub fn configure_logger() {
    atomic_create_dir(&*ZELLIJ_TMP_DIR).unwrap();
    atomic_create_dir(&*ZELLIJ_TMP_LOG_DIR).unwrap();
    atomic_create_file(&*ZELLIJ_TMP_LOG_FILE).unwrap();

    let trigger = SizeTrigger::new(LOG_MAX_BYTES);
    let roller = FixedWindowRoller::builder()
        .build(
            ZELLIJ_TMP_LOG_DIR
                .join(format!("{}.log.old.{{}}", crate::distribution::name()))
                .to_str()
                .unwrap(),
            1,
        )
        .unwrap();

    // {n} means platform dependent newline
    // module is padded to exactly 25 bytes and thread is padded to be between 10 and 15 bytes.
    let file_pattern = "{highlight({level:<6})} |{module:<25.25}| {date(%Y-%m-%d %H:%M:%S.%3f)} [{thread:<10.15}] {file}:{line}: {message} {n}";

    // default zellij appender, should be used across most of the codebase.
    let log_file = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(file_pattern)))
        .build(
            &*ZELLIJ_TMP_LOG_FILE,
            Box::new(CompoundPolicy::new(
                Box::new(trigger),
                Box::new(roller.clone()),
            )),
        )
        .unwrap();

    // plugin appender. To be used in logging_pipe to forward stderr output from plugins. We do some formatting
    // in logging_pipe to print plugin name as 'module' and plugin_id instead of thread.
    let log_plugin = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{highlight({level:<6})} {message} {n}",
        )))
        .build(
            &*ZELLIJ_TMP_LOG_FILE,
            Box::new(CompoundPolicy::new(Box::new(trigger), Box::new(roller))),
        )
        .unwrap();

    // Set the default logging level to "info" and log it to zellij.log file
    // Decrease verbosity for `wasmtime_wasi` module because it has a lot of useless info logs
    // For `zellij_server::logging_pipe`, we use custom format as we use logging macros to forward stderr output from plugins
    let config = Config::builder()
        .appender(Appender::builder().build("logFile", Box::new(log_file)))
        .appender(Appender::builder().build("logPlugin", Box::new(log_plugin)))
        // reduce the verbosity of isahc, otherwise it logs on every failed web request
        .logger(
            Logger::builder()
                .appender("logFile")
                .build("isahc", LevelFilter::Error),
        )
        .logger(Logger::builder().build("notify", LevelFilter::Error))
        .logger(
            Logger::builder()
                .appender("logPlugin")
                .build("wasmtime_wasi", LevelFilter::Warn),
        )
        .logger(
            Logger::builder()
                .appender("logPlugin")
                .additive(false)
                .build("zellij_server::logging_pipe", LevelFilter::Trace),
        )
        .build(Root::builder().appender("logFile").build(LevelFilter::Info))
        .unwrap();

    let _ = log4rs::init_config(config).unwrap();
}

pub fn atomic_create_file(file_name: &Path) -> io::Result<()> {
    let _ = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(file_name)?;
    set_permissions(file_name, 0o600)
}

pub fn atomic_create_dir(dir_name: &Path) -> io::Result<()> {
    let result = if let Err(e) = fs::create_dir(dir_name) {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            Ok(())
        } else {
            Err(e)
        }
    } else {
        Ok(())
    };
    if result.is_ok() {
        set_permissions(dir_name, 0o700)?;
    }
    result
}

pub fn debug_to_file(message: &[u8], terminal_id: i32, session_id: &str) -> io::Result<()> {
    let mut path = PathBuf::new();
    path.push(&*ZELLIJ_TMP_LOG_DIR);
    path.push("terminal-logs");
    path.push(encode_path_component(session_id));
    atomic_create_dir(&path)?;
    path.push(format!(
        "{}-{}.log",
        crate::distribution::name(),
        terminal_id
    ));

    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)?;
    set_permissions(&path, 0o600)?;
    file.write_all(message)
}

fn encode_path_component(component: &str) -> String {
    let mut encoded = String::new();
    for byte in component.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{:02X}", byte));
        }
    }
    encoded
}

#[cfg(test)]
mod debug_log_tests {
    use super::encode_path_component;

    #[test]
    fn session_log_directories_cannot_escape_their_parent() {
        assert_eq!(
            encode_path_component("../session/one"),
            "%2E%2E%2Fsession%2Fone"
        );
        assert_eq!(encode_path_component("session-1_ok"), "session-1_ok");
    }

    #[test]
    fn distinct_session_ids_cannot_encode_to_the_same_directory() {
        assert_ne!(
            encode_path_component("session/one"),
            encode_path_component("session_one")
        );
    }
}
