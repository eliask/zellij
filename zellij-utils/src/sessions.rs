#[cfg(windows)]
use crate::ipc::IpcReceiverWithContext;
use crate::session_index::{self, SessionIndex};
use crate::{
    consts::{
        is_ipc_socket, session_info_folder_for_session, session_layout_cache_file_name,
        ZELLIJ_SESSION_INFO_CACHE_DIR, ZELLIJ_SOCK_DIR,
    },
    data::SessionInfo,
    envs,
    input::layout::Layout,
    ipc::{ClientToServerMsg, IpcSenderWithContext, ServerToClientMsg},
};
use anyhow;
use humantime::format_duration;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use std::{fs, io, process};
use suggest::Suggest;
use uuid::Uuid;

// A session's socket/named pipe and its session-info cache folder are named by a short, stable
// session id rather than by the session's name, so renaming a session renames nothing on disk
// (Windows named pipes cannot be renamed). The session index maps names to ids. Sessions it
// doesn't know (started by versions from before ids) use their name as their id.

/// Generate a new session id: a short random hex string (48 bits of a UUID v4). It is kept to
/// SESSION_ID_LENGTH bytes so that the socket path stays within the platform limit (104 bytes on
/// macOS) even under a long temp dir, which a full 36-char UUID would overflow.
/// `SessionIndex::register` guards against the (astronomically unlikely) collision.
pub fn generate_session_id() -> String {
    let bytes = Uuid::new_v4().into_bytes();
    let mut id = String::with_capacity(crate::consts::SESSION_ID_LENGTH);
    for byte in &bytes[..crate::consts::SESSION_ID_LENGTH / 2] {
        id.push_str(&format!("{:02x}", byte));
    }
    id
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Register a session that is about to start and return the id to name its socket by. A session
/// being resurrected keeps its id, and so its cache folder. Without a usable session index the
/// session is named by its name, as before ids existed.
pub fn register_session(name: &str) -> String {
    match SessionIndex::open_default().and_then(|index| index.register(name, now_secs())) {
        Ok(id) => id,
        Err(e) => {
            log::error!(
                "Failed to register session {:?} in the session index, naming its socket after it: {}",
                name,
                e
            );
            name.to_owned()
        },
    }
}

/// Record a session's new name. Its socket and cache folder keep their id-based names.
pub fn rename_session(session_id: &str, new_name: &str) {
    if let Err(e) = SessionIndex::open_default()
        .and_then(|index| index.rename_by_id(session_id, new_name, now_secs()))
    {
        log::error!(
            "Failed to record the new name of session {:?} in the session index: {}",
            session_id,
            e
        );
    }
}

/// The id of the session with this name, if the session index knows it. Callers fall back to the
/// name itself, which is the id of sessions from before ids.
pub fn resolve_session_id(name: &str) -> Option<String> {
    session_index::session_id_for_name(name)
}

/// The socket path of the session with this name, if the session index knows it.
pub fn resolve_session_socket_path(name: &str) -> Option<PathBuf> {
    resolve_session_id(name).map(|id| ZELLIJ_SOCK_DIR.join(id))
}

/// The name of the session with this id (a socket or cache folder name), or the id itself for
/// sessions from before ids.
pub fn display_name_for_session_id(id: &str) -> String {
    session_index::session_name_for_id(id).unwrap_or_else(|| id.to_owned())
}

fn session_name(names_by_id: &HashMap<String, String>, id: String) -> String {
    names_by_id.get(&id).cloned().unwrap_or(id)
}

pub fn get_sessions() -> Result<Vec<(String, Duration)>, io::ErrorKind> {
    match fs::read_dir(&*ZELLIJ_SOCK_DIR) {
        Ok(files) => {
            let names_by_id = session_index::session_names_by_id();
            let mut sessions = Vec::new();
            files.for_each(|file| {
                if let Ok(file) = file {
                    let file_name = file.file_name().into_string().unwrap();
                    // try to get creation time, fall back to modification time on platforms where it's not supported (e.g., musl)
                    // for session creation time these are almost always identical (notable
                    // exceptions are session name changes)
                    let ctime = std::fs::metadata(&file.path())
                        .ok()
                        .and_then(|f| f.created().ok().or_else(|| f.modified().ok()))
                        .and_then(|d| d.elapsed().ok())
                        .unwrap_or_default();
                    let duration = Duration::from_secs(ctime.as_secs());
                    if !crate::consts::is_app_socket_name(&file_name)
                        && is_ipc_socket(&file.file_type().unwrap())
                        && assert_socket(&file_name)
                    {
                        sessions.push((session_name(&names_by_id, file_name), duration));
                    }
                }
            });
            Ok(sessions)
        },
        Err(err) if io::ErrorKind::NotFound != err.kind() => Err(err.kind()),
        Err(_) => Ok(Vec::with_capacity(0)),
    }
}

pub fn get_resurrectable_sessions() -> Vec<(String, Duration)> {
    match fs::read_dir(&*ZELLIJ_SESSION_INFO_CACHE_DIR) {
        Ok(files_in_session_info_folder) => {
            let names_by_id = session_index::session_names_by_id();
            let files_that_are_folders = files_in_session_info_folder
                .filter_map(|f| f.ok().map(|f| f.path()))
                .filter(|f| f.is_dir());
            files_that_are_folders
                .filter_map(|folder_name| {
                    let layout_file_name =
                        session_layout_cache_file_name(&folder_name.display().to_string());
                    // Try to get creation time, fall back to modification time on platforms where it's not supported (e.g., musl)
                    let ctime = std::fs::metadata(&layout_file_name)
                        .ok()
                        .and_then(|metadata| {
                            metadata.created().ok().or_else(|| metadata.modified().ok())
                        });
                    let elapsed_duration = ctime
                        .map(|ctime| {
                            Duration::from_secs(ctime.elapsed().ok().unwrap_or_default().as_secs())
                        })
                        .unwrap_or_default();
                    let session_id = folder_name
                        .file_name()
                        .map(|f| std::path::PathBuf::from(f).display().to_string())?;
                    if crate::consts::is_app_socket_name(&session_id) {
                        return None;
                    }
                    if std::path::Path::new(&layout_file_name).exists() {
                        Some((session_name(&names_by_id, session_id), elapsed_duration))
                    } else {
                        None
                    }
                })
                .collect()
        },
        Err(e) => {
            log::error!(
                "Failed to read session_info cache folder: \"{:?}\": {:?}",
                &*ZELLIJ_SESSION_INFO_CACHE_DIR,
                e
            );
            vec![]
        },
    }
}

pub fn get_resurrectable_session_names() -> Vec<String> {
    match fs::read_dir(&*ZELLIJ_SESSION_INFO_CACHE_DIR) {
        Ok(files_in_session_info_folder) => {
            let names_by_id = session_index::session_names_by_id();
            let files_that_are_folders = files_in_session_info_folder
                .filter_map(|f| f.ok().map(|f| f.path()))
                .filter(|f| f.is_dir());
            files_that_are_folders
                .filter_map(|folder_name| {
                    let folder = folder_name.display().to_string();
                    let resurrection_layout_file = session_layout_cache_file_name(&folder);
                    let hidden = folder_name
                        .file_name()
                        .map(|f| crate::consts::is_app_socket_name(&f.to_string_lossy()))
                        .unwrap_or(false);
                    if !hidden && std::path::Path::new(&resurrection_layout_file).exists() {
                        folder_name
                            .file_name()
                            .map(|f| session_name(&names_by_id, f.to_string_lossy().into_owned()))
                    } else {
                        None
                    }
                })
                .collect()
        },
        Err(e) => {
            log::error!(
                "Failed to read session_info cache folder: \"{:?}\": {:?}",
                &*ZELLIJ_SESSION_INFO_CACHE_DIR,
                e
            );
            vec![]
        },
    }
}

pub fn get_sessions_sorted_by_mtime() -> anyhow::Result<Vec<String>> {
    match fs::read_dir(&*ZELLIJ_SOCK_DIR) {
        Ok(files) => {
            let names_by_id = session_index::session_names_by_id();
            let mut sessions_with_mtime: Vec<(String, SystemTime)> = Vec::new();
            for file in files {
                let file = file?;
                let file_name = file.file_name().into_string().unwrap();
                let file_modified_at = file.metadata()?.modified()?;
                if !crate::consts::is_app_socket_name(&file_name)
                    && is_ipc_socket(&file.file_type()?)
                    && assert_socket(&file_name)
                {
                    sessions_with_mtime
                        .push((session_name(&names_by_id, file_name), file_modified_at));
                }
            }
            sessions_with_mtime.sort_by_key(|x| x.1); // the oldest one will be the first

            let sessions = sessions_with_mtime.iter().map(|x| x.0.clone()).collect();
            Ok(sessions)
        },
        Err(err) if io::ErrorKind::NotFound != err.kind() => Err(err.into()),
        Err(_) => Ok(Vec::with_capacity(0)),
    }
}

/// How long one session socket may take to answer a probe. The budget is one
/// absolute deadline over connect, the request write, and the whole reply
/// frame. It is not restarted when a few bytes arrive.
#[cfg(not(windows))]
const SESSION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Upper bound on sockets probed at once, so a directory of wedged sockets
/// does not start one thread per entry.
#[cfg(all(unix, test))]
const MAX_CONCURRENT_SESSION_PROBES: usize = 8;

/// Result of probing one session socket.
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionProbeResult {
    /// The server answered `Connected`.
    Responsive,
    /// A socket exists but liveness could not be established (timeout,
    /// unexpected reply, oversized frame, or the shared deadline was already
    /// spent). Must not be unlinked.
    Unknown,
    /// Proven gone: no socket, or the connection was refused.
    Absent,
}

#[cfg(all(unix, test))]
#[derive(Clone, Copy)]
enum ObservedSlot {
    Responsive,
    Unknown,
}

#[cfg(all(unix, test))]
struct SessionObservationPayload<T> {
    responsive: Vec<(String, T)>,
    unknown: Vec<(String, T)>,
}

/// Probe candidates concurrently. Each probe receives the time still left on
/// the shared deadline, so a trickle of bytes on one socket cannot keep the
/// scan alive and a later socket is not given a fresh budget.
#[cfg(all(unix, test))]
fn observe_sessions_with<T, F>(
    candidates: Vec<(String, String, T)>,
    deadline: Duration,
    probe: F,
) -> SessionObservationPayload<T>
where
    T: Clone + Send + Sync,
    F: Fn(&str, Duration) -> SessionProbeResult + Sync,
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Instant;

    let mut observation = SessionObservationPayload {
        responsive: Vec::new(),
        unknown: Vec::new(),
    };
    if candidates.is_empty() {
        return observation;
    }
    if candidates.len() == 1 {
        let (id, name, payload) = candidates.into_iter().next().unwrap();
        match probe(&id, deadline) {
            SessionProbeResult::Responsive => observation.responsive.push((name, payload)),
            SessionProbeResult::Unknown => observation.unknown.push((name, payload)),
            SessionProbeResult::Absent => {},
        }
        return observation;
    }
    let results: Vec<Mutex<Option<(ObservedSlot, String, T)>>> =
        candidates.iter().map(|_| Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    let started = Instant::now();
    let workers = candidates.len().min(MAX_CONCURRENT_SESSION_PROBES);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some((id, name, _)) = candidates.get(index) else {
                    break;
                };
                let remaining = deadline.saturating_sub(started.elapsed());
                let outcome = if remaining.is_zero() {
                    SessionProbeResult::Unknown
                } else {
                    probe(id, remaining)
                };
                let slot = match outcome {
                    SessionProbeResult::Responsive => Some(ObservedSlot::Responsive),
                    SessionProbeResult::Unknown => Some(ObservedSlot::Unknown),
                    SessionProbeResult::Absent => None,
                };
                if let Some(slot) = slot {
                    let payload = candidates[index].2.clone();
                    *results[index]
                        .lock()
                        .expect("session probe result lock poisoned") =
                        Some((slot, name.clone(), payload));
                }
            });
        }
    });
    for slot in results {
        if let Some((kind, name, payload)) = slot.into_inner().unwrap_or_else(|e| e.into_inner()) {
            match kind {
                ObservedSlot::Responsive => observation.responsive.push((name, payload)),
                ObservedSlot::Unknown => observation.unknown.push((name, payload)),
            }
        }
    }
    observation
}

/// Probe a session socket to check if a server is alive.
///
/// On Unix, connects and sends a `ConnStatus` message to verify the server responds.
/// On Windows, reads the server PID from the marker file and checks process liveness.
#[cfg(unix)]
fn assert_socket(name: &str) -> bool {
    let path = ZELLIJ_SOCK_DIR.join(name);
    match assert_socket_path(&path, SESSION_PROBE_TIMEOUT) {
        SessionProbeResult::Responsive => true,
        SessionProbeResult::Absent => {
            // A refused connection leaves a stale socket file. Keep that
            // cleanup. A timeout or a bad reply is Unknown and is not unlinked.
            if path.exists() {
                drop(fs::remove_file(&path));
            }
            false
        },
        SessionProbeResult::Unknown => false,
    }
}

/// Probe the socket at `path` with one absolute deadline covering connect, the
/// `ConnStatus` write, and the whole reply frame.
///
/// A socket read timeout is an inactivity timer, so it is refreshed from that
/// deadline before every read and write. Timeout, short read, hang-up, an
/// oversized length prefix, and any message other than `Connected` are
/// [`SessionProbeResult::Unknown`]. Refused and missing paths are
/// [`SessionProbeResult::Absent`]. This function does not unlink the path.
#[cfg(unix)]
fn assert_socket_path(path: &Path, timeout: Duration) -> SessionProbeResult {
    let started = std::time::Instant::now();
    match probe_socket_path_detailed(path, timeout) {
        Ok(result) => result,
        Err(failure) => {
            log::debug!(
                "session probe {} failed during {} after {:?}: {:?}",
                path.display(),
                failure.stage,
                started.elapsed(),
                failure.error
            );
            #[cfg(test)]
            eprintln!(
                "session probe {} failed during {} after {:?}: {:?}",
                path.display(),
                failure.stage,
                started.elapsed(),
                failure.error
            );
            SessionProbeResult::Unknown
        },
    }
}

#[cfg(unix)]
struct SessionProbeFailure {
    stage: &'static str,
    error: io::Error,
}

#[cfg(unix)]
fn probe_socket_path_detailed(
    path: &Path,
    timeout: Duration,
) -> Result<SessionProbeResult, SessionProbeFailure> {
    use crate::consts::ipc_connect_with_timeout;
    use crate::ipc::{
        encode_client_frame, read_server_frame_until_deadline, write_all_until_deadline,
        PROBE_MAX_FRAME_BYTES,
    };
    use std::time::Instant;

    if timeout.is_zero() {
        return Ok(SessionProbeResult::Unknown);
    }
    let deadline = Instant::now() + timeout;
    let mut stream = match ipc_connect_with_timeout(path, timeout) {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            return Ok(SessionProbeResult::Absent)
        },
        Err(error) => {
            return Err(SessionProbeFailure {
                stage: "connect",
                error,
            })
        },
    };
    let frame = encode_client_frame(ClientToServerMsg::ConnStatus).map_err(|error| {
        SessionProbeFailure {
            stage: "encode request",
            error,
        }
    })?;
    write_all_until_deadline(&mut stream, &frame, deadline).map_err(|error| {
        SessionProbeFailure {
            stage: "write request",
            error,
        }
    })?;
    let response = read_server_frame_until_deadline(&mut stream, deadline, PROBE_MAX_FRAME_BYTES)
        .map_err(|error| SessionProbeFailure {
        stage: "read/decode reply",
        error,
    })?;
    match response {
        ServerToClientMsg::Connected => Ok(SessionProbeResult::Responsive),
        _ => Err(SessionProbeFailure {
            stage: "validate reply",
            error: io::Error::new(io::ErrorKind::InvalidData, "expected Connected reply"),
        }),
    }
}

/// On Windows, reads the server PID from the marker file and checks whether
/// the process is still alive via `OpenProcess`. Cleans up stale marker files.
#[cfg(windows)]
fn assert_socket(name: &str) -> bool {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    let path = &*ZELLIJ_SOCK_DIR.join(name);
    let pid_str = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => {
            drop(fs::remove_file(path));
            return false;
        },
    };
    let pid: u32 = match pid_str.trim().parse() {
        Ok(p) => p,
        Err(_) => {
            // Marker file exists but has no valid PID (e.g. empty from old version).
            // Treat as stale.
            drop(fs::remove_file(path));
            return false;
        },
    };
    let alive = unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            false
        } else {
            CloseHandle(handle);
            true
        }
    };
    if !alive {
        drop(fs::remove_file(path));
    }
    alive
}

#[cfg(not(any(unix, windows)))]
fn assert_socket(_name: &str) -> bool {
    true
}

pub fn print_sessions(
    mut sessions: Vec<(String, Duration, bool)>,
    no_formatting: bool,
    short: bool,
    reverse: bool,
) {
    // (session_name, timestamp, is_dead)
    let curr_session = envs::get_session_name().unwrap_or_else(|_| "".into());
    sessions.sort_by(|a, b| {
        if reverse {
            // sort by `Duration` ascending (newest would be first)
            a.1.cmp(&b.1)
        } else {
            b.1.cmp(&a.1)
        }
    });
    sessions
        .iter()
        .for_each(|(session_name, timestamp, is_dead)| {
            if short {
                println!("{}", session_name);
                return;
            }
            if no_formatting {
                let suffix = if curr_session == *session_name {
                    format!("(current)")
                } else if *is_dead {
                    format!("(EXITED - attach to resurrect)")
                } else {
                    String::new()
                };
                let timestamp = format!("[Created {} ago]", format_duration(*timestamp));
                println!("{} {} {}", session_name, timestamp, suffix);
            } else {
                let formatted_session_name = format!("\u{1b}[32;1m{}\u{1b}[m", session_name);
                let suffix = if curr_session == *session_name {
                    format!("(current)")
                } else if *is_dead {
                    format!("(\u{1b}[31;1mEXITED\u{1b}[m - attach to resurrect)")
                } else {
                    String::new()
                };
                let timestamp = format!(
                    "[Created \u{1b}[35;1m{}\u{1b}[m ago]",
                    format_duration(*timestamp)
                );
                println!("{} {} {}", formatted_session_name, timestamp, suffix);
            }
        })
}

pub fn print_sessions_with_index(sessions: Vec<String>) {
    let curr_session = envs::get_session_name().unwrap_or_else(|_| "".into());
    for (i, session) in sessions.iter().enumerate() {
        let suffix = if curr_session == *session {
            " (current)"
        } else {
            ""
        };
        println!("{}: {}{}", i, session, suffix);
    }
}

pub enum ActiveSession {
    None,
    One(String),
    Many,
}

pub fn get_active_session() -> ActiveSession {
    match get_sessions() {
        Ok(sessions) if sessions.is_empty() => ActiveSession::None,
        Ok(mut sessions) if sessions.len() == 1 => ActiveSession::One(sessions.pop().unwrap().0),
        Ok(_) => ActiveSession::Many,
        Err(e) => {
            eprintln!("Error occurred: {:?}", e);
            process::exit(1);
        },
    }
}

pub fn kill_session(name: &str) {
    let path = &*resolve_session_socket_path(name).unwrap_or_else(|| ZELLIJ_SOCK_DIR.join(name));
    if let Err(e) = send_kill_session(path) {
        eprintln!("Error occurred: {:?}", e);
        process::exit(1);
    }
}

/// On Windows the server answers on the connection's reply pipe, and waiting for that answer
/// keeps the request from being dropped before the server has read it.
#[cfg(windows)]
pub fn send_kill_session(path: &std::path::Path) -> std::io::Result<()> {
    let (stream, reply_stream) = crate::pipe_pairs::connect_pair(path)?;
    let _ = IpcSenderWithContext::<ClientToServerMsg>::new(stream)
        .send_client_msg(ClientToServerMsg::KillSession);
    let mut receiver: IpcReceiverWithContext<ServerToClientMsg> =
        IpcReceiverWithContext::new(reply_stream);
    let _ = receiver.recv_server_msg();
    Ok(())
}

#[cfg(not(windows))]
pub fn send_kill_session(path: &std::path::Path) -> std::io::Result<()> {
    // Bound the connect. The kill frame itself is small; the plugin path
    // waits for a decoded Exit under the same kind of deadline.
    let stream = crate::consts::ipc_connect_with_timeout(path, SESSION_PROBE_TIMEOUT)?;
    let _ = IpcSenderWithContext::<ClientToServerMsg>::new(stream)
        .send_client_msg(ClientToServerMsg::KillSession);
    Ok(())
}

pub fn delete_session(name: &str, force: bool) {
    // The socket and the cache folder are named by the session id; the index row by the name.
    let session_id = match resolve_session_id(name) {
        Some(id) => id,
        None => {
            if display_name_for_session_id(name) != name {
                eprintln!("Session: {:?} not found.", name);
                process::exit(2);
            }
            name.to_owned()
        },
    };
    if let Err(e) = validate_session_folder_name(&session_id) {
        eprintln!("{}", e);
        process::exit(1);
    }
    if force {
        let _ = send_kill_session(&ZELLIJ_SOCK_DIR.join(&session_id));
    }
    if let Ok(index) = crate::session_index::SessionIndex::open_default() {
        let _ = index.delete(name);
    }
    if let Err(e) = std::fs::remove_dir_all(session_info_folder_for_session(&session_id)) {
        if e.kind() == std::io::ErrorKind::NotFound {
            eprintln!("Session: {:?} not found.", name);
            process::exit(2);
        } else {
            log::error!("Failed to remove session {:?}: {:?}", name, e);
        }
    } else {
        println!("Session: {:?} successfully deleted.", name);
    }
}

pub fn list_sessions(no_formatting: bool, short: bool, reverse: bool) {
    let exit_code = match get_sessions() {
        Ok(running_sessions) => {
            let resurrectable_sessions = get_resurrectable_sessions();
            let mut all_sessions: HashMap<String, (Duration, bool)> = resurrectable_sessions
                .iter()
                .map(|(name, timestamp)| (name.clone(), (timestamp.clone(), true)))
                .collect();
            for (session_name, duration) in running_sessions {
                all_sessions.insert(session_name.clone(), (duration, false));
            }
            if all_sessions.is_empty() {
                eprintln!("No active zellij sessions found.");
                1
            } else {
                print_sessions(
                    all_sessions
                        .iter()
                        .map(|(name, (timestamp, is_dead))| {
                            (name.clone(), timestamp.clone(), *is_dead)
                        })
                        .collect(),
                    no_formatting,
                    short,
                    reverse,
                );
                0
            }
        },
        Err(e) => {
            eprintln!("Error occurred: {:?}", e);
            1
        },
    };
    process::exit(exit_code);
}

#[derive(Debug, Clone)]
pub enum SessionNameMatch {
    AmbiguousPrefix(Vec<String>),
    UniquePrefix(String),
    Exact(String),
    None,
}

pub fn match_session_name(prefix: &str) -> Result<SessionNameMatch, io::ErrorKind> {
    let sessions = get_sessions()?;

    let filtered_sessions: Vec<_> = sessions
        .iter()
        .filter(|s| s.0.starts_with(prefix))
        .collect();

    if filtered_sessions.iter().any(|s| s.0 == prefix) {
        return Ok(SessionNameMatch::Exact(prefix.to_string()));
    }

    Ok({
        match &filtered_sessions[..] {
            [] => SessionNameMatch::None,
            [s] => SessionNameMatch::UniquePrefix(s.0.to_string()),
            _ => SessionNameMatch::AmbiguousPrefix(
                filtered_sessions.into_iter().map(|s| s.0.clone()).collect(),
            ),
        }
    })
}

pub fn session_exists(name: &str) -> Result<bool, io::ErrorKind> {
    match match_session_name(name) {
        Ok(SessionNameMatch::Exact(_)) => Ok(true),
        Ok(_) => Ok(false),
        Err(e) => Err(e),
    }
}

// if the session is resurrecable, the returned layout is the one to be used to resurrect it
pub fn resurrection_layout(session_name_to_resurrect: &str) -> Result<Option<Layout>, String> {
    let session_id = resolve_session_id(session_name_to_resurrect)
        .unwrap_or_else(|| session_name_to_resurrect.to_owned());
    let layout_file_name = session_layout_cache_file_name(&session_id);
    let raw_layout = match std::fs::read_to_string(&layout_file_name) {
        Ok(raw_layout) => raw_layout,
        Err(_e) => {
            return Ok(None);
        },
    };
    match Layout::from_kdl(
        &raw_layout,
        Some(layout_file_name.display().to_string()),
        None,
        None,
    ) {
        Ok(layout) => Ok(Some(layout)),
        Err(e) => {
            let error_text = crate::input::keybind_presets::config_error_text(&e);
            log::error!(
                "Failed to parse resurrection layout file {}: {}",
                layout_file_name.display(),
                error_text
            );
            return Err(format!(
                "Failed to parse resurrection layout file {}: {}.",
                layout_file_name.display(),
                error_text
            ));
        },
    }
}

pub fn assert_session(name: &str) {
    match session_exists(name) {
        Ok(result) => {
            if result {
                return;
            } else {
                println!("No session named {:?} found.", name);
                if let Some(sugg) = get_sessions()
                    .unwrap()
                    .iter()
                    .map(|s| s.0.clone())
                    .collect::<Vec<_>>()
                    .suggest(name)
                {
                    println!("  help: Did you mean `{}`?", sugg);
                }
            }
        },
        Err(e) => {
            eprintln!("Error occurred: {:?}", e);
        },
    };
    process::exit(1);
}

pub fn assert_dead_session(name: &str, force: bool) {
    match session_exists(name) {
        Ok(exists) => {
            if exists && !force {
                println!(
                    "A session by the name {:?} exists and is active, use --force to delete it.",
                    name
                )
            } else if exists && force {
                println!("A session by the name {:?} exists and is active, but will be force killed and deleted.", name);
                return;
            } else {
                return;
            }
        },
        Err(e) => {
            eprintln!("Error occurred: {:?}", e);
        },
    };
    process::exit(1);
}

pub fn validate_session_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err(
            "Session name cannot be empty. Please provide a specific session name.".to_string(),
        );
    }
    if name == "." || name == ".." {
        return Err(format!("Invalid session name: \"{}\".", name));
    }
    if name.contains('/') {
        return Err("Session name cannot contain '/'.".to_string());
    }
    #[cfg(windows)]
    if name.contains('\\') {
        return Err("Session name cannot contain '\\'.".to_string());
    }
    #[cfg(windows)]
    if let Some(problem) = windows_file_name_problem(name) {
        return Err(problem);
    }
    Ok(())
}

/// Why Windows can't use `name` as a file name, which a session's socket falls back to when the
/// session index can't be written.
#[cfg(windows)]
fn windows_file_name_problem(name: &str) -> Option<String> {
    if let Some(c) = name
        .chars()
        .find(|c| c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
    {
        return Some(format!("Session name cannot contain {:?}.", c));
    }
    if name.ends_with(['.', ' ']) {
        return Some("Session name cannot end with a dot or a space.".to_string());
    }
    // Device names are reserved even with an extension, as in "nul.txt".
    let stem = name
        .split('.')
        .next()
        .unwrap_or(name)
        .trim_end()
        .to_uppercase();
    let letters: Vec<char> = stem.chars().collect();
    let numbered_device = letters.len() == 4
        && matches!(letters[..3], ['C', 'O', 'M'] | ['L', 'P', 'T'])
        && matches!(letters[3], '0'..='9' | '¹' | '²' | '³');
    if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL") || numbered_device {
        return Some(format!(
            "Session name cannot be {:?}: Windows reserves that name.",
            name
        ));
    }
    None
}

pub fn validate_session_folder_name(name: &str) -> Result<(), String> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(component)), None)
            if component == std::ffi::OsStr::new(name) =>
        {
            Ok(())
        },
        _ => Err(format!("Invalid session name or id: {:?}.", name)),
    }
}

pub fn assert_session_ne(name: &str) {
    if let Err(e) = validate_session_name(name) {
        eprintln!("{}", e);
        process::exit(1);
    }

    match session_exists(name) {
        Ok(result) if !result => {
            let resurrectable_sessions = get_resurrectable_session_names();
            if resurrectable_sessions.iter().find(|s| s == &name).is_some() {
                println!("Session with name {:?} already exists, but is dead. Use the attach command to resurrect it or, the delete-session command to kill it or specify a different name.", name);
            } else {
                return
            }
        }
        Ok(_) => println!("Session with name {:?} already exists. Use attach command to connect to it or specify a different name.", name),
        Err(e) => eprintln!("Error occurred: {:?}", e),
    };
    process::exit(1);
}

pub fn session_listing_error_message(kind: io::ErrorKind) -> String {
    format!(
        "Failed to list existing Zellij sessions in the socket directory:\n  {}\n\n\
         Reason: {}\n\n\
         This usually means the directory (or one of its parents) is not readable \
         by the current user - for example if $ZELLIJ_SOCKET_DIR or $XDG_RUNTIME_DIR \
         points to a directory you do not own.\n\
         To fix this, set a readable and writable socket directory, eg.:\n  \
         ZELLIJ_SOCKET_DIR=/tmp/zellij-$USER zellij",
        ZELLIJ_SOCK_DIR.display(),
        io::Error::from(kind)
    )
}

pub fn read_live_session_states(
    current_session_name: &str,
    sock_dir: &Path,
    session_info_cache_dir: &Path,
) -> BTreeMap<String, SessionInfo> {
    let mut other_session_names: Vec<(String, Duration)> = vec![];
    let mut session_infos_on_machine = BTreeMap::new();
    if let Ok(files) = fs::read_dir(sock_dir) {
        files.for_each(|file| {
            if let Ok(file) = file {
                if let Ok(file_name) = file.file_name().into_string() {
                    if !crate::consts::is_app_socket_name(&file_name)
                        && file
                            .file_type()
                            .map(|file_type| is_ipc_socket(&file_type))
                            .unwrap_or(false)
                    {
                        let creation_time = std::fs::metadata(&file.path())
                            .ok()
                            .and_then(|f| f.created().ok().or_else(|| f.modified().ok()))
                            .and_then(|d| d.elapsed().ok())
                            .unwrap_or_default();
                        other_session_names.push((file_name, creation_time));
                    }
                }
            }
        });
    }

    let names_by_id = session_index::session_names_by_id();
    for (session_id, creation_time) in other_session_names {
        let session_cache_file_name = session_info_cache_dir
            .join(&session_id)
            .join("session-metadata.kdl");
        if let Ok(raw_session_info) = fs::read_to_string(&session_cache_file_name) {
            if let Ok(mut session_info) =
                SessionInfo::from_string(&raw_session_info, current_session_name)
            {
                session_info.creation_time = creation_time;
                session_infos_on_machine
                    .insert(session_name(&names_by_id, session_id), session_info);
            }
        }
    }
    session_infos_on_machine
}

pub fn read_live_session_states_default_dirs(
    current_session_name: &str,
) -> BTreeMap<String, SessionInfo> {
    read_live_session_states(
        current_session_name,
        &*ZELLIJ_SOCK_DIR,
        &*ZELLIJ_SESSION_INFO_CACHE_DIR,
    )
}

pub fn generate_unique_session_name() -> Option<String> {
    let sessions = get_sessions().map(|sessions| {
        sessions
            .iter()
            .map(|s| s.0.clone())
            .collect::<Vec<String>>()
    });
    let dead_sessions = get_resurrectable_session_names();
    let sessions = match sessions {
        Ok(sessions) => sessions,
        Err(kind) => {
            eprintln!("{}", session_listing_error_message(kind));
            return None;
        },
    };

    let name = get_name_generator()
        .take(1000)
        .find(|name| !sessions.contains(name) && !dead_sessions.contains(name));

    if let Some(name) = name {
        return Some(name);
    } else {
        return None;
    }
}

/// Create a new random name generator
///
/// Used to provide a memorable handle for a session when users don't specify a session name when the session is
/// created.
///
/// Uses the list of adjectives and nouns defined below, with the intention of avoiding unfortunate
/// and offensive combinations. Care should be taken when adding or removing to either list due to the birthday paradox/
/// hash collisions, e.g. with 4096 unique names, the likelihood of a collision in 10 session names is 1%.
pub fn get_name_generator() -> impl Iterator<Item = String> {
    names::Generator::new(&ADJECTIVES, &NOUNS, names::Name::Plain)
}

/// Generates a random human-readable name using curated adjectives and nouns.
/// Returns a single name in the format: AdjectiveNoun (e.g., "BraveRustacean")
pub fn generate_random_name() -> String {
    get_name_generator().next().unwrap()
}

const ADJECTIVES: &[&'static str] = &[
    "adamant",
    "adept",
    "adventurous",
    "arcadian",
    "auspicious",
    "awesome",
    "blossoming",
    "brave",
    "charming",
    "chatty",
    "circular",
    "considerate",
    "cubic",
    "curious",
    "delighted",
    "didactic",
    "diligent",
    "effulgent",
    "erudite",
    "excellent",
    "exquisite",
    "fabulous",
    "fascinating",
    "friendly",
    "glowing",
    "gracious",
    "gregarious",
    "hopeful",
    "implacable",
    "inventive",
    "joyous",
    "judicious",
    "jumping",
    "kind",
    "likable",
    "loyal",
    "lucky",
    "marvellous",
    "mellifluous",
    "nautical",
    "oblong",
    "outstanding",
    "polished",
    "polite",
    "profound",
    "quadratic",
    "quiet",
    "rectangular",
    "remarkable",
    "rusty",
    "sensible",
    "sincere",
    "sparkling",
    "splendid",
    "stellar",
    "tenacious",
    "tremendous",
    "triangular",
    "undulating",
    "unflappable",
    "unique",
    "verdant",
    "vitreous",
    "wise",
    "zippy",
];

const NOUNS: &[&'static str] = &[
    "aardvark",
    "accordion",
    "apple",
    "apricot",
    "bee",
    "brachiosaur",
    "cactus",
    "capsicum",
    "clarinet",
    "cowbell",
    "crab",
    "cuckoo",
    "cymbal",
    "diplodocus",
    "donkey",
    "drum",
    "duck",
    "echidna",
    "elephant",
    "foxglove",
    "galaxy",
    "glockenspiel",
    "goose",
    "hill",
    "horse",
    "iguanodon",
    "jellyfish",
    "kangaroo",
    "lake",
    "lemon",
    "lemur",
    "magpie",
    "megalodon",
    "mountain",
    "mouse",
    "muskrat",
    "newt",
    "oboe",
    "ocelot",
    "orange",
    "panda",
    "peach",
    "pepper",
    "petunia",
    "pheasant",
    "piano",
    "pigeon",
    "platypus",
    "quasar",
    "rhinoceros",
    "river",
    "rustacean",
    "salamander",
    "sitar",
    "stegosaurus",
    "tambourine",
    "tiger",
    "tomato",
    "triceratops",
    "ukulele",
    "viola",
    "weasel",
    "xylophone",
    "yak",
    "zebra",
];

#[cfg(test)]
mod session_id_tests {
    use super::*;

    #[test]
    fn generated_id_is_short_hex() {
        let id = generate_session_id();
        assert_eq!(id.len(), crate::consts::SESSION_ID_LENGTH);
        assert!(
            id.bytes().all(|b| b.is_ascii_hexdigit()),
            "id should be hex: {}",
            id
        );
        assert_ne!(id, generate_session_id());
    }
}

#[cfg(test)]
mod session_folder_name_tests {
    use super::*;

    #[test]
    fn session_folder_name_accepts_plain_names() {
        assert!(validate_session_folder_name(&generate_session_id()).is_ok());
        assert!(validate_session_folder_name("my-session").is_ok());
        assert!(validate_session_folder_name("my session.v2").is_ok());
    }

    #[test]
    fn session_folder_name_rejects_paths() {
        for name in [
            "",
            ".",
            "..",
            "../x",
            "../../../../poc",
            "a/b",
            "/abs",
            "a/",
            "./a",
        ] {
            assert!(
                validate_session_folder_name(name).is_err(),
                "should reject {:?}",
                name
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn session_folder_name_rejects_windows_paths() {
        for name in ["..\\x", "a\\b", "C:\\x", "C:x", "\\\\server\\share"] {
            assert!(
                validate_session_folder_name(name).is_err(),
                "should reject {:?}",
                name
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn validate_session_name_rejects_backslash_on_windows() {
        assert!(validate_session_name("a\\b").is_err());
    }

    #[cfg(windows)]
    #[test]
    fn validate_session_name_rejects_what_windows_cannot_name_a_file() {
        for name in [
            "a:b",
            "a*",
            "a?",
            "a\"b",
            "a<b",
            "a>b",
            "a|b",
            "a\tb",
            "trailing.",
            "trailing ",
            "CON",
            "nul",
            "Aux.txt",
            "com1",
            "LPT9.log",
            "COM¹",
            "prn ",
        ] {
            assert!(
                validate_session_name(name).is_err(),
                "should reject {:?}",
                name
            );
        }
        for name in [
            "my-session",
            "con-artist",
            "com10",
            "lpt",
            "nullable",
            ".zj-panel",
            "v1.2",
            "café",
        ] {
            assert!(
                validate_session_name(name).is_ok(),
                "should accept {:?}",
                name
            );
        }
    }
}

#[cfg(all(test, unix))]
#[path = "probe_deadline.rs"]
mod probe_deadline;
