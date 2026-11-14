#[cfg(windows)]
use crate::ipc::{IpcReceiverWithContext, IpcSenderWithContext};
use crate::{
    consts::{
        is_ipc_socket, session_info_folder_for_session, session_layout_cache_file_name,
        ZELLIJ_SESSION_INFO_CACHE_DIR, ZELLIJ_SOCK_DIR,
    },
    data::SessionInfo,
    envs,
    input::layout::Layout,
    ipc::{ClientToServerMsg, ServerToClientMsg},
    session_index::{self, SessionIndex},
};
use anyhow;
use humantime::format_duration;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};
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

/// Folder id for `name`: the index id, or `name` when the index has no row.
/// A read error is `Err` and is not `name`.
pub fn session_folder_id(name: &str) -> Result<String, String> {
    match session_index::session_id_for_name_strict(name) {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Ok(name.to_owned()),
        Err(error) => Err(format!("Failed to read the session index: {error}")),
    }
}

fn session_socket_path(name: &str) -> Result<PathBuf, String> {
    Ok(ZELLIJ_SOCK_DIR.join(session_folder_id(name)?))
}

/// The name of the session with this id (a socket or cache folder name), or the id itself for
/// sessions from before ids.
pub fn display_name_for_session_id(id: &str) -> String {
    session_index::session_name_for_id(id).unwrap_or_else(|| id.to_owned())
}

fn session_name(names_by_id: &HashMap<String, String>, id: String) -> String {
    names_by_id.get(&id).cloned().unwrap_or(id)
}

/// How long one session socket may take to answer before it is unknown, not
/// dead, and how long a listing may take in total.
const SESSION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Upper bound on sockets probed at once, so a directory of wedged sockets is
/// scanned in parallel without one thread per entry.
const MAX_CONCURRENT_SESSION_PROBES: usize = 8;

/// Result of probing one session socket or marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionProbeResult {
    /// The server answered and is confirmed live.
    Responsive,
    /// A socket or marker exists but liveness could not be established.
    /// Must not be treated as absent or as a free name.
    Unknown,
    /// Proven gone: no socket, connection refused, or process death established
    /// by platform-specific evidence. Observation does not delete the path.
    Absent,
}

/// Responsive sessions plus candidates that stayed unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionObservation {
    pub responsive: Vec<(String, Duration)>,
    pub unknown: Vec<(String, Duration)>,
}

impl SessionObservation {
    pub fn is_definitely_empty(&self) -> bool {
        self.responsive.is_empty() && self.unknown.is_empty()
    }
}

/// The socket or named-pipe file name of a directory entry, if it could be a session.
fn session_socket_id(file: &fs::DirEntry) -> Option<String> {
    let file_name = file.file_name().into_string().ok()?;
    let file_type = file.file_type().ok()?;
    if crate::consts::is_app_socket_name(&file_name) || !is_ipc_socket(&file_type) {
        return None;
    }
    Some(file_name)
}

fn session_age(file: &fs::DirEntry) -> Duration {
    let age = fs::metadata(file.path())
        .ok()
        .and_then(|metadata| metadata.created().ok().or_else(|| metadata.modified().ok()))
        .and_then(|time| time.elapsed().ok())
        .unwrap_or_default();
    Duration::from_secs(age.as_secs())
}

/// Probe candidates concurrently and return only the responsive ones, in
/// candidate order. Unknown candidates are omitted; use [`observe_sessions_with`]
/// when the caller must distinguish unknown from absent.
fn live_sessions<T: Clone + Send + Sync>(
    dir: &Path,
    candidates: Vec<(String, String, T)>,
    deadline: Duration,
) -> Vec<(String, T)> {
    observe_sessions_with(candidates, deadline, |id, remaining| {
        assert_socket_in(dir, id, remaining)
    })
    .responsive
}

#[derive(Clone, Copy)]
enum ObservedSlot {
    Responsive,
    Unknown,
}

fn observe_sessions_with<T, F>(
    candidates: Vec<(String, String, T)>,
    deadline: Duration,
    probe: F,
) -> SessionObservationPayload<T>
where
    T: Clone + Send + Sync,
    F: Fn(&str, Duration) -> SessionProbeResult + Sync,
{
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
                    // Never reached before the shared deadline: unknown, not absent.
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

struct SessionObservationPayload<T> {
    responsive: Vec<(String, T)>,
    unknown: Vec<(String, T)>,
}

pub fn get_sessions() -> Result<Vec<(String, Duration)>, io::ErrorKind> {
    Ok(observe_sessions()?.responsive)
}

/// Discover sessions while keeping unknown candidates separate from absent ones.
pub fn observe_sessions() -> Result<SessionObservation, io::ErrorKind> {
    observe_sessions_in(&ZELLIJ_SOCK_DIR)
}

fn observe_sessions_in(dir: &Path) -> Result<SessionObservation, io::ErrorKind> {
    observe_sessions_in_with(dir, SESSION_PROBE_TIMEOUT)
}

fn observe_sessions_in_with(
    dir: &Path,
    deadline: Duration,
) -> Result<SessionObservation, io::ErrorKind> {
    match fs::read_dir(dir) {
        Ok(files) => {
            let names_by_id = session_index::session_names_by_id();
            let candidates = files
                .flatten()
                .filter_map(|file| {
                    let id = session_socket_id(&file)?;
                    let name = session_name(&names_by_id, id.clone());
                    Some((id, name, session_age(&file)))
                })
                .collect();
            let observed = observe_sessions_with(candidates, deadline, |id, remaining| {
                assert_socket_in(dir, id, remaining)
            });
            Ok(SessionObservation {
                responsive: observed.responsive,
                unknown: observed.unknown,
            })
        },
        Err(err) if io::ErrorKind::NotFound != err.kind() => Err(err.kind()),
        Err(_) => Ok(SessionObservation::default()),
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
                    if layout_file_is_resurrectable(Path::new(&layout_file_name)) {
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
                    if !hidden && layout_file_is_resurrectable(Path::new(&resurrection_layout_file))
                    {
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
            let mut candidates = Vec::new();
            for file in files.flatten() {
                let file_name = match file.file_name().into_string() {
                    Ok(name) => name,
                    Err(_) => continue,
                };
                if crate::consts::is_app_socket_name(&file_name) {
                    continue;
                }
                // An inspection failure is not "no session": callers create a
                // replacement when this list is empty.
                let file_type = file.file_type()?;
                if !is_ipc_socket(&file_type) {
                    continue;
                }
                let modified_at = session_mtime_result(file.metadata())?;
                let name = session_name(&names_by_id, file_name.clone());
                candidates.push((file_name, name, modified_at));
            }
            let mut sessions_with_mtime =
                live_sessions(&ZELLIJ_SOCK_DIR, candidates, SESSION_PROBE_TIMEOUT);
            sessions_with_mtime.sort_by_key(|x| x.1);

            let sessions = sessions_with_mtime.iter().map(|x| x.0.clone()).collect();
            Ok(sessions)
        },
        Err(err) if io::ErrorKind::NotFound != err.kind() => Err(err.into()),
        Err(_) => Ok(Vec::with_capacity(0)),
    }
}

/// Probe a session socket. Ordinary probing is observational: a refused
/// connection proves absence for discovery but does not unlink the path.
#[cfg(unix)]
fn assert_socket_in(dir: &Path, name: &str, timeout: Duration) -> SessionProbeResult {
    assert_socket_path(&dir.join(name), timeout)
}

/// One absolute deadline covers connect, the `ConnStatus` write, and the reply.
/// Timeout, short read, hang-up, an oversized frame, and any message other than
/// `Connected` are [`SessionProbeResult::Unknown`]. Refused and missing paths
/// are [`SessionProbeResult::Absent`] and are not unlinked.
#[cfg(unix)]
fn assert_socket_path(path: &Path, timeout: Duration) -> SessionProbeResult {
    let started = Instant::now();
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

/// Classification of a Windows `OpenProcess` result for a session marker PID.
///
/// A successful handle only means some process currently has that PID, not that
/// it is the recorded server. Access denied stays unknown. Only a clear
/// no-such-PID error treats the marker as dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowsPidLiveness {
    /// `OpenProcess` returned a handle. Identity with the recorded server is not proven.
    AliveUnverified,
    /// Process is gone (`ERROR_INVALID_PARAMETER` after a null handle).
    Dead,
    /// Inspection failed without proving death.
    Unknown,
}

/// `ERROR_ACCESS_DENIED` from Win32.
pub const WINDOWS_ERROR_ACCESS_DENIED: u32 = 5;
/// `ERROR_INVALID_PARAMETER` from Win32, documented for `OpenProcess` when the PID does not exist.
pub const WINDOWS_ERROR_INVALID_PARAMETER: u32 = 87;

/// Access denied is not death. `last_error` matters only when `handle_is_null`.
pub fn classify_windows_open_process(handle_is_null: bool, last_error: u32) -> WindowsPidLiveness {
    if !handle_is_null {
        return WindowsPidLiveness::AliveUnverified;
    }
    match last_error {
        WINDOWS_ERROR_INVALID_PARAMETER => WindowsPidLiveness::Dead,
        _ => WindowsPidLiveness::Unknown,
    }
}

/// A reused PID is not proof of the recorded server, so an unverified live
/// handle is [`SessionProbeResult::Unknown`], not responsive.
pub fn probe_result_for_windows_liveness(liveness: WindowsPidLiveness) -> SessionProbeResult {
    match liveness {
        WindowsPidLiveness::AliveUnverified => SessionProbeResult::Unknown,
        WindowsPidLiveness::Dead => SessionProbeResult::Absent,
        WindowsPidLiveness::Unknown => SessionProbeResult::Unknown,
    }
}

/// Observation never deletes Windows markers.
#[cfg(windows)]
fn assert_socket_in(dir: &Path, name: &str, timeout: Duration) -> SessionProbeResult {
    assert_socket_path(&dir.join(name), timeout)
}

#[cfg(windows)]
fn assert_socket_path(path: &Path, _timeout: Duration) -> SessionProbeResult {
    use windows_sys::Win32::Foundation::{CloseHandle, GetLastError};
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    let pid_str = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return SessionProbeResult::Absent,
        Err(_) => return SessionProbeResult::Unknown,
    };
    let pid: u32 = match pid_str.trim().parse() {
        Ok(pid) => pid,
        Err(_) => return SessionProbeResult::Absent,
    };
    let (handle_is_null, last_error) = unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            (true, GetLastError())
        } else {
            CloseHandle(handle);
            (false, 0)
        }
    };
    probe_result_for_windows_liveness(classify_windows_open_process(handle_is_null, last_error))
}

#[cfg(not(any(unix, windows)))]
fn assert_socket_in(_dir: &Path, _name: &str, _timeout: Duration) -> SessionProbeResult {
    SessionProbeResult::Responsive
}

#[cfg(not(any(unix, windows)))]
fn assert_socket_path(_path: &Path, _timeout: Duration) -> SessionProbeResult {
    SessionProbeResult::Responsive
}

/// [`Path::exists`] maps inspection failures to `false`. This does not.
fn path_try_exists(path: &Path) -> Result<bool, io::ErrorKind> {
    path.try_exists().map_err(|err| err.kind())
}

/// Presence of a resurrection layout. An inspection error is not absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutPresence {
    Present,
    Absent,
    Unknown,
}

pub fn layout_presence(try_exists: Result<bool, io::ErrorKind>) -> LayoutPresence {
    match try_exists {
        Ok(true) => LayoutPresence::Present,
        Ok(false) => LayoutPresence::Absent,
        Err(_) => LayoutPresence::Unknown,
    }
}

fn layout_counts_as_resurrectable(presence: LayoutPresence) -> bool {
    !matches!(presence, LayoutPresence::Absent)
}

fn layout_file_is_resurrectable(path: &Path) -> bool {
    layout_counts_as_resurrectable(layout_presence(path_try_exists(path)))
}

/// Metadata used to order sessions. `Err` must not collapse into "no session".
fn session_mtime_result(metadata: io::Result<fs::Metadata>) -> anyhow::Result<SystemTime> {
    Ok(metadata?.modified()?)
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
    /// Socket(s) exist but none answered before the probe deadline.
    Unresponsive(Vec<String>),
}

pub fn get_active_session() -> ActiveSession {
    match observe_sessions() {
        Ok(observation) if observation.is_definitely_empty() => ActiveSession::None,
        Ok(observation) if observation.responsive.is_empty() => ActiveSession::Unresponsive(
            observation
                .unknown
                .into_iter()
                .map(|(name, _)| name)
                .collect(),
        ),
        Ok(mut observation) if observation.responsive.len() == 1 => {
            ActiveSession::One(observation.responsive.pop().unwrap().0)
        },
        Ok(_) => ActiveSession::Many,
        Err(e) => {
            eprintln!("Error occurred: {:?}", e);
            process::exit(1);
        },
    }
}

/// Saved sessions `delete-all-sessions` may remove.
///
/// Unresponsive sockets stay active unless `force` explicitly authorizes
/// killing and deleting them.
pub fn resurrectable_sessions_to_delete(
    force: bool,
    observation: &SessionObservation,
    resurrectable_sessions: Vec<(String, Duration)>,
) -> Vec<(String, Duration)> {
    if force {
        return resurrectable_sessions;
    }
    let active_sessions: BTreeSet<&str> = observation
        .responsive
        .iter()
        .chain(observation.unknown.iter())
        .map(|(name, _)| name.as_str())
        .collect();
    resurrectable_sessions
        .into_iter()
        .filter(|(name, _)| !active_sessions.contains(name.as_str()))
        .collect()
}

fn report_kill_session_error(action: &str, name: &str, error: &io::Error) {
    if matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    ) {
        eprintln!(
            "Timed out while trying to {} session {:?}; the session socket did not respond \
             and its state was left intact.",
            action, name
        );
    } else {
        eprintln!("Failed to {} session {:?}: {}", action, name, error);
    }
}

/// On Windows the server answers on the connection's reply pipe, and waiting for that answer
/// keeps the request from being dropped before the server has read it. The reply read is bounded
/// by [`SESSION_PROBE_TIMEOUT`].
#[cfg(windows)]
pub fn send_kill_session(path: &std::path::Path) -> std::io::Result<()> {
    use interprocess::local_socket::traits::Stream as _;

    let (stream, reply_stream) = crate::pipe_pairs::connect_pair(path)?;
    // Named-pipe streams report that recv timeouts are unsupported. The call
    // still names the bound we want; a platform that ignores it keeps the
    // untimed reply read from the paired connection.
    let _ = reply_stream.set_recv_timeout(Some(SESSION_PROBE_TIMEOUT));
    let _ = IpcSenderWithContext::<ClientToServerMsg>::new(stream)
        .send_client_msg(ClientToServerMsg::KillSession);
    let mut receiver: IpcReceiverWithContext<ServerToClientMsg> =
        IpcReceiverWithContext::new(reply_stream);
    let _ = receiver.recv_server_msg();
    Ok(())
}

/// Connect and write `KillSession` under [`SESSION_PROBE_TIMEOUT`]. A timeout is not success.
#[cfg(unix)]
pub fn send_kill_session(path: &std::path::Path) -> std::io::Result<()> {
    use crate::ipc::{encode_client_frame, write_all_until_deadline};

    let deadline = Instant::now() + SESSION_PROBE_TIMEOUT;
    let mut stream = crate::consts::ipc_connect_with_timeout(path, SESSION_PROBE_TIMEOUT)?;
    let frame = encode_client_frame(ClientToServerMsg::KillSession)?;
    write_all_until_deadline(&mut stream, &frame, deadline)
}

pub fn kill_session(name: &str) {
    let path = match session_socket_path(name) {
        Ok(path) => path,
        Err(error) => {
            eprintln!(
                "Failed to read the session index for {:?}: {}. The session was not killed.",
                name, error
            );
            process::exit(1);
        },
    };
    if let Err(e) = send_kill_session(&path) {
        report_kill_session_error("kill", name, &e);
        process::exit(1);
    }
}

pub fn delete_session(name: &str, force: bool) {
    // The socket and the cache folder are named by the session id; the index row by the name.
    // A failed index read must not delete the folder named by the display name.
    let session_id = match session_folder_id(name) {
        Ok(session_id) => session_id,
        Err(error) => {
            eprintln!(
                "Failed to read the session index for {:?}: {}. The session was not deleted.",
                name, error
            );
            process::exit(1);
        },
    };
    if let Err(e) = validate_session_folder_name(&session_id) {
        eprintln!("{}", e);
        process::exit(1);
    }
    if force {
        let path = ZELLIJ_SOCK_DIR.join(&session_id);
        if let Err(e) = send_kill_session(&path) {
            if !matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) {
                report_kill_session_error("force-delete", name, &e);
                eprintln!(
                    "Continuing because --force explicitly permits deleting session {:?}.",
                    name
                );
            }
        }
    }
    // The index row goes only after the cache folder is gone.
    match std::fs::remove_dir_all(session_info_folder_for_session(&session_id)) {
        Ok(()) => {
            if let Ok(index) = crate::session_index::SessionIndex::open_default() {
                let _ = index.delete(name);
            }
            println!("Session: {:?} successfully deleted.", name);
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("Session: {:?} not found.", name);
            process::exit(2);
        },
        Err(e) => {
            eprintln!("Failed to delete session {:?}: {}", name, e);
            log::error!("Failed to delete session {:?}: {}", name, e);
            process::exit(1);
        },
    }
}

/// Unknown sockets that also have a saved layout must not be marked EXITED.
fn sessions_for_display(
    observation: &SessionObservation,
    resurrectable: Vec<(String, Duration)>,
) -> Vec<(String, Duration, bool)> {
    let mut all_sessions: HashMap<String, (Duration, bool)> = resurrectable
        .into_iter()
        .map(|(name, timestamp)| (name, (timestamp, true)))
        .collect();
    for (session_name, duration) in &observation.responsive {
        all_sessions.insert(session_name.clone(), (*duration, false));
    }
    for (session_name, _) in &observation.unknown {
        all_sessions.remove(session_name);
    }
    all_sessions
        .into_iter()
        .map(|(name, (timestamp, is_dead))| (name, timestamp, is_dead))
        .collect()
}

pub fn list_sessions(no_formatting: bool, short: bool, reverse: bool) {
    let exit_code = match observe_sessions() {
        Ok(observation) => {
            let all_sessions = sessions_for_display(&observation, get_resurrectable_sessions());
            if all_sessions.is_empty() && observation.unknown.is_empty() {
                eprintln!("No active zellij sessions found.");
                1
            } else if all_sessions.is_empty() {
                eprintln!(
                    "Found session socket(s) that did not respond in time; \
                     cannot claim there are no active sessions:"
                );
                for (name, _) in &observation.unknown {
                    eprintln!("  {} (unresponsive)", name);
                }
                1
            } else {
                print_sessions(all_sessions, no_formatting, short, reverse);
                if !observation.unknown.is_empty() {
                    eprintln!("Also found session socket(s) that did not respond in time:");
                    for (name, _) in &observation.unknown {
                        eprintln!("  {} (unresponsive)", name);
                    }
                }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionNameMatch {
    AmbiguousPrefix(Vec<String>),
    UniquePrefix(String),
    Exact(String),
    None,
}

/// `Responsive` is exact even when the listing omitted the name.
/// `Unknown` is [`io::ErrorKind::TimedOut`], never [`SessionNameMatch::None`].
/// An unknown prefix candidate stays in the ambiguous set, or times out when it is the only match.
pub fn decide_session_name_match(
    prefix: &str,
    responsive: &[(String, Duration)],
    unknown: &[(String, Duration)],
    direct: SessionProbeResult,
) -> Result<SessionNameMatch, io::ErrorKind> {
    match direct {
        SessionProbeResult::Responsive => {
            return Ok(SessionNameMatch::Exact(prefix.to_owned()));
        },
        SessionProbeResult::Unknown => return Err(io::ErrorKind::TimedOut),
        SessionProbeResult::Absent => {},
    }

    let mut matches = Vec::new();
    for (name, _) in responsive {
        if name == prefix {
            continue;
        }
        if name.starts_with(prefix) {
            matches.push(name.clone());
        }
    }
    let mut saw_unknown = false;
    for (name, _) in unknown {
        if name.starts_with(prefix) && !matches.iter().any(|existing| existing == name) {
            saw_unknown = true;
            matches.push(name.clone());
        }
    }
    if saw_unknown && matches.len() == 1 {
        return Err(io::ErrorKind::TimedOut);
    }
    if saw_unknown {
        return Ok(SessionNameMatch::AmbiguousPrefix(matches));
    }
    Ok(match matches.as_slice() {
        [] => SessionNameMatch::None,
        [one] => SessionNameMatch::UniquePrefix(one.clone()),
        _ => SessionNameMatch::AmbiguousPrefix(matches),
    })
}

pub fn match_session_name(prefix: &str) -> Result<SessionNameMatch, io::Error> {
    let observation = observe_sessions().map_err(io::Error::from)?;
    let direct = probe_exact_session(prefix)?;
    decide_session_name_match(
        prefix,
        &observation.responsive,
        &observation.unknown,
        direct,
    )
    .map_err(io::Error::from)
}

fn probe_exact_session(name: &str) -> Result<SessionProbeResult, io::Error> {
    // An index error must not probe the display-name path.
    let socket_path =
        session_socket_path(name).map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
    match path_try_exists(&socket_path) {
        Ok(false) => Ok(SessionProbeResult::Absent),
        Ok(true) => Ok(assert_socket_path(&socket_path, SESSION_PROBE_TIMEOUT)),
        Err(kind) => Err(io::Error::from(kind)),
    }
}

pub fn session_exists(name: &str) -> Result<bool, io::Error> {
    match probe_exact_session(name)? {
        SessionProbeResult::Responsive => Ok(true),
        SessionProbeResult::Absent => Ok(false),
        SessionProbeResult::Unknown => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "session socket did not respond before the probe deadline",
        )),
    }
}

// if the session is resurrecable, the returned layout is the one to be used to resurrect it
pub fn resurrection_layout(session_name_to_resurrect: &str) -> Result<Option<Layout>, String> {
    let session_id = session_folder_id(session_name_to_resurrect)?;
    let layout_file_name = session_layout_cache_file_name(&session_id);
    let raw_layout = match std::fs::read_to_string(&layout_file_name) {
        Ok(raw_layout) => raw_layout,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Failed to read resurrection layout file {}: {}.",
                layout_file_name.display(),
                error
            ));
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
                    .unwrap_or_default()
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
        Err(error) => {
            if error.kind() != io::ErrorKind::TimedOut && error.get_ref().is_some() {
                eprintln!(
                    "Failed to read the session index for {:?}: {}. The session was not deleted.",
                    name, error
                );
            } else {
                eprintln!("Error occurred: {}", error);
            }
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
    if kind == io::ErrorKind::TimedOut {
        return "A session socket exists but did not respond before the probe deadline.\n\
                Refusing to treat it as absent or to create a replacement session.\n\
                Re-try once the session responds, or inspect it with `zellij list-sessions`."
            .to_owned();
    }
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
    read_live_session_states_with_timeout(
        current_session_name,
        sock_dir,
        session_info_cache_dir,
        SESSION_PROBE_TIMEOUT,
    )
}

/// Responsive and unknown sockets are live. A refused connection is not.
/// Missing metadata is synthesized so an unresponsive socket is not dropped.
#[doc(hidden)]
pub fn read_live_session_states_with_timeout(
    current_session_name: &str,
    sock_dir: &Path,
    session_info_cache_dir: &Path,
    timeout: Duration,
) -> BTreeMap<String, SessionInfo> {
    let names_by_id = session_index::session_names_by_id();
    let candidates = fs::read_dir(sock_dir)
        .map(|files| {
            files
                .flatten()
                .filter_map(|file| {
                    let id = session_socket_id(&file)?;
                    let name = session_name(&names_by_id, id.clone());
                    let age = session_age(&file);
                    Some((id.clone(), name, (id, age)))
                })
                .collect()
        })
        .unwrap_or_default();
    let observed = observe_sessions_with(candidates, timeout, |id, remaining| {
        assert_socket_in(sock_dir, id, remaining)
    });

    let mut session_infos_on_machine = BTreeMap::new();
    for (name, (id, creation_time)) in observed.responsive.into_iter().chain(observed.unknown) {
        if session_infos_on_machine.contains_key(&name) {
            continue;
        }
        let session_cache_file_name = session_info_cache_dir
            .join(&id)
            .join("session-metadata.kdl");
        let mut session_info = fs::read_to_string(&session_cache_file_name)
            .ok()
            .and_then(|raw| SessionInfo::from_string(&raw, current_session_name).ok())
            .unwrap_or_else(|| SessionInfo::new(name.clone()));
        session_info.name = name.clone();
        session_info.is_current_session = name == current_session_name;
        session_info.creation_time = creation_time;
        session_infos_on_machine.insert(name, session_info);
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
    // A missing index is not an error. An unreadable index must not mint a name.
    if let Err(error) = session_index::session_id_for_name_strict("") {
        eprintln!(
            "Failed to read the session index: {error}. Refusing to create a replacement session."
        );
        return None;
    }
    match fs::read_dir(&*ZELLIJ_SESSION_INFO_CACHE_DIR) {
        Ok(_) => {},
        Err(error) if error.kind() == io::ErrorKind::NotFound => {},
        Err(error) => {
            eprintln!("{}", session_listing_error_message(error.kind()));
            return None;
        },
    }
    let observation = match observe_sessions() {
        Ok(observation) => observation,
        Err(kind) => {
            eprintln!("{}", session_listing_error_message(kind));
            return None;
        },
    };
    let dead_sessions = get_resurrectable_session_names();
    get_name_generator().take(1000).find(|name| {
        !observation
            .responsive
            .iter()
            .any(|(existing, _)| existing == name)
            && !observation
                .unknown
                .iter()
                .any(|(existing, _)| existing == name)
            && !dead_sessions.iter().any(|existing| existing == name)
    })
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

#[cfg(test)]
mod absent_is_not_unknown_tests {
    use super::*;
    use std::thread;

    #[test]
    fn candidates_never_reached_before_the_deadline_are_unknown_not_omitted_as_absent() {
        let mut candidates: Vec<(String, String, ())> = (0..MAX_CONCURRENT_SESSION_PROBES)
            .map(|index| {
                let name = format!("blocker-{index}");
                (name.clone(), name, ())
            })
            .collect();
        candidates.push(("late".to_owned(), "late".to_owned(), ()));

        let observed =
            observe_sessions_with(candidates, Duration::from_millis(80), |id, remaining| {
                if id == "late" {
                    SessionProbeResult::Responsive
                } else {
                    thread::sleep(remaining);
                    SessionProbeResult::Unknown
                }
            });

        assert!(
            observed.responsive.is_empty(),
            "late candidate should not have been probed as responsive: {:?}",
            observed.responsive
        );
        assert!(
            observed.unknown.iter().any(|(name, _)| name == "late"),
            "unprobed late candidate must stay unknown, got {:?}",
            observed.unknown
        );
        assert_eq!(observed.unknown.len(), MAX_CONCURRENT_SESSION_PROBES + 1);
    }

    #[test]
    fn discovery_error_is_preserved_by_observe_sessions_in() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("not-a-dir");
        fs::write(&file_path, b"x").unwrap();
        let err = observe_sessions_in(&file_path).unwrap_err();
        assert_ne!(err, io::ErrorKind::NotFound);
    }

    #[test]
    fn unknown_sessions_with_layouts_are_not_marked_exited() {
        let observation = SessionObservation {
            responsive: vec![("live".to_owned(), Duration::from_secs(1))],
            unknown: vec![("slow".to_owned(), Duration::from_secs(2))],
        };
        let displayed = sessions_for_display(
            &observation,
            vec![
                ("slow".to_owned(), Duration::from_secs(9)),
                ("dead".to_owned(), Duration::from_secs(8)),
            ],
        );
        let by_name: HashMap<_, _> = displayed
            .into_iter()
            .map(|(name, timestamp, is_dead)| (name, (timestamp, is_dead)))
            .collect();
        assert_eq!(by_name.get("live"), Some(&(Duration::from_secs(1), false)));
        assert_eq!(by_name.get("dead"), Some(&(Duration::from_secs(8), true)));
        assert!(!by_name.contains_key("slow"));
    }

    #[test]
    fn non_force_delete_treats_unresponsive_sessions_as_active() {
        let observation = SessionObservation {
            responsive: vec![("live".to_owned(), Duration::from_secs(1))],
            unknown: vec![("wedged".to_owned(), Duration::from_secs(2))],
        };
        let resurrectable = vec![
            ("live".to_owned(), Duration::from_secs(1)),
            ("wedged".to_owned(), Duration::from_secs(2)),
            ("dead".to_owned(), Duration::from_secs(3)),
        ];
        assert_eq!(
            resurrectable_sessions_to_delete(false, &observation, resurrectable),
            vec![("dead".to_owned(), Duration::from_secs(3))]
        );
    }

    #[test]
    fn force_delete_includes_unresponsive_sessions() {
        let observation = SessionObservation {
            responsive: vec![],
            unknown: vec![("wedged".to_owned(), Duration::from_secs(2))],
        };
        let resurrectable = vec![("wedged".to_owned(), Duration::from_secs(2))];
        assert_eq!(
            resurrectable_sessions_to_delete(true, &observation, resurrectable.clone()),
            resurrectable
        );
    }

    #[test]
    fn session_name_mtime_error_is_not_absence() {
        let error = session_mtime_result(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "metadata",
        )));
        assert!(error.is_err());
    }

    #[test]
    fn resurrectable_layout_presence_error_is_not_absence() {
        assert_eq!(
            layout_presence(Err(io::ErrorKind::PermissionDenied)),
            LayoutPresence::Unknown
        );
        assert!(layout_counts_as_resurrectable(layout_presence(Err(
            io::ErrorKind::PermissionDenied
        ))));
        assert!(!layout_counts_as_resurrectable(layout_presence(Ok(false))));
    }

    #[test]
    fn exact_name_responsive_probe_is_exact_when_the_listing_omits_it() {
        let exact = decide_session_name_match(
            "kept",
            &[("other".to_owned(), Duration::from_secs(1))],
            &[],
            SessionProbeResult::Responsive,
        )
        .unwrap();
        assert_eq!(exact, SessionNameMatch::Exact("kept".to_owned()));
    }

    #[test]
    fn exact_name_unknown_probe_is_not_reported_missing() {
        let timed_out = decide_session_name_match(
            "kept",
            &[("other".to_owned(), Duration::from_secs(1))],
            &[],
            SessionProbeResult::Unknown,
        );
        assert_eq!(timed_out.unwrap_err(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn unknown_prefix_candidate_is_not_reported_missing() {
        let timed_out = decide_session_name_match(
            "kept",
            &[],
            &[("kept".to_owned(), Duration::from_secs(2))],
            SessionProbeResult::Absent,
        );
        assert_eq!(timed_out.unwrap_err(), io::ErrorKind::TimedOut);

        let ambiguous = decide_session_name_match(
            "pre",
            &[("present".to_owned(), Duration::from_secs(1))],
            &[("prefix-unknown".to_owned(), Duration::from_secs(2))],
            SessionProbeResult::Absent,
        )
        .unwrap();
        match ambiguous {
            SessionNameMatch::AmbiguousPrefix(names) => {
                assert!(names.iter().any(|name| name == "present"));
                assert!(names.iter().any(|name| name == "prefix-unknown"));
            },
            other => panic!("unknown prefix candidate must stay visible, got {other:?}"),
        }
    }

    #[test]
    fn access_denied_is_not_a_dead_windows_process() {
        assert_eq!(
            classify_windows_open_process(true, WINDOWS_ERROR_ACCESS_DENIED),
            WindowsPidLiveness::Unknown
        );
        assert_eq!(
            classify_windows_open_process(true, WINDOWS_ERROR_INVALID_PARAMETER),
            WindowsPidLiveness::Dead
        );
        assert_eq!(
            probe_result_for_windows_liveness(WindowsPidLiveness::AliveUnverified),
            SessionProbeResult::Unknown
        );
        assert_eq!(
            probe_result_for_windows_liveness(WindowsPidLiveness::Dead),
            SessionProbeResult::Absent
        );
    }

    #[cfg(unix)]
    #[test]
    fn observational_probe_does_not_unlink_on_connection_refused() {
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale-session");
        {
            let _listener = UnixListener::bind(&path).unwrap();
        }
        assert!(path_try_exists(&path).unwrap());
        assert_eq!(
            assert_socket_path(&path, Duration::from_secs(1)),
            SessionProbeResult::Absent
        );
        assert!(
            path_try_exists(&path).unwrap(),
            "ordinary probing must not unlink a refused socket"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_stale_socket_is_not_listed_as_live_and_an_unresponsive_one_is() {
        use std::os::unix::net::UnixListener as StdUnixListener;

        let sock_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let stale = sock_dir.path().join("crashed-session");
        {
            let _listener = StdUnixListener::bind(&stale).unwrap();
        }
        let _wedged = StdUnixListener::bind(sock_dir.path().join("wedged")).unwrap();

        let states = read_live_session_states_with_timeout(
            "current",
            sock_dir.path(),
            cache_dir.path(),
            Duration::from_millis(50),
        );
        assert!(
            !states.contains_key("crashed-session"),
            "connection refused is absence, not a live session"
        );
        let info = states
            .get("wedged")
            .expect("unknown sessions must not be treated as absent");
        assert_eq!(info.name, "wedged");
        assert!(!info.is_current_session);
    }
}
