use crate::{
    consts::{
        is_ipc_socket, session_incarnation_path, session_layout_cache_file_name,
        session_publication_lock_path, ZELLIJ_SESSION_INFO_CACHE_DIR, ZELLIJ_SOCK_DIR,
    },
    data::SessionInfo,
    envs,
    input::layout::Layout,
    ipc::{ClientToServerMsg, IpcReceiverWithContext, IpcSenderWithContext, ServerToClientMsg},
    session_index::{self, SessionIndex},
};
use anyhow;
use humantime::format_duration;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
                    // The layout file is replaced atomically, so its own birth time is the
                    // latest replacement. The sidecar keeps the original creation time.
                    let ctime = session_layout_creation_time(Path::new(&layout_file_name)).ok();
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

/// Probe a session socket to check if a server is alive.
///
/// On Unix, connects and sends a `ConnStatus` message to verify the server responds.
/// On Windows, reads the server PID from the marker file and checks process liveness.
#[cfg(unix)]
fn assert_socket(name: &str) -> bool {
    use crate::consts::ipc_connect;
    let path = &*ZELLIJ_SOCK_DIR.join(name);
    match ipc_connect(path) {
        Ok(stream) => {
            // a wedged server accepts connections but never replies; without a
            // read timeout `zellij ls` would block forever on recv below
            {
                use interprocess::local_socket::prelude::*;
                let _ = stream.set_recv_timeout(Some(std::time::Duration::from_secs(5)));
            }
            let mut sender: IpcSenderWithContext<ClientToServerMsg> =
                IpcSenderWithContext::new(stream);
            let _ = sender.send_client_msg(ClientToServerMsg::ConnStatus);
            let mut receiver: IpcReceiverWithContext<ServerToClientMsg> = sender.get_receiver();
            match receiver.recv_server_msg() {
                Some((ServerToClientMsg::Connected, _)) => true,
                None | Some((_, _)) => false,
            }
        },
        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
            drop(fs::remove_file(path));
            false
        },
        Err(_) => false,
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
    let stream = crate::consts::ipc_connect(path)?;
    let _ = IpcSenderWithContext::<ClientToServerMsg>::new(stream)
        .send_client_msg(ClientToServerMsg::KillSession);
    Ok(())
}

/// Sidecar recording when a session's layout file was first written.
///
/// The layout file is replaced through a temporary file, so its own creation
/// timestamp is that of the latest replacement rather than of the session.
const SESSION_LAYOUT_CREATED_FILE_NAME: &str = "session-layout-created";

fn session_layout_created_marker(layout_file: &Path) -> PathBuf {
    layout_file.with_file_name(SESSION_LAYOUT_CREATED_FILE_NAME)
}

/// Remember when `layout_file` first came into existence, before it is
/// replaced. Does nothing once recorded. A layout file that already exists
/// contributes its own creation (or modification) time; otherwise now.
pub fn record_session_layout_creation_time(layout_file: &Path) {
    let marker = session_layout_created_marker(layout_file);
    if marker.exists() {
        return;
    }
    let created = fs::metadata(layout_file)
        .and_then(|metadata| metadata.created().or_else(|_| metadata.modified()))
        .unwrap_or_else(|_| SystemTime::now());
    let seconds = created
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|since_epoch| since_epoch.as_secs())
        .unwrap_or_default();
    if let Err(error) = fs::write(&marker, seconds.to_string()) {
        log::error!("Failed to record session creation time: {:?}", error);
    }
}

/// When the session owning `layout_file` was created: the recorded time if
/// any, else the layout file's creation (falling back to modification) time.
/// Fails with the metadata error when the layout file does not exist.
pub fn session_layout_creation_time(layout_file: &Path) -> io::Result<SystemTime> {
    let metadata = fs::metadata(layout_file)?;
    if let Some(recorded) = fs::read_to_string(session_layout_created_marker(layout_file))
        .ok()
        .and_then(|contents| contents.trim().parse::<u64>().ok())
    {
        if let Some(created) = SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(recorded)) {
            return Ok(created);
        }
    }
    metadata.created().or_else(|_| metadata.modified())
}

/// Why a publication attempt was refused.
///
/// Returned as the inner error of an [`io::Error`] with
/// [`io::ErrorKind::PermissionDenied`]. A fenced save is never `Ok(())`.
#[derive(Debug)]
pub struct SessionPublicationFenced {
    pub session_id: String,
    pub incarnation: u128,
}

impl std::fmt::Display for SessionPublicationFenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "session publication fenced for {} incarnation {}",
            self.session_id, self.incarnation
        )
    }
}

impl std::error::Error for SessionPublicationFenced {}

/// `true` when `error` is a fenced publication ([`SessionPublicationFenced`]).
pub fn is_session_publication_fenced(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.is::<SessionPublicationFenced>())
}

fn publication_fenced_error(session_id: &str, incarnation: u128) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        SessionPublicationFenced {
            session_id: session_id.to_owned(),
            incarnation,
        },
    )
}

/// Outcome of [`remove_fenced_session_cache`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionCacheRemoval {
    Removed,
    NotFound,
}

/// Exclusive cross-process lock for one session id.
///
/// The lock file is a sibling of the session directory, so `remove_dir_all`
/// of that directory does not drop the lock. Hold the guard from the
/// eligibility check through staging and persist, and across the tombstone
/// write plus directory removal.
pub struct SessionPublicationLock {
    #[cfg(unix)]
    _file: nix::fcntl::Flock<fs::File>,
    #[cfg(windows)]
    _file: fs::File,
    #[cfg(not(any(unix, windows)))]
    _file: (),
}

/// Lock file, incarnation file, deletion tombstone, and session folder under `cache_dir`.
fn cache_publication_paths(
    cache_dir: &Path,
    session_id: &str,
) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    (
        cache_dir.join(format!("{session_id}.lock")),
        cache_dir.join(format!("{session_id}.incarnation")),
        cache_dir.join(format!("{session_id}.deleted")),
        cache_dir.join(session_id),
    )
}

/// Block until this process holds the per-session publication lock.
pub fn lock_session_publication(session_id: &str) -> io::Result<SessionPublicationLock> {
    lock_session_publication_at(&session_publication_lock_path(session_id))
}

fn lock_session_publication_at(path: &Path) -> io::Result<SessionPublicationLock> {
    if let Some(parent) = path.parent() {
        // The cache directory, not the session folder. Creating the session
        // folder is what a fenced writer must not do.
        fs::create_dir_all(parent)?;
    }
    let mut options = fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    SessionPublicationLock::from_file(file)
}

impl SessionPublicationLock {
    #[cfg(unix)]
    fn from_file(file: fs::File) -> io::Result<Self> {
        use nix::fcntl::{Flock, FlockArg};
        let file = Flock::lock(file, FlockArg::LockExclusive)
            .map_err(|(_file, errno)| io::Error::from(errno))?;
        Ok(SessionPublicationLock { _file: file })
    }

    #[cfg(windows)]
    fn from_file(file: fs::File) -> io::Result<Self> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{LockFileEx, LOCKFILE_EXCLUSIVE_LOCK};
        use windows_sys::Win32::System::IO::OVERLAPPED;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let locked = unsafe {
            LockFileEx(
                file.as_raw_handle() as _,
                LOCKFILE_EXCLUSIVE_LOCK,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if locked == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(SessionPublicationLock { _file: file })
    }

    #[cfg(not(any(unix, windows)))]
    fn from_file(_file: fs::File) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "session publication lock is unavailable on this platform",
        ))
    }
}

/// Read the incarnation file once, when a writer is created.
///
/// Store the value and pass it to later publication attempts. Re-reading at
/// publish time and adopting a newer value would let a new owner reauthorize
/// a writer that was admitted under the deleted incarnation.
pub fn read_session_incarnation(session_id: &str) -> io::Result<u128> {
    match read_incarnation_file(&session_incarnation_path(session_id))? {
        Some(incarnation) => Ok(incarnation),
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no publication incarnation for session {session_id}"),
        )),
    }
}

/// Caller must hold [`lock_session_publication`] for `session_id` across this
/// check and the following persist.
///
/// `Ok(())` only when `incarnation` equals the incarnation file and the
/// tombstone does not name it. Otherwise a [`SessionPublicationFenced`] error.
pub fn ensure_session_publication_allowed(session_id: &str, incarnation: u128) -> io::Result<()> {
    ensure_session_publication_allowed_in(&*ZELLIJ_SESSION_INFO_CACHE_DIR, session_id, incarnation)
}

pub(crate) fn ensure_session_publication_allowed_in(
    cache_dir: &Path,
    session_id: &str,
    incarnation: u128,
) -> io::Result<()> {
    let (_, incarnation_path, tombstone_path, _) = cache_publication_paths(cache_dir, session_id);
    let live = read_incarnation_file(&incarnation_path)?;
    let blocked = tombstone_blocks_path(&tombstone_path, incarnation)?;
    if live != Some(incarnation) || blocked {
        Err(publication_fenced_error(session_id, incarnation))
    } else {
        Ok(())
    }
}

/// Fence `session_id` and remove its cache folder while holding the
/// per-session publication lock. On fence failure the folder stays.
pub fn remove_fenced_session_cache(session_id: &str) -> io::Result<SessionCacheRemoval> {
    remove_fenced_session_cache_in(&*ZELLIJ_SESSION_INFO_CACHE_DIR, session_id)
}

pub(crate) fn remove_fenced_session_cache_in(
    cache_dir: &Path,
    session_id: &str,
) -> io::Result<SessionCacheRemoval> {
    let (lock_path, incarnation_path, tombstone_path, folder) =
        cache_publication_paths(cache_dir, session_id);
    let _lock = lock_session_publication_at(&lock_path)?;
    fence_and_remove_locked(&incarnation_path, &tombstone_path, &folder)
}

/// Incarnation observed when a session was classified dead.
///
/// `Absent` is a missing incarnation file, not "any incarnation". A file that
/// appears later is a different lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectedSessionLifetime {
    Absent,
    Present(u128),
}

pub(crate) fn selected_session_lifetime(
    cache_dir: &Path,
    session_id: &str,
) -> io::Result<SelectedSessionLifetime> {
    let (_, incarnation_path, _, _) = cache_publication_paths(cache_dir, session_id);
    match read_incarnation_file(&incarnation_path)? {
        Some(incarnation) => Ok(SelectedSessionLifetime::Present(incarnation)),
        None => Ok(SelectedSessionLifetime::Absent),
    }
}

fn selected_lifetime_is_current(selected: SelectedSessionLifetime, current: Option<u128>) -> bool {
    match (selected, current) {
        (SelectedSessionLifetime::Absent, None) => true,
        (SelectedSessionLifetime::Present(selected), Some(current)) => selected == current,
        _ => false,
    }
}

#[derive(Debug)]
struct SelectedLifetimeMismatch {
    session_id: String,
}

impl std::fmt::Display for SelectedLifetimeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "session {} changed incarnation after it was selected for deletion",
            self.session_id
        )
    }
}

impl std::error::Error for SelectedLifetimeMismatch {}

pub(crate) fn is_selected_lifetime_mismatch(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.is::<SelectedLifetimeMismatch>())
}

fn selected_lifetime_mismatch(session_id: &str) -> io::Error {
    io::Error::other(SelectedLifetimeMismatch {
        session_id: session_id.to_owned(),
    })
}

/// Fence and remove only if the incarnation under the publication lock is still
/// `selected`. A newer incarnation is not tombstoned and its folder stays.
pub(crate) fn remove_fenced_session_cache_for_lifetime(
    cache_dir: &Path,
    session_id: &str,
    selected: SelectedSessionLifetime,
) -> io::Result<SessionCacheRemoval> {
    let (lock_path, incarnation_path, tombstone_path, folder) =
        cache_publication_paths(cache_dir, session_id);
    let _lock = lock_session_publication_at(&lock_path)?;
    let current = read_incarnation_file(&incarnation_path)?;
    if !selected_lifetime_is_current(selected, current) {
        return Err(selected_lifetime_mismatch(session_id));
    }
    fence_and_remove_locked(&incarnation_path, &tombstone_path, &folder)
}

fn fence_and_remove_locked(
    incarnation_path: &Path,
    tombstone_path: &Path,
    folder: &Path,
) -> io::Result<SessionCacheRemoval> {
    write_deletion_tombstone_at(incarnation_path, tombstone_path)?;
    match fs::remove_dir_all(folder) {
        Ok(()) => Ok(SessionCacheRemoval::Removed),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(SessionCacheRemoval::NotFound),
        Err(error) => Err(error),
    }
}

fn session_is_not_definitely_dead(session_id: &str) -> io::Error {
    io::Error::other(format!(
        "session {session_id} is not definitely dead; its cache and index row were left in place"
    ))
}

/// Exclusive ownership of the session socket lock used by [`crate`]'s server
/// bind (`.{session_id}.lock` beside the socket). Holding it stops a
/// resurrection from binding and admitting a new incarnation.
///
/// On Windows the pipe name itself is exclusive, so this claim is a no-op and
/// the publication lock plus a fresh socket check is the fence.
pub(crate) struct HeldSessionSocketClaim {
    #[cfg(unix)]
    _lock: nix::fcntl::Flock<fs::File>,
    #[cfg(unix)]
    lock_path: PathBuf,
}

impl HeldSessionSocketClaim {
    pub(crate) fn release(self) {
        #[cfg(unix)]
        {
            let _ = fs::remove_file(&self.lock_path);
        }
    }
}

fn session_socket_lock_path(sock_dir: &Path, session_id: &str) -> io::Result<PathBuf> {
    validate_session_folder_name(session_id)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    Ok(sock_dir.join(format!(".{session_id}.lock")))
}

/// `Ok(None)` means another owner already holds the claim. That session is not
/// dead: server bind takes this claim before it can listen or admit a new
/// incarnation. `AddrInUse` must not be treated as death.
pub(crate) fn try_acquire_session_socket_claim(
    sock_dir: &Path,
    session_id: &str,
) -> io::Result<Option<HeldSessionSocketClaim>> {
    #[cfg(unix)]
    {
        use nix::fcntl::{Flock, FlockArg};
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let lock_path = session_socket_lock_path(sock_dir, session_id)?;
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .mode(0o600)
            .open(&lock_path)?;
        let lock = match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => lock,
            Err((_, errno)) if errno == nix::errno::Errno::EWOULDBLOCK => return Ok(None),
            Err((_, errno)) => return Err(io::Error::from(errno)),
        };
        let held = lock.metadata()?;
        match fs::metadata(&lock_path) {
            Ok(current) if current.dev() == held.dev() && current.ino() == held.ino() => {
                Ok(Some(HeldSessionSocketClaim {
                    _lock: lock,
                    lock_path,
                }))
            },
            _ => Ok(None),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (sock_dir, session_id);
        Ok(Some(HeldSessionSocketClaim {}))
    }
}

/// Remove `session_id`'s cache only when its socket claim is free and a socket
/// inventory still says it is dead.
///
/// The claim is held across the death check, the incarnation read, and the
/// fenced delete. A resurrection has to take that claim before it can bind, so
/// it cannot admit a new incarnation into the lifetime this function reads.
pub fn remove_fenced_dead_session_cache(
    sock_dir: &Path,
    cache_dir: &Path,
    session_id: &str,
) -> io::Result<SessionCacheRemoval> {
    let Some(claim) = try_acquire_session_socket_claim(sock_dir, session_id)? else {
        return Err(session_is_not_definitely_dead(session_id));
    };
    let removal = (|| {
        if !crate::session_index::session_id_is_definitely_dead(sock_dir, cache_dir, session_id) {
            return Err(session_is_not_definitely_dead(session_id));
        }
        let selected = selected_session_lifetime(cache_dir, session_id)?;
        remove_fenced_session_cache_for_lifetime(cache_dir, session_id, selected)
    })();
    claim.release();
    removal
}

/// Admit a new incarnation and drop any deletion tombstone.
///
/// Called before writers are started. A writer that already captured the
/// previous incarnation stays refused after this returns.
pub fn clear_session_deletion_fence(session_id: &str) {
    if session_id.is_empty() {
        return;
    }
    if let Err(error) = admit_session_incarnation(session_id) {
        log::error!(
            "Failed to clear session deletion fence for {}: {}",
            session_id,
            error
        );
    }
}

fn admit_session_incarnation(session_id: &str) -> io::Result<()> {
    admit_session_incarnation_in(&*ZELLIJ_SESSION_INFO_CACHE_DIR, session_id).map(|_| ())
}

pub(crate) fn admit_session_incarnation_in(cache_dir: &Path, session_id: &str) -> io::Result<u128> {
    let (lock_path, incarnation_path, tombstone_path, _) =
        cache_publication_paths(cache_dir, session_id);
    let _lock = lock_session_publication_at(&lock_path)?;
    let incarnation = mint_session_incarnation();
    write_sibling_atomically(&incarnation_path, format!("{incarnation}\n").as_bytes())?;
    match fs::remove_file(tombstone_path) {
        Ok(()) => Ok(incarnation),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(incarnation),
        Err(error) => Err(error),
    }
}

fn mint_session_incarnation() -> u128 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tick = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let pid = std::process::id() as u128;
    let low = (pid << 32) | (tick & 0xffff_ffff);
    let value = ((nanos as u64) as u128) << 64 | low;
    if value == 0 {
        1
    } else {
        value
    }
}

fn read_incarnation_file(path: &Path) -> io::Result<Option<u128>> {
    match fs::read_to_string(path) {
        Ok(text) => {
            let text = text.trim();
            if text.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty session incarnation file",
                ));
            }
            text.parse::<u128>().map(Some).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid session incarnation: {error}"),
                )
            })
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn tombstone_blocks_path(path: &Path, incarnation: u128) -> io::Result<bool> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    // A tombstone that does not name an incarnation fences every writer. A
    // named one fences only that incarnation.
    match named_tombstone_incarnation(&text) {
        Some(named) => Ok(named == incarnation),
        None => Ok(true),
    }
}

fn named_tombstone_incarnation(text: &str) -> Option<u128> {
    for line in text.lines() {
        if let Some(value) = line.trim().strip_prefix("incarnation=") {
            return value.trim().parse().ok();
        }
    }
    None
}

fn write_deletion_tombstone_at(incarnation_path: &Path, tombstone_path: &Path) -> io::Result<()> {
    let incarnation = read_incarnation_file(incarnation_path)?.unwrap_or(0);
    let contents = format!("deleted\nincarnation={incarnation}\n");
    write_sibling_atomically(tombstone_path, contents.as_bytes())
}

/// Atomic replace of a cache-directory sibling. Does not create the session folder.
fn write_sibling_atomically(path: &Path, contents: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "session publication path has no parent directory",
            )
        })?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".zellij-session-meta-")
        .tempfile_in(parent)?;
    temporary.write_all(contents)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

/// Read a saved layout together with every pane-content file it references.
///
/// Content files are content-addressed and may be reclaimed after later saves.
/// On a mismatch between referenced and loaded files, restart from the layout
/// currently on disk rather than returning a silently incomplete resurrection.
///
/// `after_layout_read` runs after the layout text is read and before its
/// content files are loaded.
#[doc(hidden)]
pub fn resurrection_layout_from_path_with_reader_barrier<F>(
    layout_file_name: &Path,
    mut after_layout_read: F,
) -> Result<Option<Layout>, String>
where
    F: FnMut(),
{
    let read_layout = || {
        let raw_layout = fs::read_to_string(layout_file_name).map_err(|e| {
            crate::input::config::ConfigError::IoPath(e, layout_file_name.to_path_buf())
        })?;
        after_layout_read();
        Ok((
            Some(layout_file_name.display().to_string()),
            raw_layout,
            None,
        ))
    };
    match crate::input::layout::read_layout_with_verified_contents(read_layout) {
        Ok((layout, _raw_layout)) => Ok(Some(layout)),
        Err(crate::input::config::ConfigError::IoPath(error, _))
            if error.kind() == io::ErrorKind::NotFound =>
        {
            Ok(None)
        },
        Err(error) => {
            let error_text = crate::input::keybind_presets::config_error_text(&error);
            log::error!(
                "Failed to read resurrection layout file {}: {}",
                layout_file_name.display(),
                error_text
            );
            Err(format!(
                "Failed to read resurrection layout file {}: {}.",
                layout_file_name.display(),
                error_text
            ))
        },
    }
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
    // Fence and remove under one per-session lock. The index row is removed
    // only after that removal succeeds, so a failed fence cannot drop the row
    // of a session whose cache is still there.
    match remove_fenced_session_cache(&session_id) {
        Ok(SessionCacheRemoval::Removed) => {
            if let Ok(index) = crate::session_index::SessionIndex::open_default() {
                let _ = index.delete(name);
            }
            println!("Session: {:?} successfully deleted.", name);
        },
        Ok(SessionCacheRemoval::NotFound) => {
            eprintln!("Session: {:?} not found.", name);
            process::exit(2);
        },
        Err(error) => {
            eprintln!("Failed to delete session {:?}: {}", name, error);
            log::error!("Failed to delete session {:?}: {}", name, error);
            process::exit(1);
        },
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
    resurrection_layout_from_path_with_reader_barrier(&layout_file_name, || {})
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

#[cfg(test)]
mod publication_generation_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn recorded_layout_age_survives_replacing_the_layout_file() {
        let dir = tempdir().unwrap();
        let layout = dir.path().join("session-layout.kdl");
        fs::write(&layout, "layout { }\n").unwrap();
        record_session_layout_creation_time(&layout);
        let first = session_layout_creation_time(&layout).unwrap();
        let replacement = dir.path().join("replacement.kdl");
        fs::write(&replacement, "layout { pane }\n").unwrap();
        fs::rename(&replacement, &layout).unwrap();
        assert_eq!(session_layout_creation_time(&layout).unwrap(), first);
    }

    #[test]
    fn a_deleted_incarnation_stays_unpublished_after_a_new_one_is_admitted() {
        let cache = tempdir().unwrap();
        let session_id = "incarnation-fence";
        let folder = cache.path().join(session_id);
        fs::create_dir_all(&folder).unwrap();
        fs::write(folder.join("session-layout.kdl"), "layout { }\n").unwrap();
        let incarnation = admit_session_incarnation_in(cache.path(), session_id).unwrap();
        ensure_session_publication_allowed_in(cache.path(), session_id, incarnation).unwrap();

        let selected = selected_session_lifetime(cache.path(), session_id).unwrap();
        assert_eq!(
            remove_fenced_session_cache_for_lifetime(cache.path(), session_id, selected).unwrap(),
            SessionCacheRemoval::Removed
        );
        assert!(!folder.exists());
        let fenced = ensure_session_publication_allowed_in(cache.path(), session_id, incarnation)
            .unwrap_err();
        assert!(is_session_publication_fenced(&fenced));

        let admitted = admit_session_incarnation_in(cache.path(), session_id).unwrap();
        assert_ne!(admitted, incarnation);
        ensure_session_publication_allowed_in(cache.path(), session_id, admitted).unwrap();
        let still_fenced =
            ensure_session_publication_allowed_in(cache.path(), session_id, incarnation)
                .unwrap_err();
        assert!(is_session_publication_fenced(&still_fenced));
    }

    #[test]
    fn a_stale_deletion_does_not_tombstone_a_newer_incarnation() {
        let cache = tempdir().unwrap();
        let session_id = "resurrected";
        let folder = cache.path().join(session_id);
        fs::create_dir_all(&folder).unwrap();
        fs::write(folder.join("session-layout.kdl"), "layout { }\n").unwrap();
        admit_session_incarnation_in(cache.path(), session_id).unwrap();
        let selected = selected_session_lifetime(cache.path(), session_id).unwrap();
        let admitted = admit_session_incarnation_in(cache.path(), session_id).unwrap();
        let error = remove_fenced_session_cache_for_lifetime(cache.path(), session_id, selected)
            .unwrap_err();
        assert!(is_selected_lifetime_mismatch(&error));
        assert!(folder.join("session-layout.kdl").exists());
        ensure_session_publication_allowed_in(cache.path(), session_id, admitted).unwrap();
        assert!(!cache.path().join(format!("{session_id}.deleted")).exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_held_socket_claim_keeps_a_dead_cache_until_the_claim_is_released() {
        let root = tempdir().unwrap();
        let sock_dir = root.path().join("sock");
        let cache_dir = root.path().join("cache");
        fs::create_dir_all(&sock_dir).unwrap();
        let session_id = "dead-claimed";
        fs::create_dir_all(cache_dir.join(session_id)).unwrap();
        let claim = try_acquire_session_socket_claim(&sock_dir, session_id)
            .unwrap()
            .expect("the test should take the socket claim");
        assert_eq!(
            crate::session_index::delete_dead_sessions_in(&sock_dir, &cache_dir, None).unwrap(),
            0
        );
        assert!(cache_dir.join(session_id).exists());
        claim.release();
        assert_eq!(
            crate::session_index::delete_dead_sessions_in(&sock_dir, &cache_dir, None).unwrap(),
            1
        );
        assert!(!cache_dir.join(session_id).exists());
    }
}
