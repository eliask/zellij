//! IPC stuff for starting to split things into a client and server model.
use crate::{
    data::{ClientId, ConnectToSession, HostTerminalThemeMode, KeyWithModifier, PaneId, Style},
    errors::{prelude::*, ErrorContext},
    input::{actions::Action, cli_assets::CliAssets, terminal::TerminalSettings},
    pane_size::{Size, SizeInPixels},
};
use interprocess::local_socket::Stream as LocalSocketStream;
use log::warn;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Error, Formatter},
    io::{self, Read, Write},
    marker::PhantomData,
    time::{Duration, Instant},
};

// Protobuf imports
use crate::client_server_contract::client_server_contract::{
    ClientToServerMsg as ProtoClientToServerMsg, ServerToClientMsg as ProtoServerToClientMsg,
};
use prost::Message;

mod attention_conversion;
mod enum_conversions;
mod protobuf_conversion;

#[cfg(test)]
mod tests;

type SessionId = u64;

/// A bidirectional byte stream that supports cloning for simultaneous read/write.
pub trait IpcStream: Read + Write + Send + 'static {
    fn try_clone_stream(&self) -> io::Result<Box<dyn IpcStream>>;
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
}

impl IpcStream for LocalSocketStream {
    fn try_clone_stream(&self) -> io::Result<Box<dyn IpcStream>> {
        use interprocess::TryClone;
        Ok(Box::new(self.try_clone()?))
    }
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        use interprocess::local_socket::traits::Stream;
        self.set_recv_timeout(timeout)
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Session {
    // Unique ID for this session
    id: SessionId,
    // Identifier for the underlying IPC primitive (socket, pipe)
    conn_name: String,
    // User configured alias for the session
    alias: String,
}

// How do we want to connect to a session?
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientType {
    Reader,
    Writer,
}

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct ClientAttributes {
    pub size: Size,
    pub style: Style,
}

#[derive(Default, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct PixelDimensions {
    pub text_area_size: Option<SizeInPixels>,
    pub character_cell_size: Option<SizeInPixels>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct PaneReference {
    pub pane_id: u32,
    pub is_plugin: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct ColorRegister {
    pub index: usize,
    pub color: String,
}

impl PixelDimensions {
    pub fn merge(&mut self, other: PixelDimensions) {
        if let Some(text_area_size) = other.text_area_size {
            self.text_area_size = Some(text_area_size);
        }
        if let Some(character_cell_size) = other.character_cell_size {
            self.character_cell_size = Some(character_cell_size);
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MobileSizePayload {
    pub cols: usize,
    pub rows: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MobileActivePanePayload {
    pub pane_id: u32,
    pub is_plugin: bool,
    pub tab_position: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MobileTabPayload {
    pub position: usize,
    pub name: String,
    pub active: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MobilePanePayload {
    pub tab_position: usize,
    pub pane_id: u32,
    pub is_plugin: bool,
    pub title: String,
    pub is_floating: bool,
    pub last_activity_secs_ago: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MobileSessionPayload {
    pub name: String,
    pub web_clients_allowed: bool,
    pub tab_count: usize,
    pub pane_count: usize,
    pub connected_clients: usize,
    pub creation_secs_ago: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct MobileRenderPrefsPayload {
    pub single_pane: bool,
    pub fit: bool,
    pub active_pane_is_fullscreen: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct MobileStatePayload {
    pub session_name: String,
    pub now_secs: u64,
    pub is_welcome_screen: bool,
    pub desktop_client_connected: bool,
    pub desktop_size: Option<MobileSizePayload>,
    pub active_pane: Option<MobileActivePanePayload>,
    pub tabs: Vec<MobileTabPayload>,
    pub panes: Vec<MobilePanePayload>,
    pub sessions: Vec<MobileSessionPayload>,
    pub render_prefs: MobileRenderPrefsPayload,
}

// Types of messages sent from the client to the server
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ClientToServerMsg {
    DetachSession {
        client_ids: Vec<ClientId>,
    },
    TerminalPixelDimensions {
        pixel_dimensions: PixelDimensions,
    },
    BackgroundColor {
        color: String,
    },
    ForegroundColor {
        color: String,
    },
    ColorRegisters {
        color_registers: Vec<ColorRegister>,
    },
    TerminalResize {
        new_size: Size,
    },
    FirstClientConnected {
        cli_assets: CliAssets,
        is_web_client: bool,
    },
    AttachClient {
        cli_assets: CliAssets,
        tab_position_to_focus: Option<usize>,
        pane_to_focus: Option<PaneReference>,
        is_web_client: bool,
    },
    AttachWatcherClient {
        terminal_size: Size,
        is_web_client: bool,
    },
    Action {
        action: Action,
        terminal_id: Option<u32>,
        client_id: Option<ClientId>,
        is_cli_client: bool,
    },
    Key {
        key: KeyWithModifier,
        raw_bytes: Vec<u8>,
        is_kitty_keyboard_protocol: bool,
    },
    ClientExited,
    KillSession,
    ConnStatus,
    WebServerStarted {
        base_url: String,
    },
    FailedToStartWebServer {
        error: String,
    },
    SubscribeToPaneRenders {
        pane_ids: Vec<PaneId>,
        scrollback: Option<usize>,
        ansi: bool,
    },
    DesktopNotificationResponse {
        raw_bytes: Vec<u8>,
    },
    ForwardedReplyFromHost {
        token: u32,
        reply_bytes: Vec<u8>,
    },
    HostTerminalThemeChanged {
        mode: HostTerminalThemeMode,
    },
    SoftKeyboardVisibilityChanged {
        visible: bool,
    },
    NestedSessionFrameFromHost {
        payload_bytes: Vec<u8>,
    },
    KittyGraphicsSupport {
        supported: bool,
        local_media: bool,
    },
    KittyZlibSupport {
        supported: bool,
    },
    SixelSupport {
        supported: bool,
    },
    RequestSessionList,
    RequestSessionPreview {
        tab_index: Option<u32>,
        pane_id: Option<(u32, bool)>,
        anchor: Option<crate::data::PreviewAnchor>,
    },
    RequestSessionAttention {
        include_command_text: bool,
    },
    PeerAttentionChanged {
        session_name: String,
        attention: Option<crate::attention::SessionAttention>,
    },
    SetMobileRenderPreferences {
        single_pane: bool,
        fit: bool,
    },
    HostTerminalFocusChanged {
        focused: bool,
    },
    StructuredRenderSupport {
        supported: bool,
        row_shifts: bool,
    },
    ClipboardPasteSupport {
        supported: bool,
        shortcut: Option<String>,
    },
    RenderFrameAck {
        seq: u64,
    },
    FontFamilies {
        families: Vec<String>,
    },
    PaneMirror {
        payload: Vec<u8>,
    },
    AdoptSessionHistory {
        history: crate::session_history::SessionHistory,
    },
}

// Types of messages sent from the server to the client
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ServerToClientMsg {
    Render {
        content: String,
    },
    RenderFrame {
        frame: Vec<u8>,
    },
    UnblockInputThread,
    Exit {
        exit_reason: ExitReason,
    },
    Connected,
    Log {
        lines: Vec<String>,
    },
    LogError {
        lines: Vec<String>,
    },
    SwitchSession {
        connect_to_session: ConnectToSession,
    },
    UnblockCliPipeInput {
        pipe_name: String,
        exit_code: Option<i32>,
    },
    CliPipeOutput {
        pipe_name: String,
        output: String,
    },
    QueryTerminalSize,
    SetSoftKeyboard {
        on: bool,
    },
    PasteFromClipboard {
        pane_id: Option<PaneId>,
    },
    StartWebServer,
    RenamedSession {
        name: String,
    },
    ConfigFileUpdated,
    TerminalSettingsChanged {
        settings: Box<TerminalSettings>,
    },
    HostTerminalThemeChanged {
        mode: HostTerminalThemeMode,
    },
    PaneRenderUpdate {
        pane_id: PaneId,
        viewport: Vec<String>,
        scrollback: Option<Vec<String>>,
        is_initial: bool,
    },
    SubscribedPaneClosed {
        pane_id: PaneId,
    },
    ForwardQueryToHost {
        token: u32,
        query_bytes: Vec<u8>,
        resolve_async: bool,
    },
    EmitNestedSessionFrame {
        payload_bytes: Vec<u8>,
    },
    MobileState {
        payload: MobileStatePayload,
    },
    HostProgramStatus {
        status: Option<crate::attention::HostProgramStatus>,
    },
    SessionAttention {
        attention: crate::attention::SessionAttention,
    },
    SessionPreview {
        preview: Option<crate::data::SessionPreview>,
    },
    PaneMirror {
        payload: Vec<u8>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ExitReason {
    Normal,
    NormalDetached,
    ForceDetached,
    CannotAttach,
    Disconnect,
    WebClientsForbidden,
    KickedByHost,
    CustomExitStatus(i32),
    Error(String),
}

impl Display for ExitReason {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            Self::Normal => write!(f, "Bye from Zellij!"),
            Self::NormalDetached => write!(f, "Session detached"),
            Self::ForceDetached => write!(
                f,
                "Session was detached from this client (possibly because another client connected)"
            ),
            Self::CannotAttach => write!(
                f,
                "Session attached to another client. Use --force flag to force connect."
            ),
            Self::WebClientsForbidden => write!(
                f,
                "Web clients are not allowed in this session - cannot attach"
            ),
            Self::Disconnect => {
                let session_tip = match crate::envs::get_session_name() {
                    Ok(name) => format!("`zellij attach {}`", name),
                    Err(_) => "see `zellij ls` and `zellij attach`".to_string(),
                };
                write!(
                    f,
                    "
Your zellij client lost connection to the zellij server.

As a safety measure, you have been disconnected from the current zellij session.
However, the session should still exist and none of your data should be lost.

This usually means that your terminal didn't process server messages quick
enough. Maybe your system is currently under high load, or your terminal
isn't performant enough.

There are a few things you can try now:
    - Reattach to your previous session and see if it works out better this
      time: {session_tip}
    - Try using a faster (maybe GPU-accelerated) terminal emulator
    "
                )
            },
            Self::KickedByHost => write!(f, "Disconnected by host"),
            Self::CustomExitStatus(exit_status) => write!(f, "Exit {}", exit_status),
            Self::Error(e) => write!(f, "Error occurred in server:\n{}", e),
        }
    }
}

/// Sends messages on a stream socket, along with an [`ErrorContext`].
pub struct IpcSenderWithContext<T: Serialize> {
    sender: io::BufWriter<Box<dyn IpcStream>>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize> IpcSenderWithContext<T> {
    /// Returns a sender to the given [LocalSocketStream](interprocess::local_socket::LocalSocketStream).
    pub fn new(sender: LocalSocketStream) -> Self {
        Self {
            sender: io::BufWriter::new(Box::new(sender)),
            _phantom: PhantomData,
        }
    }

    fn from_boxed(sender: Box<dyn IpcStream>) -> Self {
        Self {
            sender: io::BufWriter::new(sender),
            _phantom: PhantomData,
        }
    }

    pub fn send_client_msg(&mut self, msg: ClientToServerMsg) -> Result<()> {
        let proto_msg: ProtoClientToServerMsg = msg.into();
        write_protobuf_message(&mut self.sender, &proto_msg)?;
        let _ = self.sender.flush();
        Ok(())
    }

    pub fn send_server_msg(&mut self, msg: ServerToClientMsg) -> Result<()> {
        let proto_msg: ProtoServerToClientMsg = msg.into();
        write_protobuf_message(&mut self.sender, &proto_msg)?;
        let _ = self.sender.flush();
        Ok(())
    }

    /// Returns an [`IpcReceiverWithContext`] with the same socket as this sender.
    pub fn get_receiver<F>(&self) -> IpcReceiverWithContext<F>
    where
        F: for<'de> Deserialize<'de> + Serialize,
    {
        let socket = self.sender.get_ref().try_clone_stream().unwrap();
        IpcReceiverWithContext::from_boxed(socket)
    }
}

/// Receives messages on a stream socket, along with an [`ErrorContext`].
pub struct IpcReceiverWithContext<T> {
    receiver: io::BufReader<Box<dyn IpcStream>>,
    _phantom: PhantomData<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpcReceiveError {
    Disconnected,
    Undecodable,
}

impl Display for IpcReceiveError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), Error> {
        match self {
            IpcReceiveError::Disconnected => write!(f, "the peer closed the connection"),
            IpcReceiveError::Undecodable => write!(f, "received a message that could not be read"),
        }
    }
}

impl<T> IpcReceiverWithContext<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    /// Returns a receiver to the given [LocalSocketStream](interprocess::local_socket::LocalSocketStream).
    pub fn new(receiver: LocalSocketStream) -> Self {
        Self {
            receiver: io::BufReader::new(Box::new(receiver)),
            _phantom: PhantomData,
        }
    }

    fn from_boxed(receiver: Box<dyn IpcStream>) -> Self {
        Self {
            receiver: io::BufReader::new(receiver),
            _phantom: PhantomData,
        }
    }

    pub fn recv_client_msg(&mut self) -> Option<(ClientToServerMsg, ErrorContext)> {
        self.try_recv_client_msg().ok()
    }

    pub fn recv_server_msg(&mut self) -> Option<(ServerToClientMsg, ErrorContext)> {
        self.try_recv_server_msg().ok()
    }

    pub fn try_recv_client_msg(
        &mut self,
    ) -> std::result::Result<(ClientToServerMsg, ErrorContext), IpcReceiveError> {
        let proto_msg = read_protobuf_message::<ProtoClientToServerMsg>(&mut self.receiver)?;
        match proto_msg.try_into() {
            Ok(rust_msg) => Ok((rust_msg, ErrorContext::default())),
            Err(e) => {
                warn!("Error converting protobuf to ClientToServerMsg: {:?}", e);
                Err(IpcReceiveError::Undecodable)
            },
        }
    }

    pub fn try_recv_server_msg(
        &mut self,
    ) -> std::result::Result<(ServerToClientMsg, ErrorContext), IpcReceiveError> {
        let proto_msg = read_protobuf_message::<ProtoServerToClientMsg>(&mut self.receiver)?;
        match proto_msg.try_into() {
            Ok(rust_msg) => Ok((rust_msg, ErrorContext::default())),
            Err(e) => {
                warn!("Error converting protobuf to ServerToClientMsg: {:?}", e);
                Err(IpcReceiveError::Undecodable)
            },
        }
    }

    /// Returns an [`IpcSenderWithContext`] with the same socket as this receiver.
    pub fn get_sender<F: Serialize>(&self) -> IpcSenderWithContext<F> {
        let socket = self.receiver.get_ref().try_clone_stream().unwrap();
        IpcSenderWithContext::from_boxed(socket)
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.receiver.get_ref().set_read_timeout(timeout)
    }
}

// Protobuf wire format utilities
fn read_protobuf_message<T: Message + Default>(
    reader: &mut impl Read,
) -> std::result::Result<T, IpcReceiveError> {
    // Read length-prefixed protobuf message
    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .map_err(|_| IpcReceiveError::Disconnected)?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .map_err(|_| IpcReceiveError::Disconnected)?;

    T::decode(&buf[..]).map_err(|_| IpcReceiveError::Undecodable)
}

fn write_protobuf_message<T: Message>(writer: &mut impl Write, msg: &T) -> Result<()> {
    let encoded = msg.encode_to_vec();
    let len = encoded.len() as u32;

    // we measure the length of the message and transmit it first so that the reader will be able
    // to first read exactly 4 bytes (representing this length) and then read that amount of bytes
    // as the actual message - this is so that we are able to distinct whole messages over the wire
    // stream
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&encoded)?;
    Ok(())
}

/// Cap for one session-liveness probe reply. A silent or hostile peer must not
/// make the probe allocate an unbounded frame. This does not change the
/// ordinary client/server frame limit.
pub const PROBE_MAX_FRAME_BYTES: usize = 64 * 1024;

#[cfg(unix)]
fn remaining_until(deadline: Instant) -> io::Result<Duration> {
    // Sub-millisecond values round to a zero timeval, and Linux treats
    // SO_RCVTIMEO/SO_SNDTIMEO of zero as "block forever".
    match deadline.checked_duration_since(Instant::now()) {
        Some(remaining) if remaining >= Duration::from_millis(1) => Ok(remaining),
        _ => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "deadline exhausted",
        )),
    }
}

#[cfg(unix)]
fn decode_server_frame(buf: &[u8]) -> io::Result<ServerToClientMsg> {
    let proto = ProtoServerToClientMsg::decode(buf)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    ServerToClientMsg::try_from(proto)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
}

/// Socket timeouts are inactivity timers. A probe that needs one absolute
/// deadline refreshes them before every syscall. On macOS the read path does
/// not use SO_RCVTIMEO: a just-connected Unix socket has returned EINVAL there.
#[cfg(unix)]
pub(crate) trait DeadlineIo {
    fn refresh_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn refresh_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn wait_readable(&self, deadline: Instant) -> io::Result<()>;
}

#[cfg(unix)]
impl DeadlineIo for LocalSocketStream {
    fn refresh_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        use interprocess::local_socket::traits::Stream;
        self.set_recv_timeout(timeout)
    }
    fn refresh_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        use interprocess::local_socket::traits::Stream;
        self.set_send_timeout(timeout)
    }
    fn wait_readable(&self, deadline: Instant) -> io::Result<()> {
        use nix::errno::Errno;
        use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
        use std::os::fd::AsFd;

        let interprocess::local_socket::Stream::UdSocket(stream) = self;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "deadline exhausted",
                ));
            }
            let poll_timeout = PollTimeout::try_from(remaining).unwrap_or(PollTimeout::MAX);
            let mut fds = [PollFd::new(stream.as_fd(), PollFlags::POLLIN)];
            match poll(&mut fds, poll_timeout) {
                Err(Errno::EINTR) => continue,
                Err(error) => return Err(error.into()),
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "deadline exhausted",
                    ))
                },
                Ok(_) => return Ok(()),
            }
        }
    }
}

/// Errors that mean the peer is not ready yet, not that the probe is finished.
///
/// macOS `poll` reports `POLLHUP` with `SO_ERROR == 0` for a Unix socket that
/// has connected and is sitting in the accept queue. Before `accept`, a read
/// or write can return would-block, an immediate EOF, a reset, or EINVAL.
/// Retrying those is bounded by the caller's deadline and does not open a new
/// socket.
#[cfg(unix)]
pub(crate) fn error_means_peer_not_ready(
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
    apple: bool,
) -> bool {
    if matches!(
        kind,
        io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
            | io::ErrorKind::NotConnected
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    ) {
        return true;
    }
    if apple && raw_os_error == Some(nix::errno::Errno::EPROTOTYPE as i32) {
        return true;
    }
    // macOS has returned EINVAL while a just-connected Unix socket was not yet
    // ready for the probe read. Retry only that platform, and only inside the
    // probe deadline.
    apple && kind == io::ErrorKind::InvalidInput
}

#[cfg(unix)]
fn peer_not_ready(err: &io::Error) -> bool {
    error_means_peer_not_ready(
        err.kind(),
        err.raw_os_error(),
        cfg!(target_vendor = "apple"),
    )
}

#[cfg(unix)]
fn pause_until_peer_ready(deadline: Instant) -> io::Result<()> {
    let remaining = remaining_until(deadline)?;
    std::thread::sleep(Duration::from_millis(1).min(remaining));
    Ok(())
}

#[cfg(unix)]
fn trace_probe_retry_once(
    traced: &mut bool,
    stage: &str,
    completed: usize,
    total: usize,
    error: &io::Error,
) {
    if *traced {
        return;
    }
    *traced = true;
    log::debug!("probe {stage} first retry at {completed}/{total} bytes: {error:?}");
}

/// `read_exact` against one absolute deadline. Bytes already stored stay stored.
/// On macOS, readiness is `poll` only: `SO_RCVTIMEO` is not set.
#[cfg(unix)]
pub(crate) fn read_exact_until_deadline<R>(
    reader: &mut R,
    buf: &mut [u8],
    deadline: Instant,
) -> io::Result<()>
where
    R: Read + DeadlineIo,
{
    let mut filled = 0;
    let mut traced = false;
    while filled < buf.len() {
        #[cfg(not(target_vendor = "apple"))]
        {
            let remaining = remaining_until(deadline)?;
            reader.refresh_read_timeout(Some(remaining))?;
        }
        #[cfg(target_vendor = "apple")]
        remaining_until(deadline)?;
        reader.wait_readable(deadline)?;
        match reader.read(&mut buf[filled..]) {
            Ok(0) => {
                trace_probe_retry_once(
                    &mut traced,
                    "read",
                    filled,
                    buf.len(),
                    &io::Error::new(io::ErrorKind::UnexpectedEof, "read returned zero"),
                );
                pause_until_peer_ready(deadline)?;
            },
            Ok(n) => filled += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) if peer_not_ready(&err) => {
                trace_probe_retry_once(&mut traced, "read", filled, buf.len(), &err);
                pause_until_peer_ready(deadline)?;
            },
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// `write_all` against one absolute deadline. A short write keeps the bytes
/// already counted and retries only the tail.
#[cfg(unix)]
pub(crate) fn write_all_until_deadline<W>(
    writer: &mut W,
    buf: &[u8],
    deadline: Instant,
) -> io::Result<()>
where
    W: Write + DeadlineIo,
{
    let mut written = 0;
    let mut traced = false;
    while written < buf.len() {
        let remaining = remaining_until(deadline)?;
        writer.refresh_write_timeout(Some(remaining))?;
        match writer.write(&buf[written..]) {
            Ok(0) => {
                trace_probe_retry_once(
                    &mut traced,
                    "write",
                    written,
                    buf.len(),
                    &io::Error::new(io::ErrorKind::WriteZero, "write returned zero"),
                );
                pause_until_peer_ready(deadline)?;
            },
            Ok(n) => written += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) if peer_not_ready(&err) => {
                trace_probe_retry_once(&mut traced, "write", written, buf.len(), &err);
                pause_until_peer_ready(deadline)?;
            },
            Err(err) => return Err(err),
        }
    }
    loop {
        let remaining = remaining_until(deadline)?;
        writer.refresh_write_timeout(Some(remaining))?;
        match writer.flush() {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) if peer_not_ready(&err) => {
                trace_probe_retry_once(&mut traced, "flush", written, buf.len(), &err);
                pause_until_peer_ready(deadline)?;
            },
            Err(err) => return Err(err),
        }
    }
}

#[cfg(unix)]
pub(crate) fn encode_client_frame(msg: ClientToServerMsg) -> io::Result<Vec<u8>> {
    let proto: ProtoClientToServerMsg = msg.into();
    let encoded = proto.encode_to_vec();
    if encoded.len() > PROBE_MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "encoded IPC frame exceeds the probe cap",
        ));
    }
    let len = u32::try_from(encoded.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame length exceeds u32"))?;
    let mut frame = Vec::with_capacity(4 + encoded.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&encoded);
    Ok(frame)
}

/// Read one length-prefixed server frame, refusing to allocate above `max_frame`.
#[cfg(unix)]
pub(crate) fn read_server_frame_until_deadline<R>(
    reader: &mut R,
    deadline: Instant,
    max_frame: usize,
) -> io::Result<ServerToClientMsg>
where
    R: Read + DeadlineIo,
{
    let mut len_bytes = [0u8; 4];
    read_exact_until_deadline(reader, &mut len_bytes, deadline)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > max_frame {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds the allowed cap",
        ));
    }
    let mut buf = vec![0u8; len];
    read_exact_until_deadline(reader, &mut buf, deadline)?;
    decode_server_frame(&buf)
}

#[cfg(all(test, unix))]
mod probe_deadline_tests {
    use super::error_means_peer_not_ready;
    use std::io::ErrorKind;

    #[test]
    fn macos_probe_read_einval_is_retried_and_other_invalid_input_is_not() {
        assert!(error_means_peer_not_ready(
            ErrorKind::InvalidInput,
            Some(22),
            true
        ));
        assert!(!error_means_peer_not_ready(
            ErrorKind::InvalidInput,
            Some(22),
            false
        ));
        assert!(error_means_peer_not_ready(
            ErrorKind::WouldBlock,
            None,
            false
        ));
    }
}

// Protobuf helper functions
pub fn send_protobuf_client_to_server(
    sender: &mut IpcSenderWithContext<ClientToServerMsg>,
    msg: ClientToServerMsg,
) -> Result<()> {
    let proto_msg: ProtoClientToServerMsg = msg.into();
    write_protobuf_message(&mut sender.sender, &proto_msg)?;
    let _ = sender.sender.flush();
    Ok(())
}

pub fn send_protobuf_server_to_client(
    sender: &mut IpcSenderWithContext<ServerToClientMsg>,
    msg: ServerToClientMsg,
) -> Result<()> {
    let proto_msg: ProtoServerToClientMsg = msg.into();
    write_protobuf_message(&mut sender.sender, &proto_msg)?;
    let _ = sender.sender.flush();
    Ok(())
}

pub fn recv_protobuf_client_to_server(
    receiver: &mut IpcReceiverWithContext<ClientToServerMsg>,
) -> Option<(ClientToServerMsg, ErrorContext)> {
    receiver.try_recv_client_msg().ok()
}

pub fn recv_protobuf_server_to_client(
    receiver: &mut IpcReceiverWithContext<ServerToClientMsg>,
) -> Option<(ServerToClientMsg, ErrorContext)> {
    receiver.try_recv_server_msg().ok()
}

/// Asynchronously send `ClientToServerMsg::KillSession` to the peer at `path`
/// and wait until the peer's existing shutdown path replies (or its socket
/// closes). Either of those outcomes confirms the kill landed; the caller
/// wraps this in `tokio::time::timeout` to bound the wait against a wedged
/// peer.
///
/// On Unix the local socket is bidirectional, so the same async stream is
/// used for both send and receive. On Windows the named pipe is half-duplex
/// and the existing sync `ipc_connect` / `ipc_connect_reply` flow is
/// dispatched onto a blocking task.
#[cfg(unix)]
pub async fn async_send_kill_and_await(path: &std::path::Path) -> io::Result<()> {
    use interprocess::local_socket::traits::tokio::Stream as _;
    use interprocess::local_socket::{prelude::*, GenericFilePath};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let fs_name = path.to_fs_name::<GenericFilePath>()?;
    let mut stream: interprocess::local_socket::tokio::Stream =
        interprocess::local_socket::tokio::Stream::connect(fs_name).await?;

    let proto_msg: ProtoClientToServerMsg = crate::ipc::ClientToServerMsg::KillSession.into();
    let encoded = proto_msg.encode_to_vec();
    let len_bytes = (encoded.len() as u32).to_le_bytes();

    stream.write_all(&len_bytes).await?;
    stream.write_all(&encoded).await?;
    // Best-effort flush; failing here doesn't mean the kill failed.
    let _ = stream.flush().await;

    // The peer's shutdown path sends `ServerToClientMsg::Exit { Normal }`
    // (zellij-server/src/lib.rs ServerInstruction::KillSession) over this
    // same socket before exiting; if it dies without ACKing, the stream
    // closes. Either outcome -- a successful 4-byte length-prefix read or a
    // read error/EOF -- confirms the kill is no longer in flight.
    let mut len_buf = [0u8; 4];
    let _ = stream.read_exact(&mut len_buf).await;
    Ok(())
}

#[cfg(windows)]
pub async fn async_send_kill_and_await(path: &std::path::Path) -> io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let (stream, reply_stream) = crate::pipe_pairs::connect_pair(&path)?;
        let mut sender = IpcSenderWithContext::<ClientToServerMsg>::new(stream);
        sender
            .send_client_msg(ClientToServerMsg::KillSession)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let mut receiver: IpcReceiverWithContext<ServerToClientMsg> =
            IpcReceiverWithContext::new(reply_stream);
        let _ = receiver.recv_server_msg();
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
}
