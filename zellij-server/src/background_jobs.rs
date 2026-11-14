#[allow(unused_imports)] // some imports used only with web_server_capability feature
use zellij_utils::consts::{
    session_info_cache_file_name, session_info_folder_for_session, session_layout_cache_file_name,
    VERSION, ZELLIJ_SESSION_INFO_CACHE_DIR, ZELLIJ_SOCK_DIR,
};
#[allow(unused_imports)]
use zellij_utils::data::{Event, HttpVerb, LayoutInfo, SessionInfo, WebServerStatus};
use zellij_utils::errors::{prelude::*, BackgroundJobContext, ContextType};
use zellij_utils::input::layout::RunPlugin;
#[allow(unused_imports)]
use zellij_utils::shared::parse_base_url;

#[cfg(feature = "web_server_capability")]
use zellij_utils::web_server_commands::{
    discover_webserver_sockets, query_webserver_with_response, InstructionForWebServer,
    WebServerResponse,
};

use isahc::prelude::*;
use isahc::AsyncReadResponseExt;
use isahc::{config::RedirectPolicy, HttpClient, Request};

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use crate::os_input_output::PaneEnv;
use crate::panes::PaneId;
use crate::plugins::{PluginId, PluginInstruction};
use crate::pty::PtyInstruction;
use crate::screen::ScreenInstruction;
use crate::thread_bus::Bus;
use crate::{ClientId, ServerInstruction};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum BackgroundJob {
    DisplayPaneError(Vec<PaneId>, String),
    AnimatePluginLoading(u32),                            // u32 - plugin_id
    StopPluginLoadingAnimation(u32),                      // u32 - plugin_id
    ReportSessionInfo(String, SessionInfo),               // String - session name
    ReportPluginList(BTreeMap<PluginId, RunPlugin>),      // String - session name
    ReportLayoutInfo((String, BTreeMap<String, String>)), // BTreeMap<file_name, pane_contents>
    RunCommand(
        PluginId,
        ClientId,
        String,
        Vec<String>,
        BTreeMap<String, String>,
        PathBuf,
        BTreeMap<String, String>,
    ), // command, args, env_variables, cwd, context
    RunInBackground {
        command: PathBuf,
        args: Vec<String>,
        cwd: Option<PathBuf>,
        env: PaneEnv,
    },
    WebRequest(
        PluginId,
        ClientId,
        String, // url
        HttpVerb,
        BTreeMap<String, String>, // headers
        Vec<u8>,                  // body
        BTreeMap<String, String>, // context
    ),
    HighlightPanesWithMessage(Vec<PaneId>, String),
    RenderToClients,
    RenderToClientsNow,
    QueryZellijWebServerStatus,
    ClearHelpText {
        client_id: ClientId,
    },
    RevealPopupAfter {
        plugin_id: u32,
        delay_ms: u64,
    },
    MirrorTickAfter {
        delay_ms: u64,
    },
    ClearCommandOutputFlash {
        pane_id: PaneId,
    },
    FlashPaneBell(Vec<PaneId>),
    StopFlashPaneBell(Vec<PaneId>),
    FlashTabBell(usize),     // usize = tab_id
    StopFlashTabBell(usize), // usize = tab_id
    StartNestedGuestPing(PaneId),
    StopNestedGuestPing(PaneId),
    TrimAllocator,
    SessionSuggestions(crate::session_suggestions::SessionSuggestionsJob),
    SessionPreview {
        plugin_id: PluginId,
        client_id: ClientId,
        session_name: String,
        tab_index: Option<usize>,
        pane_id: Option<(u32, bool)>,
        anchor: Option<zellij_utils::data::PreviewAnchor>,
    },
    PeerAttention(crate::attention::peers::PeerAttentionJob),
    SavedSessionPreview {
        plugin_id: PluginId,
        client_id: ClientId,
        session_name: String,
    },
    AttentionIdleCheck(PaneId, u64),
    ReportHostProgramStatus(Option<zellij_utils::attention::HostProgramStatus>),
    Exit,
}

impl From<&BackgroundJob> for BackgroundJobContext {
    fn from(background_job: &BackgroundJob) -> Self {
        match *background_job {
            BackgroundJob::DisplayPaneError(..) => BackgroundJobContext::DisplayPaneError,
            BackgroundJob::AnimatePluginLoading(..) => BackgroundJobContext::AnimatePluginLoading,
            BackgroundJob::StopPluginLoadingAnimation(..) => {
                BackgroundJobContext::StopPluginLoadingAnimation
            },
            BackgroundJob::ReportSessionInfo(..) => BackgroundJobContext::ReportSessionInfo,
            BackgroundJob::ReportLayoutInfo(..) => BackgroundJobContext::ReportLayoutInfo,
            BackgroundJob::RunCommand(..) => BackgroundJobContext::RunCommand,
            BackgroundJob::RunInBackground { .. } => BackgroundJobContext::RunInBackground,
            BackgroundJob::WebRequest(..) => BackgroundJobContext::WebRequest,
            BackgroundJob::ReportPluginList(..) => BackgroundJobContext::ReportPluginList,
            BackgroundJob::RenderToClients | BackgroundJob::RenderToClientsNow => {
                BackgroundJobContext::RenderToClients
            },
            BackgroundJob::HighlightPanesWithMessage(..) => {
                BackgroundJobContext::HighlightPanesWithMessage
            },
            BackgroundJob::QueryZellijWebServerStatus => {
                BackgroundJobContext::QueryZellijWebServerStatus
            },
            BackgroundJob::ClearHelpText { .. } => BackgroundJobContext::ClearHelpText,
            BackgroundJob::RevealPopupAfter { .. } => BackgroundJobContext::RevealPopup,
            BackgroundJob::MirrorTickAfter { .. } => BackgroundJobContext::MirrorTick,
            BackgroundJob::ClearCommandOutputFlash { .. } => {
                BackgroundJobContext::ClearCommandOutputFlash
            },
            BackgroundJob::FlashPaneBell(..) => BackgroundJobContext::FlashPaneBell,
            BackgroundJob::StopFlashPaneBell(..) => BackgroundJobContext::StopFlashPaneBell,
            BackgroundJob::FlashTabBell(..) => BackgroundJobContext::FlashTabBell,
            BackgroundJob::StopFlashTabBell(..) => BackgroundJobContext::StopFlashTabBell,
            BackgroundJob::StartNestedGuestPing(..) => BackgroundJobContext::StartNestedGuestPing,
            BackgroundJob::StopNestedGuestPing(..) => BackgroundJobContext::StopNestedGuestPing,
            BackgroundJob::TrimAllocator => BackgroundJobContext::TrimAllocator,
            BackgroundJob::SessionSuggestions(..) => BackgroundJobContext::SessionSuggestions,
            BackgroundJob::SessionPreview { .. } => BackgroundJobContext::SessionPreview,
            BackgroundJob::SavedSessionPreview { .. } => BackgroundJobContext::SavedSessionPreview,
            BackgroundJob::PeerAttention(..) => BackgroundJobContext::PeerAttention,
            BackgroundJob::AttentionIdleCheck(..) => BackgroundJobContext::AttentionIdleCheck,
            BackgroundJob::ReportHostProgramStatus(..) => {
                BackgroundJobContext::ReportHostProgramStatus
            },
            BackgroundJob::Exit => BackgroundJobContext::Exit,
        }
    }
}

static LONG_FLASH_DURATION_MS: u64 = 1000;
static TRIM_ALLOCATOR_DEBOUNCE_MS: u64 = 2000;
static STARTUP_TRIM_ALLOCATOR_DELAY_MS: u64 = 10000;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_allocator() {
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_allocator() {}
static FLASH_DURATION_MS: u64 = 400; // Doherty threshold
static PLUGIN_ANIMATION_OFFSET_DURATION_MD: u64 = 500;
static SESSION_METADATA_WRITE_INTERVAL_MS: u64 = 1000;
static UPDATE_AND_REPORT_CWDS_INTERVAL_MS: u64 = 1000;
static DEFAULT_SERIALIZATION_INTERVAL: u64 = 60000;
const SESSION_SUGGESTIONS_EXIT_WAIT: Duration = Duration::from_secs(3);
pub const RENDER_QUIET_PERIOD: Duration = Duration::from_millis(4);
pub const RENDER_MAX_DELAY: Duration = Duration::from_millis(10);

static IMMEDIATE_RENDER_REQUESTS: AtomicUsize = AtomicUsize::new(0);

pub fn immediate_render_requests() -> usize {
    IMMEDIATE_RENDER_REQUESTS.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingRender {
    first_request: Instant,
    last_request: Instant,
    immediate_at: Option<Instant>,
}

impl PendingRender {
    fn due(&self) -> Instant {
        let due =
            (self.last_request + RENDER_QUIET_PERIOD).min(self.first_request + RENDER_MAX_DELAY);
        match self.immediate_at {
            Some(immediate_at) => due.min(immediate_at),
            None => due,
        }
    }
}

#[derive(Debug, Default)]
struct RenderSchedule {
    pending: Option<PendingRender>,
    exiting: bool,
}

impl RenderSchedule {
    fn request(&mut self, now: Instant) -> bool {
        match self.pending.as_mut() {
            Some(pending) => {
                pending.last_request = now;
                false
            },
            None => {
                self.pending = Some(PendingRender {
                    first_request: now,
                    last_request: now,
                    immediate_at: None,
                });
                true
            },
        }
    }

    fn request_now(&mut self, now: Instant) {
        match self.pending.as_mut() {
            Some(pending) => {
                pending.last_request = now;
                pending.immediate_at = Some(now);
            },
            None => {
                self.pending = Some(PendingRender {
                    first_request: now,
                    last_request: now,
                    immediate_at: Some(now),
                });
            },
        }
    }

    fn step(&mut self, now: Instant) -> RenderStep {
        if self.exiting {
            return RenderStep::Exit;
        }
        match self.pending {
            None => RenderStep::Idle,
            Some(pending) if now >= pending.due() => {
                self.pending = None;
                RenderStep::Render
            },
            Some(pending) => RenderStep::WaitFor(pending.due() - now),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RenderStep {
    Render,
    WaitFor(Duration),
    Idle,
    Exit,
}

type SharedRenderSchedule = Arc<(Mutex<RenderSchedule>, std::sync::Condvar)>;

fn spawn_render_scheduler(senders: crate::thread_bus::ThreadSenders) -> SharedRenderSchedule {
    let schedule: SharedRenderSchedule = Arc::new((
        Mutex::new(RenderSchedule::default()),
        std::sync::Condvar::new(),
    ));
    let _ = thread::Builder::new().name("repaint".to_string()).spawn({
        let schedule = schedule.clone();
        move || {
            let (lock, wake) = &*schedule;
            let mut state = lock.lock().unwrap();
            loop {
                match state.step(Instant::now()) {
                    RenderStep::Exit => break,
                    RenderStep::Idle => {
                        state = wake.wait(state).unwrap();
                    },
                    RenderStep::WaitFor(duration) => {
                        state = wake.wait_timeout(state, duration).unwrap().0;
                    },
                    RenderStep::Render => {
                        drop(state);
                        if senders
                            .send_to_screen(ScreenInstruction::RenderToClients)
                            .is_err()
                        {
                            break;
                        }
                        state = lock.lock().unwrap();
                    },
                }
            }
        }
    });
    schedule
}

static HELP_TEXT_DEBOUNCE_DURATION: u64 = 5000;
static COMMAND_OUTPUT_FLASH_DURATION_MS: u64 = 400;
static HOST_PROGRAM_STATUS_INTERVAL_MS: u64 = 500;

#[derive(Clone)]
pub struct SessionScanState {
    pub current_session_name: Arc<Mutex<String>>,
    pub current_session_info: Arc<Mutex<SessionInfo>>,
    pub current_session_plugin_list: Arc<Mutex<BTreeMap<PluginId, RunPlugin>>>,
}

static SESSION_SCAN_STATE: std::sync::OnceLock<SessionScanState> = std::sync::OnceLock::new();

pub fn session_scan_state() -> Option<&'static SessionScanState> {
    SESSION_SCAN_STATE.get()
}

#[allow(unused_variables)] // web_server_base_url used only with web_server_capability feature
pub(crate) fn background_jobs_main(
    bus: Bus<BackgroundJob>,
    serialization_interval: Option<u64>,
    disable_session_metadata: bool,
    web_server_base_url: String,
    session_id: String,
    live_sessions: crate::attention::peers::LiveSessions,
) -> Result<()> {
    let err_context = || "failed to write to pty".to_string();
    let mut running_jobs: HashMap<BackgroundJob, Instant> = HashMap::new();
    let mut loading_plugins: HashMap<u32, Arc<AtomicBool>> = HashMap::new(); // u32 - plugin_id
    let current_session_name = Arc::new(Mutex::new(String::default()));
    let current_session_info = Arc::new(Mutex::new(SessionInfo::default()));
    let current_session_plugin_list: Arc<Mutex<BTreeMap<PluginId, RunPlugin>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let current_session_layout = Arc::new(Mutex::new((String::new(), BTreeMap::new())));

    let _ = SESSION_SCAN_STATE.set(SessionScanState {
        current_session_name: current_session_name.clone(),
        current_session_info: current_session_info.clone(),
        current_session_plugin_list: current_session_plugin_list.clone(),
    });
    let last_serialization_time = Arc::new(Mutex::new(Instant::now()));
    let serialization_interval = serialization_interval.map(|s| s * 1000); // convert to
                                                                           // milliseconds
    let render_schedule = spawn_render_scheduler(bus.senders.clone());
    let (session_suggestions, mut session_suggestions_thread) =
        crate::session_suggestions::spawn_session_suggestions_service(
            bus.senders.clone(),
            live_sessions.clone(),
        );
    let peer_attention = crate::attention::peers::spawn_peer_attention_service(
        bus.senders.clone(),
        current_session_name.clone(),
        live_sessions,
        session_suggestions.clone(),
    );
    let pending_help_text_clear: Arc<Mutex<HashMap<ClientId, Instant>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let pending_command_output_flash_clear: Arc<Mutex<HashMap<PaneId, Instant>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut flashing_pane_bells: HashMap<PaneId, Arc<AtomicBool>> = HashMap::new();
    let mut flashing_tab_bells: HashMap<usize, Arc<AtomicBool>> = HashMap::new();
    let mut nested_guest_pings: HashMap<PaneId, Arc<AtomicBool>> = HashMap::new();
    let pending_allocator_trim: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let pending_attention_idle_checks: Arc<Mutex<HashMap<PaneId, Instant>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let host_program_status: Arc<
        Mutex<(
            Option<Instant>,
            Option<Option<zellij_utils::attention::HostProgramStatus>>,
        )>,
    > = Arc::new(Mutex::new((None, None)));

    let http_client = HttpClient::builder()
        // TODO: timeout?
        .redirect_policy(RedirectPolicy::Follow)
        .build()
        .ok();
    // We needn't do anything with the runtime, but it should exist at this point.
    let runtime = crate::global_async_runtime::get_tokio_runtime();

    runtime.spawn(async move {
        tokio::time::sleep(Duration::from_millis(STARTUP_TRIM_ALLOCATOR_DELAY_MS)).await;
        trim_allocator();
    });

    {
        let senders = bus.senders.clone();
        let serialization_ms = serialization_interval.unwrap_or(DEFAULT_SERIALIZATION_INTERVAL);
        runtime.spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_millis(serialization_ms));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let _ = senders.send_to_screen(ScreenInstruction::SerializeLayoutForResurrection);
            }
        });
    }

    {
        let senders = bus.senders.clone();
        runtime.spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                UPDATE_AND_REPORT_CWDS_INTERVAL_MS,
            ));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let _ = senders.send_to_pty(PtyInstruction::UpdateAndReportCwds);
            }
        });
    }

    let session_metadata_writer_stopped = Arc::new(Mutex::new(false));
    if !disable_session_metadata {
        let current_session_name = current_session_name.clone();
        let current_session_info = current_session_info.clone();
        let current_session_layout = current_session_layout.clone();
        let session_id = session_id.clone();
        let stopped = session_metadata_writer_stopped.clone();
        runtime.spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                SESSION_METADATA_WRITE_INTERVAL_MS,
            ));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let keep_running = {
                    let stopped_guard = stopped.lock().unwrap();
                    if *stopped_guard {
                        false
                    } else {
                        let name = current_session_name.lock().unwrap().clone();
                        if !name.is_empty() && !session_id.is_empty() {
                            let info = current_session_info.lock().unwrap().clone();
                            let layout = current_session_layout.lock().unwrap().clone();
                            write_session_state_to_disk(session_id.clone(), info, layout);
                        }
                        true
                    }
                };
                if !keep_running {
                    return;
                }
            }
        });
    }

    loop {
        let (event, mut err_ctx) = bus.recv().with_context(err_context)?;
        err_ctx.add_call(ContextType::BackgroundJob((&event).into()));
        let job = event.clone();
        match event {
            BackgroundJob::DisplayPaneError(pane_ids, text) => {
                if job_already_running(job, &mut running_jobs) {
                    continue;
                }
                runtime.spawn({
                    let senders = bus.senders.clone();
                    async move {
                        let _ = senders.send_to_screen(
                            ScreenInstruction::AddRedPaneFrameColorOverride(
                                pane_ids.clone(),
                                Some(text),
                            ),
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            LONG_FLASH_DURATION_MS,
                        ))
                        .await;
                        let _ = senders.send_to_screen(
                            ScreenInstruction::ClearPaneFrameColorOverride(pane_ids),
                        );
                    }
                });
            },
            BackgroundJob::AnimatePluginLoading(pid) => {
                let loading_plugin = Arc::new(AtomicBool::new(true));
                if job_already_running(job, &mut running_jobs) {
                    continue;
                }
                runtime.spawn({
                    let senders = bus.senders.clone();
                    let loading_plugin = loading_plugin.clone();
                    async move {
                        while loading_plugin.load(Ordering::SeqCst) {
                            let _ = senders.send_to_screen(
                                ScreenInstruction::ProgressPluginLoadingOffset(pid),
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                PLUGIN_ANIMATION_OFFSET_DURATION_MD,
                            ))
                            .await;
                        }
                    }
                });
                loading_plugins.insert(pid, loading_plugin);
            },
            BackgroundJob::StopPluginLoadingAnimation(pid) => {
                if let Some(loading_plugin) = loading_plugins.remove(&pid) {
                    loading_plugin.store(false, Ordering::SeqCst);
                }
            },
            BackgroundJob::ReportSessionInfo(session_name, session_info) => {
                *current_session_name.lock().unwrap() = session_name;
                *current_session_info.lock().unwrap() = session_info;
            },
            BackgroundJob::ReportPluginList(plugin_list) => {
                *current_session_plugin_list.lock().unwrap() = plugin_list;
            },
            BackgroundJob::ReportLayoutInfo(session_layout) => {
                *current_session_layout.lock().unwrap() = session_layout;

                // Update session save time for plugin query
                let timestamp_millis = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let _ = bus
                    .senders
                    .send_to_plugin(PluginInstruction::UpdateSessionSaveTime(timestamp_millis));
            },
            BackgroundJob::RunCommand(
                plugin_id,
                client_id,
                command,
                args,
                env_variables,
                cwd,
                context,
            ) => {
                runtime.spawn({
                    let senders = bus.senders.clone();
                    async move {
                        let output = run_background_command(BackgroundCommand {
                            program: command,
                            args,
                            env: env_variables,
                            cwd: Some(cwd),
                            input: None,
                            timeout: None,
                        })
                        .await;
                        match output {
                            Ok(output) => {
                                let stdout = output.stdout;
                                let stderr = output.stderr;
                                let exit_code = output.exit_code;
                                let _ = senders.send_to_plugin(PluginInstruction::Update(vec![(
                                    Some(plugin_id),
                                    Some(client_id),
                                    Event::RunCommandResult(exit_code, stdout, stderr, context),
                                )]));
                            },
                            Err(e) => {
                                log::error!("Failed to run command: {}", e);
                                let stdout = vec![];
                                let stderr = format!("{}", e).as_bytes().to_vec();
                                let exit_code = Some(2);
                                let _ = senders.send_to_plugin(PluginInstruction::Update(vec![(
                                    Some(plugin_id),
                                    Some(client_id),
                                    Event::RunCommandResult(exit_code, stdout, stderr, context),
                                )]));
                            },
                        }
                    }
                });
            },
            BackgroundJob::RunInBackground {
                command,
                args,
                cwd,
                env,
            } => {
                runtime.spawn(run_in_background(command, args, cwd, env));
            },
            BackgroundJob::WebRequest(plugin_id, client_id, url, verb, headers, body, context) => {
                runtime.spawn({
                    let senders = bus.senders.clone();
                    let http_client = http_client.clone();
                    async move {
                        async fn web_request(
                            url: String,
                            verb: HttpVerb,
                            headers: BTreeMap<String, String>,
                            body: Vec<u8>,
                            http_client: HttpClient,
                        ) -> Result<
                            (u16, BTreeMap<String, String>, Vec<u8>), // status_code, headers, body
                            isahc::Error,
                        > {
                            let mut request = match verb {
                                HttpVerb::Get => Request::get(url),
                                HttpVerb::Post => Request::post(url),
                                HttpVerb::Put => Request::put(url),
                                HttpVerb::Delete => Request::delete(url),
                            };
                            for (header, value) in headers {
                                request = request.header(header.as_str(), value);
                            }
                            let mut res = if !body.is_empty() {
                                let req = request.body(body)?;
                                http_client.send_async(req).await?
                            } else {
                                let req = request.body(())?;
                                http_client.send_async(req).await?
                            };

                            let status_code = res.status();
                            let headers: BTreeMap<String, String> = res
                                .headers()
                                .iter()
                                .filter_map(|(name, value)| match value.to_str() {
                                    Ok(value) => Some((name.to_string(), value.to_string())),
                                    Err(e) => {
                                        log::error!(
                                            "Failed to convert header {:?} to string: {:?}",
                                            name,
                                            e
                                        );
                                        None
                                    },
                                })
                                .collect();
                            let body = res.bytes().await?;
                            Ok((status_code.as_u16(), headers, body))
                        }
                        let Some(http_client) = http_client else {
                            log::error!("Cannot perform http request, likely due to a misconfigured http client");
                            return;
                        };

                        match web_request(url, verb, headers, body, http_client).await {
                            Ok((status, headers, body)) => {
                                let _ = senders.send_to_plugin(PluginInstruction::Update(vec![(
                                    Some(plugin_id),
                                    Some(client_id),
                                    Event::WebRequestResult(status, headers, body, context),
                                )]));
                            },
                            Err(e) => {
                                log::error!("Failed to send web request: {}", e);
                                let error_body = e.to_string().as_bytes().to_vec();
                                let _ = senders.send_to_plugin(PluginInstruction::Update(vec![(
                                    Some(plugin_id),
                                    Some(client_id),
                                    Event::WebRequestResult(
                                        400,
                                        BTreeMap::new(),
                                        error_body,
                                        context,
                                    ),
                                )]));
                            },
                        }
                    }
                });
            },
            BackgroundJob::QueryZellijWebServerStatus => {
                #[cfg(feature = "web_server_capability")]
                {
                    let status = query_webserver_via_ipc(&web_server_base_url)
                        .unwrap_or(WebServerStatus::Offline);
                    runtime.spawn({
                        let senders = bus.senders.clone();
                        let _web_server_base_url = web_server_base_url.clone();
                        async move {
                            let _ = senders.send_to_plugin(PluginInstruction::Update(vec![(
                                None,
                                None,
                                Event::WebServerStatus(status),
                            )]));
                        }
                    });
                }
            },
            BackgroundJob::RenderToClients => {
                let (lock, wake) = &*render_schedule;
                if lock.lock().unwrap().request(Instant::now()) {
                    wake.notify_one();
                }
            },
            BackgroundJob::RenderToClientsNow => {
                IMMEDIATE_RENDER_REQUESTS.fetch_add(1, Ordering::Relaxed);
                let (lock, wake) = &*render_schedule;
                lock.lock().unwrap().request_now(Instant::now());
                wake.notify_one();
            },
            BackgroundJob::HighlightPanesWithMessage(pane_ids, text) => {
                if job_already_running(job, &mut running_jobs) {
                    continue;
                }
                runtime.spawn({
                    let senders = bus.senders.clone();
                    async move {
                        let _ = senders.send_to_screen(
                            ScreenInstruction::AddHighlightPaneFrameColorOverride(
                                pane_ids.clone(),
                                Some(text),
                            ),
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(FLASH_DURATION_MS))
                            .await;
                        let _ = senders.send_to_screen(
                            ScreenInstruction::ClearPaneFrameColorOverride(pane_ids),
                        );
                    }
                });
            },
            BackgroundJob::RevealPopupAfter {
                plugin_id,
                delay_ms,
            } => {
                runtime.spawn({
                    let senders = bus.senders.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        let _ = senders.send_to_screen(ScreenInstruction::RevealPopup(plugin_id));
                    }
                });
            },
            BackgroundJob::MirrorTickAfter { delay_ms } => {
                runtime.spawn({
                    let senders = bus.senders.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        let _ = senders.send_to_screen(ScreenInstruction::MirrorTick);
                    }
                });
            },
            BackgroundJob::ClearHelpText { client_id } => {
                let should_spawn = {
                    let mut pending = pending_help_text_clear.lock().unwrap();
                    let current_time = Instant::now();
                    let should_spawn = !pending.contains_key(&client_id);
                    pending.insert(client_id, current_time);
                    should_spawn
                };

                if should_spawn {
                    runtime.spawn({
                        let senders = bus.senders.clone();
                        let pending = pending_help_text_clear.clone();
                        let debounce_duration = Duration::from_millis(HELP_TEXT_DEBOUNCE_DURATION);
                        async move {
                            tokio::time::sleep(debounce_duration).await;
                            loop {
                                let next_sleep_duration = {
                                    let mut pending = pending.lock().unwrap();
                                    match pending.get(&client_id) {
                                        Some(&last_motion_time) => {
                                            let time_since_motion =
                                                Instant::now().duration_since(last_motion_time);
                                            if time_since_motion >= debounce_duration {
                                                pending.remove(&client_id);
                                                None
                                            } else {
                                                let remaining = debounce_duration
                                                    .saturating_sub(time_since_motion);
                                                Some(remaining)
                                            }
                                        },
                                        None => break,
                                    }
                                };

                                match next_sleep_duration {
                                    Some(duration) => {
                                        tokio::time::sleep(duration).await;
                                    },
                                    None => {
                                        let _ = senders.send_to_server(
                                            ServerInstruction::ClearMouseHelpText(client_id),
                                        );
                                        break;
                                    },
                                }
                            }
                        }
                    });
                }
            },
            BackgroundJob::AttentionIdleCheck(pane_id, delay_ms) => {
                let should_spawn = {
                    let mut pending = pending_attention_idle_checks.lock().unwrap();
                    let deadline = Instant::now() + Duration::from_millis(delay_ms);
                    let should_spawn = !pending.contains_key(&pane_id);
                    pending.insert(pane_id, deadline);
                    should_spawn
                };
                if should_spawn {
                    runtime.spawn({
                        let senders = bus.senders.clone();
                        let pending = pending_attention_idle_checks.clone();
                        async move {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            loop {
                                let next_sleep_duration = {
                                    let mut pending = pending.lock().unwrap();
                                    match pending.get(&pane_id) {
                                        Some(&deadline) => {
                                            let now = Instant::now();
                                            if now >= deadline {
                                                pending.remove(&pane_id);
                                                None
                                            } else {
                                                Some(deadline.saturating_duration_since(now))
                                            }
                                        },
                                        None => break,
                                    }
                                };
                                match next_sleep_duration {
                                    Some(duration) => {
                                        tokio::time::sleep(duration).await;
                                    },
                                    None => {
                                        let _ = senders.send_to_screen(
                                            ScreenInstruction::AttentionIdleCheck(pane_id),
                                        );
                                        break;
                                    },
                                }
                            }
                        }
                    });
                }
            },
            BackgroundJob::ReportHostProgramStatus(status) => {
                let interval = Duration::from_millis(HOST_PROGRAM_STATUS_INTERVAL_MS);
                let send_now = {
                    let mut state = host_program_status.lock().unwrap();
                    let now = Instant::now();
                    let can_send = state
                        .0
                        .map(|last| now.duration_since(last) >= interval)
                        .unwrap_or(true);
                    let task_pending = state.1.is_some();
                    if can_send && !task_pending {
                        state.0 = Some(now);
                        Some(status)
                    } else {
                        let should_spawn = !task_pending;
                        state.1 = Some(status);
                        if should_spawn {
                            let wait = state
                                .0
                                .map(|last| interval.saturating_sub(now.duration_since(last)))
                                .unwrap_or_default();
                            runtime.spawn({
                                let senders = bus.senders.clone();
                                let state = host_program_status.clone();
                                async move {
                                    tokio::time::sleep(wait).await;
                                    let status = {
                                        let mut state = state.lock().unwrap();
                                        state.0 = Some(Instant::now());
                                        state.1.take()
                                    };
                                    if let Some(status) = status {
                                        let _ = senders.send_to_server(
                                            ServerInstruction::HostProgramStatus(status),
                                        );
                                    }
                                }
                            });
                        }
                        None
                    }
                };
                if let Some(status) = send_now {
                    let _ = bus
                        .senders
                        .send_to_server(ServerInstruction::HostProgramStatus(status));
                }
            },
            BackgroundJob::ClearCommandOutputFlash { pane_id } => {
                let should_spawn = {
                    let mut pending = pending_command_output_flash_clear.lock().unwrap();
                    let current_time = Instant::now();
                    let should_spawn = !pending.contains_key(&pane_id);
                    pending.insert(pane_id, current_time);
                    should_spawn
                };

                if should_spawn {
                    runtime.spawn({
                        let senders = bus.senders.clone();
                        let pending = pending_command_output_flash_clear.clone();
                        let flash_duration =
                            Duration::from_millis(COMMAND_OUTPUT_FLASH_DURATION_MS);
                        async move {
                            tokio::time::sleep(flash_duration).await;
                            loop {
                                let next_sleep_duration = {
                                    let mut pending = pending.lock().unwrap();
                                    match pending.get(&pane_id) {
                                        Some(&last_flash_time) => {
                                            let time_since_flash =
                                                Instant::now().duration_since(last_flash_time);
                                            if time_since_flash >= flash_duration {
                                                pending.remove(&pane_id);
                                                None
                                            } else {
                                                Some(
                                                    flash_duration.saturating_sub(time_since_flash),
                                                )
                                            }
                                        },
                                        None => break,
                                    }
                                };

                                match next_sleep_duration {
                                    Some(duration) => {
                                        tokio::time::sleep(duration).await;
                                    },
                                    None => {
                                        let _ = senders.send_to_server(
                                            ServerInstruction::ClearCommandOutputFlash(pane_id),
                                        );
                                        break;
                                    },
                                }
                            }
                        }
                    });
                }
            },
            BackgroundJob::FlashPaneBell(pane_ids) => {
                let is_flashing = Arc::new(AtomicBool::new(true));
                for &pane_id in &pane_ids {
                    flashing_pane_bells.insert(pane_id, is_flashing.clone());
                }
                runtime.spawn({
                    let senders = bus.senders.clone();
                    let pane_ids_clone = pane_ids.clone();
                    let flag = is_flashing.clone();
                    async move {
                        let _ = senders.send_to_screen(
                            ScreenInstruction::AddHighlightPaneFrameColorOverride(
                                pane_ids_clone.clone(),
                                None,
                            ),
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(FLASH_DURATION_MS))
                            .await;
                        if flag.load(Ordering::SeqCst) {
                            let _ = senders.send_to_screen(
                                ScreenInstruction::ClearPaneFrameColorOverride(pane_ids_clone),
                            );
                        }
                    }
                });
            },
            BackgroundJob::StopFlashPaneBell(pane_ids) => {
                for &pane_id in &pane_ids {
                    if let Some(flag) = flashing_pane_bells.remove(&pane_id) {
                        flag.store(false, Ordering::SeqCst);
                    }
                }
                let _ = bus
                    .senders
                    .send_to_screen(ScreenInstruction::ClearPaneFrameColorOverride(pane_ids));
            },
            BackgroundJob::FlashTabBell(tab_id) => {
                let is_flashing = Arc::new(AtomicBool::new(true));
                flashing_tab_bells.insert(tab_id, is_flashing.clone());
                runtime.spawn({
                    let senders = bus.senders.clone();
                    let flag = is_flashing.clone();
                    async move {
                        let _ = senders
                            .send_to_screen(ScreenInstruction::SetTabBellFlash(tab_id, true));
                        tokio::time::sleep(std::time::Duration::from_millis(FLASH_DURATION_MS))
                            .await;
                        if flag.load(Ordering::SeqCst) {
                            let _ = senders
                                .send_to_screen(ScreenInstruction::SetTabBellFlash(tab_id, false));
                        }
                    }
                });
            },
            BackgroundJob::StopFlashTabBell(tab_id) => {
                if let Some(flag) = flashing_tab_bells.remove(&tab_id) {
                    flag.store(false, Ordering::SeqCst);
                }
                let _ = bus
                    .senders
                    .send_to_screen(ScreenInstruction::SetTabBellFlash(tab_id, false));
            },
            BackgroundJob::StartNestedGuestPing(pane_id) => {
                if !nested_guest_pings.contains_key(&pane_id) {
                    let is_pinging = Arc::new(AtomicBool::new(true));
                    nested_guest_pings.insert(pane_id, is_pinging.clone());
                    runtime.spawn({
                        let senders = bus.senders.clone();
                        let flag = is_pinging.clone();
                        let ping_interval_ms = crate::nested_guest::ping_interval_ms();
                        async move {
                            while flag.load(Ordering::SeqCst) {
                                let _ = senders.send_to_screen(
                                    ScreenInstruction::NestedGuestPingTick { pane_id },
                                );
                                tokio::time::sleep(Duration::from_millis(ping_interval_ms)).await;
                            }
                        }
                    });
                }
            },
            BackgroundJob::StopNestedGuestPing(pane_id) => {
                if let Some(flag) = nested_guest_pings.remove(&pane_id) {
                    flag.store(false, Ordering::SeqCst);
                }
            },
            BackgroundJob::TrimAllocator => {
                let should_spawn = {
                    let mut pending = pending_allocator_trim.lock().unwrap();
                    let should_spawn = pending.is_none();
                    *pending = Some(Instant::now());
                    should_spawn
                };
                if should_spawn {
                    let pending = pending_allocator_trim.clone();
                    runtime.spawn(async move {
                        let debounce = Duration::from_millis(TRIM_ALLOCATOR_DEBOUNCE_MS);
                        tokio::time::sleep(debounce).await;
                        loop {
                            let remaining = {
                                let mut pending = pending.lock().unwrap();
                                match *pending {
                                    Some(last_request) => {
                                        let elapsed = Instant::now().duration_since(last_request);
                                        if elapsed >= debounce {
                                            *pending = None;
                                            None
                                        } else {
                                            Some(debounce.saturating_sub(elapsed))
                                        }
                                    },
                                    None => return,
                                }
                            };
                            match remaining {
                                Some(duration) => tokio::time::sleep(duration).await,
                                None => {
                                    trim_allocator();
                                    return;
                                },
                            }
                        }
                    });
                }
            },
            BackgroundJob::SessionSuggestions(job) => {
                let _ = session_suggestions.send(job);
            },
            BackgroundJob::PeerAttention(job) => {
                let _ = peer_attention.send(job);
            },
            BackgroundJob::SessionPreview {
                plugin_id,
                client_id,
                session_name,
                tab_index,
                pane_id,
                anchor,
            } => {
                let senders = bus.senders.clone();
                let is_current = *current_session_name.lock().unwrap() == session_name;
                thread::spawn(move || {
                    let preview = if is_current {
                        crate::session_previews::current_session_preview(
                            &senders,
                            &session_name,
                            tab_index,
                            pane_id,
                            anchor,
                        )
                    } else {
                        crate::session_previews::running_session_preview(
                            &session_name,
                            tab_index,
                            pane_id,
                            anchor,
                        )
                    };
                    let _ = senders.send_to_plugin(PluginInstruction::Update(vec![(
                        Some(plugin_id),
                        Some(client_id),
                        Event::SessionPreview(preview),
                    )]));
                });
            },
            BackgroundJob::SavedSessionPreview {
                plugin_id,
                client_id,
                session_name,
            } => {
                let senders = bus.senders.clone();
                thread::spawn(move || {
                    let preview = crate::session_previews::saved_session_preview(&session_name);
                    let _ = senders.send_to_plugin(PluginInstruction::Update(vec![(
                        Some(plugin_id),
                        Some(client_id),
                        Event::SavedSessionPreview(preview),
                    )]));
                });
            },
            BackgroundJob::Exit => {
                *session_metadata_writer_stopped.lock().unwrap() = true;
                let _ = session_suggestions
                    .send(crate::session_suggestions::SessionSuggestionsJob::Exit);
                if let Some(handle) = session_suggestions_thread.take() {
                    crate::session_suggestions::join_with_deadline(
                        handle,
                        SESSION_SUGGESTIONS_EXIT_WAIT,
                    );
                }
                let _ = peer_attention.send(crate::attention::peers::PeerAttentionJob::Exit);
                {
                    let (lock, wake) = &*render_schedule;
                    lock.lock().unwrap().exiting = true;
                    wake.notify_one();
                }
                for loading_plugin in loading_plugins.values() {
                    loading_plugin.store(false, Ordering::SeqCst);
                }
                for nested_guest_ping in nested_guest_pings.values() {
                    nested_guest_ping.store(false, Ordering::SeqCst);
                }

                // Remove the metadata cache file (keyed by session id) so the
                // session is no longer considered live; the layout file is kept
                // for resurrection.
                if !session_id.is_empty() {
                    let _ = std::fs::remove_file(session_info_cache_file_name(&session_id));
                }
                return Ok(());
            },
        }
    }
}

fn job_already_running(
    job: BackgroundJob,
    running_jobs: &mut HashMap<BackgroundJob, Instant>,
) -> bool {
    match running_jobs.get_mut(&job) {
        Some(current_running_job_start_time) => {
            if current_running_job_start_time.elapsed()
                > Duration::from_millis(LONG_FLASH_DURATION_MS)
            {
                *current_running_job_start_time = Instant::now();
                false
            } else {
                true
            }
        },
        None => {
            running_jobs.insert(job.clone(), Instant::now());
            false
        },
    }
}

fn file_content_changed(path: &std::path::Path, new_content: &[u8]) -> bool {
    match std::fs::read(path) {
        Ok(existing) => existing != new_content,
        Err(_) => true,
    }
}

pub fn write_session_state_to_disk(
    session_id: String,
    current_session_info: SessionInfo,
    current_session_layout: (String, BTreeMap<String, String>),
) {
    let metadata_cache_file_name = session_info_cache_file_name(&session_id);
    let (current_session_layout, layout_files_to_write) = current_session_layout;
    let new_metadata = current_session_info.to_string();
    if file_content_changed(&metadata_cache_file_name, new_metadata.as_bytes()) {
        if let Err(error) =
            write_file_atomically(&metadata_cache_file_name, new_metadata.as_bytes())
        {
            log::error!("Failed to write session metadata file: {:?}", error);
        }
    }

    if !current_session_layout.is_empty() {
        let layout_cache_file_name = session_layout_cache_file_name(&session_id);
        if file_content_changed(&layout_cache_file_name, current_session_layout.as_bytes()) {
            // Replacing the file gives it a new creation time; keep the session's own.
            zellij_utils::sessions::record_session_layout_creation_time(&layout_cache_file_name);
            if let Err(error) =
                write_file_atomically(&layout_cache_file_name, current_session_layout.as_bytes())
            {
                log::error!("Failed to write session layout file: {:?}", error);
            }
        }
        let session_info_folder = session_info_folder_for_session(&session_id);
        for (external_file_name, external_file_contents) in layout_files_to_write {
            let external_file_path = session_info_folder.join(&external_file_name);
            if file_content_changed(&external_file_path, external_file_contents.as_bytes()) {
                if let Err(error) =
                    write_file_atomically(&external_file_path, external_file_contents.as_bytes())
                {
                    log::error!("Failed to write layout metadata file: {:?}", error);
                }
            }
        }
    }
}

/// Publish one state file by renaming a unique sibling temporary over it.
///
/// The temporary is synced before the rename, so a failed write or sync leaves
/// the previous file in place. Callers must not share one temporary path.
fn write_file_atomically(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let file = stage_atomic_write(path, contents)?;
    persist_atomic_write(file, path)
}

fn stage_atomic_write(path: &Path, contents: &[u8]) -> std::io::Result<tempfile::NamedTempFile> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "file path has no parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;
    let mut temporary_file = tempfile::Builder::new()
        .prefix(".zellij-session-")
        .tempfile_in(parent)?;
    temporary_file.write_all(contents)?;
    // Sync before rename so a failed sync drops the temporary and keeps the
    // previous file. The rename is what readers observe.
    temporary_file.as_file().sync_all()?;
    Ok(temporary_file)
}

fn persist_atomic_write(file: tempfile::NamedTempFile, path: &Path) -> std::io::Result<()> {
    file.persist(path).map_err(|error| error.error)?;
    sync_parent_directory(path)
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let directory = fs::File::open(parent)?;
        match directory.sync_all() {
            Ok(()) => {},
            // Some filesystems cannot sync a directory. The rename already
            // published the file; that is not a failed save.
            Err(error) if directory_sync_unsupported(&error) => {},
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn directory_sync_unsupported(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code) if code == libc::EINVAL || code == libc::ENOTSUP
    )
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

pub fn scan_session_list(
    current_session_name: &str,
    available_layouts: &[LayoutInfo],
    current_session_plugin_list: &BTreeMap<PluginId, RunPlugin>,
    sock_dir: &Path,
    session_info_cache_dir: &Path,
) -> (BTreeMap<String, SessionInfo>, BTreeMap<String, Duration>) {
    let mut session_infos_on_machine = zellij_utils::sessions::read_live_session_states(
        current_session_name,
        sock_dir,
        session_info_cache_dir,
    );
    for (name, info) in session_infos_on_machine.iter_mut() {
        if name == current_session_name {
            info.populate_plugin_list(current_session_plugin_list.clone());
            info.available_layouts = available_layouts.to_vec();
        }
    }
    let resurrectable_sessions =
        find_resurrectable_sessions(&session_infos_on_machine, session_info_cache_dir);
    (session_infos_on_machine, resurrectable_sessions)
}

pub fn scan_session_list_default_dirs(
    current_session_name: &str,
    available_layouts: &[LayoutInfo],
    current_session_plugin_list: &BTreeMap<PluginId, RunPlugin>,
) -> (BTreeMap<String, SessionInfo>, BTreeMap<String, Duration>) {
    scan_session_list(
        current_session_name,
        available_layouts,
        current_session_plugin_list,
        &*ZELLIJ_SOCK_DIR,
        &*ZELLIJ_SESSION_INFO_CACHE_DIR,
    )
}

fn find_resurrectable_sessions(
    session_infos_on_machine: &BTreeMap<String, SessionInfo>,
    session_info_cache_dir: &Path,
) -> BTreeMap<String, Duration> {
    // Cache folders are keyed by session id; map each to its display name so we
    // can compare against the (name-keyed) live set and report a readable name.
    let names_by_id = zellij_utils::session_index::session_names_by_id();
    match fs::read_dir(session_info_cache_dir) {
        Ok(files_in_session_info_folder) => {
            let files_that_are_folders = files_in_session_info_folder
                .filter_map(|f| f.ok().map(|f| f.path()))
                .filter(|f| f.is_dir());
            files_that_are_folders
                .filter_map(|folder_name| {
                    let id = folder_name.file_name()?.to_str()?.to_owned();
                    if zellij_utils::consts::is_app_socket_name(&id) {
                        return None;
                    }
                    let session_name = names_by_id.get(&id).cloned().unwrap_or_else(|| id.clone());
                    if session_infos_on_machine.contains_key(&session_name) {
                        // this is not a dead session...
                        return None;
                    }
                    let layout_file_name = folder_name.join("session-layout.kdl");
                    let ctime = match zellij_utils::sessions::session_layout_creation_time(
                        &layout_file_name,
                    ) {
                        Ok(created) => Some(created),
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::NotFound {
                                return None; // no layout file, cannot resurrect session, let's not
                                             // list it
                            } else {
                                log::error!(
                                    "Failed to read created stamp of resurrection file: {:?}",
                                    e
                                );
                            }
                            None
                        },
                    };
                    let elapsed_duration = ctime
                        .and_then(|ctime| ctime.elapsed().ok())
                        .unwrap_or_default();
                    Some((session_name, elapsed_duration))
                })
                .collect()
        },
        Err(e) => {
            log::error!("Failed to read session info cache dir: {:?}", e);
            BTreeMap::new()
        },
    }
}

#[cfg(feature = "web_server_capability")]
fn query_webserver_via_ipc(web_server_base_url: &str) -> Result<WebServerStatus> {
    let expected_addr =
        parse_base_url(web_server_base_url).context("Failed to parse web server base URL")?;

    let sockets = discover_webserver_sockets().context("Failed to discover web server sockets")?;

    if sockets.is_empty() {
        return Ok(WebServerStatus::Offline);
    }

    for socket_path in sockets {
        let path_str = socket_path.to_str().unwrap_or("");

        match query_webserver_with_response(path_str, InstructionForWebServer::QueryVersion, 500) {
            Ok(WebServerResponse::Version(info)) => {
                let matches_expected =
                    info.ip == expected_addr.ip && info.port == expected_addr.port;

                if !matches_expected {
                    continue;
                }

                if info.version == VERSION {
                    return Ok(WebServerStatus::Online(web_server_base_url.to_string()));
                } else {
                    return Ok(WebServerStatus::DifferentVersion(info.version));
                }
            },
            Err(_) => continue,
        }
    }

    Ok(WebServerStatus::Offline)
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;
    use tempfile::tempdir;
    use zellij_utils::data::SessionInfo;

    fn make_socket(dir: &std::path::Path, name: &str) -> UnixListener {
        UnixListener::bind(dir.join(name)).expect("bind unix socket")
    }

    fn write_metadata(info_dir: &std::path::Path, session: &str, info: &SessionInfo) {
        let folder = info_dir.join(session);
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("session-metadata.kdl"), info.to_string()).unwrap();
    }

    fn write_layout(info_dir: &std::path::Path, session: &str) {
        let folder = info_dir.join(session);
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("session-layout.kdl"), "layout { }").unwrap();
    }

    #[test]
    fn scan_session_list_returns_empty_when_no_peers() {
        let sock_dir = tempdir().unwrap();
        let info_dir = tempdir().unwrap();
        let (live, resurrectable) = scan_session_list(
            "me",
            &[],
            &BTreeMap::new(),
            sock_dir.path(),
            info_dir.path(),
        );
        assert!(live.is_empty());
        assert!(resurrectable.is_empty());
    }

    #[test]
    fn scan_session_list_finds_peer_from_socket_and_metadata() {
        let sock_dir = tempdir().unwrap();
        let info_dir = tempdir().unwrap();
        let peer = "peer-alpha";
        let _listener = make_socket(sock_dir.path(), peer);
        write_metadata(info_dir.path(), peer, &SessionInfo::new(peer.to_string()));

        let (live, resurrectable) = scan_session_list(
            "me",
            &[],
            &BTreeMap::new(),
            sock_dir.path(),
            info_dir.path(),
        );
        assert_eq!(live.len(), 1);
        assert!(live.contains_key(peer));
        assert!(resurrectable.is_empty());
    }

    #[test]
    fn scan_session_list_finds_resurrectable_from_orphan_metadata() {
        let sock_dir = tempdir().unwrap();
        let info_dir = tempdir().unwrap();
        write_layout(info_dir.path(), "dead-beta");

        let (live, resurrectable) = scan_session_list(
            "me",
            &[],
            &BTreeMap::new(),
            sock_dir.path(),
            info_dir.path(),
        );
        assert!(live.is_empty());
        assert_eq!(resurrectable.len(), 1);
        assert!(resurrectable.contains_key("dead-beta"));
    }

    #[test]
    fn scan_session_list_separates_live_from_resurrectable() {
        let sock_dir = tempdir().unwrap();
        let info_dir = tempdir().unwrap();
        for name in ["live-a", "live-b", "live-c"] {
            let _listener = make_socket(sock_dir.path(), name);
            write_metadata(info_dir.path(), name, &SessionInfo::new(name.to_string()));
            std::mem::forget(_listener);
        }
        for name in ["dead-a", "dead-b"] {
            write_layout(info_dir.path(), name);
        }

        let (live, resurrectable) = scan_session_list(
            "me",
            &[],
            &BTreeMap::new(),
            sock_dir.path(),
            info_dir.path(),
        );
        assert_eq!(live.len(), 3);
        assert_eq!(resurrectable.len(), 2);
        for name in ["live-a", "live-b", "live-c"] {
            assert!(!resurrectable.contains_key(name));
        }
    }

    #[test]
    fn the_hidden_app_sessions_are_neither_live_nor_resurrectable() {
        let sock_dir = tempdir().unwrap();
        let info_dir = tempdir().unwrap();
        let panel = zellij_utils::consts::APP_PANEL_SESSION;
        let _panel = make_socket(sock_dir.path(), panel);
        write_metadata(info_dir.path(), panel, &SessionInfo::new(panel.to_string()));
        write_layout(info_dir.path(), panel);
        write_layout(info_dir.path(), ".old-app-thing");
        let _live = make_socket(sock_dir.path(), "visible");
        write_metadata(
            info_dir.path(),
            "visible",
            &SessionInfo::new("visible".to_string()),
        );
        write_layout(info_dir.path(), "exited");

        let (live, resurrectable) = scan_session_list(
            "me",
            &[],
            &BTreeMap::new(),
            sock_dir.path(),
            info_dir.path(),
        );
        assert_eq!(live.keys().collect::<Vec<_>>(), vec!["visible"]);
        assert_eq!(resurrectable.keys().collect::<Vec<_>>(), vec!["exited"]);
        let read = zellij_utils::sessions::read_live_session_states(
            "me",
            sock_dir.path(),
            info_dir.path(),
        );
        assert_eq!(read.keys().collect::<Vec<_>>(), vec!["visible"]);
    }

    #[test]
    fn atomic_file_write_replaces_contents_without_leaving_a_temporary_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("session-state");
        std::fs::write(&path, b"old").unwrap();

        write_file_atomically(&path, b"new").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        let entries = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(entries, 1);
    }

    #[test]
    fn concurrent_atomic_writes_never_expose_mixed_contents() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("session-state");
        std::fs::write(&path, vec![b'Z'; 4096]).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let stop = Arc::new(AtomicBool::new(false));
        let torn = Arc::new(AtomicBool::new(false));
        let reader = {
            let path = path.clone();
            let stop = Arc::clone(&stop);
            let torn = Arc::clone(&torn);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match std::fs::read(&path) {
                        Ok(buf) if buf.len() == 4096 && buf.iter().all(|byte| *byte == buf[0]) => {
                        },
                        Ok(_) => {
                            torn.store(true, Ordering::Relaxed);
                            break;
                        },
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            torn.store(true, Ordering::Relaxed);
                            break;
                        },
                        Err(_) => {},
                    }
                }
            })
        };
        let handles = (0..8)
            .map(|index| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let contents = vec![b'a' + index; 4096];
                    barrier.wait();
                    write_file_atomically(&path, &contents).unwrap();
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().unwrap();

        assert!(
            !torn.load(Ordering::Relaxed),
            "a reader observed a partial or mixed state file"
        );
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), 4096);
        assert!(contents.windows(2).all(|window| window[0] == window[1]));
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn a_reader_during_save_sees_only_whole_files() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("session-state");
        let width = 1024 * 1024;
        std::fs::write(&path, vec![b'Z'; width]).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let torn = Arc::new(AtomicBool::new(false));
        let reader = {
            let path = path.clone();
            let stop = Arc::clone(&stop);
            let torn = Arc::clone(&torn);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match std::fs::read(&path) {
                        Ok(buf) if buf.len() == width && buf.iter().all(|byte| *byte == buf[0]) => {
                        },
                        _ => {
                            torn.store(true, Ordering::Relaxed);
                            break;
                        },
                    }
                }
            })
        };
        for index in 0..4 {
            let contents = vec![b'a' + index; width];
            write_file_atomically(&path, &contents).unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().unwrap();
        assert!(
            !torn.load(Ordering::Relaxed),
            "a reader observed a partial state file during save"
        );
        assert_eq!(std::fs::read(&path).unwrap(), vec![b'd'; width]);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn a_failed_write_leaves_the_previous_file_intact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("session-state");
        std::fs::write(&path, b"previous").unwrap();
        let _restore = RestoreDirectoryMode::readonly(dir.path());

        let error = write_file_atomically(&path, b"next").unwrap_err();

        drop(_restore);
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn a_failed_rename_leaves_the_previous_file_intact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("session-state");
        std::fs::write(&path, b"previous").unwrap();
        let staged = stage_atomic_write(&path, b"next").unwrap();
        let _restore = RestoreDirectoryMode::readonly(dir.path());

        let error = persist_atomic_write(staged, &path).unwrap_err();

        // The failed rename drops its temporary while the directory is still
        // read-only, so that unlink can fail. The published file must still
        // be the previous contents. Restore write permission before cleanup.
        drop(_restore);
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(".zellij-session-")
            {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn dropping_a_staged_file_before_rename_keeps_the_previous_contents() {
        // A failed file sync returns from staging before persist. Dropping the
        // temporary is that failure's cleanup.
        let dir = tempdir().unwrap();
        let path = dir.path().join("session-state");
        std::fs::write(&path, b"previous").unwrap();
        drop(stage_atomic_write(&path, b"next").unwrap());

        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn unsupported_parent_directory_sync_is_not_a_save_failure() {
        use std::io::Error;

        assert!(directory_sync_unsupported(&Error::from_raw_os_error(
            libc::EINVAL
        )));
        assert!(directory_sync_unsupported(&Error::from_raw_os_error(
            libc::ENOTSUP
        )));
        assert!(!directory_sync_unsupported(&Error::from_raw_os_error(
            libc::EIO
        )));
    }

    #[test]
    fn a_parent_directory_sync_error_fails_without_creating_a_file() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("gone").join("session-state");

        let error = sync_parent_directory(&missing).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn saving_a_session_keeps_an_existing_layouts_creation_time() {
        let session_id = format!(
            "atomic-state-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let folder = session_info_folder_for_session(&session_id);
        let _cleanup = RemoveTree(folder.clone());
        std::fs::create_dir_all(&folder).unwrap();
        let layout_path = session_layout_cache_file_name(&session_id);
        std::fs::write(&layout_path, "layout { old }").unwrap();
        std::thread::sleep(Duration::from_millis(1100));

        write_session_state_to_disk(
            session_id.clone(),
            SessionInfo::new(session_id.clone()),
            (
                "layout { new }".to_owned(),
                BTreeMap::from([("pane.txt".to_owned(), "contents".to_owned())]),
            ),
        );

        assert_eq!(std::fs::read(&layout_path).unwrap(), b"layout { new }");
        assert_eq!(std::fs::read(folder.join("pane.txt")).unwrap(), b"contents");
        let recorded = zellij_utils::sessions::session_layout_creation_time(&layout_path).unwrap();
        assert!(
            recorded.elapsed().unwrap() >= Duration::from_secs(1),
            "the save replaced the layout but must not reset its creation time: {:?}",
            recorded.elapsed()
        );
        let leftovers = std::fs::read_dir(&folder)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".zellij-session-")
            })
            .count();
        assert_eq!(leftovers, 0);
    }

    #[test]
    fn a_dead_sessions_age_survives_replacing_its_layout_file() {
        let sock_dir = tempdir().unwrap();
        let info_dir = tempdir().unwrap();
        write_layout(info_dir.path(), "long-running");
        let layout = info_dir.path().join("long-running/session-layout.kdl");
        // the session started two hours ago
        let started = std::time::SystemTime::now() - Duration::from_secs(2 * 3600);
        let started_secs = started
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        std::fs::write(
            layout.with_file_name("session-layout-created"),
            started_secs.to_string(),
        )
        .unwrap();

        // every later save replaces the file, which gets a fresh timestamp
        write_file_atomically(&layout, b"layout { pane }").unwrap();
        zellij_utils::sessions::record_session_layout_creation_time(&layout);

        let (_, resurrectable) = scan_session_list(
            "me",
            &[],
            &BTreeMap::new(),
            sock_dir.path(),
            info_dir.path(),
        );
        assert!(resurrectable["long-running"] >= Duration::from_secs(2 * 3600 - 10));
    }

    struct RestoreDirectoryMode(std::path::PathBuf);

    impl RestoreDirectoryMode {
        fn readonly(path: &std::path::Path) -> Self {
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o555);
            std::fs::set_permissions(path, perms).unwrap();
            Self(path.to_path_buf())
        }
    }

    impl Drop for RestoreDirectoryMode {
        fn drop(&mut self) {
            if let Ok(metadata) = std::fs::metadata(&self.0) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o755);
                let _ = std::fs::set_permissions(&self.0, perms);
            }
        }
    }

    struct RemoveTree(std::path::PathBuf);

    impl Drop for RemoveTree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

async fn run_in_background(
    command: PathBuf,
    args: Vec<String>,
    cwd: Option<PathBuf>,
    env: PaneEnv,
) {
    let mut process = tokio::process::Command::new(&command);
    process
        .args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    for (name, value) in env {
        match value {
            Some(value) => process.env(name, value),
            None => process.env_remove(name),
        };
    }
    if let Some(cwd) = cwd {
        if cwd.is_dir() {
            process.current_dir(cwd);
        } else {
            log::error!(
                "Cannot run '{}' in '{}': not a folder",
                command.display(),
                cwd.display()
            );
        }
    }
    match process.output().await {
        Ok(output) if !output.status.success() => {
            log::error!(
                "Background command '{}' exited with {}: {}",
                command.display(),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim_end()
            );
        },
        Ok(_) => {},
        Err(e) => {
            log::error!(
                "Failed to start background command '{}': {}",
                command.display(),
                e
            );
        },
    }
}

#[cfg(test)]
mod render_schedule_tests {
    use super::*;

    #[test]
    fn a_lone_request_renders_after_the_quiet_period() {
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        assert!(schedule.request(start));
        assert_eq!(
            schedule.step(start),
            RenderStep::WaitFor(RENDER_QUIET_PERIOD)
        );
        assert_eq!(
            schedule.step(start + RENDER_QUIET_PERIOD),
            RenderStep::Render
        );
        assert_eq!(schedule.step(start + RENDER_QUIET_PERIOD), RenderStep::Idle);
    }

    #[test]
    fn requests_arriving_within_the_quiet_period_postpone_the_render() {
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        assert!(schedule.request(start));
        let later = start + RENDER_QUIET_PERIOD / 2;
        assert!(!schedule.request(later));
        assert_eq!(
            schedule.step(start + RENDER_QUIET_PERIOD),
            RenderStep::WaitFor(RENDER_QUIET_PERIOD / 2)
        );
        assert_eq!(
            schedule.step(later + RENDER_QUIET_PERIOD),
            RenderStep::Render
        );
    }

    #[test]
    fn a_redraw_split_by_a_short_stall_under_load_renders_once() {
        let stall = Duration::from_millis(3);
        assert!(stall < RENDER_QUIET_PERIOD);
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        assert!(schedule.request(start));
        assert_ne!(schedule.step(start + stall), RenderStep::Render);
        assert!(!schedule.request(start + stall));
        assert_eq!(
            schedule.step(start + stall + RENDER_QUIET_PERIOD),
            RenderStep::Render
        );
    }

    #[test]
    fn continuous_requests_still_render_once_the_maximum_delay_passes() {
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        let step = RENDER_QUIET_PERIOD / 2;
        let mut now = start;
        schedule.request(now);
        while now < start + RENDER_MAX_DELAY {
            assert_ne!(schedule.step(now), RenderStep::Render);
            now += step;
            schedule.request(now);
        }
        assert_eq!(schedule.step(start + RENDER_MAX_DELAY), RenderStep::Render);
        assert!(schedule.request(now));
    }

    #[test]
    fn exiting_stops_the_scheduler_even_with_a_pending_render() {
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        schedule.request(start);
        schedule.exiting = true;
        assert_eq!(schedule.step(start + RENDER_MAX_DELAY), RenderStep::Exit);
    }

    #[test]
    fn an_immediate_request_on_an_idle_schedule_renders_at_once() {
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        schedule.request_now(start);
        assert_eq!(schedule.step(start), RenderStep::Render);
        assert_eq!(schedule.step(start), RenderStep::Idle);
    }

    #[test]
    fn an_immediate_request_makes_a_pending_render_due_at_once() {
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        assert!(schedule.request(start));
        let later = start + RENDER_QUIET_PERIOD / 4;
        assert_eq!(
            schedule.step(later),
            RenderStep::WaitFor(RENDER_QUIET_PERIOD * 3 / 4)
        );
        schedule.request_now(later);
        assert_eq!(schedule.step(later), RenderStep::Render);
        assert_eq!(schedule.step(later), RenderStep::Idle);
    }

    #[test]
    fn a_normal_request_after_an_immediate_render_waits_for_the_quiet_period_again() {
        let start = Instant::now();
        let mut schedule = RenderSchedule::default();
        schedule.request_now(start);
        assert_eq!(schedule.step(start), RenderStep::Render);
        let later = start + RENDER_QUIET_PERIOD;
        assert!(schedule.request(later));
        assert_eq!(
            schedule.step(later),
            RenderStep::WaitFor(RENDER_QUIET_PERIOD)
        );
        assert_eq!(
            schedule.step(later + RENDER_QUIET_PERIOD),
            RenderStep::Render
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundCommandOutput {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug)]
pub enum BackgroundCommandError {
    Spawn(std::io::Error),
    Io(std::io::Error),
    TimedOut,
}

impl std::fmt::Display for BackgroundCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackgroundCommandError::Spawn(e) => write!(f, "failed to start command: {}", e),
            BackgroundCommandError::Io(e) => write!(f, "failed to run command: {}", e),
            BackgroundCommandError::TimedOut => write!(f, "command timed out"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BackgroundCommand {
    pub program: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: Option<PathBuf>,
    pub input: Option<Vec<u8>>,
    pub timeout: Option<Duration>,
}

pub async fn run_background_command(
    command: BackgroundCommand,
) -> std::result::Result<BackgroundCommandOutput, BackgroundCommandError> {
    use tokio::io::AsyncWriteExt;
    let mut process = tokio::process::Command::new(&command.program);
    process
        .args(&command.args)
        .envs(&command.env)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(cwd) = &command.cwd {
        process.current_dir(cwd);
    }
    if command.input.is_some() {
        process.stdin(std::process::Stdio::piped());
    } else {
        process.stdin(std::process::Stdio::null());
    }
    let mut child = process.spawn().map_err(BackgroundCommandError::Spawn)?;
    if let (Some(input), Some(mut stdin)) = (command.input.clone(), child.stdin.take()) {
        tokio::spawn(async move {
            let _ = stdin.write_all(&input).await;
            let _ = stdin.shutdown().await;
        });
    }
    let output = child.wait_with_output();
    let output = match command.timeout {
        Some(timeout) => match tokio::time::timeout(timeout, output).await {
            Ok(output) => output,
            Err(_) => return Err(BackgroundCommandError::TimedOut),
        },
        None => output.await,
    }
    .map_err(BackgroundCommandError::Io)?;
    Ok(BackgroundCommandOutput {
        exit_code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

pub fn run_background_command_blocking(
    command: BackgroundCommand,
) -> std::result::Result<BackgroundCommandOutput, BackgroundCommandError> {
    crate::global_async_runtime::get_tokio_runtime().block_on(run_background_command(command))
}
