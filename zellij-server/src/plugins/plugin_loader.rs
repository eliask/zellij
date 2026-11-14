use crate::plugins::plugin_map::{
    PluginEnv, PluginMap, PluginMetadata, RunningPlugin, SharedEnv, VecDequeInputStream,
    WriteOutputStream,
};
use crate::plugins::plugin_worker::{plugin_worker, RunningWorker};
use crate::plugins::wasm_bridge::{LoadingContext, PluginCache};
use crate::plugins::zellij_exports::{wasi_write_object, zellij_exports};
use crate::plugins::PluginId;
use prost::Message;
use std::io::{self, Write};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use wasmi::{Engine, Instance, Linker, Module, Store, StoreLimits};
use wasmi_wasi::sync::WasiCtxBuilder;
use wasmi_wasi::wasi_common::pipe::{ReadPipe, WritePipe};
use wasmi_wasi::Dir;
use wasmi_wasi::WasiCtx;

use crate::{
    logging_pipe::LoggingPipe, thread_bus::ThreadSenders,
    ui::loading_indication::LoadingIndication, ClientId, SharedKeybinds,
};

use zellij_utils::data::{Event, FileMetadata};
use zellij_utils::plugin_api::action::ProtobufPluginConfiguration;
use zellij_utils::plugin_api::event::event_to_protobuf_with_keybinds;
use zellij_utils::{
    consts::ZELLIJ_TMP_DIR, data::InputMode, errors::prelude::*, input::command::TerminalAction,
    input::plugins::PluginConfig, pane_size::Size,
};

/// Open a directory as a `File` handle for WASI pre-opening.
/// On Windows, `FILE_FLAG_BACKUP_SEMANTICS` is required to open directories.
#[cfg(not(windows))]
fn open_dir(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

#[cfg(windows)]
fn open_dir(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x02000000) // FILE_FLAG_BACKUP_SEMANTICS
        .open(path)
}

fn create_plugin_fs_entries(plugin_own_data_dir: &PathBuf, plugin_own_cache_dir: &PathBuf) {
    // Create filesystem entries mounted into WASM.
    // We create them here to get expressive error messages in case they fail.
    if let Err(e) = fs::create_dir_all(&plugin_own_data_dir) {
        log::error!("Failed to create plugin data dir: {}", e);
    };
    if let Err(e) = fs::create_dir_all(&plugin_own_cache_dir) {
        log::error!("Failed to create plugin cache dir: {}", e);
    }
    if let Err(e) = fs::create_dir_all(ZELLIJ_TMP_DIR.as_path()) {
        log::error!("Failed to create plugin tmp dir: {}", e);
    }
}

pub struct PluginLoader<'a> {
    skip_cache: bool,
    plugin_id: PluginId,
    client_id: ClientId,
    plugin_cwd: PathBuf,
    plugin_own_data_dir: PathBuf,
    plugin_own_cache_dir: PathBuf,
    plugin_config: PluginConfig,
    tab_index: Option<usize>,
    path_to_default_shell: PathBuf,
    session_env_vars: std::collections::BTreeMap<String, String>,
    default_shell: Option<TerminalAction>,
    layout_dir: Option<PathBuf>,
    default_mode: InputMode,
    keybinds: SharedKeybinds,
    plugin_dir: PathBuf,
    size: Size,
    loading_indication: LoadingIndication,
    senders: ThreadSenders,
    engine: Engine,
    plugin_cache: PluginCache,
    plugin_map: &'a mut PluginMap, // we receive a mutable reference rather than the Arc so that it
    // will be held for the lifetime of this struct and thus loading
    // plugins for all connected clients will be one transaction
    connected_clients: Option<Arc<Mutex<Vec<ClientId>>>>,
}

impl<'a> PluginLoader<'a> {
    pub fn new(
        skip_cache: bool,
        loading_context: LoadingContext,
        senders: ThreadSenders,
        engine: Engine,
        plugin_cache: PluginCache,
        plugin_map: &'a mut PluginMap,
        connected_clients: Arc<Mutex<Vec<ClientId>>>,
    ) -> Self {
        let loading_indication = LoadingIndication::new("".into());
        create_plugin_fs_entries(
            &loading_context.plugin_own_data_dir,
            &loading_context.plugin_own_cache_dir,
        );
        Self {
            plugin_id: loading_context.plugin_id,
            client_id: loading_context.client_id,
            plugin_cwd: loading_context.plugin_cwd,
            plugin_own_data_dir: loading_context.plugin_own_data_dir,
            plugin_own_cache_dir: loading_context.plugin_own_cache_dir,
            plugin_config: loading_context.plugin_config,
            tab_index: loading_context.tab_index,
            path_to_default_shell: loading_context.path_to_default_shell,
            session_env_vars: loading_context.session_env_vars,
            default_shell: loading_context.default_shell,
            layout_dir: loading_context.layout_dir,
            default_mode: loading_context.default_mode,
            keybinds: loading_context.keybinds,
            plugin_dir: loading_context.plugin_dir,
            size: loading_context.size,

            skip_cache,
            senders,
            engine,
            plugin_cache,
            plugin_map,
            connected_clients: Some(connected_clients),
            loading_indication,
        }
    }
    pub fn without_connected_clients(mut self) -> Self {
        self.connected_clients = None;
        self
    }
    pub fn start_plugin(&mut self) -> Result<()> {
        self.record_plugin_metadata();
        let module = if self.skip_cache {
            self.interpret_module()?
        } else {
            self.load_module_from_memory()
                .or_else(|_e| self.interpret_module())?
        };
        let (store, instance) = self.create_plugin_environment(module)?;
        self.load_plugin_instance(store, &instance, false)?;
        self.clone_instance_for_other_clients()?;
        Ok(())
    }
    pub fn start_shared_plugin(&mut self) -> Result<Arc<Mutex<RunningPlugin>>> {
        let module = if self.skip_cache {
            self.interpret_module()?
        } else {
            self.load_module_from_memory()
                .or_else(|_e| self.interpret_module())?
        };
        let (mut store, instance) = self.create_plugin_environment(module)?;
        store.data_mut().shared = Some(SharedEnv::default());
        self.load_plugin_instance(store, &instance, true)
    }
    fn record_plugin_metadata(&mut self) {
        self.plugin_map.insert_metadata(
            self.plugin_id,
            PluginMetadata {
                plugin_config: self.plugin_config.clone(),
                tab_index: self.tab_index,
                rows: self.size.rows,
                columns: self.size.cols,
                cwd: self.plugin_cwd.clone(),
                is_background: self.tab_index.is_none(),
            },
        );
    }
    fn interpret_module(&mut self) -> Result<Module> {
        self.loading_indication.override_previous_error();
        let wasm_bytes = self.plugin_config.resolve_wasm_bytes(&self.plugin_dir)?;
        let timer = std::time::Instant::now();
        let module = Module::new(&self.engine, &wasm_bytes)?;
        log::info!(
            "Loaded plugin '{}' in {:?}",
            self.plugin_config.path.display(),
            timer.elapsed()
        );
        Ok(module)
    }
    fn load_module_from_memory(&mut self) -> Result<Module> {
        let module = self
            .plugin_cache
            .lock()
            .unwrap()
            .remove(&self.plugin_config.path) // TODO: do we still bring it back later?
            // maybe we can forgo this dance?
            .ok_or(anyhow!("Plugin is not stored in memory"))?;
        Ok(module)
    }
    fn load_plugin_instance(
        &mut self,
        mut store: Store<PluginEnv>,
        instance: &Instance,
        shared: bool,
    ) -> Result<Arc<Mutex<RunningPlugin>>> {
        let err_context = || format!("failed to load plugin from instance {instance:#?}");
        let main_user_instance = instance.clone();
        let start_function = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .with_context(err_context)?;
        let load_function = instance
            .get_typed_func::<(), ()>(&mut store, "load")
            .with_context(err_context)?;
        let mut workers = HashMap::new();
        // Each `_worker` export builds another store and instance, and `.instances(1)`
        // applies only inside that store. Count the exports before any of those stores
        // exist so a long export list cannot multiply plugin memory.
        let mut worker_names = Vec::new();
        for export in instance.exports(&mut store) {
            let name = export.name().to_owned();
            if name.ends_with("_worker") && export.into_func().is_some() {
                worker_names.push(name);
            }
        }
        enforce_worker_cap(self.plugin_id, worker_names.len())?;
        for function_name in worker_names {
            let (mut store, instance) = self.create_plugin_instance_and_wasi_env_for_worker()?;
            let start_function_for_worker = instance
                .get_typed_func::<(), ()>(&mut store, "_start")
                .with_context(err_context)?;
            crate::plugins::shared::refuel_for_callback(&mut store);
            start_function_for_worker
                .call(&mut store, ())
                .with_context(err_context)?;

            let worker = RunningWorker::new(store, instance, &function_name);
            let worker_sender = plugin_worker(worker);
            workers.insert(function_name, worker_sender);
        }

        let subscriptions = store.data().subscriptions.clone();
        let in_blocking_call = store
            .data()
            .shared
            .as_ref()
            .map(|shared| shared.in_blocking_call.clone());
        let plugin = Arc::new(Mutex::new(RunningPlugin::new(
            store,
            main_user_instance,
            self.size.rows,
            self.size.cols,
        )));
        if shared {
            self.plugin_map.insert_shared(
                self.plugin_id,
                (plugin.clone(), subscriptions, workers),
                in_blocking_call,
            );
        } else {
            self.plugin_map.insert(
                self.plugin_id,
                self.client_id,
                plugin.clone(),
                subscriptions,
                workers,
            );
        }

        crate::plugins::shared::refuel_for_callback(&mut plugin.lock().unwrap().store);
        start_function
            .call(&mut plugin.lock().unwrap().store, ())
            .with_context(err_context)?;

        let protobuf_plugin_configuration: ProtobufPluginConfiguration = self
            .plugin_config
            .initial_userspace_configuration
            .clone()
            .try_into()
            .map_err(|e| anyhow!("Failed to serialize user configuration: {:?}", e))?;
        let protobuf_bytes = protobuf_plugin_configuration.encode_to_vec();
        wasi_write_object(plugin.lock().unwrap().store.data(), &protobuf_bytes)
            .with_context(err_context)?;
        crate::plugins::shared::refuel_for_callback(&mut plugin.lock().unwrap().store);
        load_function
            .call(&mut plugin.lock().unwrap().store, ())
            .with_context(err_context)?;

        Ok(plugin)
    }
    pub fn create_plugin_environment(
        &self,
        module: Module,
    ) -> Result<(Store<PluginEnv>, Instance)> {
        let err_context = || {
            format!(
                "Failed to create instance, plugin env and subscriptions for plugin {}",
                self.plugin_id
            )
        };
        let stdin_pipe = Arc::new(Mutex::new(VecDeque::new()));
        let stdout_pipe = Arc::new(Mutex::new(VecDeque::new()));

        let wasi_ctx = PluginLoader::create_wasi_ctx(
            &self.plugin_cwd,
            &self.plugin_own_data_dir,
            &self.plugin_own_cache_dir,
            &ZELLIJ_TMP_DIR,
            &self.plugin_config.location.to_string(),
            self.plugin_id,
            stdin_pipe.clone(),
            stdout_pipe.clone(),
        )?;
        let plugin_path = self.plugin_config.path.clone();
        let plugin_env = PluginEnv {
            plugin_id: self.plugin_id,
            client_id: self.client_id,
            plugin: self.plugin_config.clone(), // TODO: change field name in PluginEnv to plugin_config
            permissions: Arc::new(Mutex::new(None)),
            senders: self.senders.clone(),
            wasi_ctx,
            plugin_own_data_dir: self.plugin_own_data_dir.clone(),
            plugin_own_cache_dir: self.plugin_own_cache_dir.clone(),
            path_to_default_shell: self.path_to_default_shell.clone(),
            default_shell: self.default_shell.clone(),
            plugin_cwd: self.plugin_cwd.clone(),
            session_env_vars: self.session_env_vars.clone(),
            input_pipes_to_unblock: Arc::new(Mutex::new(HashSet::new())),
            input_pipes_to_block: Arc::new(Mutex::new(HashSet::new())),
            layout_dir: self.layout_dir.clone(),
            default_mode: self.default_mode.clone(),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
            keybinds: self.keybinds.clone(),
            intercepting_key_presses: false,
            pending_popup_size: None,
            stdin_pipe,
            stdout_pipe,
            store_limits: create_optimized_store_limits(),
            shared: None,
        };
        let mut store = Store::new(&self.engine, plugin_env);

        // Apply optimized resource limits for memory efficiency
        store.limiter(|plugin_env| &mut plugin_env.store_limits);

        let mut linker = Linker::new(&self.engine);
        wasmi_wasi::add_to_linker(&mut linker, |plugin_env: &mut PluginEnv| {
            &mut plugin_env.wasi_ctx
        })?;
        zellij_exports(&mut linker);

        let instance = crate::plugins::shared::instantiate_metered(&linker, &mut store, &module)
            .with_context(err_context)?;

        if let Some(func) = instance.get_func(&mut store, "_initialize") {
            if let Ok(typed_func) = func.typed::<(), ()>(&store) {
                crate::plugins::shared::refuel_for_callback(&mut store);
                let _ = typed_func.call(&mut store, ());
            }
        }

        self.plugin_cache
            .lock()
            .unwrap()
            .insert(plugin_path.clone(), module);
        Ok((store, instance))
    }
    pub fn clone_instance_for_other_clients(&mut self) -> Result<()> {
        let Some(connected_clients) = self.connected_clients.as_ref() else {
            return Ok(());
        };
        let connected_clients: Vec<ClientId> =
            connected_clients.lock().unwrap().iter().copied().collect();
        if !connected_clients.is_empty() {
            self.connected_clients = None; // so we don't have infinite loops
            for client_id in connected_clients {
                if client_id == self.client_id {
                    // don't reload the plugin once more for ourselves
                    continue;
                }
                self.client_id = client_id;
                self.start_plugin()?;
            }
        }
        Ok(())
    }
    pub fn create_plugin_instance_and_wasi_env_for_worker(
        &self,
    ) -> Result<(Store<PluginEnv>, Instance)> {
        let plugin_id = self.plugin_id;
        let err_context = || {
            format!(
                "Failed to create instance and plugin env for worker {}",
                plugin_id
            )
        };
        let module = self
            .plugin_cache
            .lock()
            .unwrap()
            .get(&self.plugin_config.path)
            .with_context(err_context)?
            .clone();
        let (store, instance) = self.create_plugin_instance_env(&module)?;
        Ok((store, instance))
    }
    fn create_plugin_instance_env(&self, module: &Module) -> Result<(Store<PluginEnv>, Instance)> {
        let err_context = || {
            format!(
                "Failed to create instance, plugin env and subscriptions for plugin {}",
                self.plugin_id
            )
        };
        let stdin_pipe = Arc::new(Mutex::new(VecDeque::new()));
        let stdout_pipe = Arc::new(Mutex::new(VecDeque::new()));

        let wasi_ctx = PluginLoader::create_wasi_ctx(
            &self.plugin_cwd,
            &self.plugin_own_data_dir,
            &self.plugin_own_cache_dir,
            &ZELLIJ_TMP_DIR,
            &self.plugin_config.location.to_string(),
            self.plugin_id,
            stdin_pipe.clone(),
            stdout_pipe.clone(),
        )?;
        let plugin_config = self.plugin_config.clone();
        let plugin_env = PluginEnv {
            plugin_id: self.plugin_id,
            client_id: self.client_id,
            plugin: plugin_config,
            permissions: Arc::new(Mutex::new(None)),
            senders: self.senders.clone(),
            wasi_ctx,
            plugin_own_data_dir: self.plugin_own_data_dir.clone(),
            plugin_own_cache_dir: self.plugin_own_cache_dir.clone(),
            path_to_default_shell: self.path_to_default_shell.clone(),
            default_shell: self.default_shell.clone(),
            plugin_cwd: self.plugin_cwd.clone(),
            session_env_vars: self.session_env_vars.clone(),
            input_pipes_to_unblock: Arc::new(Mutex::new(HashSet::new())),
            input_pipes_to_block: Arc::new(Mutex::new(HashSet::new())),
            layout_dir: self.layout_dir.clone(),
            default_mode: self.default_mode.clone(),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
            keybinds: self.keybinds.clone(),
            intercepting_key_presses: false,
            pending_popup_size: None,
            stdin_pipe,
            stdout_pipe,
            store_limits: create_optimized_store_limits(),
            shared: None,
        };
        let mut store = Store::new(&self.engine, plugin_env);

        // Apply optimized resource limits for memory efficiency
        store.limiter(|plugin_env| &mut plugin_env.store_limits);

        let mut linker = Linker::new(&self.engine);
        wasmi_wasi::add_to_linker(&mut linker, |plugin_env: &mut PluginEnv| {
            &mut plugin_env.wasi_ctx
        })?;
        zellij_exports(&mut linker);

        let instance = crate::plugins::shared::instantiate_metered(&linker, &mut store, module)
            .with_context(err_context)?;

        if let Some(func) = instance.get_func(&mut store, "_initialize") {
            if let Ok(typed_func) = func.typed::<(), ()>(&store) {
                crate::plugins::shared::refuel_for_callback(&mut store);
                let _ = typed_func.call(&mut store, ());
            }
        }

        Ok((store, instance))
    }
    pub fn create_wasi_ctx(
        host_dir: &PathBuf,
        data_dir: &PathBuf,
        cache_dir: &PathBuf,
        tmp_dir: &PathBuf,
        plugin_url: &String,
        plugin_id: PluginId,
        stdin_pipe: Arc<Mutex<VecDeque<u8>>>,
        stdout_pipe: Arc<Mutex<VecDeque<u8>>>,
    ) -> Result<WasiCtx> {
        let _err_context = || format!("Failed to create wasi_ctx");
        let dirs = vec![
            ("/host".to_owned(), host_dir.clone()),
            ("/data".to_owned(), data_dir.clone()),
            ("/cache".to_owned(), cache_dir.clone()),
            ("/tmp".to_owned(), tmp_dir.clone()),
        ];
        let dirs = dirs.into_iter().filter(|(_dir_name, dir)| {
            // note that this does not protect against TOCTOU errors
            // eg. if one or more of these folders existed at the time of check but was deleted
            // before we mounted in the wasi environment, we'll crash
            // when we move to a new wasi environment, we should address this with locking if
            // there's no built-in solution
            dir.try_exists().ok().unwrap_or(false)
        });

        let mut builder = WasiCtxBuilder::new();
        builder.inherit_env()?;

        // Mount directories using the builder
        for (guest_path, host_path) in dirs {
            match open_dir(&host_path) {
                Ok(dir_file) => {
                    let dir = Dir::from_std_file(dir_file);
                    builder.preopened_dir(dir, guest_path)?;
                },
                Err(e) => {
                    log::warn!("Failed to mount directory {:?}: {}", host_path, e);
                },
            }
        }

        let ctx = builder.build();

        // Set up custom stdin/stdout/stderr.
        // Stdout is drained only when the host renders, so guest writes have to stop at
        // the cap even if several callbacks return without rendering. A write that does
        // not fit is refused whole: the pipe is not grown and the host call fails.
        ctx.set_stdin(Box::new(ReadPipe::new(VecDequeInputStream(
            stdin_pipe.clone(),
        ))));
        ctx.set_stdout(Box::new(WritePipe::new(CappedHostPipe::new(
            stdout_pipe.clone(),
            "stdout",
        ))));
        ctx.set_stderr(Box::new(WritePipe::new(WriteOutputStream(Arc::new(
            Mutex::new(LoggingPipe::new(plugin_url, plugin_id)),
        )))));

        Ok(ctx)
    }
}

/// How many tables one plugin store may create. This is a count, not an element budget.
const MAX_TABLES_PER_STORE: usize = 16;
/// Element budget for one store.
///
/// Wasmi 1.1 `StoreLimitsBuilder::table_elements` applies to each table and defaults to
/// unlimited, so a one-page module can still ask for a huge initial table under the 16 MiB
/// linear-memory cap. The per-table cap is this total divided by [`MAX_TABLES_PER_STORE`]:
/// sixteen full tables are at most 1_048_576 elements (8 bytes each without simd). The check
/// runs for the initial minimum, before those elements are allocated, and for later growth.
const MAX_TABLE_ELEMENTS_TOTAL: usize = 1_048_576;
const MAX_TABLE_ELEMENTS: usize = MAX_TABLE_ELEMENTS_TOTAL / MAX_TABLES_PER_STORE;

/// `_worker` exports that one load may instantiate. Each one is another store.
const MAX_PLUGIN_WORKERS: usize = 8;

/// Bytes one WASI host pipe may retain.
///
/// Stdout is drained only when the host renders, so a later callback must still
/// fail once this many bytes are sitting in the pipe. Stdin appends from the host
/// use the same check.
pub(crate) const WASI_HOST_PIPE_CAP: usize = 256 * 1024;

/// One directory scan reads at most this many entries, then reports the listing incomplete.
pub(crate) const MAX_DIRECTORY_SCAN_ENTRIES: usize = 4096;

/// Byte length of the stdin frame `apply_event_to_plugin` writes for `event`
/// with no keybind table: protobuf bytes as JSON, plus the `\r\n` terminator.
pub(crate) fn host_stdin_frame_len(event: &Event) -> Option<usize> {
    let protobuf = event_to_protobuf_with_keybinds(event.clone(), None).ok()?;
    let json = serde_json::to_string(&protobuf.encode_to_vec()).ok()?;
    Some(json.len().saturating_add(2))
}

fn event_fits_host_pipe(event: &Event) -> bool {
    host_stdin_frame_len(event).is_some_and(|len| len <= WASI_HOST_PIPE_CAP)
}

/// Read-order prefix of `entries` whose `FileSystemUpdateIncomplete` stdin frame fits
/// [`WASI_HOST_PIPE_CAP`]. The incomplete payload tag is wider than the update tag, so the
/// frame is measured on the event that will actually be sent. One entry that cannot fit
/// becomes an empty list.
fn incomplete_listing_prefix(
    entries: Vec<(PathBuf, Option<FileMetadata>)>,
) -> Vec<(PathBuf, Option<FileMetadata>)> {
    let total = entries.len();
    if total == 0 || event_fits_host_pipe(&Event::FileSystemUpdateIncomplete(entries.clone())) {
        return entries;
    }
    let mut lo = 0usize;
    let mut hi = total;
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if event_fits_host_pipe(&Event::FileSystemUpdateIncomplete(entries[..mid].to_vec())) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    // Frame length is normally monotonic, but a varint boundary can move it.
    // Walk back to a prefix that fits, then forward while another entry still does.
    while lo > 0
        && !event_fits_host_pipe(&Event::FileSystemUpdateIncomplete(entries[..lo].to_vec()))
    {
        lo -= 1;
    }
    while lo < total
        && event_fits_host_pipe(&Event::FileSystemUpdateIncomplete(
            entries[..lo + 1].to_vec(),
        ))
    {
        lo += 1;
    }
    if lo == 0 {
        log::warn!(
            "FileSystemUpdate entry does not fit in the host pipe cap of {WASI_HOST_PIPE_CAP} bytes; sending an empty incomplete listing"
        );
        return Vec::new();
    }
    if lo < total {
        log::warn!(
            "FileSystemUpdate trimmed from {total} entries to {lo} so its stdin frame fits the host pipe cap of {WASI_HOST_PIPE_CAP} bytes"
        );
    }
    entries.into_iter().take(lo).collect()
}

/// Filesystem listing delivered to plugins.
///
/// A listing that fits whole is `FileSystemUpdate`. A listing stopped by the directory
/// entry cap, or trimmed so its stdin frame fits, is `FileSystemUpdateIncomplete` and is
/// not labeled as a complete update. `stopped_early` means the producer did not read
/// every entry.
pub(crate) fn filesystem_listing_event(
    entries: Vec<(PathBuf, Option<FileMetadata>)>,
    stopped_early: bool,
) -> Event {
    if !stopped_early && event_fits_host_pipe(&Event::FileSystemUpdate(entries.clone())) {
        return Event::FileSystemUpdate(entries);
    }
    Event::FileSystemUpdateIncomplete(incomplete_listing_prefix(entries))
}

fn enforce_worker_cap(plugin_id: PluginId, worker_count: usize) -> Result<()> {
    if worker_count > MAX_PLUGIN_WORKERS {
        return Err(anyhow!(
            "plugin {plugin_id} exports {worker_count} `_worker` functions, which exceeds the cap of {MAX_PLUGIN_WORKERS}"
        ));
    }
    Ok(())
}

/// Append `bytes` to a host pipe, or refuse the whole write once it would pass the cap.
/// Nothing is stored when the call fails, so a short write cannot be reported as success.
pub(crate) fn append_capped_host_pipe(
    buffer: &mut VecDeque<u8>,
    bytes: &[&[u8]],
    direction: &str,
) -> io::Result<usize> {
    let mut total = 0usize;
    for chunk in bytes {
        total = match total.checked_add(chunk.len()) {
            Some(total) => total,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::QuotaExceeded,
                    format!("{direction} host buffer cap of {WASI_HOST_PIPE_CAP} bytes exceeded"),
                ));
            },
        };
    }
    if total == 0 {
        return Ok(0);
    }
    let len = buffer.len();
    if len.saturating_add(total) > WASI_HOST_PIPE_CAP {
        return Err(io::Error::new(
            io::ErrorKind::QuotaExceeded,
            format!(
                "{direction} host buffer cap of {WASI_HOST_PIPE_CAP} bytes exceeded (holding {len}, refusing {total})"
            ),
        ));
    }
    if buffer.try_reserve(total).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{direction} host buffer could not reserve {total} bytes"),
        ));
    }
    for chunk in bytes {
        buffer.extend(*chunk);
    }
    Ok(total)
}

/// Guest stdout shares the `VecDeque` the renderer drains.
struct CappedHostPipe {
    buffer: Arc<Mutex<VecDeque<u8>>>,
    direction: &'static str,
}

impl CappedHostPipe {
    fn new(buffer: Arc<Mutex<VecDeque<u8>>>, direction: &'static str) -> Self {
        Self { buffer, direction }
    }
}

impl Write for CappedHostPipe {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self.buffer.lock().unwrap();
        append_capped_host_pipe(&mut guard, &[buf], self.direction)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let mut guard = self.buffer.lock().unwrap();
        let chunks: Vec<&[u8]> = bufs.iter().map(|buf| &buf[..]).collect();
        append_capped_host_pipe(&mut guard, &chunks, self.direction)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn create_optimized_store_limits() -> StoreLimits {
    use wasmi::StoreLimitsBuilder;
    StoreLimitsBuilder::new()
        .instances(1) // One instance per plugin
        .memories(4) // Max 4 linear memories per plugin
        .memory_size(16 * 1024 * 1024) // 16MB per memory maximum
        .tables(MAX_TABLES_PER_STORE)
        .table_elements(MAX_TABLE_ELEMENTS)
        .trap_on_grow_failure(true) // Fail fast on resource exhaustion
        .build()
}

#[cfg(test)]
mod host_bound_tests {
    use super::*;
    use std::io::Write;
    use wasmi::ResourceLimiter;

    #[test]
    fn table_element_limit_rejects_growth_past_the_store_budget() {
        let mut limits = create_optimized_store_limits();
        assert_eq!(limits.tables(), MAX_TABLES_PER_STORE);
        assert_eq!(
            MAX_TABLES_PER_STORE * MAX_TABLE_ELEMENTS,
            MAX_TABLE_ELEMENTS_TOTAL
        );
        assert!(limits.table_growing(0, MAX_TABLE_ELEMENTS, None).is_ok());
        assert!(limits
            .table_growing(MAX_TABLE_ELEMENTS, MAX_TABLE_ELEMENTS + 1, None)
            .is_err());
    }

    #[test]
    fn worker_cap_rejects_the_ninth_worker_export() {
        assert!(enforce_worker_cap(1, MAX_PLUGIN_WORKERS).is_ok());
        let error = enforce_worker_cap(1, MAX_PLUGIN_WORKERS + 1)
            .expect_err("the ninth _worker export has to fail the load");
        let message = format!("{error:#}");
        assert!(
            message.contains("cap of 8"),
            "the load error must name the worker cap, got {message}"
        );
        assert!(
            message.contains("_worker"),
            "the load error must name the worker exports, got {message}"
        );
    }

    #[test]
    fn wasi_buffer_stdout_write_past_the_cap_keeps_the_cap() {
        let pipe = Arc::new(Mutex::new(VecDeque::new()));
        let mut stdout = CappedHostPipe::new(pipe.clone(), "stdout");
        let filled = vec![7u8; WASI_HOST_PIPE_CAP];
        assert_eq!(
            stdout.write_vectored(&[io::IoSlice::new(&filled)]).unwrap(),
            WASI_HOST_PIPE_CAP
        );

        let error = stdout
            .write_vectored(&[io::IoSlice::new(&[9])])
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::QuotaExceeded);
        assert!(
            error.to_string().contains("stdout"),
            "the overrun error must name the direction, got {error}"
        );
        assert!(
            error.to_string().contains(&WASI_HOST_PIPE_CAP.to_string()),
            "the overrun error must name the cap, got {error}"
        );
        assert_eq!(pipe.lock().unwrap().len(), WASI_HOST_PIPE_CAP);
        assert!(pipe.lock().unwrap().iter().all(|byte| *byte == 7));

        let pipe = Arc::new(Mutex::new(VecDeque::new()));
        let mut stdout = CappedHostPipe::new(pipe.clone(), "stdout");
        stdout.write(&vec![1u8; WASI_HOST_PIPE_CAP - 1]).unwrap();
        let error = stdout.write(&[2, 3]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::QuotaExceeded);
        assert_eq!(pipe.lock().unwrap().len(), WASI_HOST_PIPE_CAP - 1);
        assert!(pipe.lock().unwrap().iter().all(|byte| *byte == 1));

        let mut stdin = VecDeque::new();
        let body = vec![4u8; WASI_HOST_PIPE_CAP - 2];
        assert_eq!(
            append_capped_host_pipe(&mut stdin, &[&body, b"\r\n"], "stdin").unwrap(),
            WASI_HOST_PIPE_CAP
        );
        let error = append_capped_host_pipe(&mut stdin, &[b"x", b"\r\n"], "stdin").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::QuotaExceeded);
        assert!(error.to_string().contains("stdin"), "{error}");
        assert_eq!(stdin.len(), WASI_HOST_PIPE_CAP);
    }

    #[test]
    fn filesystem_listing_that_fits_stays_a_complete_update() {
        let entries = vec![(PathBuf::from("/host/a"), None)];
        let event = filesystem_listing_event(entries.clone(), false);
        assert_eq!(event, Event::FileSystemUpdate(entries));
        let frame = host_stdin_frame_len(&event).expect("frame");
        assert!(frame <= WASI_HOST_PIPE_CAP);
    }

    #[test]
    fn filesystem_listing_stopped_early_is_incomplete_even_when_it_fits() {
        let entries = vec![(PathBuf::from("/host/a"), None)];
        let event = filesystem_listing_event(entries.clone(), true);
        assert_eq!(event, Event::FileSystemUpdateIncomplete(entries));
    }

    #[test]
    fn one_oversized_filesystem_entry_is_an_empty_incomplete_listing() {
        let huge_name = "x".repeat(WASI_HOST_PIPE_CAP);
        let entries = vec![(PathBuf::from(format!("/host/{huge_name}")), None)];
        match filesystem_listing_event(entries, false) {
            Event::FileSystemUpdateIncomplete(listed) => assert!(listed.is_empty()),
            other => panic!("expected an empty incomplete listing, got {other:?}"),
        }
    }

    #[test]
    fn a_long_filesystem_listing_keeps_a_fitting_incomplete_prefix() {
        let piece = "p".repeat(1024);
        let entries: Vec<_> = (0..4_000)
            .map(|index| (PathBuf::from(format!("/host/{piece}-{index}")), None))
            .collect();
        let total = entries.len();
        match filesystem_listing_event(entries, false) {
            Event::FileSystemUpdateIncomplete(prefix) => {
                assert!(!prefix.is_empty());
                assert!(prefix.len() < total);
                let frame = host_stdin_frame_len(&Event::FileSystemUpdateIncomplete(prefix))
                    .expect("incomplete frame");
                assert!(
                    frame <= WASI_HOST_PIPE_CAP,
                    "sent incomplete frame is {frame} bytes"
                );
            },
            other => panic!("expected an incomplete listing, got {other:?}"),
        }
    }
}
