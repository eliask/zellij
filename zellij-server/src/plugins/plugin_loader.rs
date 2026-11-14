use crate::plugins::plugin_map::{
    PluginEnv, PluginMap, PluginMetadata, RunningPlugin, SharedEnv, VecDequeInputStream,
    WriteOutputStream,
};
use crate::plugins::plugin_worker::{plugin_worker, RunningWorker};
use crate::plugins::wasm_bridge::{LoadingContext, PluginCache};
use crate::plugins::zellij_exports::{wasi_write_object, zellij_exports};
use crate::plugins::PluginId;
use prost::Message;
use sha2::{Digest, Sha256};
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

use zellij_utils::plugin_api::action::ProtobufPluginConfiguration;
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
    generation: u64,
    plugin_digest: String,
    /// Module instantiated for this load. Other clients and workers of the load reuse it instead
    /// of reading the path again.
    module: Option<Module>,
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
            generation: loading_context.generation,
            plugin_digest: String::new(),
            module: None,
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
        let module = self.load_module()?;
        let (store, instance) = self.create_plugin_environment(module)?;
        self.load_plugin_instance(store, &instance, false)?;
        self.clone_instance_for_other_clients()?;
        Ok(())
    }
    pub fn start_shared_plugin(&mut self) -> Result<Arc<Mutex<RunningPlugin>>> {
        let module = self.load_module()?;
        let (mut store, instance) = self.create_plugin_environment(module)?;
        store.data_mut().shared = Some(SharedEnv::default());
        self.load_plugin_instance(store, &instance, true)
    }
    /// Read this load's bytes once and fingerprint them. A cached module is used only when it was
    /// compiled from those exact bytes. Clients and workers of this load reuse that module.
    fn load_module(&mut self) -> Result<Module> {
        if let Some(module) = &self.module {
            return Ok(module.clone());
        }
        let wasm_bytes = self.plugin_config.resolve_wasm_bytes(&self.plugin_dir)?;
        self.plugin_digest = plugin_digest_from_bytes(&wasm_bytes);
        if !self.skip_cache {
            if let Some(module) = self.cached_module_matching_digest() {
                self.module = Some(module.clone());
                return Ok(module);
            }
        }
        let module = self.interpret_module(&wasm_bytes)?;
        self.module = Some(module.clone());
        Ok(module)
    }
    fn cached_module_matching_digest(&self) -> Option<Module> {
        let cached = self
            .plugin_cache
            .lock()
            .unwrap()
            .get(&self.plugin_config.path)
            .cloned();
        match cached {
            Some((digest, module)) if digest == self.plugin_digest => Some(module),
            _ => None,
        }
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
    fn interpret_module(&mut self, wasm_bytes: &[u8]) -> Result<Module> {
        self.loading_indication.override_previous_error();
        let timer = std::time::Instant::now();
        let module = Module::new(&self.engine, wasm_bytes)?;
        log::info!(
            "Loaded plugin '{}' in {:?}",
            self.plugin_config.path.display(),
            timer.elapsed()
        );
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
        for function_name in instance
            .exports(&mut store)
            .filter_map(|export| export.clone().into_func().map(|_| export.name()))
        {
            if function_name.ends_with("_worker") {
                let (mut store, instance) =
                    self.create_plugin_instance_and_wasi_env_for_worker()?;
                let start_function_for_worker = instance
                    .get_typed_func::<(), ()>(&mut store, "_start")
                    .with_context(err_context)?;
                start_function_for_worker
                    .call(&mut store, ())
                    .with_context(err_context)?;

                let worker = RunningWorker::new(store, instance, &function_name);
                let worker_sender = plugin_worker(worker);
                workers.insert(function_name.into(), worker_sender);
            }
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
            generation: self.generation,
            client_id: self.client_id,
            plugin: self.plugin_config.clone(), // TODO: change field name in PluginEnv to plugin_config
            plugin_digest: self.plugin_digest.clone(),
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

        let instance = linker
            .instantiate_and_start(&mut store, &module)
            .with_context(err_context)?;

        if let Some(func) = instance.get_func(&mut store, "_initialize") {
            if let Ok(typed_func) = func.typed::<(), ()>(&store) {
                let _ = typed_func.call(&mut store, ());
            }
        }

        self.plugin_cache
            .lock()
            .unwrap()
            .insert(plugin_path.clone(), (self.plugin_digest.clone(), module));
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
        let module = self.module.clone().with_context(err_context)?;
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
            generation: self.generation,
            client_id: self.client_id,
            plugin: plugin_config,
            plugin_digest: self.plugin_digest.clone(),
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

        let instance = linker
            .instantiate_and_start(&mut store, module)
            .with_context(err_context)?;

        if let Some(func) = instance.get_func(&mut store, "_initialize") {
            if let Ok(typed_func) = func.typed::<(), ()>(&store) {
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

        // Set up custom stdin/stdout/stderr
        ctx.set_stdin(Box::new(ReadPipe::new(VecDequeInputStream(
            stdin_pipe.clone(),
        ))));
        ctx.set_stdout(Box::new(WritePipe::new(WriteOutputStream(
            stdout_pipe.clone(),
        ))));
        ctx.set_stderr(Box::new(WritePipe::new(WriteOutputStream(Arc::new(
            Mutex::new(LoggingPipe::new(plugin_url, plugin_id)),
        )))));

        Ok(ctx)
    }
}

pub(crate) fn plugin_digest_from_bytes(wasm_bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(wasm_bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn create_optimized_store_limits() -> StoreLimits {
    use wasmi::StoreLimitsBuilder;
    StoreLimitsBuilder::new()
        .instances(1) // One instance per plugin
        .memories(4) // Max 4 linear memories per plugin
        .memory_size(16 * 1024 * 1024) // 16MB per memory maximum
        .tables(16) // Small table element limit
        .trap_on_grow_failure(true) // Fail fast on resource exhaustion
        .build()
}

#[cfg(test)]
mod module_digest_tests {
    use super::*;
    use wasmi::{Engine, Linker, Module, Store};

    fn wasm_with_export(name: &str) -> Vec<u8> {
        wat::parse_str(format!("(module (func (export \"{name}\")))")).unwrap()
    }

    fn has_export(engine: &Engine, module: &Module, name: &str) -> bool {
        let mut store = Store::<()>::new(engine, ());
        let linker = Linker::new(engine);
        let instance = linker
            .instantiate_and_start(&mut store, module)
            .expect("instantiate");
        instance.get_func(&store, name).is_some()
    }

    /// `cached` is `(digest stored beside the module, bytes the module was compiled from)`.
    fn load_case(
        skip_cache: bool,
        file_bytes: &[u8],
        cached: Option<(String, Vec<u8>)>,
    ) -> (String, bool, bool, Option<String>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("guest.wasm");
        std::fs::write(&path, file_bytes).unwrap();
        let mut plugin_map = PluginMap::default();
        let plugin_cache: PluginCache = Arc::new(Mutex::new(HashMap::new()));
        let context = LoadingContext {
            plugin_id: 1,
            client_id: 1,
            plugin_cwd: dir.path().to_path_buf(),
            plugin_own_data_dir: dir.path().join("data"),
            plugin_own_cache_dir: dir.path().join("cache"),
            plugin_config: PluginConfig {
                path: path.clone(),
                _allow_exec_host_cmd: false,
                location: zellij_utils::input::layout::RunPluginLocation::File(path.clone()),
                initial_userspace_configuration: Default::default(),
                initial_cwd: None,
            },
            tab_index: None,
            path_to_default_shell: PathBuf::from("/bin/sh"),
            session_env_vars: Default::default(),
            default_shell: None,
            layout_dir: None,
            default_mode: Default::default(),
            keybinds: Arc::new(Vec::new()),
            plugin_dir: dir.path().to_path_buf(),
            size: Size { rows: 2, cols: 2 },
            generation: 1,
        };
        let mut loader = PluginLoader::new(
            skip_cache,
            context,
            ThreadSenders::default(),
            Engine::default(),
            plugin_cache.clone(),
            &mut plugin_map,
            Arc::new(Mutex::new(Vec::new())),
        );
        if let Some((digest, bytes)) = cached {
            let module = Module::new(&loader.engine, &bytes).unwrap();
            plugin_cache
                .lock()
                .unwrap()
                .insert(path.clone(), (digest, module));
        }
        let module = loader.load_module().unwrap();
        let marker_a = has_export(&loader.engine, &module, "marker_a");
        let marker_b = has_export(&loader.engine, &module, "marker_b");
        let cache_digest = plugin_cache
            .lock()
            .unwrap()
            .get(&path)
            .map(|(digest, _)| digest.clone());
        (
            loader.plugin_digest.clone(),
            marker_a,
            marker_b,
            cache_digest,
        )
    }

    #[test]
    fn a_cached_module_is_reused_only_when_its_digest_matches_the_file() {
        let bytes_a = wasm_with_export("marker_a");
        let bytes_b = wasm_with_export("marker_b");
        let digest_a = plugin_digest_from_bytes(&bytes_a);
        let digest_b = plugin_digest_from_bytes(&bytes_b);

        let (digest, marker_a, marker_b, cache_digest) =
            load_case(false, &bytes_b, Some((digest_a.clone(), bytes_a.clone())));
        assert_eq!(digest, digest_b);
        assert!(!marker_a && marker_b);
        assert_eq!(cache_digest.as_deref(), Some(digest_a.as_str()));

        let (digest, marker_a, marker_b, _) =
            load_case(false, &bytes_a, Some((digest_a.clone(), bytes_a.clone())));
        assert_eq!(digest, digest_a);
        assert!(marker_a && !marker_b);

        // A matching digest is the cache identity, so a poisoned entry is what the load reuses.
        let (digest, marker_a, marker_b, _) =
            load_case(false, &bytes_b, Some((digest_b.clone(), bytes_a.clone())));
        assert_eq!(digest, digest_b);
        assert!(marker_a && !marker_b);

        let (digest, marker_a, marker_b, _) =
            load_case(true, &bytes_b, Some((digest_b.clone(), bytes_a)));
        assert_eq!(digest, digest_b);
        assert!(
            !marker_a && marker_b,
            "skip_cache recompiles instead of reusing the cached module"
        );
    }

    #[test]
    fn the_same_load_reuses_its_module_after_the_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("guest.wasm");
        let bytes_a = wasm_with_export("marker_a");
        let bytes_b = wasm_with_export("marker_b");
        std::fs::write(&path, &bytes_a).unwrap();
        let mut plugin_map = PluginMap::default();
        let context = LoadingContext {
            plugin_id: 3,
            client_id: 1,
            plugin_cwd: dir.path().to_path_buf(),
            plugin_own_data_dir: dir.path().join("data"),
            plugin_own_cache_dir: dir.path().join("cache"),
            plugin_config: PluginConfig {
                path: path.clone(),
                _allow_exec_host_cmd: false,
                location: zellij_utils::input::layout::RunPluginLocation::File(path),
                initial_userspace_configuration: Default::default(),
                initial_cwd: None,
            },
            tab_index: None,
            path_to_default_shell: PathBuf::from("/bin/sh"),
            session_env_vars: Default::default(),
            default_shell: None,
            layout_dir: None,
            default_mode: Default::default(),
            keybinds: Arc::new(Vec::new()),
            plugin_dir: dir.path().to_path_buf(),
            size: Size { rows: 2, cols: 2 },
            generation: 9,
        };
        let mut loader = PluginLoader::new(
            true,
            context,
            ThreadSenders::default(),
            Engine::default(),
            Arc::new(Mutex::new(HashMap::new())),
            &mut plugin_map,
            Arc::new(Mutex::new(Vec::new())),
        );
        let first = loader.load_module().unwrap();
        assert!(has_export(&loader.engine, &first, "marker_a"));
        let digest = loader.plugin_digest.clone();
        std::fs::write(&loader.plugin_config.path, bytes_b).unwrap();
        let second = loader.load_module().unwrap();
        assert_eq!(loader.plugin_digest, digest);
        assert!(has_export(&loader.engine, &second, "marker_a"));
        assert!(!has_export(&loader.engine, &second, "marker_b"));
    }
}
