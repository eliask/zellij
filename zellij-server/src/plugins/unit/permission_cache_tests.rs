//! Host-side grant and denial behavior for the permission cache.
//! The durable merge itself is covered in `zellij-utils`; these tests lock the
//! bridge rule that a later grant adds and a denial writes nothing.

use super::WasmBridge;
use crate::plugins::plugin_loader::PluginLoader;
use crate::plugins::plugin_map::{PluginEnv, RunningPlugin};
use crate::thread_bus::ThreadSenders;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use wasmi::{Engine, Linker, Module, Store};
use zellij_utils::data::{InputMode, PermissionStatus, PermissionType};
use zellij_utils::input::layout::{PluginUserConfiguration, RunPluginLocation};
use zellij_utils::input::permission::PermissionCache;
use zellij_utils::input::plugins::PluginConfig;

fn module_exporting(names: &[&str]) -> Vec<u8> {
    let mut exports = vec![names.len() as u8];
    for name in names {
        exports.push(name.len() as u8);
        exports.extend_from_slice(name.as_bytes());
        exports.extend_from_slice(&[0, 0]);
    }
    let mut bytes = b"\0asm\x01\0\0\0".to_vec();
    bytes.extend_from_slice(&[1, 4, 1, 0x60, 0, 0]);
    bytes.extend_from_slice(&[3, 2, 1, 0]);
    bytes.push(7);
    bytes.push(exports.len() as u8);
    bytes.extend(exports);
    bytes.extend_from_slice(&[10, 4, 1, 2, 0, 0x0b]);
    bytes
}

fn test_bridge() -> (WasmBridge, tempfile::TempDir) {
    let plugin_dir = tempdir().unwrap();
    let senders = ThreadSenders {
        should_silently_fail: true,
        ..Default::default()
    };
    let bridge = WasmBridge::new(
        senders,
        Engine::default(),
        plugin_dir.path().to_path_buf(),
        "/bin/sh".into(),
        plugin_dir.path().to_path_buf(),
        Default::default(),
        None,
        None,
        vec![],
        vec![],
        Default::default(),
        Default::default(),
    );
    (bridge, plugin_dir)
}

fn install_running_plugin(bridge: &WasmBridge, plugin_id: u32, client_id: u32, location: &str) {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    // The directory must outlive the WASI preopens. The cache file under test is elsewhere.
    std::mem::forget(dir);
    let stdin_pipe = Arc::new(Mutex::new(VecDeque::new()));
    let stdout_pipe = Arc::new(Mutex::new(VecDeque::new()));
    let wasi_ctx = PluginLoader::create_wasi_ctx(
        &dir_path,
        &dir_path,
        &dir_path,
        &dir_path,
        &location.to_owned(),
        plugin_id,
        stdin_pipe.clone(),
        stdout_pipe.clone(),
    )
    .unwrap();
    let env = PluginEnv {
        plugin_id,
        client_id,
        plugin: PluginConfig {
            path: PathBuf::from(location),
            _allow_exec_host_cmd: false,
            location: RunPluginLocation::File(PathBuf::from(location)),
            initial_userspace_configuration: PluginUserConfiguration::new(BTreeMap::new()),
            initial_cwd: None,
        },
        permissions: Arc::new(Mutex::new(None)),
        senders: ThreadSenders {
            should_silently_fail: true,
            ..Default::default()
        },
        wasi_ctx,
        plugin_own_data_dir: dir_path.clone(),
        plugin_own_cache_dir: dir_path.clone(),
        path_to_default_shell: PathBuf::from("/bin/sh"),
        default_shell: None,
        layout_dir: None,
        plugin_cwd: dir_path,
        session_env_vars: BTreeMap::new(),
        input_pipes_to_unblock: Arc::new(Mutex::new(HashSet::new())),
        input_pipes_to_block: Arc::new(Mutex::new(HashSet::new())),
        default_mode: InputMode::Normal,
        subscriptions: Arc::new(Mutex::new(HashSet::new())),
        stdin_pipe,
        stdout_pipe,
        keybinds: Default::default(),
        intercepting_key_presses: false,
        pending_popup_size: None,
        store_limits: wasmi::StoreLimitsBuilder::new().build(),
        shared: None,
    };
    let engine = Engine::default();
    let module = Module::new(&engine, &module_exporting(&["load"])[..]).expect("valid module");
    let mut store = Store::new(&engine, env);
    let instance = Linker::new(&engine)
        .instantiate_and_start(&mut store, &module)
        .expect("instantiates");
    let running = Arc::new(Mutex::new(RunningPlugin::new(store, instance, 5, 80)));
    bridge.plugin_map.lock().unwrap().insert(
        plugin_id,
        client_id,
        running,
        Arc::new(Mutex::new(HashSet::new())),
        HashMap::new(),
    );
}

fn runtime_permissions(
    bridge: &WasmBridge,
    plugin_id: u32,
    client_id: u32,
) -> HashSet<PermissionType> {
    bridge
        .plugin_map
        .lock()
        .unwrap()
        .get_running_plugin(plugin_id, Some(client_id))
        .unwrap()
        .lock()
        .unwrap()
        .store
        .data()
        .permissions
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default()
}

fn disk_permissions(cache_path: &std::path::Path, key: &str) -> HashSet<PermissionType> {
    PermissionCache::from_path_or_default(Some(cache_path.to_path_buf()))
        .get_permissions(key.to_owned())
        .into_iter()
        .flatten()
        .copied()
        .collect()
}

#[test]
fn first_denial_publishes_nothing_and_leaves_runtime_empty() {
    let (mut bridge, _plugin_dir) = test_bridge();
    let plugin_id = 7;
    let client_id = 1;
    let location = "/plugins/grant.wasm";
    install_running_plugin(&bridge, plugin_id, client_id, location);
    let cache_dir = tempdir().unwrap();
    let cache_path = cache_dir.path().join("permissions.kdl");

    bridge
        .cache_plugin_permissions(
            plugin_id,
            Some(client_id),
            vec![
                PermissionType::ReadApplicationState,
                PermissionType::RunCommands,
            ],
            PermissionStatus::Denied,
            Some(cache_path.clone()),
        )
        .unwrap();

    assert!(
        runtime_permissions(&bridge, plugin_id, client_id).is_empty(),
        "a denial must not invent runtime grants"
    );
    assert!(
        disk_permissions(&cache_path, location).is_empty(),
        "a denial must not invent durable grants"
    );
    assert!(
        !cache_path.exists(),
        "a denial must not publish a permissions file"
    );
}

#[test]
fn granted_permissions_add_up_and_a_denial_keeps_them() {
    let (mut bridge, _plugin_dir) = test_bridge();
    let plugin_id = 7;
    let client_id = 1;
    let location = "/plugins/grant.wasm";
    install_running_plugin(&bridge, plugin_id, client_id, location);
    let cache_dir = tempdir().unwrap();
    let cache_path = cache_dir.path().join("permissions.kdl");

    assert!(PermissionCache::grant_and_persist(
        Some(cache_path.clone()),
        location.to_owned(),
        vec![PermissionType::WriteToStdin],
    )
    .unwrap());

    let mut respond = |permissions: Vec<PermissionType>, status| {
        bridge
            .cache_plugin_permissions(
                plugin_id,
                Some(client_id),
                permissions,
                status,
                Some(cache_path.clone()),
            )
            .unwrap();
        (
            runtime_permissions(&bridge, plugin_id, client_id),
            disk_permissions(&cache_path, location),
        )
    };

    assert_eq!(
        respond(
            vec![PermissionType::ReadApplicationState],
            PermissionStatus::Granted,
        ),
        (
            HashSet::from([PermissionType::ReadApplicationState]),
            HashSet::from([
                PermissionType::ReadApplicationState,
                PermissionType::WriteToStdin,
            ]),
        )
    );
    let granted = (
        HashSet::from([
            PermissionType::ReadApplicationState,
            PermissionType::RunCommands,
        ]),
        HashSet::from([
            PermissionType::ReadApplicationState,
            PermissionType::RunCommands,
            PermissionType::WriteToStdin,
        ]),
    );
    assert_eq!(
        respond(vec![PermissionType::RunCommands], PermissionStatus::Granted),
        granted
    );

    let untouched = "// untouched\n";
    std::fs::OpenOptions::new()
        .append(true)
        .open(&cache_path)
        .unwrap()
        .write_all(untouched.as_bytes())
        .unwrap();
    assert_eq!(
        respond(
            vec![PermissionType::ChangeApplicationState],
            PermissionStatus::Denied,
        ),
        granted
    );
    assert_eq!(
        respond(
            vec![PermissionType::ReadApplicationState],
            PermissionStatus::Granted,
        ),
        granted
    );
    assert!(
        std::fs::read_to_string(&cache_path)
            .unwrap()
            .ends_with(untouched),
        "an unchanged re-grant and a denial must not rewrite the durable cache"
    );
}
