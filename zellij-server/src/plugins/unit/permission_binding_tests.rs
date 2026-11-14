//! A delayed permission answer or callback stays bound to the load that issued it.
//! Reusing the plugin id for a new generation or module digest must not inherit it.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tempfile::tempdir;
use wasmi::{Engine, Linker, Module, Store};
use zellij_utils::channels::{self, SenderWithContext};
use zellij_utils::data::{Event, InputMode, PermissionStatus, PermissionType, PluginTag};
use zellij_utils::input::layout::{PluginUserConfiguration, RunPluginLocation};
use zellij_utils::input::permission::PermissionCache;
use zellij_utils::input::plugins::PluginConfig;

use crate::plugins::plugin_loader::PluginLoader;
use crate::plugins::plugin_map::{RunningPlugin, Subscriptions};
use crate::plugins::zellij_exports::zellij_exports;
use crate::thread_bus::ThreadSenders;
use crate::{ClientId, SharedKeybinds};

use super::{
    apply_event_to_plugin, event_is_scoped_to_load, stale_timer_for_load, EventOrPipeMessage,
    WasmBridge,
};

const PLUGIN_ID: u32 = 7;
const CLIENT_ID: ClientId = 1;
const GENERATION: u64 = 4;
const DIGEST: &str = "digest-a";

fn bridge(dir: &Path) -> WasmBridge {
    let (to_plugin, _plugin_receiver) = channels::unbounded();
    let (to_screen, _screen_receiver) = channels::unbounded();
    let senders = ThreadSenders {
        to_plugin: Some(SenderWithContext::new(to_plugin)),
        to_screen: Some(SenderWithContext::new(to_screen)),
        should_silently_fail: true,
        ..Default::default()
    };
    let keybinds: SharedKeybinds = Arc::new(Vec::new());
    WasmBridge::new(
        senders,
        Engine::default(),
        dir.to_path_buf(),
        PathBuf::from("/bin/sh"),
        dir.to_path_buf(),
        BTreeMap::new(),
        None,
        None,
        vec![],
        vec![],
        InputMode::Normal,
        keybinds,
    )
}

fn plugin_config(path: PathBuf, builtin: bool) -> PluginConfig {
    let location = if builtin {
        RunPluginLocation::Zellij(PluginTag::new("status-bar"))
    } else {
        RunPluginLocation::File(path.clone())
    };
    PluginConfig {
        path,
        _allow_exec_host_cmd: false,
        location,
        initial_userspace_configuration: PluginUserConfiguration::default(),
        initial_cwd: None,
    }
}

fn insert_plugin(bridge: &WasmBridge, dir: &Path, builtin: bool) -> Arc<Mutex<RunningPlugin>> {
    std::fs::create_dir_all(dir).unwrap();
    let stdin_pipe = Arc::new(Mutex::new(VecDeque::new()));
    let stdout_pipe = Arc::new(Mutex::new(VecDeque::new()));
    let config = plugin_config(dir.join("guest.wasm"), builtin);
    let wasi_ctx = PluginLoader::create_wasi_ctx(
        &dir.to_path_buf(),
        &dir.to_path_buf(),
        &dir.to_path_buf(),
        &dir.to_path_buf(),
        &config.location.to_string(),
        PLUGIN_ID,
        stdin_pipe.clone(),
        stdout_pipe.clone(),
    )
    .unwrap();
    let env = crate::plugins::plugin_map::PluginEnv {
        plugin_id: PLUGIN_ID,
        generation: GENERATION,
        plugin: config,
        plugin_digest: DIGEST.to_owned(),
        permissions: Arc::new(Mutex::new(None)),
        senders: ThreadSenders {
            should_silently_fail: true,
            ..Default::default()
        },
        wasi_ctx,
        client_id: CLIENT_ID,
        plugin_own_data_dir: dir.to_path_buf(),
        plugin_own_cache_dir: dir.to_path_buf(),
        path_to_default_shell: PathBuf::from("/bin/sh"),
        default_shell: None,
        layout_dir: None,
        plugin_cwd: dir.to_path_buf(),
        session_env_vars: BTreeMap::new(),
        input_pipes_to_unblock: Arc::new(Mutex::new(HashSet::new())),
        input_pipes_to_block: Arc::new(Mutex::new(HashSet::new())),
        default_mode: InputMode::Normal,
        subscriptions: Arc::new(Mutex::new(HashSet::new())),
        stdin_pipe,
        stdout_pipe,
        keybinds: Arc::new(Vec::new()),
        intercepting_key_presses: false,
        pending_popup_size: None,
        store_limits: wasmi::StoreLimitsBuilder::new().build(),
        shared: None,
    };
    let engine = Engine::default();
    // No `update` export: a callback that is actually delivered fails instead of rendering.
    let wasm = wat::parse_str(
        r#"(module
            (func (export "_start"))
            (func (export "load"))
            (func (export "zellij_plugin_contract")))"#,
    )
    .unwrap();
    let module = Module::new(&engine, &wasm).unwrap();
    let mut store = Store::new(&engine, env);
    let mut linker = Linker::new(&engine);
    wasmi_wasi::add_to_linker(
        &mut linker,
        |env: &mut crate::plugins::plugin_map::PluginEnv| &mut env.wasi_ctx,
    )
    .unwrap();
    zellij_exports(&mut linker);
    let instance = linker.instantiate_and_start(&mut store, &module).unwrap();
    let running = Arc::new(Mutex::new(RunningPlugin::new(store, instance, 0, 0)));
    bridge.plugin_map.lock().unwrap().insert(
        PLUGIN_ID,
        CLIENT_ID,
        running.clone(),
        Arc::new(Mutex::new(Subscriptions::new())),
        HashMap::new(),
    );
    running
}

fn permissions_of(running: &Arc<Mutex<RunningPlugin>>) -> HashSet<PermissionType> {
    running
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

fn cache_key(running: &Arc<Mutex<RunningPlugin>>) -> String {
    let running = running.lock().unwrap();
    format!(
        "{}#{}",
        running.store.data().plugin.location,
        running.store.data().plugin_digest
    )
}

fn disk_permissions(path: &Path, key: &str) -> Option<Vec<PermissionType>> {
    PermissionCache::from_path_or_default(Some(path.to_path_buf()))
        .get_permissions(key.to_owned())
        .cloned()
}

fn shutdown() -> tokio::sync::mpsc::Sender<()> {
    tokio::sync::mpsc::channel(1).0
}

fn park(bridge: &mut WasmBridge) {
    bridge
        .cached_events_for_pending_plugins
        .entry(PLUGIN_ID)
        .or_default()
        .push(EventOrPipeMessage::Event(
            Event::PastedText(String::new()),
            None,
            None,
        ));
}

fn replace_load(running: &Arc<Mutex<RunningPlugin>>) -> (u64, String) {
    let mut running = running.lock().unwrap();
    let data = running.store.data_mut();
    data.generation = data.generation.wrapping_add(1);
    data.plugin_digest = format!("{}-replaced", data.plugin_digest);
    (data.generation, data.plugin_digest.clone())
}

#[test]
fn old_prompt_after_reload_does_not_grant_the_replacement() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    bridge.cache_plugin_events(PLUGIN_ID, GENERATION, DIGEST.to_owned(), 1);
    bridge
        .reload_plugin_with_id(PLUGIN_ID)
        .expect("reload clears the prompt before the replacement is published");
    assert!(bridge.permission_prompt_identity(PLUGIN_ID).is_none());
    park(&mut bridge);
    bridge
        .apply_permission_prompt_result(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::RunCommands],
            PermissionStatus::Granted,
            Some(dir.path().join("permissions.kdl")),
            1,
            shutdown(),
        )
        .unwrap();
    assert!(
        permissions_of(&running).is_empty(),
        "an answer that arrives after reload starts must not grant the plugin id"
    );
    assert!(bridge
        .cached_events_for_pending_plugins
        .contains_key(&PLUGIN_ID));
}

#[test]
fn a_stale_prompt_does_not_grant_the_replacement_prompt() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    bridge.cache_plugin_events(PLUGIN_ID, GENERATION, DIGEST.to_owned(), 1);
    let (generation_b, digest_b) = replace_load(&running);
    bridge.cache_plugin_events(PLUGIN_ID, generation_b, digest_b, 2);
    bridge
        .apply_permission_prompt_result(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::RunCommands],
            PermissionStatus::Granted,
            Some(dir.path().join("permissions.kdl")),
            1,
            shutdown(),
        )
        .unwrap();
    assert!(permissions_of(&running).is_empty());
    assert_eq!(
        bridge
            .permission_prompt_identity(PLUGIN_ID)
            .map(|(id, _, _)| id),
        Some(2),
        "a different prompt id must not clear the prompt stored for the replacement"
    );
    assert!(bridge
        .cached_events_for_pending_plugins
        .contains_key(&PLUGIN_ID));
}

#[test]
fn the_same_prompt_id_is_cleared_once_its_load_is_gone() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    bridge.cache_plugin_events(PLUGIN_ID, GENERATION, DIGEST.to_owned(), 1);
    replace_load(&running);
    bridge
        .apply_permission_prompt_result(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::RunCommands],
            PermissionStatus::Granted,
            Some(dir.path().join("permissions.kdl")),
            1,
            shutdown(),
        )
        .unwrap();
    assert!(permissions_of(&running).is_empty());
    assert!(bridge.permission_prompt_identity(PLUGIN_ID).is_none());
}

#[test]
fn module_digest_or_generation_mismatch_drops_a_known_grant() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    let cache_path = dir.path().join("permissions.kdl");
    park(&mut bridge);
    bridge
        .grant_known_permissions(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::ReadApplicationState],
            GENERATION.wrapping_add(1),
            DIGEST,
            Some(cache_path.clone()),
            shutdown(),
        )
        .unwrap();
    bridge
        .grant_known_permissions(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::RunCommands],
            GENERATION,
            "not-the-running-module",
            Some(cache_path.clone()),
            shutdown(),
        )
        .unwrap();
    assert!(permissions_of(&running).is_empty());
    assert!(!cache_path.exists());
    assert!(bridge
        .cached_events_for_pending_plugins
        .contains_key(&PLUGIN_ID));
    assert!(!bridge.running_instance_matches(
        PLUGIN_ID,
        Some(CLIENT_ID),
        GENERATION,
        "not-the-running-module"
    ));
}

#[test]
fn plugin_id_reuse_keeps_the_previous_load_from_granting_the_new_one() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    let (generation_b, digest_b) = replace_load(&running);
    park(&mut bridge);
    let wrote_old = bridge
        .cache_plugin_permissions(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::RunCommands],
            PermissionStatus::Granted,
            Some(dir.path().join("permissions.kdl")),
            Some((GENERATION, DIGEST)),
            None,
        )
        .unwrap();
    assert!(!wrote_old);
    assert!(permissions_of(&running).is_empty());
    bridge
        .grant_known_permissions(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::ReadApplicationState],
            generation_b,
            &digest_b,
            Some(dir.path().join("permissions.kdl")),
            shutdown(),
        )
        .unwrap();
    assert_eq!(
        permissions_of(&running),
        HashSet::from([PermissionType::ReadApplicationState]),
        "the replacement load can still receive its own grant"
    );
    assert!(!bridge
        .cached_events_for_pending_plugins
        .contains_key(&PLUGIN_ID));
}

#[test]
fn initial_grant_applies_only_for_the_matching_load_and_digest_key() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    let cache_path = dir.path().join("permissions.kdl");
    park(&mut bridge);
    bridge
        .grant_known_permissions(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::ReadApplicationState],
            GENERATION,
            DIGEST,
            Some(cache_path.clone()),
            shutdown(),
        )
        .unwrap();
    assert_eq!(
        permissions_of(&running),
        HashSet::from([PermissionType::ReadApplicationState])
    );
    let key = cache_key(&running);
    assert_eq!(
        disk_permissions(&cache_path, &key),
        Some(vec![PermissionType::ReadApplicationState])
    );
    let location = {
        let running = running.lock().unwrap();
        running.store.data().plugin.location.to_string()
    };
    assert_eq!(disk_permissions(&cache_path, &location), None);
    assert!(!bridge
        .cached_events_for_pending_plugins
        .contains_key(&PLUGIN_ID));
}

#[test]
fn grants_add_and_a_denial_does_not_wipe_or_rewrite_the_cache() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    let cache_path = dir.path().join("permissions.kdl");
    let key = cache_key(&running);
    let mut seeded = PermissionCache::from_path_or_default(Some(cache_path.clone()));
    seeded.cache(key.clone(), vec![PermissionType::WriteToStdin]);
    seeded.write_to_file().unwrap();

    let grant = |bridge: &mut WasmBridge, permissions, status| {
        bridge
            .cache_plugin_permissions(
                PLUGIN_ID,
                Some(CLIENT_ID),
                permissions,
                status,
                Some(cache_path.clone()),
                Some((GENERATION, DIGEST)),
                None,
            )
            .unwrap()
    };
    assert!(grant(
        &mut bridge,
        vec![PermissionType::ReadApplicationState],
        PermissionStatus::Granted
    ));
    assert!(grant(
        &mut bridge,
        vec![PermissionType::RunCommands],
        PermissionStatus::Granted
    ));
    assert_eq!(
        permissions_of(&running),
        HashSet::from([
            PermissionType::ReadApplicationState,
            PermissionType::RunCommands,
        ])
    );
    let durable = disk_permissions(&cache_path, &key).unwrap();
    assert!(durable.contains(&PermissionType::WriteToStdin));
    assert!(durable.contains(&PermissionType::ReadApplicationState));
    assert!(durable.contains(&PermissionType::RunCommands));
    let location = {
        let running = running.lock().unwrap();
        running.store.data().plugin.location.to_string()
    };
    assert_eq!(disk_permissions(&cache_path, &location), None);

    let frozen = std::fs::read(&cache_path).unwrap();
    assert!(grant(
        &mut bridge,
        vec![PermissionType::ChangeApplicationState],
        PermissionStatus::Denied
    ));
    assert!(grant(
        &mut bridge,
        vec![PermissionType::ReadApplicationState],
        PermissionStatus::Granted
    ));
    assert_eq!(permissions_of(&running).len(), 2);
    assert!(
        !permissions_of(&running).contains(&PermissionType::ChangeApplicationState),
        "a denial must not add or remove runtime grants"
    );
    assert_eq!(
        std::fs::read(&cache_path).unwrap(),
        frozen,
        "a denial or an unchanged re-grant must not rewrite the durable cache"
    );
}

#[test]
fn a_builtin_grant_stays_in_process() {
    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), true);
    let cache_path = dir.path().join("permissions.kdl");
    let wrote = bridge
        .cache_plugin_permissions(
            PLUGIN_ID,
            Some(CLIENT_ID),
            vec![PermissionType::ReadApplicationState],
            PermissionStatus::Granted,
            Some(cache_path.clone()),
            Some((GENERATION, DIGEST)),
            None,
        )
        .unwrap();
    assert!(wrote);
    assert_eq!(
        permissions_of(&running),
        HashSet::from([PermissionType::ReadApplicationState])
    );
    assert!(!cache_path.exists());
}

#[test]
fn stale_timer_is_not_delivered_and_other_events_keep_their_own_generation() {
    assert!(event_is_scoped_to_load(&Event::Timer(1.0)));
    assert!(event_is_scoped_to_load(&Event::RunCommandResult(
        Some(0),
        vec![],
        vec![],
        BTreeMap::new()
    )));
    assert!(event_is_scoped_to_load(&Event::WebRequestResult(
        200,
        BTreeMap::new(),
        vec![],
        BTreeMap::new()
    )));
    assert!(event_is_scoped_to_load(&Event::ActionComplete(
        zellij_utils::input::actions::Action::Quit,
        None,
        BTreeMap::new()
    )));
    assert!(!event_is_scoped_to_load(&Event::PastedText(String::new())));
    assert!(stale_timer_for_load(&Event::Timer(1.0), Some(3), 4));
    assert!(!stale_timer_for_load(&Event::Timer(1.0), Some(4), 4));
    assert!(!stale_timer_for_load(&Event::Timer(1.0), None, 4));
    assert!(!stale_timer_for_load(
        &Event::PastedText(String::new()),
        Some(3),
        4
    ));

    let dir = tempdir().unwrap();
    let mut bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    let senders = ThreadSenders {
        should_silently_fail: true,
        ..Default::default()
    };
    let deliver = |scheduled| {
        let mut running = running.lock().unwrap();
        apply_event_to_plugin(
            PLUGIN_ID,
            CLIENT_ID,
            &mut running,
            &Event::Timer(1.0),
            &mut Vec::new(),
            senders.clone(),
            &HashSet::new(),
            scheduled,
        )
    };
    assert!(
        deliver(Some(GENERATION - 1)).is_ok(),
        "a timer scheduled by the previous load is dropped before the guest update"
    );
    assert!(
        deliver(Some(GENERATION)).is_err(),
        "a timer for the running generation is delivered"
    );
    assert!(deliver(None).is_err());
    let mut running_plugin = running.lock().unwrap();
    let unscoped = apply_event_to_plugin(
        PLUGIN_ID,
        CLIENT_ID,
        &mut running_plugin,
        &Event::PastedText("kept".to_owned()),
        &mut Vec::new(),
        senders,
        &HashSet::new(),
        Some(GENERATION - 1),
    );
    assert!(
        unscoped.is_err(),
        "an event that is not load-scoped is not dropped because a timer generation is set"
    );
    drop(running_plugin);

    bridge.scheduled_timer_generation = Some(GENERATION);
    bridge
        .cached_events_for_pending_plugins
        .insert(PLUGIN_ID, Vec::new());
    bridge
        .update_plugins(
            vec![
                (Some(PLUGIN_ID), Some(CLIENT_ID), Event::Timer(1.0)),
                (
                    Some(PLUGIN_ID),
                    Some(CLIENT_ID),
                    Event::PastedText("kept".to_owned()),
                ),
            ],
            shutdown(),
        )
        .unwrap();
    let cached = bridge
        .cached_events_for_pending_plugins
        .get(&PLUGIN_ID)
        .unwrap();
    match &cached[0] {
        EventOrPipeMessage::Event(Event::Timer(_), _, scheduled) => {
            assert_eq!(*scheduled, Some(GENERATION));
        },
        other => panic!("timer was not cached with its generation: {other:?}"),
    }
    match &cached[1] {
        EventOrPipeMessage::Event(Event::PastedText(_), _, scheduled) => {
            assert_eq!(*scheduled, None);
        },
        other => panic!("unscoped event inherited a timer generation: {other:?}"),
    }
}

#[test]
fn a_shared_callback_from_an_older_load_is_stale() {
    let dir = tempdir().unwrap();
    let bridge = bridge(dir.path());
    let running = insert_plugin(&bridge, dir.path(), false);
    let assets = bridge.plugin_map.lock().unwrap().remove_plugins(PLUGIN_ID);
    let (_, subscriptions, workers) = assets.into_iter().next().map(|(_, assets)| assets).unwrap();
    bridge.plugin_map.lock().unwrap().insert_shared(
        PLUGIN_ID,
        (running, subscriptions, workers),
        None,
    );
    assert!(bridge.shared_callback_is_stale(PLUGIN_ID, &Event::Timer(1.0), Some(GENERATION - 1)));
    assert!(!bridge.shared_callback_is_stale(PLUGIN_ID, &Event::Timer(1.0), Some(GENERATION)));
    assert!(!bridge.shared_callback_is_stale(
        PLUGIN_ID,
        &Event::PastedText(String::new()),
        Some(GENERATION - 1)
    ));
    assert!(!bridge.shared_callback_is_stale(PLUGIN_ID, &Event::Timer(1.0), None));
    assert!(bridge.shared_callback_is_stale(99, &Event::Timer(1.0), Some(GENERATION)));
}
