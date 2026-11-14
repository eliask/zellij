use super::{
    event_or_pipe_message_bytes, replaceable_host_dir_key, replaceable_plugin_settings_key,
    EventOrPipeMessage, WasmBridge, PENDING_CACHE_ENTRY_OVERHEAD,
};
use crate::thread_bus::ThreadSenders;
use std::collections::HashMap;
use std::path::PathBuf;
use zellij_utils::data::{
    BareKey, Event, KeyWithModifier, ModeInfo, PaneContents, PaneId, PaneManifest,
    PaneRenderReport, PermissionStatus, SessionInfo, TabInfo,
};
use zellij_utils::input::command::TerminalAction;

fn test_bridge() -> WasmBridge {
    let mut senders = ThreadSenders::default();
    senders.should_silently_fail = true;
    WasmBridge::new(
        senders,
        wasmi::Engine::default(),
        PathBuf::from("/tmp"),
        PathBuf::from("/bin/sh"),
        PathBuf::from("/tmp"),
        Default::default(),
        None::<TerminalAction>,
        None,
        Vec::new(),
        Vec::new(),
        Default::default(),
        Default::default(),
    )
}

fn mode(name: &str) -> Event {
    let mut mode = ModeInfo::default();
    mode.session_name = Some(name.to_owned());
    Event::ModeUpdate(mode)
}

#[test]
fn replaceable_keys_include_destination_and_generation() {
    assert_ne!(
        replaceable_host_dir_key(11, 0),
        replaceable_host_dir_key(12, 0)
    );
    assert_ne!(
        replaceable_host_dir_key(11, 1),
        replaceable_host_dir_key(11, 2)
    );
    assert_ne!(
        replaceable_plugin_settings_key(11, 0),
        replaceable_host_dir_key(11, 0)
    );
}

#[test]
fn pending_cache_replaces_snapshots_without_crossing_ordered_events() {
    let mut bridge = test_bridge();
    let plugin_id = 3;
    bridge
        .cached_events_for_pending_plugins
        .insert_empty(plugin_id);

    bridge
        .cached_events_for_pending_plugins
        .try_push(
            plugin_id,
            EventOrPipeMessage::Event(Event::PastedText("keep".to_owned()), Some(1)),
            false,
        )
        .unwrap();
    bridge
        .cached_events_for_pending_plugins
        .try_push(
            plugin_id,
            EventOrPipeMessage::Event(mode("aa"), Some(1)),
            false,
        )
        .unwrap();
    let bytes_after_one_mode = bridge
        .cached_events_for_pending_plugins
        .cached_bytes(plugin_id);

    for index in 0..50 {
        bridge
            .cached_events_for_pending_plugins
            .try_push(
                plugin_id,
                EventOrPipeMessage::Event(mode(&format!("{index:02}")), Some(1)),
                false,
            )
            .unwrap();
    }
    for index in 0..50 {
        let mut tab = TabInfo::default();
        tab.name = format!("tab-{index:02}");
        bridge
            .cached_events_for_pending_plugins
            .try_push(
                plugin_id,
                EventOrPipeMessage::Event(Event::TabUpdate(vec![tab]), Some(1)),
                false,
            )
            .unwrap();
    }
    for index in 0..50 {
        let mut manifest = PaneManifest::default();
        manifest.panes.insert(index, vec![]);
        bridge
            .cached_events_for_pending_plugins
            .try_push(
                plugin_id,
                EventOrPipeMessage::Event(Event::PaneUpdate(manifest), Some(1)),
                false,
            )
            .unwrap();
    }
    for index in 0..50 {
        bridge
            .cached_events_for_pending_plugins
            .try_push(
                plugin_id,
                EventOrPipeMessage::Event(
                    Event::SessionUpdate(
                        vec![SessionInfo {
                            name: format!("session-{index:02}"),
                            ..SessionInfo::default()
                        }],
                        vec![],
                    ),
                    Some(1),
                ),
                false,
            )
            .unwrap();
    }
    bridge
        .cached_events_for_pending_plugins
        .try_push(
            plugin_id,
            EventOrPipeMessage::Event(mode("other-client"), Some(2)),
            false,
        )
        .unwrap();
    bridge
        .cached_events_for_pending_plugins
        .try_push(
            plugin_id,
            EventOrPipeMessage::Event(
                Event::PermissionRequestResult(PermissionStatus::Granted),
                Some(1),
            ),
            false,
        )
        .unwrap();

    let cached = bridge
        .cached_events_for_pending_plugins
        .cached_events(plugin_id);
    assert_eq!(cached.len(), 7, "snapshots replace; ordered events stay");
    assert!(matches!(
        &cached[0],
        EventOrPipeMessage::Event(Event::PastedText(text), _) if text == "keep"
    ));
    assert!(matches!(
        &cached[1],
        EventOrPipeMessage::Event(Event::ModeUpdate(mode), Some(1))
            if mode.session_name.as_deref() == Some("49")
    ));
    assert_eq!(
        bridge
            .cached_events_for_pending_plugins
            .cached_bytes(plugin_id),
        bytes_after_one_mode
            + cached
                .iter()
                .skip(2)
                .map(event_or_pipe_message_bytes)
                .fold(0usize, usize::saturating_add)
            + 5 * PENDING_CACHE_ENTRY_OVERHEAD
    );

    bridge.cached_events_for_pending_plugins.reset(plugin_id);
    for event in [
        mode("before-key"),
        Event::Key(KeyWithModifier::new(BareKey::Char('x'))),
        mode("after-key"),
    ] {
        bridge
            .cached_events_for_pending_plugins
            .try_push(plugin_id, EventOrPipeMessage::Event(event, Some(1)), false)
            .unwrap();
    }
    let cached = bridge
        .cached_events_for_pending_plugins
        .cached_events(plugin_id);
    assert_eq!(cached.len(), 3, "a key between snapshots is a barrier");
    assert!(matches!(
        &cached[0],
        EventOrPipeMessage::Event(Event::ModeUpdate(mode), _)
            if mode.session_name.as_deref() == Some("before-key")
    ));
    assert!(matches!(
        &cached[1],
        EventOrPipeMessage::Event(Event::Key(_), _)
    ));
    assert!(matches!(
        &cached[2],
        EventOrPipeMessage::Event(Event::ModeUpdate(mode), _)
            if mode.session_name.as_deref() == Some("after-key")
    ));
}

fn pane_report(client_id: u32, panes: &[(PaneId, &str)]) -> PaneRenderReport {
    let mut report = PaneRenderReport::default();
    for (pane_id, line) in panes {
        report.add_pane_contents(
            &[client_id],
            *pane_id,
            PaneContents {
                viewport: vec![(*line).to_owned()],
                ..PaneContents::default()
            },
        );
    }
    report
}

fn viewport_of(report: &HashMap<PaneId, PaneContents>, pane_id: PaneId) -> &str {
    report
        .get(&pane_id)
        .and_then(|contents| contents.viewport.first())
        .map(String::as_str)
        .unwrap_or("")
}

#[test]
fn pending_pane_render_reports_keep_disjoint_panes() {
    let mut bridge = test_bridge();
    let plugin_id = 11;
    bridge
        .cached_events_for_pending_plugins
        .insert_empty(plugin_id);
    let (shutdown_sender, _shutdown_receiver) = tokio::sync::mpsc::channel(1);
    let pane_a = PaneId::Terminal(1);
    let pane_b = PaneId::Terminal(2);
    bridge
        .handle_pane_render_report(
            pane_report(1, &[(pane_a, "alpha")]),
            shutdown_sender.clone(),
        )
        .unwrap();
    bridge
        .handle_pane_render_report(
            pane_report(1, &[(pane_a, "alpha"), (pane_b, "beta")]),
            shutdown_sender.clone(),
        )
        .unwrap();
    bridge
        .handle_pane_render_report(
            pane_report(1, &[(pane_a, "alpha-2"), (pane_b, "beta")]),
            shutdown_sender.clone(),
        )
        .unwrap();
    bridge
        .handle_pane_render_report(pane_report(2, &[(pane_b, "other-client")]), shutdown_sender)
        .unwrap();

    let released = bridge
        .cached_events_for_pending_plugins
        .cached_events(plugin_id);
    assert_eq!(
        released.len(),
        2,
        "each client keeps one coalesced pane report"
    );
    let EventOrPipeMessage::Event(Event::PaneRenderReport(panes), Some(1)) = &released[0] else {
        panic!("pending delivery was not client 1's pane report: {released:?}");
    };
    assert_eq!(
        panes.len(),
        2,
        "a later pane must not erase an earlier pending pane"
    );
    assert_eq!(viewport_of(panes, pane_a), "alpha-2");
    assert_eq!(viewport_of(panes, pane_b), "beta");
    let EventOrPipeMessage::Event(Event::PaneRenderReport(other_panes), Some(2)) = &released[1]
    else {
        panic!("other client report missing: {released:?}");
    };
    assert_eq!(viewport_of(other_panes, pane_b), "other-client");
    assert_eq!(
        bridge
            .cached_events_for_pending_plugins
            .cached_bytes(plugin_id),
        released
            .iter()
            .map(event_or_pipe_message_bytes)
            .fold(0usize, usize::saturating_add)
            + released.len() * PENDING_CACHE_ENTRY_OVERHEAD
    );
}

#[test]
fn an_overflowed_loading_cache_is_not_replayed() {
    let mut bridge = test_bridge();
    let plugin_id = 7;
    bridge
        .cached_events_for_pending_plugins
        .insert_empty(plugin_id);

    let oversized = Event::PastedText("x".repeat(super::MAX_PENDING_CACHE_BYTES_PER_PLUGIN + 1));
    assert!(bridge
        .cached_events_for_pending_plugins
        .try_push(plugin_id, EventOrPipeMessage::Event(oversized, None), false,)
        .is_err());
    assert!(
        bridge
            .cached_events_for_pending_plugins
            .is_incomplete(plugin_id),
        "a one-shot event that cannot fit marks the cache incomplete"
    );
    bridge.cached_events_for_pending_plugins.reset(plugin_id);

    for _ in 0..super::MAX_PENDING_CACHE_ENTRIES_PER_PLUGIN {
        bridge
            .cached_events_for_pending_plugins
            .try_push(
                plugin_id,
                EventOrPipeMessage::Event(Event::PastedText("x".to_owned()), Some(1)),
                false,
            )
            .unwrap();
    }
    assert!(bridge
        .cached_events_for_pending_plugins
        .try_push(
            plugin_id,
            EventOrPipeMessage::Event(mode("latest"), Some(1)),
            false,
        )
        .is_err());
    assert!(
        !bridge
            .cached_events_for_pending_plugins
            .is_incomplete(plugin_id),
        "dropping a snapshot must not mark one-shot history incomplete"
    );
    assert!(bridge
        .cached_events_for_pending_plugins
        .try_push(
            plugin_id,
            EventOrPipeMessage::Event(
                Event::PermissionRequestResult(PermissionStatus::Granted),
                Some(1),
            ),
            false,
        )
        .is_err());
    assert!(bridge
        .cached_events_for_pending_plugins
        .is_incomplete(plugin_id));
    assert!(bridge.take_replayable_cached_events(plugin_id).is_none());
    assert!(!bridge
        .cached_events_for_pending_plugins
        .contains(&plugin_id));
}
