//! #26 against in-process mock stormblocks: a head that misses
//! `poll.fail_threshold` polls marks its own leg lost and the volume
//! degraded; when it answers again and still holds the array with its
//! member active, the leg is back and the volume assembled. A head that
//! answers without the array (its engine restarted, #15) leaves the volume
//! degraded, reported once.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::{AssemblyState, DistVolume, FedState, Leg, LegState};

#[derive(Default)]
struct Mock {
    /// Every call answers 503: a stalled engine.
    down: bool,
    /// The array the engine holds, if any: id → [(member uuid, state)].
    array: Option<(String, Vec<(String, String)>)>,
}

type M = Arc<Mutex<Mock>>;

async fn capacity(State(m): State<M>) -> (StatusCode, Json<Value>) {
    if m.lock().unwrap().down {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
    }
    (StatusCode::OK, Json(json!({"total_bytes": 100u64 << 30, "free_bytes": 50u64 << 30})))
}

async fn get_array(State(m): State<M>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    let m = m.lock().unwrap();
    if m.down {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
    }
    match &m.array {
        Some((a, members)) if *a == id => {
            let members: Vec<Value> = members
                .iter()
                .map(|(u, st)| json!({"uuid": u, "state": st, "device_path": ""}))
                .collect();
            (StatusCode::OK, Json(json!({"id": id, "members": members, "status": {"state": "clean"}})))
        }
        _ => (StatusCode::NOT_FOUND, Json(json!({"message": format!("array {id}")}))),
    }
}

async fn mock_engine(array: Option<(String, Vec<(String, String)>)>) -> (String, M) {
    let m: M = Arc::new(Mutex::new(Mock { down: false, array }));
    let app = Router::new()
        .route("/v1/nodes/capacity", get(capacity))
        .route("/api/v1/arrays/{id}", get(get_array))
        .with_state(m.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr.to_string(), m)
}

fn leg(node: &str, member: &str) -> Leg {
    Leg {
        node: node.into(),
        volume_id: Some(format!("{node}-vol")),
        state: LegState::Created,
        message: None,
        master_node: None,
        export: None,
        drive_uuid: None,
        member_uuid: Some(member.into()),
        epoch: None,
    }
}

/// Head node-a holds array "arr" of the legs on node-a (m-a) and node-b
/// (m-b), both active.
async fn setup() -> (Arc<AppState>, M) {
    let mut config = Config::default();
    config.local.enabled = false;
    config.kubernetes.enabled = false;
    let members = vec![("m-a".to_string(), "active".to_string()), ("m-b".to_string(), "active".to_string())];
    let (addr_a, head) = mock_engine(Some(("arr".into(), members))).await;
    let (addr_b, _) = mock_engine(None).await;
    for (n, addr) in [("node-a", addr_a), ("node-b", addr_b)] {
        config.nodes.push(
            toml::from_str::<NodeConfig>(&format!("name = \"{n}\"\nengine_url = \"http://{addr}\""))
                .unwrap(),
        );
    }
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    fed.volumes.insert(
        "v".into(),
        DistVolume {
            name: "v".into(),
            size_bytes: 1 << 30,
            pool: None,
            replicas: 2,
            rung: "node".into(),
            legs: vec![leg("node-a", "m-a"), leg("node-b", "m-b")],
            assembly: AssemblyState::Assembled,
            head: Some("node-a".into()),
            array_id: Some("arr".into()),
            created_at: SystemTime::now(),
            replacing: None,
            next_releg_after: None,
            next_assemble_after: None,
            export: Default::default(),
            epoch: 1,
            fenced: false,
            bandwidth_class: Default::default(),
            extent_size_bytes: None,
            dual_attach: None,
            migration: None,
        },
    );
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    assert_eq!(state.fed.read().await.volumes["v"].assembly, AssemblyState::Assembled);
    (state, head)
}

/// Stall the head past fail_threshold: its leg is lost, the volume degraded.
async fn stall_head(state: &Arc<AppState>, head: &M) {
    head.lock().unwrap().down = true;
    for _ in 0..state.config.poll.fail_threshold {
        stormstorage::registry::poll_once(state).await;
    }
    let fed = state.fed.read().await;
    assert!(!fed.nodes["node-a"].status.healthy);
    let v = &fed.volumes["v"];
    assert_eq!(v.assembly, AssemblyState::Degraded);
    assert_eq!(v.legs[0].state, LegState::Lost);
}

async fn messages(state: &Arc<AppState>) -> Vec<String> {
    state.events.read().await.since(0).into_iter().map(|e| e.message).collect()
}

#[tokio::test]
async fn head_that_answers_again_with_its_array_is_assembled_again() {
    let (state, head) = setup().await;
    stall_head(&state, &head).await;

    head.lock().unwrap().down = false;
    stormstorage::registry::poll_once(&state).await;
    {
        let fed = state.fed.read().await;
        let v = &fed.volumes["v"];
        assert_eq!(v.legs[0].state, LegState::Created);
        assert_eq!(v.legs[0].message, None);
        assert_eq!(v.assembly, AssemblyState::Assembled);
        assert!(v.replacing.is_none(), "no re-leg of the head");
    }
    let ev = messages(&state).await;
    assert!(ev.iter().any(|m| m.contains("leg back, volume assembled")), "{ev:?}");

    // Stays assembled.
    stormstorage::registry::poll_once(&state).await;
    assert_eq!(state.fed.read().await.volumes["v"].assembly, AssemblyState::Assembled);
}

#[tokio::test]
async fn head_that_answers_without_its_array_stays_degraded() {
    let (state, head) = setup().await;
    stall_head(&state, &head).await;

    {
        let mut m = head.lock().unwrap();
        m.down = false;
        m.array = None; // the engine restarted and did not reassemble (#15)
    }
    for _ in 0..3 {
        stormstorage::registry::poll_once(&state).await;
    }
    let fed = state.fed.read().await;
    let v = &fed.volumes["v"];
    assert_eq!(v.assembly, AssemblyState::Degraded);
    assert_eq!(v.legs[0].state, LegState::Lost);
    assert!(v.legs[0].message.as_deref().unwrap_or_default().contains("#15"));
    drop(fed);
    let ev = messages(&state).await;
    let gone = ev.iter().filter(|m| m.contains("no longer holds array arr")).count();
    assert_eq!(gone, 1, "reported once: {ev:?}");
}
