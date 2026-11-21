//! #113 against in-process mock stormblocks: the leg on node-b (not the
//! head) is lost when node-b stops answering, and comes back when it
//! answers again — as itself, never re-legged elsewhere (with two nodes
//! there is nowhere else). Three ways back:
//! - the head's array already has its member active again: leg back,
//!   nothing copied;
//! - the member is missing: the stale member leaves, the same leg volume
//!   is attached and added again, rebuilt, and the cost recorded;
//! - the leg volume is gone from node-b: nothing is changed, an error
//!   event, retried after the cooldown (a re-leg's business).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::{AssemblyState, DistVolume, FedState, Leg, LegState};

#[derive(Default)]
struct Mock {
    /// Every call answers 503: a node powered off.
    down: bool,
    /// The array the engine holds: id → [(member uuid, state)].
    array: Option<(String, Vec<(String, String)>)>,
    /// Engine volumes by id (the leg volumes).
    volumes: Vec<String>,
    /// Members added (drive uuids) and removed (member uuids).
    added: Vec<String>,
    removed: Vec<String>,
    attaches: usize,
}

type M = Arc<Mutex<Mock>>;

fn unavailable() -> (StatusCode, Json<Value>) {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})))
}

async fn capacity(State(m): State<M>) -> (StatusCode, Json<Value>) {
    if m.lock().unwrap().down {
        return unavailable();
    }
    (StatusCode::OK, Json(json!({"total_bytes": 100u64 << 30, "free_bytes": 50u64 << 30})))
}

async fn get_array(State(m): State<M>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    let m = m.lock().unwrap();
    if m.down {
        return unavailable();
    }
    match &m.array {
        Some((a, members)) if *a == id => {
            let members: Vec<Value> = members
                .iter()
                .map(|(u, st)| json!({"uuid": u, "state": st, "device_path": ""}))
                .collect();
            (StatusCode::OK, Json(json!({"id": id, "members": members, "status": {"state": "degraded"}})))
        }
        _ => (StatusCode::NOT_FOUND, Json(json!({"message": format!("array {id}")}))),
    }
}

async fn add_member(State(m): State<M>, Path(id): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    let drive = b["drive_uuid"].as_str().unwrap_or_default().to_string();
    m.added.push(drive);
    let uuid = format!("m-new-{}", m.added.len());
    match &mut m.array {
        Some((a, members)) if *a == id => {
            // The mock rebuilds at once.
            members.push((uuid.clone(), "active".into()));
            (StatusCode::OK, Json(json!({"member_uuid": uuid})))
        }
        _ => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

async fn remove_member(State(m): State<M>, Path((id, member)): Path<(String, String)>) -> StatusCode {
    let mut m = m.lock().unwrap();
    m.removed.push(member.clone());
    match &mut m.array {
        Some((a, members)) if *a == id => {
            members.retain(|(u, _)| *u != member);
            StatusCode::OK
        }
        _ => StatusCode::NOT_FOUND,
    }
}

async fn rebuild_rate() -> StatusCode {
    StatusCode::OK
}

async fn add_drive() -> Json<Value> {
    Json(json!({"uuid": "drive-b"}))
}

async fn delete_drive() -> StatusCode {
    StatusCode::OK
}

async fn get_volume(State(m): State<M>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    let m = m.lock().unwrap();
    if m.down {
        return unavailable();
    }
    if m.volumes.contains(&id) {
        (StatusCode::OK, Json(json!({"id": id, "name": "v"})))
    } else {
        (StatusCode::NOT_FOUND, Json(json!({"message": "no such volume"})))
    }
}

async fn attach(State(m): State<M>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    m.attaches += 1;
    (
        StatusCode::OK,
        Json(json!({
            "transport": "nvme_tcp",
            "nqn": format!("nqn.2024-01.io.stormblock:{id}"),
            "nsid": 1,
            "addresses": [{"traddr": "127.0.0.1", "trsvcid": 4420}],
        })),
    )
}

/// A /v1 create: a new leg volume (a re-leg onto this node).
async fn v1_create(State(m): State<M>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    if m.down {
        return unavailable();
    }
    let id = format!("{}-new", b["name"].as_str().unwrap_or("v"));
    m.volumes.push(id.clone());
    (StatusCode::OK, Json(json!({"id": id, "replicas": [{"node": "node-c", "role": "master"}]})))
}

async fn mock_engine(mock: Mock) -> (String, M) {
    let m: M = Arc::new(Mutex::new(mock));
    let app = Router::new()
        .route("/v1/nodes/capacity", get(capacity))
        .route("/api/v1/arrays/{id}", get(get_array))
        .route("/api/v1/arrays/{id}/members", post(add_member))
        .route("/api/v1/arrays/{id}/members/{member}", delete(remove_member))
        .route("/api/v1/arrays/{id}/rebuild", put(rebuild_rate))
        .route("/api/v1/drives", post(add_drive))
        .route("/api/v1/drives/{id}", delete(delete_drive))
        .route("/api/v1/volumes/{id}", get(get_volume))
        .route("/v1/volumes", post(v1_create))
        .route("/v1/volumes/{id}/attach", post(attach))
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
/// (m-b), both active; node-b holds its leg volume.
async fn setup() -> (Arc<AppState>, M, M) {
    let (state, head, b, _) = setup_with(None, Config::default()).await;
    (state, head, b)
}

/// [`setup`], with a third node `node-c` when given (finding
/// 2cf5182e3a22: somewhere a re-leg can go).
async fn setup_with(c: Option<Mock>, mut config: Config) -> (Arc<AppState>, M, M, Option<M>) {
    config.local.enabled = false;
    config.kubernetes.enabled = false;
    let members = vec![("m-a".to_string(), "active".to_string()), ("m-b".to_string(), "active".to_string())];
    let (addr_a, head) = mock_engine(Mock { array: Some(("arr".into(), members)), ..Default::default() }).await;
    let (addr_b, b) = mock_engine(Mock { volumes: vec!["node-b-vol".into()], ..Default::default() }).await;
    let mut nodes = vec![("node-a", addr_a), ("node-b", addr_b)];
    let mut c_mock = None;
    if let Some(c) = c {
        let (addr_c, cm) = mock_engine(c).await;
        nodes.push(("node-c", addr_c));
        c_mock = Some(cm);
    }
    for (n, addr) in nodes {
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
            rate_pending: false,
            dual_attach: None,
            migration: None,
            clone_of: None,
            expanding: None,
            last_resync: None,
            next_rejoin_after: None,
            tier: None,
            seed: None,
            owner: None,
        },
    );
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    assert_eq!(state.fed.read().await.volumes["v"].assembly, AssemblyState::Assembled);
    (state, head, b, c_mock)
}

/// Power node-b off: past fail_threshold its leg is lost, the volume
/// degraded, and no re-leg can start (there is no third node).
async fn power_off_b(state: &Arc<AppState>, b: &M) {
    b.lock().unwrap().down = true;
    for _ in 0..state.config.poll.fail_threshold {
        stormstorage::registry::poll_once(state).await;
    }
    let fed = state.fed.read().await;
    assert!(!fed.nodes["node-b"].status.healthy);
    let v = &fed.volumes["v"];
    assert_eq!(v.assembly, AssemblyState::Degraded);
    assert_eq!(v.legs[1].state, LegState::Lost);
    assert!(v.replacing.is_none());
}

async fn power_on_b(state: &Arc<AppState>, b: &M) {
    b.lock().unwrap().down = false;
    stormstorage::registry::poll_once(state).await;
}

/// Wait for the rebuild task (spawned by the poll) to finish.
async fn until_assembled(state: &Arc<AppState>) {
    for _ in 0..100 {
        if state.fed.read().await.volumes["v"].assembly == AssemblyState::Assembled {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("never assembled: {:?}", state.fed.read().await.volumes["v"]);
}

async fn messages(state: &Arc<AppState>) -> Vec<String> {
    state.events.read().await.since(0).into_iter().map(|e| e.message).collect()
}

#[tokio::test]
async fn leg_with_missing_member_is_attached_again_and_rebuilt() {
    let (state, head, b) = setup().await;
    power_off_b(&state, &b).await;
    // The head saw its NVMe/TCP leg go: the member is missing.
    head.lock().unwrap().array.as_mut().unwrap().1[1].1 = "missing".into();

    power_on_b(&state, &b).await;
    until_assembled(&state).await;

    let fed = state.fed.read().await;
    let v = &fed.volumes["v"];
    assert_eq!(v.legs.len(), 2);
    let lb = &v.legs[1];
    assert_eq!(lb.node, "node-b");
    assert_eq!(lb.state, LegState::Created);
    assert_eq!(lb.volume_id.as_deref(), Some("node-b-vol"), "the same leg volume");
    assert_eq!(lb.member_uuid.as_deref(), Some("m-new-1"));
    assert_eq!(lb.drive_uuid.as_deref(), Some("drive-b"));
    assert!(lb.export.is_some() && lb.message.is_none());
    assert!(v.replacing.is_none());
    let r = v.last_resync.as_ref().expect("resync recorded");
    assert_eq!((r.node.as_str(), r.mode.as_str(), r.bytes), ("node-b", "rebuild", 1 << 30));
    drop(fed);

    {
        let h = head.lock().unwrap();
        assert_eq!(h.removed, vec!["m-b".to_string()], "the stale member left");
        assert_eq!(h.added, vec!["drive-b".to_string()]);
        let members: Vec<&str> = h.array.as_ref().unwrap().1.iter().map(|(u, _)| u.as_str()).collect();
        assert_eq!(members, vec!["m-a", "m-new-1"]);
    }
    {
        let bm = b.lock().unwrap();
        assert_eq!(bm.attaches, 1);
        assert!(bm.volumes.contains(&"node-b-vol".to_string()), "never deleted");
    }

    let ev = messages(&state).await;
    assert!(ev.iter().any(|m| m.contains("leg on node-b rejoining")), "{ev:?}");
    assert!(ev.iter().any(|m| m.contains("rebuilt 1073741824 bytes") && m.contains("volume assembled")), "{ev:?}");

    // Stays assembled; nothing more is added.
    stormstorage::registry::poll_once(&state).await;
    assert_eq!(state.fed.read().await.volumes["v"].assembly, AssemblyState::Assembled);
    assert_eq!(head.lock().unwrap().added.len(), 1);
}

#[tokio::test]
async fn leg_whose_member_is_active_again_is_back_with_nothing_copied() {
    let (state, head, b) = setup().await;
    power_off_b(&state, &b).await;
    // m-b reads active: the head's initiator reconnected and took it back.
    power_on_b(&state, &b).await;
    until_assembled(&state).await;

    let fed = state.fed.read().await;
    let v = &fed.volumes["v"];
    assert_eq!(v.legs[1].state, LegState::Created);
    assert_eq!(v.legs[1].member_uuid.as_deref(), Some("m-b"));
    let r = v.last_resync.as_ref().expect("resync recorded");
    assert_eq!((r.mode.as_str(), r.bytes), ("member-active", 0));
    drop(fed);
    {
        let h = head.lock().unwrap();
        assert!(h.added.is_empty() && h.removed.is_empty());
    }
    assert_eq!(b.lock().unwrap().attaches, 0);
    let ev = messages(&state).await;
    assert!(ev.iter().any(|m| m.contains("active again, nothing copied")), "{ev:?}");
}

#[tokio::test]
async fn leg_volume_gone_from_its_node_is_not_rejoined() {
    let (state, head, b) = setup().await;
    power_off_b(&state, &b).await;
    head.lock().unwrap().array.as_mut().unwrap().1[1].1 = "missing".into();
    b.lock().unwrap().volumes.clear();

    power_on_b(&state, &b).await;
    let fed = state.fed.read().await;
    let v = &fed.volumes["v"];
    assert_eq!(v.assembly, AssemblyState::Degraded);
    assert_eq!(v.legs[1].state, LegState::Lost);
    assert!(v.replacing.is_none());
    assert!(v.next_rejoin_after.is_some(), "cooling down");
    drop(fed);
    {
        let h = head.lock().unwrap();
        assert!(h.added.is_empty() && h.removed.is_empty(), "nothing changed on the head");
    }
    let ev = messages(&state).await;
    assert!(ev.iter().any(|m| m.contains("did not rejoin") && m.contains("re-legged")), "{ev:?}");
    assert!(state.fed.read().await.volumes["v"].legs[1].volume_id.is_none(), "no rejoin of a volume that is gone");
}

/// Finding 2cf5182e3a22: node-b answers again without its leg volume, and
/// a third node is there. The leg is re-legged onto node-c once the re-leg
/// cooldown (set while node-c was down too) has passed; it used to be
/// tried as a rejoin on every pass and never re-legged.
#[tokio::test]
async fn a_leg_whose_volume_is_gone_is_re_legged_after_the_cooldown() {
    let mut config = Config::default();
    config.recovery.cooldown_secs = 1;
    let (state, head, b, c) = setup_with(Some(Mock { down: true, ..Default::default() }), config).await;
    let c = c.unwrap();
    // node-b goes, with node-c down: nowhere to re-leg, a cooldown is set.
    power_off_b(&state, &b).await;
    assert!(state.fed.read().await.volumes["v"].next_releg_after.is_some(), "a failed re-leg cools down");
    head.lock().unwrap().array.as_mut().unwrap().1[1].1 = "missing".into();
    b.lock().unwrap().volumes.clear();
    power_on_b(&state, &b).await;
    assert!(state.fed.read().await.volumes["v"].legs[1].volume_id.is_none(), "the gone volume is let go");

    c.lock().unwrap().down = false;
    let mut done = false;
    for _ in 0..60 {
        stormstorage::registry::poll_once(&state).await;
        let fed = state.fed.read().await;
        let v = &fed.volumes["v"];
        if v.assembly == AssemblyState::Assembled && v.legs.iter().any(|l| l.node == "node-c") {
            done = true;
            break;
        }
        drop(fed);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let fed = state.fed.read().await;
    let v = &fed.volumes["v"];
    assert!(done, "never re-legged onto node-c: {v:?}\n{:?}", messages(&state).await);
    let nodes: Vec<&str> = v.legs.iter().map(|l| l.node.as_str()).collect();
    assert_eq!(nodes, vec!["node-a", "node-c"], "node-b's leg retired");
    assert!(c.lock().unwrap().volumes.contains(&"v-new".to_string()), "a new leg volume on node-c");
    assert_eq!(b.lock().unwrap().attaches, 0, "never rejoined");
}
