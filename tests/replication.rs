//! #33 against in-process mock stormblocks: sync state read from the
//! head's array, the epoch CAS (fence) carried down to every leg's /v1
//! epoch, promote onto a surviving leg's node after the head is lost (the
//! array put back together from the legs' superblocks, the served volume
//! that came with it served again), the former head cleaned up only once
//! it no longer holds the array, dual-attach windows, prestage with a
//! bandwidth class, and sync from the slave's superblock once the head is
//! lost (#48).
//!
//! The mocks keep the rules stormblock v19.4 enforces (src/mgmt/api/{v1,
//! arrays,volumes}.rs): /v1 fence is a CAS on the volume's epoch (412 +
//! `current_epoch`); `POST /api/v1/arrays/assemble` finds an array by its
//! members' superblocks and adopts the slab with its volumes, which /v1
//! does not know, so they are attached through `/api/v1/volumes/{id}/attach`.
//! Superblocks and slabs live on the legs, so they are shared between the
//! mock engines.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::FedState;

/// What lives on the legs themselves: member superblocks (leg namespace →
/// array id) and each array's slab (array id → [(volume id, name)]).
#[derive(Default)]
struct Disks {
    superblocks: BTreeMap<String, String>,
    slabs: BTreeMap<String, Vec<(String, String)>>,
}

struct Member {
    uuid: String,
    path: String,
    state: String,
    rebuilt: Option<u64>,
}

#[derive(Default)]
struct Mock {
    node: String,
    next: u32,
    /// /v1 volumes: id → (name, pinned array, epoch).
    volumes: BTreeMap<String, (String, Option<String>, u64)>,
    /// Volumes adopted with an assembled array (no /v1 record).
    adopted: BTreeMap<String, String>,
    drives: BTreeMap<String, String>,
    arrays: BTreeMap<String, Vec<Member>>,
    rates: BTreeMap<String, u64>,
    /// /v1 volume id → the RAID superblock read from it (stormblock#309);
    /// unset = 404, as on an engine without the route.
    sbs: BTreeMap<String, Value>,
    /// Extent sizes this node has pools for (stormblock#156); empty = any.
    extent_sizes: Vec<u64>,
    log: Vec<String>,
}

#[derive(Clone)]
struct S {
    m: Arc<Mutex<Mock>>,
    disks: Arc<Mutex<Disks>>,
}

/// The namespace a drive path opens: the URI without `hostnqn=`, which
/// names who connects (#27), not what is on the disk.
fn disk_of(path: &str) -> String {
    path.split("&hostnqn=").next().unwrap().to_string()
}

fn coords(node: &str, nsid: u32) -> Value {
    json!({"transport": "nvme_tcp", "nqn": format!("nqn.2024.io.stormblock:{node}"), "nsid": nsid,
           "addresses": [{"traddr": "0.0.0.0", "trsvcid": 4420}]})
}

async fn v1_create(State(s): State<S>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = s.m.lock().unwrap();
    let name = b["name"].as_str().unwrap().to_string();
    let pin = b["placement"]["array_id"].as_str().map(|x| x.to_string());
    let node = m.node.clone();
    let extent = b.get("extent_size_bytes").and_then(|x| x.as_u64());
    m.log.push(format!("create {name} extent={extent:?}"));
    if let Some(e) = extent {
        if pin.is_none() && !m.extent_sizes.is_empty() && !m.extent_sizes.contains(&e) {
            let msg = format!("backing volume create failed: no data slab with {e}-byte slots (this node has {:?})", m.extent_sizes);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"code": "internal", "message": msg})));
        }
    }
    let existing = m.volumes.iter().find(|(_, v)| v.0 == name).map(|(id, _)| id.clone());
    let id = existing.unwrap_or_else(|| {
        m.next += 1;
        let id = format!("{node}-vol-{}", m.next);
        m.volumes.insert(id.clone(), (name.clone(), pin.clone(), 1));
        if let Some(a) = &pin {
            s.disks.lock().unwrap().slabs.entry(a.clone()).or_default().push((id.clone(), name.clone()));
        }
        id
    });
    (StatusCode::OK, Json(json!({"id": id, "name": name, "replicas": [{"node": node, "role": "master"}]})))
}

async fn v1_get(State(s): State<S>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    let m = s.m.lock().unwrap();
    match m.volumes.get(&id) {
        Some((name, _, epoch)) => (StatusCode::OK, Json(json!({"id": id, "name": name, "epoch": epoch}))),
        None => (StatusCode::NOT_FOUND, Json(json!({"code": "not_found"}))),
    }
}

async fn v1_fence(State(s): State<S>, Path(id): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = s.m.lock().unwrap();
    let Some(v) = m.volumes.get_mut(&id) else {
        return (StatusCode::NOT_FOUND, Json(json!({})));
    };
    if b["expected_epoch"].as_u64() != Some(v.2) {
        let c = v.2;
        return (StatusCode::PRECONDITION_FAILED, Json(json!({"code": "stale_epoch", "current_epoch": c})));
    }
    v.2 += 1;
    let e = v.2;
    m.log.push(format!("fence {id} -> {e}"));
    (StatusCode::OK, Json(json!({"epoch": e})))
}

async fn v1_attach(State(s): State<S>, Path(id): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = s.m.lock().unwrap();
    if !m.volumes.contains_key(&id) || b["node"] != m.node.as_str() || b["transport"] != "nvme_tcp" {
        return (StatusCode::NOT_FOUND, Json(json!({"message": id})));
    }
    m.log.push(format!("attach {id} host={} epoch={}", b["host_nqn"].as_str().unwrap_or("-"), b["epoch"]));
    let nsid = id.rsplit('-').next().unwrap().parse::<u32>().unwrap();
    let node = m.node.clone();
    (StatusCode::OK, Json(coords(&node, nsid)))
}

async fn v1_detach(State(s): State<S>, Path(id): Path<String>) -> Json<Value> {
    s.m.lock().unwrap().log.push(format!("detach {id}"));
    Json(json!({}))
}

async fn v1_delete(State(s): State<S>, Path(id): Path<String>) -> StatusCode {
    let mut m = s.m.lock().unwrap();
    m.volumes.remove(&id);
    m.log.push(format!("delete {id}"));
    StatusCode::NO_CONTENT
}

async fn v1_superblock(State(s): State<S>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    match s.m.lock().unwrap().sbs.get(&id) {
        Some(v) => (StatusCode::OK, Json(v.clone())),
        None => (StatusCode::NOT_FOUND, Json(json!({"code": "no_superblock"}))),
    }
}

async fn add_drive(State(s): State<S>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = s.m.lock().unwrap();
    m.next += 1;
    let uuid = format!("{}-drive-{}", m.node, m.next);
    m.drives.insert(uuid.clone(), b["path"].as_str().unwrap().to_string());
    Json(json!({"uuid": uuid}))
}

async fn close_drive(State(s): State<S>, Path(p): Path<String>) -> StatusCode {
    let mut m = s.m.lock().unwrap();
    m.drives.retain(|_, path| *path != p);
    m.log.push(format!("close drive {p}"));
    StatusCode::NO_CONTENT
}

fn array_json(id: &str, members: &[Member]) -> Value {
    let ms: Vec<Value> = members
        .iter()
        .enumerate()
        .map(|(i, x)| {
            json!({"index": i, "uuid": x.uuid, "state": x.state, "device_path": x.path, "rebuilt_bytes": x.rebuilt})
        })
        .collect();
    json!({"id": id, "member_data_bytes": 1000, "events": 7, "status": {"state": "clean"}, "members": ms})
}

async fn create_array(State(s): State<S>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = s.m.lock().unwrap();
    m.next += 1;
    let id = format!("00000000-0000-0000-0000-{:012}", m.next);
    let members: Vec<Member> = b["drive_uuids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            let path = m.drives[d.as_str().unwrap()].clone();
            Member { uuid: format!("m-{path}"), path, state: "active".into(), rebuilt: None }
        })
        .collect();
    {
        let mut disks = s.disks.lock().unwrap();
        for x in &members {
            disks.superblocks.insert(disk_of(&x.path), id.clone());
        }
    }
    let out = array_json(&id, &members);
    m.arrays.insert(id.clone(), members);
    m.log.push(format!("array {id}"));
    Json(out)
}

async fn list_arrays(State(s): State<S>) -> Json<Value> {
    let m = s.m.lock().unwrap();
    let items: Vec<Value> = m.arrays.iter().map(|(id, ms)| array_json(id, ms)).collect();
    Json(json!({"items": items}))
}

async fn get_array(State(s): State<S>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    let m = s.m.lock().unwrap();
    match m.arrays.get(&id) {
        Some(ms) => (StatusCode::OK, Json(array_json(&id, ms))),
        None => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

async fn delete_array(State(s): State<S>, Path(id): Path<String>) -> StatusCode {
    let mut m = s.m.lock().unwrap();
    m.arrays.remove(&id);
    m.log.push(format!("delete array {id}"));
    StatusCode::NO_CONTENT
}

async fn assemble(State(s): State<S>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = s.m.lock().unwrap();
    let disks = s.disks.lock().unwrap();
    let mut found: BTreeMap<String, Vec<Member>> = BTreeMap::new();
    for d in b["drive_uuids"].as_array().unwrap() {
        let path = m.drives[d.as_str().unwrap()].clone();
        if let Some(a) = disks.superblocks.get(&disk_of(&path)) {
            found.entry(a.clone()).or_default().push(Member {
                uuid: format!("m-{path}"),
                path,
                state: "active".into(),
                rebuilt: None,
            });
        }
    }
    let mut arrays = Vec::new();
    for (id, members) in found {
        for (vid, name) in disks.slabs.get(&id).cloned().unwrap_or_default() {
            m.adopted.insert(vid, name);
        }
        arrays.push(json!({"id": id, "state": "degraded", "already": false}));
        m.arrays.insert(id.clone(), members);
        m.log.push(format!("assemble {id}"));
    }
    Json(json!({"arrays": arrays, "refused": []}))
}

async fn set_rate(State(s): State<S>, Path(id): Path<String>, Json(b): Json<Value>) -> Json<Value> {
    s.m.lock().unwrap().rates.insert(id, b["max_bytes_per_sec"].as_u64().unwrap());
    Json(json!({}))
}

async fn add_member(State(s): State<S>, Path(id): Path<String>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = s.m.lock().unwrap();
    let path = m.drives[b["drive_uuid"].as_str().unwrap()].clone();
    let uuid = format!("m-{path}");
    m.arrays.get_mut(&id).unwrap().push(Member {
        uuid: uuid.clone(),
        path,
        state: "active".into(),
        rebuilt: None,
    });
    m.log.push(format!("add member {uuid}"));
    Json(json!({"member_uuid": uuid}))
}

async fn remove_member(State(s): State<S>, Path((id, mu)): Path<(String, String)>) -> StatusCode {
    let mut m = s.m.lock().unwrap();
    if let Some(ms) = m.arrays.get_mut(&id) {
        ms.retain(|x| x.uuid != mu);
    }
    m.log.push(format!("remove member {mu}"));
    StatusCode::NO_CONTENT
}

async fn engine_volumes(State(s): State<S>) -> Json<Value> {
    let m = s.m.lock().unwrap();
    let items: Vec<Value> = m
        .volumes
        .iter()
        .map(|(id, v)| (id.clone(), v.0.clone()))
        .chain(m.adopted.iter().map(|(id, n)| (id.clone(), n.clone())))
        .map(|(id, name)| json!({"id": id, "name": name}))
        .collect();
    Json(json!({"items": items}))
}

async fn any_attach(State(s): State<S>, Path(id): Path<String>, body: axum::body::Bytes) -> (StatusCode, Json<Value>) {
    let mut m = s.m.lock().unwrap();
    if !m.adopted.contains_key(&id) && !m.volumes.contains_key(&id) {
        return (StatusCode::NOT_FOUND, Json(json!({})));
    }
    let b: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let node = m.node.clone();
    // A named consumer host (#51): served from that host's own subsystem.
    if let Some(h) = b["host_nqn"].as_str() {
        m.log.push(format!("attach-any {id} host={h}"));
        let mut c = coords(&node, 77);
        c["nqn"] = json!(format!("nqn.2024.io.stormblock:{node}:host:{h}"));
        return (StatusCode::OK, Json(c));
    }
    m.log.push(format!("attach-any {id}"));
    (StatusCode::OK, Json(coords(&node, 77)))
}

async fn any_detach(State(s): State<S>, Path(id): Path<String>) -> StatusCode {
    s.m.lock().unwrap().log.push(format!("detach-any {id}"));
    StatusCode::NO_CONTENT
}

async fn any_delete(State(s): State<S>, Path(id): Path<String>) -> StatusCode {
    let mut m = s.m.lock().unwrap();
    m.adopted.remove(&id);
    m.volumes.remove(&id);
    m.log.push(format!("delete-any {id}"));
    StatusCode::NO_CONTENT
}

async fn mock_engine(node: &str, disks: Arc<Mutex<Disks>>) -> (String, Arc<Mutex<Mock>>) {
    let m = Arc::new(Mutex::new(Mock { node: node.into(), ..Default::default() }));
    let s = S { m: m.clone(), disks };
    let app = Router::new()
        .route(
            "/v1/nodes/capacity",
            get(|| async { Json(json!({"total_bytes": 100u64 << 30, "free_bytes": 50u64 << 30})) }),
        )
        .route("/v1/volumes", post(v1_create))
        .route("/v1/volumes/{id}", get(v1_get).delete(v1_delete))
        .route("/v1/volumes/{id}/attach", post(v1_attach))
        .route("/v1/volumes/{id}/detach", post(v1_detach))
        .route("/v1/volumes/{id}/fence", post(v1_fence))
        .route("/v1/volumes/{id}/raid-superblock", get(v1_superblock))
        .route("/api/v1/drives", post(add_drive))
        .route("/api/v1/drives/{p}", delete(close_drive))
        .route("/api/v1/arrays", post(create_array).get(list_arrays))
        .route("/api/v1/arrays/assemble", post(assemble))
        .route("/api/v1/arrays/{id}", get(get_array).delete(delete_array))
        .route("/api/v1/arrays/{id}/rebuild", put(set_rate))
        .route("/api/v1/arrays/{id}/members", post(add_member))
        .route("/api/v1/arrays/{id}/members/{m}", delete(remove_member))
        .route("/api/v1/volumes", get(engine_volumes))
        .route("/api/v1/volumes/{id}", delete(any_delete))
        .route("/api/v1/volumes/{id}/attach", post(any_attach).delete(any_detach))
        .with_state(s);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr.to_string(), m)
}

type Mocks = BTreeMap<String, Arc<Mutex<Mock>>>;

async fn setup(nodes: &[&str]) -> (String, Arc<AppState>, Mocks) {
    setup_cfg(nodes, |_| {}).await
}

async fn setup_cfg(nodes: &[&str], tweak: impl FnOnce(&mut Config)) -> (String, Arc<AppState>, Mocks) {
    let mut config = Config::default();
    tweak(&mut config);
    config.local.enabled = false;
    config.kubernetes.enabled = false;
    let disks = Arc::new(Mutex::new(Disks::default()));
    let mut mocks = BTreeMap::new();
    for n in nodes {
        let (addr, m) = mock_engine(n, disks.clone()).await;
        config
            .nodes
            .push(toml::from_str::<NodeConfig>(&format!("name = \"{n}\"\nengine_url = \"http://{addr}\"")).unwrap());
        mocks.insert(n.to_string(), m);
    }
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{api}"), state, mocks)
}

async fn call(url: String, body: Value) -> (u16, Value) {
    let r = reqwest::Client::new().post(url).json(&body).send().await.unwrap();
    let st = r.status().as_u16();
    (st, r.json().await.unwrap_or(Value::Null))
}

async fn get_json(url: String) -> Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

async fn create_mirror(api: &str, name: &str) -> (String, String, String) {
    let (st, v) = call(format!("{api}/api/v1/volumes"), json!({"name": name, "size_bytes": 1u64 << 30, "replicas": 2})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["assembly"], "assembled", "{v}");
    assert_eq!(v["export"]["state"], "published", "{v}");
    let head = v["head"].as_str().unwrap().to_string();
    let other = v["legs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["node"].as_str().unwrap().to_string())
        .find(|n| *n != head)
        .unwrap();
    (head, other, v["array_id"].as_str().unwrap().to_string())
}

#[tokio::test]
async fn sync_state_comes_from_the_head_array() {
    let (api, state, mocks) = setup(&["node-a", "node-b"]).await;
    let (head, other, array) = create_mirror(&api, "s").await;

    // Not read yet: never in sync on no evidence.
    let r = get_json(format!("{api}/api/v1/volumes/s/replicas")).await;
    assert!(r["replicas"].as_array().unwrap().iter().all(|x| x["sync"]["state"] == "detached"), "{r}");
    assert_eq!(r["health"], "faulted");

    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/s/replicas")).await;
    assert_eq!(r["health"], "healthy", "{r}");
    assert_eq!(r["epoch"], 1);
    let reps = r["replicas"].as_array().unwrap();
    let master = reps.iter().find(|x| x["role"] == "master").unwrap();
    assert_eq!(master["node"], head.as_str());
    assert!(reps.iter().all(|x| x["sync"] == json!({"state": "in_sync"})), "{r}");

    // The slave's member rebuilding a quarter of the way.
    {
        let mut m = mocks[&head].lock().unwrap();
        let ms = m.arrays.get_mut(&array).unwrap();
        let x = ms.iter_mut().find(|x| x.path.contains(&other)).unwrap();
        x.state = "rebuilding".into();
        x.rebuilt = Some(250);
    }
    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/s/replicas")).await;
    let slave = r["replicas"].as_array().unwrap().iter().find(|x| x["role"] == "slave").unwrap().clone();
    assert_eq!(slave["sync"], json!({"state": "resyncing", "progress_pct": 25.0, "lag_bytes": 750}));
    assert_eq!(r["health"], "degraded");
    // The full view carries it too, and the feed.
    let v = get_json(format!("{api}/api/v1/volumes/s")).await;
    assert_eq!(v["health"], "degraded");
    assert_eq!(v["replica_sync"].as_array().unwrap().len(), 2);
    let feed = stormstorage::components::collect(&state).await;
    let c = feed.iter().find(|c| c.id == "volume:s").unwrap();
    assert!(c.metrics.iter().any(|m| m.label == "in sync" && m.value == "1/2"));
    assert!(c.metrics.iter().any(|m| m.label == "resync" && m.value == format!("{other} 25.0%")));
    // Head not reachable: the reading goes.
    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = false;
    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/s/replicas")).await;
    assert_eq!(r["health"], "faulted");
}

/// #48: the head is lost, so its array cannot be read; the slave's own
/// superblock, read on the slave's engine, says whether it was in sync.
#[tokio::test]
async fn head_lost_sync_from_the_slave_superblock() {
    let (api, state, mocks) = setup(&["node-a", "node-b"]).await;
    let (head, other, array) = create_mirror(&api, "h").await;
    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/h/replicas")).await;
    assert_eq!((r["health"].as_str(), r["sync_source"].as_str()), (Some("healthy"), Some("head")), "{r}");

    let (slave_vol, members) = {
        let fed = state.fed.read().await;
        let v = &fed.volumes["h"];
        let leg = v.legs.iter().find(|l| l.node == other).unwrap();
        let members: Vec<String> = v.legs.iter().map(|l| l.member_uuid.clone().unwrap()).collect();
        (leg.volume_id.clone().unwrap(), (leg.member_uuid.clone().unwrap(), members))
    };
    let sb = |events: u64| {
        let slots: Vec<Value> = members.1.iter().enumerate()
            .map(|(i, m)| json!({"slot": i, "member_uuid": m, "state": "active", "rebuilt_to": 0}))
            .collect();
        json!({"array_uuid": array, "member_uuid": members.0, "slot": 1, "level": "raid1",
               "events": events, "data_size": 1000, "slots": slots})
    };
    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = false;

    // The engine has no superblock route (404): no evidence, detached.
    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/h/replicas")).await;
    assert_eq!(r["health"], "faulted", "{r}");

    // As new as the last live reading (events 7) and active there: in sync.
    mocks[&other].lock().unwrap().sbs.insert(slave_vol.clone(), sb(7));
    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/h/replicas")).await;
    assert_eq!(r["sync_source"], "superblock", "{r}");
    assert!(r["head_read_at"].is_object() || r["head_read_at"].is_string(), "{r}");
    let reps = r["replicas"].as_array().unwrap();
    let sync = |n: &str| reps.iter().find(|x| x["node"] == n).unwrap()["sync"]["state"].clone();
    assert_eq!(sync(&other), "in_sync", "{r}");
    assert_eq!(sync(&head), "detached");
    assert_eq!(r["health"], "degraded");
    let v = get_json(format!("{api}/api/v1/volumes/h")).await;
    assert_eq!(v["sync_source"], "superblock");
    let feed = stormstorage::components::collect(&state).await;
    let c = feed.iter().find(|c| c.id == "volume:h").unwrap();
    assert!(c.metrics.iter().any(|m| m.label == "sync from"), "{:?}", c.metrics);

    // Older than what the head last said: stale, detached.
    mocks[&other].lock().unwrap().sbs.insert(slave_vol.clone(), sb(6));
    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/h/replicas")).await;
    assert_eq!(r["health"], "faulted", "{r}");

    // The head answers again: the live reading is back.
    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = true;
    stormstorage::head::refresh(&state).await;
    let r = get_json(format!("{api}/api/v1/volumes/h/replicas")).await;
    assert_eq!((r["health"].as_str(), r["sync_source"].as_str()), (Some("healthy"), Some("head")), "{r}");
}

#[tokio::test]
async fn fence_then_promote_after_the_head_is_lost() {
    let (api, state, mocks) = setup(&["node-a", "node-b"]).await;
    let (head, target, array) = create_mirror(&api, "p").await;
    let served = state.fed.read().await.volumes["p"].export.volume_id.clone().unwrap();

    // Promote without a fence: 412 at the current epoch.
    let (st, e) = call(format!("{api}/api/v1/volumes/p/promote"), json!({"target_node": target, "fenced_epoch": 1})).await;
    assert_eq!((st, e["code"].as_str(), e["current_epoch"].as_u64()), (412, Some("stale_epoch"), Some(1)));
    // Fence is a CAS.
    let (st, e) = call(format!("{api}/api/v1/volumes/p/fence"), json!({"expected_epoch": 2})).await;
    assert_eq!((st, e["current_epoch"].as_u64()), (412, Some(1)));
    let (st, f) = call(format!("{api}/api/v1/volumes/p/fence"), json!({"expected_epoch": 1})).await;
    assert_eq!(st, 200, "{f}");
    assert_eq!(f["epoch"], 2);
    assert_eq!(f["legs_fenced"].as_array().unwrap().len(), 2, "{f}");
    let (st, e) = call(format!("{api}/api/v1/volumes/p/fence"), json!({"expected_epoch": 1})).await;
    assert_eq!((st, e["current_epoch"].as_u64()), (412, Some(2)), "a second tiebreaker loses");
    // Each leg's own /v1 epoch went up on its engine.
    for n in [&head, &target] {
        let m = mocks[n].lock().unwrap();
        assert!(m.log.iter().any(|l| l.starts_with("fence ") && l.ends_with("-> 2")), "{:?}", m.log);
    }

    // The head is alive: a handover needs stormblock#296.
    let (st, e) = call(format!("{api}/api/v1/volumes/p/promote"), json!({"target_node": target, "fenced_epoch": 2})).await;
    assert_eq!(st, 409);
    assert!(e["error"].as_str().unwrap().contains("stormblock#296"), "{e}");
    // A node with no leg.
    let (st, _) = call(format!("{api}/api/v1/volumes/p/promote"), json!({"target_node": "node-z", "fenced_epoch": 2})).await;
    assert_eq!(st, 409);

    // The head is lost.
    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = false;
    let (st, v) = call(format!("{api}/api/v1/volumes/p/promote"), json!({"target_node": target, "fenced_epoch": 2})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["head"], target.as_str());
    assert_eq!(v["array_id"], array.as_str(), "same array, put back together");
    assert_eq!(v["fenced"], false);
    assert_eq!(v["assembly"], "degraded");
    let lost = v["legs"].as_array().unwrap().iter().find(|l| l["node"] == head.as_str()).unwrap();
    assert_eq!(lost["state"], "lost");
    // Served again: the volume that came with the array, not a new one.
    let ex = &v["export"];
    assert_eq!(ex["state"], "published", "{v}");
    assert_eq!(ex["adopted"], true);
    assert_eq!(ex["volume_id"], served.as_str());
    assert_eq!(ex["node"], target.as_str());
    assert_eq!(ex["coordinates_changed"], true);
    {
        let m = mocks[&target].lock().unwrap();
        assert!(m.log.contains(&format!("assemble {array}")), "{:?}", m.log);
        assert!(m.log.contains(&format!("attach-any {served}")), "{:?}", m.log);
        assert!(!m.volumes.values().any(|v| v.0 == "p-mirror"), "no new served volume made");
        // The leg was attached at its fenced epoch.
        assert!(m.log.iter().any(|l| l.starts_with("attach ") && l.ends_with("epoch=2")), "{:?}", m.log);
        // …and served to the new head alone (#27).
        let host = format!("host=nqn.2026-10.lo.storm:stormstorage:{target} epoch=2");
        assert!(m.log.iter().any(|l| l.starts_with("attach ") && l.ends_with(&host)), "{:?}", m.log);
        assert_eq!(m.rates[&array], 200 << 20, "normal class rate on the new head");
    }
    // Promote again at the same epoch: no longer fenced.
    let (st, _) = call(format!("{api}/api/v1/volumes/p/promote"), json!({"target_node": target, "fenced_epoch": 2})).await;
    assert_eq!(st, 412);

    // The former head is recorded, and left alone while it holds the array.
    assert_eq!(state.fed.read().await.stale_heads.len(), 1);
    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = true;
    stormstorage::head::reap_stale_heads(&state).await;
    assert_eq!(state.fed.read().await.stale_heads.len(), 1, "still holds the array");
    assert!(!mocks[&head].lock().unwrap().log.iter().any(|l| l.starts_with("close drive")));
    // It restarted without the array: its leftover drives are closed.
    mocks[&head].lock().unwrap().arrays.clear();
    stormstorage::head::reap_stale_heads(&state).await;
    assert!(state.fed.read().await.stale_heads.is_empty());
    assert!(mocks[&head].lock().unwrap().log.iter().any(|l| l.starts_with("close drive")));
    let g = get_json(format!("{api}/api/v1/stale-heads")).await;
    assert_eq!(g["stale_heads"], json!([]));

    // Deleting it revokes the adopted served volume through /api/v1.
    let r = reqwest::Client::new().delete(format!("{api}/api/v1/volumes/p")).send().await.unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let m = mocks[&target].lock().unwrap();
    assert!(m.log.contains(&format!("detach-any {served}")) && m.log.contains(&format!("delete-any {served}")), "{:?}", m.log);
}

/// #51: a consumer host served before a promote is served again by the
/// new head, from its own subsystem there, through the adopted volume's
/// engine id; the record says its coordinates changed.
#[tokio::test]
async fn promote_reserves_the_consumer_hosts() {
    let (api, state, mocks) = setup(&["node-a", "node-b"]).await;
    let (head, target, _array) = create_mirror(&api, "q").await;
    let (st, h) = call(format!("{api}/api/v1/volumes/q/export/hosts"), json!({"host_nqn": "nqn.c1"})).await;
    assert_eq!(st, 200, "{h}");
    assert_eq!(h["coordinates"]["nqn"], format!("nqn.2024.io.stormblock:{head}:host:nqn.c1"));
    let served = state.fed.read().await.volumes["q"].export.volume_id.clone().unwrap();
    assert!(mocks[&head].lock().unwrap().log.contains(&format!("attach-any {served} host=nqn.c1")));

    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = false;
    let (st, _) = call(format!("{api}/api/v1/volumes/q/fence"), json!({"expected_epoch": 1})).await;
    assert_eq!(st, 200);
    let (st, v) = call(format!("{api}/api/v1/volumes/q/promote"), json!({"target_node": target, "fenced_epoch": 2})).await;
    assert_eq!(st, 200, "{v}");
    let ex = &v["export"];
    assert_eq!(ex["state"], "published", "{v}");
    assert_eq!(ex["hosts"][0]["host_nqn"], "nqn.c1");
    assert_eq!(ex["hosts"][0]["coordinates"]["nqn"], format!("nqn.2024.io.stormblock:{target}:host:nqn.c1"), "{v}");
    assert_eq!(ex["hosts"][0]["coordinates_changed"], true);
    assert_eq!(ex["coordinates_changed"], true);
    let log = mocks[&target].lock().unwrap().log.clone();
    assert!(log.contains(&format!("attach-any {served} host=nqn.c1")), "{log:?}");
    assert!(!log.contains(&format!("attach-any {served}")), "no shared attach once hosts are named: {log:?}");
}

/// #15: the head's engine restarts and forgets its runtime `nvme-tcp://`
/// leg drives and the array on them. The next poll opens the legs again,
/// puts the *same* array back together from their superblocks
/// (`/api/v1/arrays/assemble`, never a create), and serves the volume that
/// came back with its slab. A poll after that leaves it alone.
#[tokio::test]
async fn a_head_engine_restart_is_reassembled_and_served_again() {
    let (api, state, mocks) = setup(&["node-a", "node-b"]).await;
    let (head, _other, array) = create_mirror(&api, "r").await;
    let served = state.fed.read().await.volumes["r"].export.volume_id.clone().unwrap();
    {
        let mut m = mocks[&head].lock().unwrap();
        m.arrays.clear();
        m.drives.clear();
        m.log.clear();
    }
    stormstorage::registry::poll_once(&state).await;
    {
        let fed = state.fed.read().await;
        let v = &fed.volumes["r"];
        assert_eq!(v.head.as_deref(), Some(head.as_str()));
        assert_eq!(v.array_id.as_deref(), Some(array.as_str()), "the same array");
        assert_eq!(v.assembly, stormstorage::model::AssemblyState::Assembled);
        assert_eq!(v.export.state, stormstorage::model::ExportState::Published, "{:?}", v.export);
        assert_eq!(v.export.volume_id.as_deref(), Some(served.as_str()), "the served volume that came back");
        assert!(v.export.adopted);
    }
    let log = mocks[&head].lock().unwrap().log.clone();
    assert!(log.contains(&format!("assemble {array}")), "{log:?}");
    assert!(!log.iter().any(|l| l.starts_with("array ")), "never a create: {log:?}");
    assert!(log.contains(&format!("attach-any {served}")), "{log:?}");
    let ev = reqwest::get(format!("{api}/api/v1/events")).await.unwrap().text().await.unwrap();
    assert!(ev.contains("reassembled"), "{ev}");

    // Present again: the next poll does nothing to it.
    mocks[&head].lock().unwrap().log.clear();
    stormstorage::registry::poll_once(&state).await;
    let log = mocks[&head].lock().unwrap().log.clone();
    assert!(!log.iter().any(|l| l.starts_with("assemble")), "{log:?}");
}

/// The slave's superblock as the mock engine serves it (#48), in sync at
/// the head's last event count.
async fn slave_superblock(state: &Arc<AppState>, mocks: &Mocks, name: &str, slave: &str, array: &str) {
    let fed = state.fed.read().await;
    let v = &fed.volumes[name];
    let leg = v.legs.iter().find(|l| l.node == slave).unwrap();
    let slots: Vec<Value> = v
        .legs
        .iter()
        .enumerate()
        .map(|(i, l)| json!({"slot": i, "member_uuid": l.member_uuid, "state": "active", "rebuilt_to": 0}))
        .collect();
    mocks[slave].lock().unwrap().sbs.insert(
        leg.volume_id.clone().unwrap(),
        json!({"array_uuid": array, "member_uuid": leg.member_uuid, "events": 7, "data_size": 1000, "slots": slots}),
    );
}

/// The head is lost for good. With `[recovery] rehead` off (the default)
/// nothing moves; with it on, the volume is fenced and the in-sync slave's
/// node promoted (#14).
#[tokio::test]
async fn rehead_is_off_by_default_and_promotes_the_in_sync_leg_when_on() {
    for on in [false, true] {
        let (api, state, mocks) = setup_cfg(&["node-a", "node-b"], |c| {
            c.recovery.rehead = on;
            c.recovery.rehead_after_secs = 30;
        })
        .await;
        let (head, other, array) = create_mirror(&api, "rh").await;
        stormstorage::head::refresh(&state).await;
        slave_superblock(&state, &mocks, "rh", &other, &array).await;
        {
            let mut fed = state.fed.write().await;
            let n = fed.nodes.get_mut(&head).unwrap();
            n.status.healthy = false;
            n.status.consecutive_failures = 10;
        }
        stormstorage::head::refresh(&state).await;
        stormstorage::orchestrate::reconcile(&state).await;
        let fed = state.fed.read().await;
        let v = &fed.volumes["rh"];
        if !on {
            assert_eq!(v.head.as_deref(), Some(head.as_str()), "off by default: no re-head");
            assert_eq!(v.epoch, 1);
            assert!(!v.fenced);
            continue;
        }
        assert_eq!(v.head.as_deref(), Some(other.as_str()), "{v:?}");
        assert_eq!(v.epoch, 2, "fenced once");
        assert!(!v.fenced, "promoted");
        assert_eq!(v.array_id.as_deref(), Some(array.as_str()));
        assert_eq!(v.export.state, stormstorage::model::ExportState::Published, "{:?}", v.export);
        drop(fed);
        let ev = reqwest::get(format!("{api}/api/v1/events")).await.unwrap().text().await.unwrap();
        assert!(ev.contains("re-heading automatically"), "{ev}");
    }
}

/// #32: a tier migration moves the non-head leg into the destination pool
/// through the leg-move sequence, then waits on the head's handover
/// (stormblock#296); a cancel stops it; refusals for an unknown pool, a
/// pool that already holds every leg, and a single leg.
#[tokio::test]
async fn tier_migration_moves_legs_into_the_destination_pool() {
    let (api, state, _mocks) = setup_cfg(&["node-a", "node-b", "node-c", "node-d"], |c| {
        c.pools = vec![
            toml::from_str("name = \"src\"\n[selector]\nnodes = [\"node-a\", \"node-b\"]").unwrap(),
            toml::from_str("name = \"dst\"\n[selector]\nnodes = [\"node-c\", \"node-d\"]").unwrap(),
        ];
    })
    .await;
    let (st, v) = call(format!("{api}/api/v1/volumes"), json!({"name": "m", "size_bytes": 1u64 << 30, "pool": "src"})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["assembly"], "assembled", "{v}");
    let head = v["head"].as_str().unwrap().to_string();

    let (st, _) = call(format!("{api}/api/v1/volumes/m/migrate"), json!({"pool": "nope"})).await;
    assert_eq!(st, 404);
    let (st, _) = call(format!("{api}/api/v1/volumes/m/migrate"), json!({"pool": "src"})).await;
    assert_eq!(st, 409, "every leg is already there");
    let (st, m) = call(format!("{api}/api/v1/volumes/m/migrate"), json!({"pool": "dst"})).await;
    assert_eq!(st, 200, "{m}");
    assert_eq!(m["to_pool"], "dst");
    assert_eq!(m["state"], "moving");

    // The reconciler moves one leg at a time; the mock rebuilds at once.
    for _ in 0..40 {
        stormstorage::orchestrate::reconcile(&state).await;
        let done = {
            let fed = state.fed.read().await;
            let v = &fed.volumes["m"];
            v.replacing.is_none()
                && v.migration.as_ref().is_some_and(|m| m.state == stormstorage::model::MigrationState::WaitingHandover)
        };
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    {
        let fed = state.fed.read().await;
        let v = &fed.volumes["m"];
        let nodes: Vec<&str> = v.legs.iter().map(|l| l.node.as_str()).collect();
        assert!(nodes.contains(&head.as_str()), "the head's leg stays: {nodes:?}");
        assert!(nodes.iter().any(|n| *n == "node-c" || *n == "node-d"), "{nodes:?}");
        let m = v.migration.as_ref().expect("still recorded");
        assert_eq!(m.state, stormstorage::model::MigrationState::WaitingHandover, "{m:?}");
        assert!(m.message.as_deref().unwrap_or("").contains("stormblock#296"));
        assert_eq!(v.pool.as_deref(), Some("src"), "not moved until every leg is");
    }
    let feed = stormstorage::components::collect(&state).await;
    let c = feed.iter().find(|c| c.id == "volume:m").unwrap();
    assert!(c.metrics.iter().any(|m| m.label == "migrating" && m.value == "→ dst"));

    let r = reqwest::Client::new().delete(format!("{api}/api/v1/volumes/m/migrate")).send().await.unwrap();
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["cancelled"], true);
    assert!(state.fed.read().await.volumes["m"].migration.is_none());

    let (st, _) = call(format!("{api}/api/v1/volumes"), json!({"name": "one", "size_bytes": 1u64 << 30, "replicas": 1, "pool": "src"})).await;
    assert_eq!(st, 200);
    let (st, e) = call(format!("{api}/api/v1/volumes/one/migrate"), json!({"pool": "dst"})).await;
    assert_eq!(st, 409, "{e}");
}

/// #50: a preferred node becomes the head (the first leg); one that is not
/// a candidate is ignored and the answer says so.
#[tokio::test]
async fn a_preferred_node_becomes_the_head() {
    let (api, _state, _mocks) = setup(&["node-a", "node-b", "node-c"]).await;
    for want in ["node-b", "node-c"] {
        let name = format!("p-{want}");
        let (st, v) = call(
            format!("{api}/api/v1/volumes"),
            json!({"name": name, "size_bytes": 1u64 << 30, "replicas": 2, "prefer_node": want}),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        assert_eq!(v["head"], want, "{v}");
        assert_eq!(v["legs"][0]["node"], want);
        assert_eq!(v["prefer_node_honored"], true);
    }
    let (st, v) = call(
        format!("{api}/api/v1/volumes"),
        json!({"name": "p-none", "size_bytes": 1u64 << 30, "replicas": 2, "prefer_node": "node-z"}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["prefer_node_honored"], false);
    let (st, p) = call(
        format!("{api}/api/v1/placement/plan"),
        json!({"size_bytes": 1u64 << 30, "replicas": 1, "prefer_node": "node-c"}),
    )
    .await;
    assert_eq!(st, 200, "{p}");
    assert_eq!(p["legs"], json!(["node-c"]));
    assert_eq!(p["prefer_node_honored"], true);
}

#[tokio::test]
async fn dual_attach_windows() {
    let (api, state, _mocks) = setup(&["node-a", "node-b"]).await;
    let (head, target, _) = create_mirror(&api, "w").await;

    let (st, _) = call(format!("{api}/api/v1/volumes/w/dual-attach"), json!({"target_node": head, "ttl_secs": 60})).await;
    assert_eq!(st, 409, "the head is not a target");
    let (st, w) = call(format!("{api}/api/v1/volumes/w/dual-attach"), json!({"target_node": target, "ttl_secs": 60})).await;
    assert_eq!(st, 200, "{w}");
    assert_eq!(w["epoch"], 1);
    assert_eq!(w["target_node"], target.as_str());
    assert!(w["expires_at_ms"].as_u64().unwrap() > 0);
    // A promote waits for the window to close.
    let (st, _) = call(format!("{api}/api/v1/volumes/w/fence"), json!({"expected_epoch": 1})).await;
    assert_eq!(st, 200);
    let (st, _) = call(format!("{api}/api/v1/volumes/w/promote"), json!({"target_node": target, "fenced_epoch": 2})).await;
    assert_eq!(st, 409);
    // Close needs the window's epoch.
    let (st, e) = call(format!("{api}/api/v1/volumes/w/dual-attach/close"), json!({"epoch": 2, "outcome": "abort"})).await;
    assert_eq!((st, e["current_epoch"].as_u64()), (412, Some(1)));
    let (st, _) = call(format!("{api}/api/v1/volumes/w/dual-attach/close"), json!({"epoch": 1, "outcome": "abort"})).await;
    assert_eq!(st, 200);
    assert!(state.fed.read().await.volumes["w"].dual_attach.is_none());
    let (st, _) = call(format!("{api}/api/v1/volumes/w/dual-attach/close"), json!({"epoch": 1, "outcome": "abort"})).await;
    assert_eq!(st, 409, "nothing open");

    // Commit = fence + promote; with the head alive that is stormblock#296.
    let (_, w) = call(format!("{api}/api/v1/volumes/w/dual-attach"), json!({"target_node": target, "ttl_secs": 60})).await;
    let (st, e) = call(
        format!("{api}/api/v1/volumes/w/dual-attach/close"),
        json!({"epoch": w["epoch"], "outcome": "commit"}),
    )
    .await;
    assert_eq!(st, 409, "{e}");
    assert!(e["error"].as_str().unwrap().contains("stormblock#296"));
    assert_eq!(state.fed.read().await.volumes["w"].epoch, 3, "the commit fenced");

    // Expiry aborts.
    let (st, _) = call(format!("{api}/api/v1/volumes/w/dual-attach"), json!({"target_node": target, "ttl_secs": 1})).await;
    assert_eq!(st, 200);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    stormstorage::orchestrate::reconcile(&state).await;
    assert!(state.fed.read().await.volumes["w"].dual_attach.is_none());
}

#[tokio::test]
async fn prestage_replaces_the_slave_at_its_bandwidth_class() {
    let (api, state, mocks) = setup(&["node-a", "node-b", "node-c"]).await;
    let (head, slave, array) = create_mirror(&api, "r").await;
    let third = ["node-a", "node-b", "node-c"]
        .into_iter()
        .find(|n| *n != head && *n != slave)
        .unwrap()
        .to_string();

    let (st, _) = call(format!("{api}/api/v1/volumes/r/prestage"), json!({"node": head})).await;
    assert_eq!(st, 409, "anti-affinity");
    let (st, _) = call(format!("{api}/api/v1/volumes/r/prestage"), json!({"from": head})).await;
    assert_eq!(st, 409, "the master moves by promote only");

    let (st, p) = call(format!("{api}/api/v1/volumes/r/prestage"), json!({"bandwidth_class": "low"})).await;
    assert_eq!(st, 200, "{p}");
    assert_eq!(p["replacing"], slave.as_str());
    assert_eq!(p["to"], third.as_str());
    assert_eq!(p["bandwidth_class"], "low");
    assert_eq!(mocks[&head].lock().unwrap().rates[&array], 50 << 20);
    // The mock's member is active at once: the move completes.
    for _ in 0..40 {
        if state.fed.read().await.volumes["r"].replacing.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let v = state.fed.read().await.volumes["r"].clone();
    assert!(v.replacing.is_none());
    let nodes: Vec<&str> = v.legs.iter().map(|l| l.node.as_str()).collect();
    assert!(nodes.contains(&third.as_str()) && !nodes.contains(&slave.as_str()), "{nodes:?}");
}

/// #59: `extent_size_bytes` goes to every leg's /v1 create, is kept on the
/// record, and a replacement leg (prestage) is carved at the same size; the
/// served mirror (pinned to the array) is not given one. A node with no pool
/// of that size fails the create with its message, and no leg is left.
#[tokio::test]
async fn extent_size_reaches_every_leg_and_its_replacements() {
    let (api, state, mocks) = setup(&["node-a", "node-b", "node-c"]).await;
    const MIB8: u64 = 8 << 20;
    let (st, v) = call(
        format!("{api}/api/v1/volumes"),
        json!({"name": "x", "size_bytes": 1u64 << 30, "replicas": 2, "extent_size_bytes": MIB8}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["extent_size_bytes"], MIB8);
    assert_eq!(v["assembly"], "assembled", "{v}");
    let head = v["head"].as_str().unwrap().to_string();
    let legs: Vec<String> = v["legs"].as_array().unwrap().iter().map(|l| l["node"].as_str().unwrap().to_string()).collect();
    for n in &legs {
        let log = mocks[n].lock().unwrap().log.clone();
        assert!(log.contains(&format!("create x extent=Some({MIB8})")), "{n}: {log:?}");
    }
    let hl = mocks[&head].lock().unwrap().log.clone();
    assert!(hl.contains(&"create x-mirror extent=None".to_string()), "{hl:?}");
    let r = get_json(format!("{api}/api/v1/volumes/x/replicas")).await;
    assert_eq!(r["extent_size_bytes"], MIB8);

    // A replacement leg is carved at the same size.
    let third = ["node-a", "node-b", "node-c"].into_iter().find(|n| !legs.iter().any(|l| l == n)).unwrap();
    let (st, p) = call(format!("{api}/api/v1/volumes/x/prestage"), json!({})).await;
    assert_eq!(st, 200, "{p}");
    assert_eq!(p["to"], third);
    let tl = mocks[third].lock().unwrap().log.clone();
    assert!(tl.contains(&format!("create x extent=Some({MIB8})")), "{tl:?}");
    for _ in 0..40 {
        if state.fed.read().await.volumes["x"].replacing.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Not a size stormblock takes: refused here, no engine called.
    let (st, e) = call(
        format!("{api}/api/v1/volumes"),
        json!({"name": "bad", "size_bytes": 1u64 << 30, "extent_size_bytes": 3000}),
    )
    .await;
    assert_eq!(st, 400, "{e}");
    assert!(mocks.values().all(|m| !m.lock().unwrap().log.iter().any(|l| l.starts_with("create bad "))));

    // One node has no 8 MiB pool: the create fails with that node's sizes,
    // and the leg made elsewhere is rolled back.
    for (n, m) in &mocks {
        m.lock().unwrap().extent_sizes = if n == "node-c" { vec![1 << 20] } else { vec![1 << 20, MIB8] };
    }
    let (st, e) = call(
        format!("{api}/api/v1/volumes"),
        json!({"name": "y", "size_bytes": 1u64 << 30, "replicas": 3, "extent_size_bytes": MIB8}),
    )
    .await;
    assert_eq!(st, 502, "{e}");
    let msg = e["error"].as_str().unwrap();
    assert!(msg.contains("node-c") && msg.contains("1048576"), "{msg}");
    assert!(!state.fed.read().await.volumes.contains_key("y"));
    for m in mocks.values() {
        assert!(!m.lock().unwrap().volumes.values().any(|v| v.0 == "y"), "a leg of y was left");
    }
}

async fn put_json(url: String, body: Value) -> (u16, Value) {
    let r = reqwest::Client::new().put(url).json(&body).send().await.unwrap();
    let st = r.status().as_u16();
    (st, r.json().await.unwrap_or(Value::Null))
}

/// #60: a volume's bandwidth class changes after create. It is recorded,
/// its cap goes on the head's array at once, the same class again is a
/// no-op that applies the cap again, and a head that does not answer gets
/// the cap once it does.
#[tokio::test]
async fn bandwidth_class_changes_after_create() {
    let (api, state, mocks) = setup(&["node-a", "node-b"]).await;
    let (head, _other, array) = create_mirror(&api, "bw").await;
    let rc = state.config.recovery.clone();

    let (st, v) = put_json(format!("{api}/api/v1/volumes/bw/bandwidth-class"), json!({"bandwidth_class": "high"})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["bandwidth_class"], "high", "{v}");
    assert_eq!(v["rebuild_cap"]["applied"], true, "{v}");
    assert_eq!(v["rebuild_cap"]["bytes_per_sec"], rc.rate_high);
    assert_eq!(mocks[&head].lock().unwrap().rates[&array], rc.rate_high);
    assert_eq!(state.fed.read().await.volumes["bw"].bandwidth_class, stormstorage::model::BandwidthClass::High);

    // Idempotent.
    let rev = state.fed.read().await.revision;
    let (st, v) = put_json(format!("{api}/api/v1/volumes/bw/bandwidth-class"), json!({"bandwidth_class": "high"})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["rebuild_cap"]["applied"], true);
    assert_eq!(state.fed.read().await.revision, rev, "no change, no revision");

    // Head down: recorded, pending; applied once it answers.
    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = false;
    let (st, v) = put_json(format!("{api}/api/v1/volumes/bw/bandwidth-class"), json!({"bandwidth_class": "low"})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["bandwidth_class"], "low");
    assert_eq!(v["rebuild_cap"]["applied"], false, "{v}");
    assert_eq!(v["rebuild_cap"]["pending"], true);
    assert!(state.fed.read().await.volumes["bw"].rate_pending);
    assert_eq!(mocks[&head].lock().unwrap().rates[&array], rc.rate_high, "not touched while down");
    state.fed.write().await.nodes.get_mut(&head).unwrap().status.healthy = true;
    stormstorage::orchestrate::reconcile(&state).await;
    assert_eq!(mocks[&head].lock().unwrap().rates[&array], rc.rate_low);
    assert!(!state.fed.read().await.volumes["bw"].rate_pending);

    // Refusals.
    let (st, _) = put_json(format!("{api}/api/v1/volumes/nope/bandwidth-class"), json!({"bandwidth_class": "low"})).await;
    assert_eq!(st, 404);
    let (st, _) = put_json(format!("{api}/api/v1/volumes/bw/bandwidth-class"), json!({"bandwidth_class": "turbo"})).await;
    assert!(st == 400 || st == 422, "{st}");

    // A single leg has no array: recorded, nothing to cap.
    let (st, _) = call(format!("{api}/api/v1/volumes"), json!({"name": "one", "size_bytes": 1u64 << 30, "replicas": 1})).await;
    assert_eq!(st, 200);
    let (st, v) = put_json(format!("{api}/api/v1/volumes/one/bandwidth-class"), json!({"bandwidth_class": "unthrottled"})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["bandwidth_class"], "unthrottled");
    assert_eq!(v["rebuild_cap"]["pending"], false);
}
