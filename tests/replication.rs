//! #33 against in-process mock stormblocks: sync state read from the
//! head's array, the epoch CAS (fence) carried down to every leg's /v1
//! epoch, promote onto a surviving leg's node after the head is lost (the
//! array put back together from the legs' superblocks, the served volume
//! that came with it served again), the former head cleaned up only once
//! it no longer holds the array, dual-attach windows, and prestage with a
//! bandwidth class.
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

/// What lives on the legs themselves: member superblocks (drive path →
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
    log: Vec<String>,
}

#[derive(Clone)]
struct S {
    m: Arc<Mutex<Mock>>,
    disks: Arc<Mutex<Disks>>,
}

fn coords(node: &str, nsid: u32) -> Value {
    json!({"transport": "nvme_tcp", "nqn": format!("nqn.2024.io.stormblock:{node}"), "nsid": nsid,
           "addresses": [{"traddr": "0.0.0.0", "trsvcid": 4420}]})
}

async fn v1_create(State(s): State<S>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = s.m.lock().unwrap();
    let name = b["name"].as_str().unwrap().to_string();
    let pin = b["placement"]["array_id"].as_str().map(|x| x.to_string());
    let node = m.node.clone();
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
    Json(json!({"id": id, "name": name, "replicas": [{"node": node, "role": "master"}]}))
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
    json!({"id": id, "member_data_bytes": 1000, "status": {"state": "clean"}, "members": ms})
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
            disks.superblocks.insert(x.path.clone(), id.clone());
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
        if let Some(a) = disks.superblocks.get(&path) {
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

async fn any_attach(State(s): State<S>, Path(id): Path<String>) -> (StatusCode, Json<Value>) {
    let mut m = s.m.lock().unwrap();
    if !m.adopted.contains_key(&id) && !m.volumes.contains_key(&id) {
        return (StatusCode::NOT_FOUND, Json(json!({})));
    }
    m.log.push(format!("attach-any {id}"));
    let node = m.node.clone();
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
    let mut config = Config::default();
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
