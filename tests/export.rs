//! #2 end to end against in-process mock stormblocks: a two-leg volume is
//! assembled on its head and served as a volume pinned to the array there;
//! a single-leg volume is served as its leg; a republish reports unchanged
//! coordinates; delete revokes the served volume before the array goes,
//! because a dedicated array refuses deletion while a volume is pinned to
//! it. The mock keeps the rules stormblock v19.1.1 enforces
//! (src/mgmt/api/{v1,arrays}.rs): name-idempotent /v1 create,
//! `placement.array_id` must name a local array, attach answers NVMe-TCP
//! only when asked with `transport: nvme_tcp`, array delete is a 409 while
//! a volume is pinned.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::{ExportState, FedState};

#[derive(Default)]
struct Mock {
    node: String,
    next: u32,
    /// id → (name, pinned array)
    volumes: BTreeMap<String, (String, Option<String>)>,
    /// volume id → nsid
    attached: BTreeMap<String, u32>,
    arrays: Vec<String>,
    /// drive uuid → path (what `POST /api/v1/drives` opened).
    drives: BTreeMap<String, String>,
    /// array id → [(member uuid, device path)].
    members: BTreeMap<String, Vec<(String, String)>>,
    /// Array creates to refuse (500, nothing made).
    fail_creates: u32,
    /// Array creates to perform but answer 500 — a lost response (#7).
    lose_creates: u32,
    /// Every mutating call, in order.
    log: Vec<String>,
}

type M = Arc<Mutex<Mock>>;

async fn v1_create(State(m): State<M>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    let name = b["name"].as_str().unwrap().to_string();
    let pin = b["placement"]["array_id"].as_str().map(|s| s.to_string());
    if let Some(a) = &pin {
        if !m.arrays.contains(a) {
            return (StatusCode::NOT_FOUND, Json(json!({"message": format!("array {a}")})));
        }
    }
    let node = m.node.clone();
    let existing = m.volumes.iter().find(|(_, (n, _))| *n == name).map(|(id, _)| id.clone());
    let id = match existing {
        Some(id) => id,
        None => {
            m.next += 1;
            let id = format!("{node}-vol-{}", m.next);
            m.volumes.insert(id.clone(), (name.clone(), pin.clone()));
            m.log.push(format!("create {name} pin={pin:?}"));
            id
        }
    };
    (
        StatusCode::OK,
        Json(json!({"id": id, "name": name, "replicas": [{"node": node, "role": "master"}]})),
    )
}

async fn v1_attach(
    State(m): State<M>,
    Path(id): Path<String>,
    Json(b): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    if !m.volumes.contains_key(&id) {
        return (StatusCode::NOT_FOUND, Json(json!({"message": id})));
    }
    if b["transport"] != "nvme_tcp" || b["node"] != m.node.as_str() {
        return (StatusCode::OK, Json(json!({"transport": "ublk", "device": "/dev/ublkb0"})));
    }
    let n = m.attached.len() as u32 + 1;
    let nsid = *m.attached.entry(id.clone()).or_insert(n);
    m.log.push(format!("attach {id}"));
    // stormblock#210: named host → that host's own subsystem.
    let nqn = match b["host_nqn"].as_str() {
        Some(h) => format!("nqn.2024.io.stormblock:{}:host:{h}", m.node),
        None => format!("nqn.2024.io.stormblock:{}", m.node),
    };
    (
        StatusCode::OK,
        Json(json!({"transport": "nvme_tcp", "nqn": nqn, "nsid": nsid,
                    "addresses": [{"traddr": "0.0.0.0", "trsvcid": 4420}]})),
    )
}

async fn v1_detach(State(m): State<M>, Path(id): Path<String>) -> Json<Value> {
    let mut m = m.lock().unwrap();
    m.attached.remove(&id);
    m.log.push(format!("detach {id}"));
    Json(json!({}))
}

async fn v1_delete(State(m): State<M>, Path(id): Path<String>) -> StatusCode {
    let mut m = m.lock().unwrap();
    m.volumes.remove(&id);
    m.log.push(format!("delete {id}"));
    StatusCode::NO_CONTENT
}

async fn add_drive(State(m): State<M>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = m.lock().unwrap();
    m.next += 1;
    m.log.push(format!("drive {}", b["path"]));
    let uuid = format!("drive-{}", m.next);
    m.drives.insert(uuid.clone(), b["path"].as_str().unwrap_or_default().to_string());
    Json(json!({"uuid": uuid}))
}

async fn create_array(State(m): State<M>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    if m.fail_creates > 0 {
        m.fail_creates -= 1;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"message": "no"})));
    }
    m.next += 1;
    let id = format!("00000000-0000-0000-0000-{:012}", m.next);
    m.arrays.push(id.clone());
    m.log.push(format!("array {id}"));
    let rows: Vec<(String, String)> = b["drive_uuids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            let d = d.as_str().unwrap();
            (format!("m-{d}"), m.drives.get(d).cloned().unwrap_or_default())
        })
        .collect();
    m.members.insert(id.clone(), rows.clone());
    if m.lose_creates > 0 {
        m.lose_creates -= 1;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"message": "timed out"})));
    }
    (StatusCode::OK, Json(array_json(&id, &rows)))
}

fn array_json(id: &str, rows: &[(String, String)]) -> Value {
    let members: Vec<Value> = rows
        .iter()
        .enumerate()
        .map(|(i, (u, p))| json!({"index": i, "uuid": u, "state": "active", "device_path": p}))
        .collect();
    json!({"id": id, "members": members})
}

async fn list_arrays(State(m): State<M>) -> Json<Value> {
    let m = m.lock().unwrap();
    let items: Vec<Value> = m
        .arrays
        .iter()
        .map(|id| array_json(id, m.members.get(id).map(|r| r.as_slice()).unwrap_or_default()))
        .collect();
    Json(json!({"items": items, "count": items.len()}))
}

async fn delete_array(State(m): State<M>, Path(id): Path<String>) -> StatusCode {
    let mut m = m.lock().unwrap();
    if m.volumes.values().any(|(_, p)| p.as_deref() == Some(id.as_str())) {
        return StatusCode::CONFLICT;
    }
    m.arrays.retain(|a| *a != id);
    m.log.push(format!("delete array {id}"));
    StatusCode::NO_CONTENT
}

async fn mock_engine(node: &str) -> (String, M) {
    let m: M = Arc::new(Mutex::new(Mock { node: node.into(), ..Default::default() }));
    let app = Router::new()
        .route(
            "/v1/nodes/capacity",
            get(|| async { Json(json!({"total_bytes": 100u64 << 30, "free_bytes": 50u64 << 30})) }),
        )
        .route("/v1/volumes", post(v1_create))
        .route("/v1/volumes/{id}", delete(v1_delete))
        .route("/v1/volumes/{id}/attach", post(v1_attach))
        .route("/v1/volumes/{id}/detach", post(v1_detach))
        .route("/api/v1/drives", post(add_drive))
        .route("/api/v1/drives/{p}", delete(|| async { StatusCode::NO_CONTENT }))
        .route("/api/v1/arrays", post(create_array).get(list_arrays))
        .route("/api/v1/arrays/{id}", delete(delete_array))
        .with_state(m.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr.to_string(), m)
}

async fn setup() -> (String, Arc<AppState>, BTreeMap<String, M>) {
    let mut config = Config::default();
    config.local.enabled = false;
    let mut mocks = BTreeMap::new();
    for n in ["node-a", "node-b"] {
        let (addr, m) = mock_engine(n).await;
        config.nodes.push(
            toml::from_str::<NodeConfig>(&format!(
                "name = \"{n}\"\nengine_url = \"http://{addr}\""
            ))
            .unwrap(),
        );
        mocks.insert(n.to_string(), m);
    }
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    assert!(state.fed.read().await.nodes.values().all(|n| n.status.healthy));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{api}"), state, mocks)
}

fn http_post(url: String, body: Value) -> reqwest::RequestBuilder {
    reqwest::Client::new().post(url).json(&body)
}

#[tokio::test]
async fn assembled_volume_is_served_from_the_array_and_revoked_first() {
    let (api, state, mocks) = setup().await;

    let r = http_post(format!("{api}/api/v1/volumes"), json!({"name": "mir", "size_bytes": 1u64 << 30, "replicas": 2}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["assembly"], "assembled");
    let head = v["head"].as_str().unwrap().to_string();
    let array = v["array_id"].as_str().unwrap().to_string();
    let ex = &v["export"];
    assert_eq!(ex["state"], "published", "{v}");
    assert_eq!(ex["node"], head);

    // The served volume is a new volume pinned to the array, not a leg.
    let served = ex["volume_id"].as_str().unwrap().to_string();
    {
        let m = mocks[&head].lock().unwrap();
        let (name, pin) = &m.volumes[&served];
        assert_eq!(name, "mir-mirror");
        assert_eq!(pin.as_deref(), Some(array.as_str()));
        assert!(m.attached.contains_key(&served));
    }
    assert!(v["legs"].as_array().unwrap().iter().all(|l| l["volume_id"] != served.as_str()));
    // Each leg is served to the head alone (#27): attached for its host
    // NQN, opened with `hostnqn=` from the per-host subsystem.
    let host = format!("nqn.2026-10.lo.storm:stormstorage:{head}");
    {
        let m = mocks[&head].lock().unwrap();
        for leg in v["legs"].as_array().unwrap() {
            let e = &leg["export"];
            assert_eq!(e["host_nqn"], host.as_str(), "{leg}");
            assert!(e["nqn"].as_str().unwrap().ends_with(&format!(":host:{host}")), "{e}");
            let path = &m.drives[leg["drive_uuid"].as_str().unwrap()];
            assert!(path.ends_with(&format!("&hostnqn={host}")), "{path}");
        }
    }
    // A wildcard listen address is replaced by the engine's host.
    assert_eq!(ex["coordinates"]["traddr"], "127.0.0.1");
    assert_eq!(ex["coordinates"]["nqn"], format!("nqn.2024.io.stormblock:{head}"));

    // Republish: same coordinates, same volume.
    let r: Value = http_post(format!("{api}/api/v1/volumes/mir/export"), json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["state"], "published");
    assert_eq!(r["coordinates_changed"], false);
    assert_eq!(r["volume_id"], served.as_str());
    assert_eq!(mocks[&head].lock().unwrap().volumes.len(), 2, "leg + served volume, nothing new");

    // The feed shows it.
    let feed = stormstorage::components::collect(&state).await;
    let c = feed.iter().find(|c| c.id == "volume:mir").unwrap();
    assert!(c.metrics.iter().any(|m| m.label == "export" && m.value == "published"));
    assert!(c.detail.contains("served at nvme-tcp://127.0.0.1:4420/"), "{}", c.detail);
    assert!(c.actions.iter().any(|a| a.id == "export" && a.label == "Republish"));

    // Delete: the served volume goes before the array (else a 409).
    let r = reqwest::Client::new().delete(format!("{api}/api/v1/volumes/mir")).send().await.unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let m = mocks[&head].lock().unwrap();
    let pos = |needle: &str| m.log.iter().position(|l| l == needle).unwrap_or_else(|| panic!("{needle} in {:?}", m.log));
    assert!(pos(&format!("detach {served}")) < pos(&format!("delete {served}")));
    assert!(pos(&format!("delete {served}")) < pos(&format!("delete array {array}")));
    assert!(m.volumes.is_empty() && m.arrays.is_empty(), "{:?}", m.log);
    assert!(state.fed.read().await.volumes.is_empty());
}

#[tokio::test]
async fn single_leg_volume_is_served_as_its_leg() {
    let (api, _state, mocks) = setup().await;
    let v: Value = http_post(format!("{api}/api/v1/volumes"), json!({"name": "one", "size_bytes": 1u64 << 30}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["assembly"], "single_leg");
    let leg = &v["legs"][0];
    assert_eq!(v["export"]["state"], "published", "{v}");
    assert_eq!(v["export"]["volume_id"], leg["volume_id"]);
    assert_eq!(v["export"]["node"], leg["node"]);
    assert_eq!(v["export"]["coordinates"], leg["export"]);

    let node = leg["node"].as_str().unwrap().to_string();
    let r = reqwest::Client::new().delete(format!("{api}/api/v1/volumes/one")).send().await.unwrap();
    assert!(r.status().is_success());
    assert!(mocks[&node].lock().unwrap().volumes.is_empty());
}

#[tokio::test]
async fn reserved_suffix_and_unassembled_volumes_are_refused() {
    let (api, state, _mocks) = setup().await;
    let r = http_post(format!("{api}/api/v1/volumes"), json!({"name": "x-mirror", "size_bytes": 1u64 << 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    let r = http_post(format!("{api}/api/v1/volumes/nope/export"), json!({})).send().await.unwrap();
    assert_eq!(r.status(), 404);

    // A volume whose mirror is not assembled has nothing to serve.
    let v: Value = http_post(format!("{api}/api/v1/volumes"), json!({"name": "p", "size_bytes": 1u64 << 30, "replicas": 2}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["assembly"], "assembled");
    state.fed.write().await.volumes.get_mut("p").unwrap().assembly =
        stormstorage::model::AssemblyState::PendingEngineSupport;
    let r = http_post(format!("{api}/api/v1/volumes/p/export"), json!({})).send().await.unwrap();
    assert_eq!(r.status(), 409);
    assert_eq!(
        state.fed.read().await.volumes["p"].export.state,
        ExportState::Published,
        "an attempt that cannot start leaves the record alone"
    );
}

#[tokio::test]
async fn publish_on_an_unreachable_node_is_recorded_and_retried_on_recovery() {
    let (api, state, _mocks) = setup().await;
    let v: Value = http_post(format!("{api}/api/v1/volumes"), json!({"name": "flap", "size_bytes": 1u64 << 30}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["export"]["state"], "published", "{v}");
    let node = v["legs"][0]["node"].as_str().unwrap().to_string();
    let coords = v["export"]["coordinates"].clone();

    // The node stops answering polls; a publish now fails, and says so.
    state.fed.write().await.nodes.get_mut(&node).unwrap().status.healthy = false;
    let r = http_post(format!("{api}/api/v1/volumes/flap/export"), json!({})).send().await.unwrap();
    assert!(!r.status().is_success());
    {
        let fed = state.fed.read().await;
        let ex = &fed.volumes["flap"].export;
        assert_eq!(ex.state, ExportState::Failed);
        assert!(ex.message.as_deref().unwrap_or("").contains("unreachable"), "{ex:?}");
        assert_eq!(ex.node.as_deref(), Some(node.as_str()));
        assert!(ex.volume_id.is_some(), "the served volume is still known, for revoke");
    }
    let evs = state.events.read().await.since(0);
    assert!(evs.iter().any(|e| e.message.starts_with("flap: export failed")), "no failure event");

    // It answers again: the failed export is retried and published.
    state.fed.write().await.nodes.get_mut(&node).unwrap().status.healthy = true;
    stormstorage::orchestrate::republish_on(&state, &node).await;
    let fed = state.fed.read().await;
    let ex = &fed.volumes["flap"].export;
    assert_eq!(ex.state, ExportState::Published, "{ex:?}");
    assert_eq!(serde_json::to_value(&ex.coordinates).unwrap(), coords);
    assert!(!ex.coordinates_changed);
}

/// #7: a failed assembly is retried by `POST …/assemble` and by the
/// reconciler; a create whose response was lost is adopted, not built twice.
async fn pending(api: &str, mocks: &BTreeMap<String, M>, lose: bool) -> Value {
    for m in mocks.values() {
        let mut m = m.lock().unwrap();
        if lose {
            m.lose_creates = 1;
        } else {
            m.fail_creates = 1;
        }
    }
    let r = http_post(format!("{api}/api/v1/volumes"), json!({"name": "pv", "size_bytes": 1u64 << 30, "replicas": 2}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["assembly"], "pending_engine_support", "{v}");
    assert!(v["next_assemble_after"].is_object() || v["next_assemble_after"].is_string(), "cooldown set: {v}");
    v
}

fn arrays_on(mocks: &BTreeMap<String, M>) -> usize {
    mocks.values().map(|m| m.lock().unwrap().arrays.len()).sum()
}

#[tokio::test]
async fn failed_assembly_is_retried_on_request() {
    let (api, state, mocks) = setup().await;
    pending(&api, &mocks, false).await;
    let ev: Vec<String> = state.events.read().await.since(0).into_iter().map(|e| e.message).collect();
    assert!(
        ev.iter().any(|m| m.contains("assembly failed") && m.contains("/api/v1/volumes/pv/assemble")),
        "the event names the real retry path: {ev:?}"
    );
    assert_eq!(arrays_on(&mocks), 0);

    let r = http_post(format!("{api}/api/v1/volumes/pv/assemble"), json!({})).send().await.unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["assembly"], "assembled");
    assert_eq!(v["export"]["state"], "published", "served once assembled");
    assert!(v["next_assemble_after"].is_null());
    assert_eq!(arrays_on(&mocks), 1);

    let r = http_post(format!("{api}/api/v1/volumes/pv/assemble"), json!({})).send().await.unwrap();
    assert_eq!(r.status(), 409, "already assembled");
    let r = http_post(format!("{api}/api/v1/volumes/nope/assemble"), json!({})).send().await.unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn a_lost_create_response_is_adopted_not_built_twice() {
    let (api, _state, mocks) = setup().await;
    let v = pending(&api, &mocks, true).await;
    assert_eq!(arrays_on(&mocks), 1, "the create happened; only its answer was lost");
    let r = http_post(format!("{api}/api/v1/volumes/pv/assemble"), json!({})).send().await.unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let a: Value = r.json().await.unwrap();
    assert_eq!(a["assembly"], "assembled");
    assert_eq!(arrays_on(&mocks), 1, "adopted, not a second array over the same legs");
    let head = a["head"].as_str().unwrap();
    let m = mocks[head].lock().unwrap();
    assert_eq!(a["array_id"].as_str().unwrap(), m.arrays[0]);
    // Each leg's member uuid is the one whose device path is its drive.
    let rows = &m.members[&m.arrays[0]];
    for leg in a["legs"].as_array().unwrap() {
        let uuid = leg["member_uuid"].as_str().unwrap();
        let path = &rows.iter().find(|(u, _)| u == uuid).unwrap().1;
        let e = &leg["export"];
        assert!(path.contains(e["nqn"].as_str().unwrap()), "{path} vs {e}");
    }
    let _ = v;
}

#[tokio::test]
async fn the_reconciler_retries_a_pending_volume() {
    let (api, state, mocks) = setup().await;
    pending(&api, &mocks, false).await;
    // Still cooling down: nothing happens.
    stormstorage::orchestrate::reconcile(&state).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(arrays_on(&mocks), 0);
    // Past the cooldown.
    state.fed.write().await.volumes.get_mut("pv").unwrap().next_assemble_after = None;
    stormstorage::orchestrate::reconcile(&state).await;
    for _ in 0..50 {
        if state.fed.read().await.volumes["pv"].export.state == ExportState::Published {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let fed = state.fed.read().await;
    let v = &fed.volumes["pv"];
    assert_eq!(v.assembly, stormstorage::model::AssemblyState::Assembled);
    assert_eq!(v.export.state, ExportState::Published);
    assert_eq!(arrays_on(&mocks), 1);
}
