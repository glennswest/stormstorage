//! #113 / #116: a mirrored volume filled from a golden, against an
//! in-process mock stormblock as the head. The head clones the golden, serves
//! the carve on its array to itself, mirrors the clone into it
//! (stormblock#295), and at `synced` stops the mirror, withdraws, deletes
//! the clone and serves the volume. Nothing is served before that.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::{AssemblyState, DistVolume, ExportState, FedState, Leg, LegState, Seed, SeedState};

const GOLDEN_BYTES: u64 = 4 << 30;

#[derive(Default, Clone)]
struct Vol {
    name: String,
    size: u64,
    lba: u64,
    v1: Option<String>,
    source: Option<String>,
    mirror: Option<Value>,
    served: BTreeSet<String>,
}

#[derive(Default)]
struct Mock {
    node: String,
    next: u32,
    /// engine-local id → volume
    vols: BTreeMap<String, Vol>,
    shared_attach: Vec<String>,
    log: Vec<String>,
}

type M = Arc<Mutex<Mock>>;

fn find(m: &Mock, key: &str) -> Option<String> {
    m.vols.iter().find(|(id, v)| *id == key || v.name == key).map(|(id, _)| id.clone())
}

fn not_found(what: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_FOUND, Json(json!({"message": format!("{what} not found")})))
}

async fn v1_create(State(m): State<M>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = m.lock().unwrap();
    m.next += 1;
    let (node, n) = (m.node.clone(), m.next);
    let name = b["name"].as_str().unwrap().to_string();
    let v1 = format!("{node}-v1-{n}");
    let source = b["source"]["id"].as_str().map(str::to_string);
    m.log.push(format!("create {name} from {source:?}"));
    m.vols.insert(
        format!("{node}-{n:04}"),
        Vol { name: name.clone(), size: b["size_bytes"].as_u64().unwrap(), lba: 4096, v1: Some(v1.clone()), source, ..Default::default() },
    );
    Json(json!({"id": v1, "name": name, "replicas": [{"node": node, "role": "master"}]}))
}

async fn v1_attach(State(m): State<M>, Path(id): Path<String>) -> Json<Value> {
    let mut m = m.lock().unwrap();
    m.shared_attach.push(id.clone());
    Json(json!({"transport": "nvme_tcp", "nqn": "nqn.2024.io.stormblock:shared", "nsid": 7,
                "addresses": [{"traddr": "10.0.0.1", "trsvcid": 4420}]}))
}

async fn list(State(m): State<M>) -> Json<Value> {
    let m = m.lock().unwrap();
    let items: Vec<Value> =
        m.vols.iter().map(|(id, v)| json!({"id": id, "name": v.name, "virtual_size_bytes": v.size})).collect();
    Json(json!({"items": items}))
}

async fn get_vol(State(m): State<M>, Path(key): Path<String>) -> (StatusCode, Json<Value>) {
    let m = m.lock().unwrap();
    let Some(id) = find(&m, &key) else { return not_found(&key) };
    let v = &m.vols[&id];
    let mut out = json!({"id": id, "name": v.name, "virtual_size_bytes": v.size, "lba": v.lba});
    if let Some(mi) = &v.mirror {
        out["mirror"] = mi.clone();
    }
    (StatusCode::OK, Json(out))
}

async fn delete_vol(State(m): State<M>, Path(key): Path<String>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    let Some(id) = find(&m, &key) else { return not_found(&key) };
    m.log.push(format!("delete {id}"));
    m.vols.remove(&id);
    (StatusCode::OK, Json(json!({"deleted": id})))
}

async fn attach(State(m): State<M>, Path(id): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    let Some(v) = m.vols.get_mut(&id) else { return not_found(&id) };
    let host = b["host_nqn"].as_str().expect("served to one host").to_string();
    v.served.insert(host.clone());
    (
        StatusCode::OK,
        Json(json!({"transport": "nvme_tcp", "nqn": format!("nqn.2024.io.stormblock:head:host:{host}"), "nsid": 3,
                    "host_nqn": host, "addresses": [{"traddr": "10.0.0.1", "trsvcid": 4420}]})),
    )
}

async fn withdraw(State(m): State<M>, Path(id): Path<String>, Query(q): Query<BTreeMap<String, String>>) -> StatusCode {
    let mut m = m.lock().unwrap();
    let host = q.get("host_nqn").cloned().expect("a per-host withdraw");
    m.log.push(format!("withdraw {id} {host}"));
    if let Some(v) = m.vols.get_mut(&id) {
        v.served.remove(&host);
    }
    StatusCode::OK
}

async fn mirror_start(State(m): State<M>, Path(key): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    let Some(id) = find(&m, &key) else { return not_found(&key) };
    let target = b["target"].as_str().unwrap().to_string();
    m.log.push(format!("mirror {id} {target}"));
    let v = m.vols.get_mut(&id).unwrap();
    if v.mirror.is_some() {
        return (StatusCode::CONFLICT, Json(json!({"message": "already mirrored"})));
    }
    let st = json!({"target": target, "state": "copying", "bytes_total": v.size, "bytes_remaining": v.size,
                    "message": null, "started_at": 1});
    v.mirror = Some(st.clone());
    (StatusCode::ACCEPTED, Json(st))
}

async fn mirror_stop(State(m): State<M>, Path(key): Path<String>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    let Some(id) = find(&m, &key) else { return not_found(&key) };
    m.log.push(format!("unmirror {id}"));
    match m.vols.get_mut(&id).unwrap().mirror.take() {
        None => not_found("mirror"),
        Some(mut st) => {
            st["stopped"] = json!(true);
            st["flushed"] = json!(true);
            (StatusCode::OK, Json(st))
        }
    }
}

async fn get_array(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "members": [{"uuid": "m-a", "state": "active", "device_path": ""},
                                      {"uuid": "m-b", "state": "active", "device_path": ""}],
                "status": {"state": "optimal"}}))
}

async fn mock_engine(node: &str) -> (String, M) {
    let m: M = Arc::new(Mutex::new(Mock { node: node.into(), ..Default::default() }));
    let app = Router::new()
        .route(
            "/v1/nodes/capacity",
            get(|| async { Json(json!({"total_bytes": 100u64 << 30, "free_bytes": 50u64 << 30})) }),
        )
        .route("/v1/volumes", post(v1_create))
        .route("/v1/volumes/{id}/attach", post(v1_attach))
        .route("/api/v1/volumes", get(list))
        .route("/api/v1/volumes/{id}", get(get_vol).delete(delete_vol))
        .route("/api/v1/volumes/{id}/attach", post(attach).delete(withdraw))
        .route("/api/v1/volumes/{id}/mirror", post(mirror_start).delete(mirror_stop))
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
        master_node: Some(node.into()),
        export: None,
        drive_uuid: None,
        member_uuid: Some(member.into()),
        epoch: None,
    }
}

struct Fx {
    api: String,
    state: Arc<AppState>,
    head: M,
    host: String,
}

/// "root" is assembled on head node-a (array "arr", legs on node-a and
/// node-b); its carve `root-mirror` is made on the array and served to no
/// one; the golden is on node-a; the seed has not started.
async fn setup() -> Fx {
    let mut config = Config::default();
    config.local.enabled = false;
    config.kubernetes.enabled = false;
    let (aa, head) = mock_engine("node-a").await;
    let (ab, _b) = mock_engine("node-b").await;
    for (n, a) in [("node-a", aa), ("node-b", ab)] {
        config.nodes.push(toml::from_str::<NodeConfig>(&format!("name = \"{n}\"\nengine_url = \"http://{a}\"")).unwrap());
    }
    {
        let mut h = head.lock().unwrap();
        h.vols.insert("golden-uuid".into(), Vol { name: "golden-x".into(), size: GOLDEN_BYTES, lba: 4096, ..Default::default() });
        h.vols.insert(
            "carve-uuid".into(),
            Vol { name: "root-mirror".into(), size: 8 << 30, lba: 4096, v1: Some("carve-v1".into()), ..Default::default() },
        );
    }
    let host = config.legs.host_nqn_for("node-a");
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    fed.volumes.insert(
        "root".into(),
        DistVolume {
            name: "root".into(),
            size_bytes: 8 << 30,
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
            export: stormstorage::model::Export {
                volume_id: Some("carve-v1".into()),
                node: Some("node-a".into()),
                master_node: Some("node-a".into()),
                local_id: Some("carve-uuid".into()),
                ..Default::default()
            },
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
            seed: Some(Seed::new("golden-x", SystemTime::now())),
            owner: None,
        },
    );
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Fx { api: format!("http://{api}"), state, head, host }
}

async fn seed_of(fx: &Fx) -> Seed {
    fx.state.fed.read().await.volumes["root"].seed.clone().expect("a seed")
}

fn clone_id(m: &M) -> Option<String> {
    m.lock().unwrap().vols.iter().find(|(_, v)| v.name == "root-seed").map(|(id, _)| id.clone())
}

fn set_mirror(m: &M, id: &str, state: &str, remaining: u64) {
    let mut m = m.lock().unwrap();
    let mi = m.vols.get_mut(id).unwrap().mirror.as_mut().expect("mirrored");
    mi["state"] = json!(state);
    mi["bytes_remaining"] = json!(remaining);
}

async fn call(method: reqwest::Method, url: String, body: Option<Value>) -> (u16, Value) {
    let mut r = reqwest::Client::new().request(method, url);
    if let Some(b) = body {
        r = r.json(&b);
    }
    let resp = r.send().await.unwrap();
    let st = resp.status().as_u16();
    (st, resp.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
async fn a_golden_is_copied_into_the_carve_then_served() {
    let fx = setup().await;
    assert!(stormstorage::seed::step(&fx.state, "root").await.unwrap());

    // A clone of the golden on the head, mirrored into the carve, which is
    // served to the head's own host alone.
    let seed = seed_of(&fx).await;
    assert_eq!(seed.state, SeedState::Copying);
    let clone = clone_id(&fx.head).expect("the golden's clone on the head");
    assert_eq!(seed.clone_id.as_deref(), Some(clone.as_str()));
    assert_eq!(seed.node.as_deref(), Some("node-a"));
    {
        let h = fx.head.lock().unwrap();
        assert_eq!(h.vols[&clone].source.as_deref(), Some("golden-x"));
        assert_eq!(h.vols[&clone].size, GOLDEN_BYTES);
        assert_eq!(h.vols["carve-uuid"].served, BTreeSet::from([fx.host.clone()]));
        let target = h.vols[&clone].mirror.as_ref().unwrap()["target"].as_str().unwrap().to_string();
        assert!(target.ends_with(&format!("&hostnqn={}", fx.host)), "{target}");
        assert_eq!(seed.mirror_target.as_deref(), Some(target.as_str()));
        assert!(h.shared_attach.is_empty(), "served to no consumer while copying");
    }
    assert_eq!(fx.state.fed.read().await.volumes["root"].export.state, ExportState::None);

    // No consumer host while it copies.
    let e = stormstorage::orchestrate::serve_host(&fx.state, "root", "nqn.2014-08.org.nvmexpress:uuid:c", false)
        .await
        .expect_err("refused while seeding");
    assert!(format!("{e:#}").contains("being filled from golden golden-x"), "{e:#}");
    // An explicit publish holds it too.
    let ex = stormstorage::orchestrate::publish(&fx.state, "root").await.unwrap();
    assert_eq!(ex.state, ExportState::None);
    assert!(fx.head.lock().unwrap().shared_attach.is_empty());

    // Progress is recorded.
    set_mirror(&fx.head, &clone, "copying", 1 << 30);
    assert!(stormstorage::seed::step(&fx.state, "root").await.unwrap());
    assert_eq!(seed_of(&fx).await.bytes_remaining, 1 << 30);

    // Synced: mirror stopped, the head's serve withdrawn, the clone gone,
    // the volume served.
    set_mirror(&fx.head, &clone, "synced", 0);
    assert!(stormstorage::seed::step(&fx.state, "root").await.unwrap());
    let seed = seed_of(&fx).await;
    assert_eq!(seed.state, SeedState::Done);
    assert_eq!((seed.bytes_total, seed.bytes_remaining), (GOLDEN_BYTES, 0));
    assert!(seed.secs.is_some() && seed.finished_at.is_some());
    assert!(seed.clone_id.is_none());
    {
        let h = fx.head.lock().unwrap();
        assert!(!h.vols.contains_key(&clone), "clone deleted");
        assert!(h.vols["carve-uuid"].served.is_empty(), "the head no longer serves the carve to itself");
        assert_eq!(h.shared_attach, vec!["carve-v1".to_string()], "served to consumers");
        assert!(h.vols.contains_key("golden-uuid"), "the golden is untouched");
    }
    let v = fx.state.fed.read().await.volumes["root"].clone();
    assert_eq!(v.export.state, ExportState::Published);
    // Done: nothing more to do.
    assert!(!stormstorage::seed::step(&fx.state, "root").await.unwrap());

    // The record reads back through the API.
    let (st, g) = call(reqwest::Method::GET, format!("{}/api/v1/volumes/root", fx.api), None).await;
    assert_eq!(st, 200);
    assert_eq!(g["seed"]["state"], "done");
    assert_eq!(g["seed"]["golden"], "golden-x");
}

#[tokio::test]
async fn a_failed_copy_undoes_itself_and_retries_later() {
    let fx = setup().await;
    stormstorage::seed::step(&fx.state, "root").await.unwrap();
    let clone = clone_id(&fx.head).unwrap();
    set_mirror(&fx.head, &clone, "failed", 1);
    stormstorage::seed::run(&fx.state).await;
    let seed = seed_of(&fx).await;
    assert_eq!(seed.state, SeedState::Failed);
    assert!(seed.retry_after.is_some() && seed.message.is_some());
    assert!(seed.clone_id.is_none());
    {
        let h = fx.head.lock().unwrap();
        assert!(!h.vols.contains_key(&clone), "clone deleted");
        assert!(h.vols["carve-uuid"].served.is_empty());
        assert!(h.shared_attach.is_empty(), "never served");
    }
    // Not before the cooldown.
    assert!(!stormstorage::seed::step(&fx.state, "root").await.unwrap());
}

#[tokio::test]
async fn a_golden_laid_in_other_blocks_is_refused() {
    let fx = setup().await;
    fx.head.lock().unwrap().vols.get_mut("golden-uuid").unwrap().lba = 512;
    let e = stormstorage::seed::step(&fx.state, "root").await.expect_err("refused");
    assert!(format!("{e:#}").contains("512-byte"), "{e:#}");
    assert!(clone_id(&fx.head).is_none(), "nothing made");
}

#[tokio::test]
async fn delete_mid_copy_stops_the_mirror_and_deletes_the_clone() {
    let fx = setup().await;
    stormstorage::seed::step(&fx.state, "root").await.unwrap();
    let clone = clone_id(&fx.head).unwrap();
    let vol = fx.state.fed.read().await.volumes["root"].clone();
    stormstorage::seed::undo(&fx.state, &vol, vol.seed.as_ref().unwrap()).await.unwrap();
    let h = fx.head.lock().unwrap();
    assert!(!h.vols.contains_key(&clone));
    assert!(h.log.iter().any(|l| l == &format!("unmirror {clone}")), "{:?}", h.log);
    assert!(h.vols["carve-uuid"].served.is_empty());
}

#[tokio::test]
async fn create_from_golden_refuses_what_cannot_be_filled() {
    let fx = setup().await;
    let url = format!("{}/api/v1/volumes", fx.api);
    let (st, e) = call(
        reqwest::Method::POST,
        url.clone(),
        Some(json!({"name": "x", "size_bytes": 8u64 << 30, "from_golden": "golden-x", "source": {"kind": "volume", "id": "root"}})),
    )
    .await;
    assert_eq!(st, 400, "{e}");
    let (st, e) =
        call(reqwest::Method::POST, url.clone(), Some(json!({"name": "x", "size_bytes": 8u64 << 30, "from_golden": "  "}))).await;
    assert_eq!(st, 400, "{e}");
    let (st, e) = call(
        reqwest::Method::POST,
        url,
        Some(json!({"name": "x", "size_bytes": 8u64 << 30, "from_golden": "golden-x", "replicas": 1})),
    )
    .await;
    assert_eq!(st, 400, "{e}");
    assert!(e.to_string().contains("mirrored volume"), "{e}");
}
