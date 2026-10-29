//! #44 end to end against two in-process mock stormblocks, S and T: the
//! live mirror of stormblock#295 (`POST/DELETE /api/v1/volumes/{id}/mirror`,
//! `mirror` on `GET /api/v1/volumes/{id}`), a /v1 thin create on T, and
//! T's volume served to S's host NQN alone.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::FedState;

const S_HOST: &str = "nqn.2026-10.lo.storm:stormstorage:node-s";

#[derive(Default, Clone)]
struct Vol {
    name: String,
    size: u64,
    v1: Option<String>,
    mirror: Option<Value>,
    served: BTreeSet<String>,
}

#[derive(Default)]
struct Mock {
    node: String,
    next: u32,
    /// engine-local id → volume
    vols: BTreeMap<String, Vol>,
    /// Refuse a mirror start with this 409 message.
    refuse_mirror: Option<String>,
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
    m.log.push(format!("create {name} {}", b["size_bytes"]));
    m.vols.insert(
        format!("{node}-{n:04}"),
        Vol { name: name.clone(), size: b["size_bytes"].as_u64().unwrap(), v1: Some(v1.clone()), ..Default::default() },
    );
    Json(json!({"id": v1, "name": name, "replicas": [{"node": node, "role": "master"}]}))
}

async fn v1_delete(State(m): State<M>, Path(id): Path<String>) -> StatusCode {
    let mut m = m.lock().unwrap();
    m.log.push(format!("v1delete {id}"));
    m.vols.retain(|_, v| v.v1.as_deref() != Some(id.as_str()));
    StatusCode::NO_CONTENT
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
    let mut out = json!({"id": id, "name": v.name, "virtual_size_bytes": v.size});
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
    let node = m.node.clone();
    let Some(v) = m.vols.get_mut(&id) else { return not_found(&id) };
    let host = b["host_nqn"].as_str().expect("served to S's host alone").to_string();
    assert_eq!(b["transport"], "nvme_tcp");
    v.served.insert(host.clone());
    (
        StatusCode::OK,
        Json(json!({"transport": "nvme_tcp", "nqn": format!("nqn.2024.io.stormblock:{node}:host:{host}"), "nsid": 3,
                    "host_nqn": host, "addresses": [{"traddr": "10.0.0.2", "trsvcid": 4420}]})),
    )
}

async fn withdraw(
    State(m): State<M>,
    Path(id): Path<String>,
    Query(q): Query<BTreeMap<String, String>>,
) -> StatusCode {
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
    if let Some(msg) = m.refuse_mirror.clone() {
        return (StatusCode::CONFLICT, Json(json!({"message": msg})));
    }
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

async fn mock_engine(node: &str) -> (String, M) {
    let m: M = Arc::new(Mutex::new(Mock { node: node.into(), ..Default::default() }));
    let app = Router::new()
        .route(
            "/v1/nodes/capacity",
            get(|| async { Json(json!({"total_bytes": 100u64 << 30, "free_bytes": 50u64 << 30})) }),
        )
        .route("/v1/volumes", post(v1_create))
        .route("/v1/volumes/{id}", delete(v1_delete))
        .route("/api/v1/volumes", get(list))
        .route("/api/v1/volumes/{id}", get(get_vol).delete(delete_vol))
        .route("/api/v1/volumes/{id}/attach", post(attach).delete(withdraw))
        .route("/api/v1/volumes/{id}/mirror", post(mirror_start).delete(mirror_stop))
        .with_state(m.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr.to_string(), m)
}

struct Fx {
    api: String,
    state: Arc<AppState>,
    s: M,
    t: M,
}

async fn setup() -> Fx {
    let mut config = Config::default();
    config.local.enabled = false;
    config.kubernetes.enabled = false;
    let (sa, s) = mock_engine("node-s").await;
    let (ta, t) = mock_engine("node-t").await;
    for (n, a) in [("node-s", sa), ("node-t", ta)] {
        config.nodes.push(toml::from_str::<NodeConfig>(&format!("name = \"{n}\"\nengine_url = \"http://{a}\"")).unwrap());
    }
    // The VM's disk on S: a plain local volume, attached over ublk there.
    s.lock().unwrap().vols.insert(
        "s-disk-uuid".into(),
        Vol { name: "ns.vm-disk".into(), size: 8 << 30, ..Default::default() },
    );
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Fx { api: format!("http://{api}"), state, s, t }
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

async fn start(fx: &Fx) -> Value {
    let (st, m) = call(
        reqwest::Method::POST,
        format!("{}/api/v1/nodes/node-s/volumes/ns.vm-disk/migrate", fx.api),
        Some(json!({"to": "node-t"})),
    )
    .await;
    assert_eq!(st, 202, "{m}");
    m
}

fn set_mirror(m: &M, state: &str, remaining: u64, message: Option<&str>) {
    let mut m = m.lock().unwrap();
    let mi = m.vols.get_mut("s-disk-uuid").unwrap().mirror.as_mut().expect("mirrored");
    mi["state"] = json!(state);
    mi["bytes_remaining"] = json!(remaining);
    mi["message"] = json!(message);
}

#[tokio::test]
async fn mirror_sync_cut_over_complete() {
    let fx = setup().await;
    let m = start(&fx).await;
    let id = m["id"].as_str().unwrap().to_string();
    assert_eq!(m["state"], "copying");
    assert_eq!(m["source_volume_id"], "s-disk-uuid");
    assert_eq!(m["size_bytes"], 8u64 << 30);

    // T holds a same-size volume named like S's, served to S's host alone,
    // and S mirrors to it presenting that host NQN.
    let (t_id, t_vol) = {
        let t = fx.t.lock().unwrap();
        let (id, v) = t.vols.iter().next().expect("a volume on T");
        (id.clone(), v.clone())
    };
    assert_eq!(t_vol.name, "ns.vm-disk");
    assert_eq!(t_vol.size, 8 << 30);
    assert_eq!(t_vol.served, BTreeSet::from([S_HOST.to_string()]));
    assert_eq!(m["target"], json!({"node": "node-t", "name": "ns.vm-disk", "id": t_id}));
    let target = fx.s.lock().unwrap().vols["s-disk-uuid"].mirror.as_ref().unwrap()["target"].as_str().unwrap().to_string();
    assert!(target.starts_with("nvme-tcp://10.0.0.2:4420/"), "{target}");
    assert!(target.ends_with(&format!("?nsid=3&hostnqn={S_HOST}")), "{target}");

    // Not synced: no switch-over.
    let (st, e) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/cutover", fx.api), None).await;
    assert_eq!(st, 409, "{e}");
    assert!(fx.s.lock().unwrap().vols["s-disk-uuid"].mirror.is_some(), "the refusal left the mirror running");

    // Progress, then synced: GET reads it live, the reconciler records it.
    set_mirror(&fx.s, "copying", 1 << 30, None);
    let (_, g) = call(reqwest::Method::GET, format!("{}/api/v1/migrations/{id}", fx.api), None).await;
    assert_eq!((g["state"].clone(), g["bytes_remaining"].clone()), (json!("copying"), json!(1u64 << 30)));
    set_mirror(&fx.s, "synced", 0, None);
    let (_, g) = call(reqwest::Method::GET, format!("{}/api/v1/migrations/{id}", fx.api), None).await;
    assert_eq!(g["state"], "synced");
    stormstorage::vmmigrate::run(&fx.state).await;
    let rec = fx.state.fed.read().await.vm_migrations[&id].clone();
    assert_eq!(rec.state, stormstorage::model::VmMigrationState::Synced);
    let (_, l) = call(reqwest::Method::GET, format!("{}/api/v1/migrations", fx.api), None).await;
    assert_eq!(l["migrations"].as_array().unwrap().len(), 1);
    let feed = stormstorage::components::collect(&fx.state).await;
    assert!(feed.iter().any(|c| c.id == format!("vmmig:{id}")), "the migration is in the feed");

    // Switch-over: S's mirror stopped, T no longer served to S, T's volume
    // named for the kubelet; S's volume kept.
    let (st, c) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/cutover", fx.api), None).await;
    assert_eq!(st, 200, "{c}");
    assert_eq!(c["state"], "cut_over");
    assert!(c["target_attach"].is_null(), "{c}");
    assert_eq!(c["target"]["id"], t_id);
    assert!(fx.s.lock().unwrap().vols["s-disk-uuid"].mirror.is_none());
    assert!(fx.t.lock().unwrap().vols[&t_id].served.is_empty());
    assert!(fx.s.lock().unwrap().vols.contains_key("s-disk-uuid"), "S kept until complete");
    // Again: the same answer.
    let (st, c2) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/cutover", fx.api), None).await;
    assert_eq!((st, c2["state"].clone()), (200, json!("cut_over")));

    // Complete: S's volume retired; abort is refused from here on.
    let (st, d) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/complete", fx.api), None).await;
    assert_eq!((st, d["state"].clone()), (200, json!("completed")), "{d}");
    assert!(!fx.s.lock().unwrap().vols.contains_key("s-disk-uuid"));
    assert!(fx.t.lock().unwrap().vols.contains_key(&t_id), "T's volume is the disk now");
    let (st, _) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/abort", fx.api), None).await;
    assert_eq!(st, 409);
}

#[tokio::test]
async fn abort_leaves_the_source_as_it_was() {
    let fx = setup().await;
    let id = start(&fx).await["id"].as_str().unwrap().to_string();
    let (st, a) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/abort", fx.api), None).await;
    assert_eq!((st, a["state"].clone()), (200, json!("aborted")), "{a}");
    assert!(fx.t.lock().unwrap().vols.is_empty(), "T's volume deleted");
    {
        let s = fx.s.lock().unwrap();
        assert!(s.vols["s-disk-uuid"].mirror.is_none(), "S's mirror stopped");
        assert!(!s.log.iter().any(|l| l.starts_with("delete ")), "S's volume untouched: {:?}", s.log);
    }
    // A finished migration does not hold the volume: it can go again.
    start(&fx).await;
}

#[tokio::test]
async fn refusals_leave_nothing_behind() {
    let fx = setup().await;
    let url = |v: &str| format!("{}/api/v1/nodes/node-s/volumes/{v}/migrate", fx.api);
    let (st, _) = call(reqwest::Method::POST, url("nope"), Some(json!({"to": "node-t"}))).await;
    assert_eq!(st, 404);
    let (st, _) = call(reqwest::Method::POST, url("ns.vm-disk"), Some(json!({"to": "node-s"}))).await;
    assert_eq!(st, 400);
    let (st, _) = call(reqwest::Method::POST, url("ns.vm-disk"), Some(json!({"to": "node-x"}))).await;
    assert_eq!(st, 404);

    // T already holds a volume of that name: refused, unless named apart.
    fx.t.lock().unwrap().vols.insert("t-old".into(), Vol { name: "ns.vm-disk".into(), size: 1, ..Default::default() });
    let (st, e) = call(reqwest::Method::POST, url("ns.vm-disk"), Some(json!({"to": "node-t"}))).await;
    assert_eq!(st, 409, "{e}");
    assert_eq!(fx.t.lock().unwrap().vols.len(), 1);
    let (st, m) = call(reqwest::Method::POST, url("s-disk-uuid"), Some(json!({"to": "node-t", "name": "ns.vm-disk-2"}))).await;
    assert_eq!(st, 202, "{m}");
    assert_eq!(m["target_name"], "ns.vm-disk-2");

    // One migration per volume at a time.
    let (st, e) = call(reqwest::Method::POST, url("ns.vm-disk"), Some(json!({"to": "node-t", "name": "x"}))).await;
    assert_eq!(st, 409, "{e}");
    assert_eq!(fx.state.fed.read().await.vm_migrations.len(), 1);
}

#[tokio::test]
async fn a_refused_mirror_undoes_the_target() {
    let fx = setup().await;
    fx.s.lock().unwrap().refuse_mirror = Some("a secret volume: never copied off this engine".into());
    let (st, e) = call(
        reqwest::Method::POST,
        format!("{}/api/v1/nodes/node-s/volumes/ns.vm-disk/migrate", fx.api),
        Some(json!({"to": "node-t"})),
    )
    .await;
    assert_eq!(st, 409, "{e}");
    assert!(e["error"].as_str().unwrap().contains("secret volume"), "{e}");
    assert!(fx.t.lock().unwrap().vols.is_empty(), "T's volume undone");
    assert!(fx.state.fed.read().await.vm_migrations.is_empty(), "no record kept");
}

#[tokio::test]
async fn a_failed_mirror_is_reported_and_retried() {
    let fx = setup().await;
    let id = start(&fx).await["id"].as_str().unwrap().to_string();
    // S's engine restarted: the mirror reads failed until forgotten.
    set_mirror(&fx.s, "failed", 0, Some("the engine restarted; the mirror stopped with it: start it again"));
    stormstorage::vmmigrate::run(&fx.state).await;
    let (_, g) = call(reqwest::Method::GET, format!("{}/api/v1/migrations/{id}", fx.api), None).await;
    assert_eq!(g["state"], "failed");
    assert!(g["message"].as_str().unwrap().contains("engine restarted"), "{g}");
    let (st, _) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/cutover", fx.api), None).await;
    assert_eq!(st, 409);

    let (st, r) = call(reqwest::Method::POST, format!("{}/api/v1/migrations/{id}/retry", fx.api), None).await;
    assert_eq!((st, r["state"].clone()), (200, json!("copying")), "{r}");
    let s = fx.s.lock().unwrap();
    assert_eq!(s.vols["s-disk-uuid"].mirror.as_ref().unwrap()["state"], "copying");
    assert_eq!(s.log.iter().filter(|l| l.starts_with("mirror ")).count(), 2, "{:?}", s.log);
    drop(s);
    assert_eq!(fx.t.lock().unwrap().vols.len(), 1, "the same target volume, reused");
}
