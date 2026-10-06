//! #51/#53 end to end against an in-process mock of a *closed* stormblock
//! (#210): its shared NVMe subsystem admits no host, so an attach that
//! names none is a 400, and a volume is served to a named host from that
//! host's own subsystem (`POST /api/v1/volumes/{local}/attach {host_nqn,
//! dhchap}`; `DELETE …/attach?host_nqn=` withdraws that host only). Both
//! take the engine-local id, which is not the /v1 id; the mock keeps them
//! apart and lists local ids by name on `GET /api/v1/volumes`, as
//! stormblock does.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::FedState;

#[derive(Default)]
struct Mock {
    node: String,
    next: u32,
    /// /v1 id → (name, engine-local id)
    volumes: BTreeMap<String, (String, String)>,
    /// (local id, host) pairs served.
    served: BTreeSet<(String, String)>,
    /// Hosts that have been given a secret (kept, as stormblock does).
    secrets: BTreeSet<String>,
    log: Vec<String>,
}

type M = Arc<Mutex<Mock>>;

async fn v1_create(State(m): State<M>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = m.lock().unwrap();
    let name = b["name"].as_str().unwrap().to_string();
    let node = m.node.clone();
    m.next += 1;
    let id = format!("{node}-v1-{}", m.next);
    let local = format!("00000000-0000-0000-0000-{:012}", m.next);
    m.volumes.insert(id.clone(), (name.clone(), local));
    Json(json!({"id": id, "name": name, "replicas": [{"node": node, "role": "master"}]}))
}

/// The /v1 attach: closed — no host named, no attach.
async fn v1_attach(State(m): State<M>, Path(id): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    m.log.push(format!("v1attach {id}"));
    if b["host_nqn"].as_str().is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"message": "this node's shared NVMe subsystem admits no host: name the host (#210)"})),
        );
    }
    (StatusCode::OK, Json(json!({"transport": "nvme_tcp", "nqn": "x", "nsid": 1, "addresses": [{"traddr": "10.0.0.1", "trsvcid": 4420}]})))
}

async fn engine_volumes(State(m): State<M>) -> Json<Value> {
    let m = m.lock().unwrap();
    let items: Vec<Value> = m.volumes.values().map(|(n, l)| json!({"id": l, "name": n})).collect();
    Json(json!({"items": items}))
}

async fn any_attach(State(m): State<M>, Path(id): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    if !m.volumes.values().any(|(_, l)| *l == id) {
        return (StatusCode::NOT_FOUND, Json(json!({"message": format!("volume {id} not found")})));
    }
    let Some(host) = b["host_nqn"].as_str().map(str::to_string) else {
        return (StatusCode::BAD_REQUEST, Json(json!({"message": "shared subsystem admits no host (#210)"})));
    };
    assert_eq!(b["transport"], "nvme_tcp");
    let dhchap = b["dhchap"].as_bool().unwrap_or(false);
    m.log.push(format!("attach {id} {host} dhchap={dhchap}"));
    m.served.insert((id.clone(), host.clone()));
    if dhchap {
        m.secrets.insert(host.clone());
    }
    let mut out = json!({"transport": "nvme_tcp", "nqn": format!("nqn.2024.io.stormblock:{}:host:{host}", m.node),
                         "nsid": 7, "host_nqn": host, "addresses": [{"traddr": "0.0.0.0", "trsvcid": 4420}]});
    if m.secrets.contains(&host) {
        out["dhchap_secret"] = json!(format!("DHHC-1:00:{host}-secret:"));
    }
    (StatusCode::OK, Json(out))
}

async fn any_detach(
    State(m): State<M>,
    Path(id): Path<String>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    let mut m = m.lock().unwrap();
    let host = q.get("host_nqn").cloned().expect("a per-host withdraw, never all hosts");
    m.log.push(format!("withdraw {id} {host}"));
    m.served.remove(&(id.clone(), host.clone()));
    Json(json!({"id": id, "host_nqn": host, "attached": false}))
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
        .route("/api/v1/volumes", get(engine_volumes))
        .route("/api/v1/volumes/{id}/attach", post(any_attach).delete(any_detach))
        .with_state(m.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr.to_string(), m)
}

async fn setup() -> (String, Arc<AppState>, M) {
    let mut config = Config::default();
    config.local.enabled = false;
    config.kubernetes.enabled = false;
    let (addr, m) = mock_engine("node-a").await;
    config
        .nodes
        .push(toml::from_str::<NodeConfig>(&format!("name = \"node-a\"\nengine_url = \"http://{addr}\"")).unwrap());
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{api}"), state, m)
}

async fn call(method: reqwest::Method, url: String, body: Option<Value>) -> (u16, Value) {
    let mut r = reqwest::Client::new().request(method, url);
    if let Some(b) = body {
        r = r.json(&b);
    }
    let r = r.send().await.unwrap();
    let st = r.status().as_u16();
    (st, r.json().await.unwrap_or(Value::Null))
}

async fn post_json(url: String, body: Value) -> (u16, Value) {
    call(reqwest::Method::POST, url, Some(body)).await
}

/// Nothing a consumer could use to connect with a secret is kept: not in
/// the record, the API's volume view, or the events.
async fn assert_no_secret(api: &str, state: &Arc<AppState>) {
    let fed = serde_json::to_string(&*state.fed.read().await).unwrap();
    assert!(!fed.contains("DHHC-1"), "secret in the state: {fed}");
    let (_, v) = call(reqwest::Method::GET, format!("{api}/api/v1/volumes"), None).await;
    assert!(!v.to_string().contains("DHHC-1"), "{v}");
    let (_, e) = call(reqwest::Method::GET, format!("{api}/api/v1/events"), None).await;
    assert!(!e.to_string().contains("DHHC-1"), "{e}");
}

#[tokio::test]
async fn a_volume_is_served_to_named_hosts_and_withdrawn() {
    let (api, state, m) = setup().await;

    // No host named: the closed engine refuses the shared attach.
    let (st, v) = post_json(format!("{api}/api/v1/volumes"), json!({"name": "s", "size_bytes": 1u64 << 30, "replicas": 1})).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["export"]["state"], "failed", "{v}");

    // A host asks for it, with DH-HMAC-CHAP.
    let (st, h) = post_json(format!("{api}/api/v1/volumes/s/export/hosts"), json!({"host_nqn": "nqn.2014-08.org.nvmexpress:uuid:h1", "dhchap": true})).await;
    assert_eq!(st, 200, "{h}");
    assert_eq!(h["host_nqn"], "nqn.2014-08.org.nvmexpress:uuid:h1");
    assert_eq!(h["coordinates"]["nqn"], "nqn.2024.io.stormblock:node-a:host:nqn.2014-08.org.nvmexpress:uuid:h1");
    assert_eq!(h["coordinates"]["host_nqn"], "nqn.2014-08.org.nvmexpress:uuid:h1");
    assert_eq!(h["coordinates"]["nsid"], 7);
    assert_eq!(h["dhchap_secret"], "DHHC-1:00:nqn.2014-08.org.nvmexpress:uuid:h1-secret:");
    let local = m.lock().unwrap().volumes.values().next().unwrap().1.clone();
    let log = m.lock().unwrap().log.clone();
    assert!(
        log.contains(&format!("attach {local} nqn.2014-08.org.nvmexpress:uuid:h1 dhchap=true")),
        "per-host attach on the engine-local id: {log:?}"
    );
    {
        let fed = state.fed.read().await;
        let ex = &fed.volumes["s"].export;
        assert_eq!(ex.state, stormstorage::model::ExportState::Published);
        assert_eq!(ex.local_id.as_deref(), Some(local.as_str()));
        assert!(ex.coordinates.is_none(), "no shared coordinates once hosts are named");
        assert_eq!(ex.hosts.len(), 1);
    }
    assert_no_secret(&api, &state).await;

    // Asked again: the same coordinates and secret, unchanged.
    let (st, again) = post_json(format!("{api}/api/v1/volumes/s/export/hosts"), json!({"host_nqn": "nqn.2014-08.org.nvmexpress:uuid:h1"})).await;
    assert_eq!(st, 200);
    assert_eq!(again["coordinates"], h["coordinates"]);
    assert_eq!(again["dhchap"], true, "dhchap stays on");
    assert_eq!(again["coordinates_changed"], false);
    assert_eq!(again["dhchap_secret"], h["dhchap_secret"]);

    // Not a host NQN.
    let (st, _) = post_json(format!("{api}/api/v1/volumes/s/export/hosts"), json!({"host_nqn": "h1"})).await;
    assert_eq!(st, 400);
    let (st, _) = post_json(format!("{api}/api/v1/volumes/nope/export/hosts"), json!({"host_nqn": "nqn.x"})).await;
    assert_eq!(st, 404);

    // A second host through POST …/export (#53).
    let (st, ex) = post_json(format!("{api}/api/v1/volumes/s/export"), json!({"hosts": [{"host_nqn": "nqn.h2"}]})).await;
    assert_eq!(st, 200, "{ex}");
    assert_eq!(ex["hosts"].as_array().unwrap().len(), 2, "{ex}");
    assert!(m.lock().unwrap().served.contains(&(local.clone(), "nqn.h2".into())));

    // h1 is done with it: withdrawn on the engine, and from the record.
    let (st, w) = call(reqwest::Method::DELETE, format!("{api}/api/v1/volumes/s/export/hosts/nqn.2014-08.org.nvmexpress:uuid:h1"), None).await;
    assert_eq!(st, 200, "{w}");
    assert_eq!(w["withdrawn"], "done");
    assert!(!m.lock().unwrap().served.contains(&(local.clone(), "nqn.2014-08.org.nvmexpress:uuid:h1".into())));
    // A republish serves the hosts left, and only them.
    m.lock().unwrap().log.clear();
    let (st, ex) = call(reqwest::Method::POST, format!("{api}/api/v1/volumes/s/export"), None).await;
    assert_eq!(st, 200, "{ex}");
    let log = m.lock().unwrap().log.clone();
    assert_eq!(log, vec![format!("attach {local} nqn.h2 dhchap=false")], "{log:?}");

    // The engine is unreachable: the withdrawal waits, the record drops it.
    state.fed.write().await.nodes.get_mut("node-a").unwrap().status.healthy = false;
    let (st, w) = call(reqwest::Method::DELETE, format!("{api}/api/v1/volumes/s/export/hosts/nqn.h2"), None).await;
    assert_eq!(st, 200);
    assert_eq!(w["withdrawn"], "pending");
    assert!(state.fed.read().await.volumes["s"].export.hosts.is_empty());
    assert_eq!(state.fed.read().await.volumes["s"].export.withdrawing.len(), 1);
    assert!(m.lock().unwrap().served.contains(&(local.clone(), "nqn.h2".into())));
    // It answers again: withdrawn there.
    state.fed.write().await.nodes.get_mut("node-a").unwrap().status.healthy = true;
    m.lock().unwrap().log.clear();
    stormstorage::orchestrate::republish_on(&state, "node-a").await;
    assert!(!m.lock().unwrap().served.contains(&(local.clone(), "nqn.h2".into())));
    assert!(state.fed.read().await.volumes["s"].export.withdrawing.is_empty());
    // Served to no host now — and not put back on the shared subsystem.
    let log = m.lock().unwrap().log.clone();
    assert!(!log.iter().any(|l| l.starts_with("v1attach") || l.starts_with("attach")), "{log:?}");
    assert_eq!(state.fed.read().await.volumes["s"].export.state, stormstorage::model::ExportState::Published);
    assert_no_secret(&api, &state).await;
}

#[tokio::test]
async fn hosts_named_at_create_are_served_without_the_shared_subsystem() {
    let (api, _state, m) = setup().await;
    let (st, v) = post_json(
        format!("{api}/api/v1/volumes"),
        json!({"name": "c", "size_bytes": 1u64 << 30, "replicas": 1, "hosts": [{"host_nqn": "nqn.h3"}]}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["export"]["state"], "published", "{v}");
    assert_eq!(v["export"]["hosts"][0]["coordinates"]["nqn"], "nqn.2024.io.stormblock:node-a:host:nqn.h3", "{v}");
    let log = m.lock().unwrap().log.clone();
    assert!(!log.iter().any(|l| l.starts_with("v1attach")), "no shared attach: {log:?}");
    // A bad NQN at create is refused before anything is made.
    let (st, _) = post_json(
        format!("{api}/api/v1/volumes"),
        json!({"name": "d", "size_bytes": 1u64 << 30, "replicas": 1, "hosts": [{"host_nqn": "bad"}]}),
    )
    .await;
    assert_eq!(st, 400);
    assert!(!m.lock().unwrap().volumes.values().any(|(n, _)| n == "d"));
}

/// #40 for a volume served per host: the engine no longer has the served
/// volume, so the per-host attach 404s — `gone`, not retried, and a
/// single leg is never "recreated" (the leg is the data).
#[tokio::test]
async fn a_served_volume_gone_under_its_hosts_is_reported_not_retried() {
    let (api, state, m) = setup().await;
    let (st, v) = post_json(
        format!("{api}/api/v1/volumes"),
        json!({"name": "g", "size_bytes": 1u64 << 30, "replicas": 1, "hosts": [{"host_nqn": "nqn.h4"}]}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    m.lock().unwrap().volumes.clear();
    let (st, e) = post_json(format!("{api}/api/v1/volumes/g/export"), json!({})).await;
    assert_eq!(st, 409, "{e}");
    assert!(state.fed.read().await.volumes["g"].export.gone);
    m.lock().unwrap().log.clear();
    stormstorage::orchestrate::republish_on(&state, "node-a").await;
    assert!(m.lock().unwrap().log.is_empty());
    let (st, e) = post_json(format!("{api}/api/v1/volumes/g/export"), json!({"recreate": true})).await;
    assert_eq!(st, 409);
    assert!(e.to_string().contains("single leg"), "{e}");
}
