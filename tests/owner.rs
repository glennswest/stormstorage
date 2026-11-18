//! #109 against an in-process mock stormblock: `POST /api/v1/volumes` takes
//! an owner, records it, returns it, and sends it on the engine's /v1
//! create; a clone takes its own owner, never its source's; an engine
//! volume whose owner reads otherwise is set with
//! `PUT /api/v1/volumes/{id}/owner` on the next poll. The mock keeps
//! stormblock's rules for the owner (src/mgmt/api/volumes.rs `set_owner`):
//! kind and name required, a null owner forgets it.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::FedState;

#[derive(Default)]
struct Mock {
    node: String,
    next: u32,
    /// engine id → (name, owner)
    volumes: BTreeMap<String, (String, Value)>,
    /// Every /v1 create body, in order.
    creates: Vec<Value>,
    /// Every owner PUT: (engine id, owner).
    puts: Vec<(String, Value)>,
}

type M = Arc<Mutex<Mock>>;

async fn v1_create(State(m): State<M>, Json(b): Json<Value>) -> Json<Value> {
    let mut m = m.lock().unwrap();
    m.creates.push(b.clone());
    let name = b["name"].as_str().unwrap().to_string();
    let node = m.node.clone();
    let id = match m.volumes.iter().find(|(_, (n, _))| *n == name).map(|(id, _)| id.clone()) {
        Some(id) => id,
        None => {
            m.next += 1;
            let id = format!("{node}-vol-{}", m.next);
            m.volumes.insert(id.clone(), (name.clone(), b.get("owner").cloned().unwrap_or(Value::Null)));
            id
        }
    };
    Json(json!({"id": id, "name": name, "replicas": [{"node": node, "role": "master"}]}))
}

async fn v1_attach(State(m): State<M>, Path(_id): Path<String>) -> Json<Value> {
    let m = m.lock().unwrap();
    Json(json!({"transport": "nvme_tcp", "nqn": format!("nqn.2024.io.stormblock:{}", m.node), "nsid": 1,
                "addresses": [{"traddr": "127.0.0.1", "trsvcid": 4420}]}))
}

async fn list(State(m): State<M>) -> Json<Value> {
    let m = m.lock().unwrap();
    let items: Vec<Value> = m
        .volumes
        .iter()
        .map(|(id, (name, owner))| {
            let mut v = json!({"id": id, "name": name, "virtual_size_bytes": 1u64 << 30, "role": "data"});
            if !owner.is_null() {
                v["owner"] = owner.clone();
            }
            v
        })
        .collect();
    Json(json!({"items": items, "count": items.len()}))
}

async fn set_owner(State(m): State<M>, Path(id): Path<String>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut m = m.lock().unwrap();
    let owner = b["owner"].clone();
    if !owner.is_null() && (owner["kind"].as_str().unwrap_or("").is_empty() || owner["name"].as_str().unwrap_or("").is_empty()) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "an owner needs a kind and a name"})));
    }
    let Some(v) = m.volumes.get_mut(&id) else {
        return (StatusCode::NOT_FOUND, Json(json!({"error": format!("volume {id} not found")})));
    };
    v.1 = owner.clone();
    m.puts.push((id.clone(), owner.clone()));
    (StatusCode::OK, Json(json!({"id": id, "owner": owner})))
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
        .route("/api/v1/slabs", get(|| async { Json(json!({"items": [], "count": 0})) }))
        .route("/api/v1/volumes", get(list))
        .route("/api/v1/volumes/{id}/owner", put(set_owner))
        .with_state(m.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr.to_string(), m)
}

async fn setup() -> (String, Arc<AppState>, M) {
    let mut config = Config::default();
    config.local.enabled = false;
    let (addr, m) = mock_engine("node-a").await;
    config
        .nodes
        .push(toml::from_str::<NodeConfig>(&format!("name = \"node-a\"\nengine_url = \"http://{addr}\"")).unwrap());
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&state).await;
    assert!(state.fed.read().await.nodes.values().all(|n| n.status.healthy));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{api}"), state, m)
}

fn claim(uid: &str) -> Value {
    json!({"kind": "PersistentVolumeClaim", "namespace": "default", "name": "data", "uid": uid})
}

async fn create(api: &str, body: Value) -> (StatusCode, Value) {
    let r = reqwest::Client::new().post(format!("{api}/api/v1/volumes")).json(&body).send().await.unwrap();
    let status = r.status();
    (status, r.json().await.unwrap_or(Value::Null))
}

async fn get_json(url: String) -> Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

#[tokio::test]
async fn create_records_the_owner_returns_it_and_sends_it_to_the_engine() {
    let (api, state, m) = setup().await;
    let (st, v) = create(&api, json!({"name": "pvc-1", "size_bytes": 1u64 << 30, "owner": claim("u1")})).await;
    assert!(st.is_success(), "{v}");
    assert_eq!(v["owner"], claim("u1"), "{v}");
    // The /v1 create carried it.
    assert_eq!(m.lock().unwrap().creates[0]["owner"], claim("u1"));
    // GET and the listing return it.
    let one = get_json(format!("{api}/api/v1/volumes/pvc-1")).await;
    assert_eq!(one["owner"], claim("u1"), "{one}");
    assert_eq!(one["export"]["node"], "node-a");
    let all = get_json(format!("{api}/api/v1/volumes")).await;
    let listed = all["volumes"].as_array().unwrap();
    assert_eq!(listed.iter().find(|x| x["name"] == "pvc-1").unwrap()["owner"], claim("u1"));
    // Already on the engine: the poll has nothing to set.
    stormstorage::registry::poll_once(&state).await;
    assert!(m.lock().unwrap().puts.is_empty());

    // No owner: none recorded, none sent.
    let (st, v) = create(&api, json!({"name": "plain", "size_bytes": 1u64 << 30})).await;
    assert!(st.is_success(), "{v}");
    assert!(v.get("owner").is_none_or(|o| o.is_null()), "{v}");
    assert!(m.lock().unwrap().creates[1].get("owner").is_none());
}

#[tokio::test]
async fn a_partial_owner_is_refused() {
    let (api, state, m) = setup().await;
    for bad in [json!({"kind": "PersistentVolumeClaim", "name": " "}), json!({"kind": "", "name": "data"})] {
        let (st, v) = create(&api, json!({"name": "pvc-x", "size_bytes": 1u64 << 30, "owner": bad})).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    }
    assert!(m.lock().unwrap().creates.is_empty());
    assert!(state.fed.read().await.volumes.is_empty());
}

#[tokio::test]
async fn a_clone_takes_its_own_owner_never_its_sources() {
    let (api, _state, m) = setup().await;
    let (st, v) = create(&api, json!({"name": "src", "size_bytes": 1u64 << 30, "owner": claim("u-src")})).await;
    assert!(st.is_success(), "{v}");
    let (st, v) = create(
        &api,
        json!({"name": "cl", "size_bytes": 1u64 << 30, "source": {"kind": "volume", "id": "src"}, "owner": claim("u-cl")}),
    )
    .await;
    assert!(st.is_success(), "{v}");
    assert_eq!(v["owner"], claim("u-cl"), "{v}");
    let body = m.lock().unwrap().creates.last().cloned().unwrap();
    assert_eq!(body["source"]["id"], "src");
    assert_eq!(body["owner"], claim("u-cl"));

    // Without one: unclaimed, not the source's.
    let (st, v) =
        create(&api, json!({"name": "cl2", "size_bytes": 1u64 << 30, "source": {"kind": "volume", "id": "src"}})).await;
    assert!(st.is_success(), "{v}");
    assert!(v.get("owner").is_none_or(|o| o.is_null()), "{v}");
    assert!(m.lock().unwrap().creates.last().unwrap().get("owner").is_none());
}

#[tokio::test]
async fn an_engine_volume_that_reads_otherwise_is_set_on_the_next_poll() {
    let (api, state, m) = setup().await;
    let (st, v) = create(&api, json!({"name": "pvc-2", "size_bytes": 1u64 << 30, "owner": claim("u2")})).await;
    assert!(st.is_success(), "{v}");
    // The engine lost it (an older engine dropped it on create, or the
    // served volume came across on a promote with none).
    let id = {
        let mut m = m.lock().unwrap();
        let (id, v) = m.volumes.iter_mut().find(|(_, (n, _))| n == "pvc-2").unwrap();
        v.1 = Value::Null;
        id.clone()
    };
    stormstorage::registry::poll_once(&state).await;
    {
        let m = m.lock().unwrap();
        assert_eq!(m.puts, vec![(id.clone(), claim("u2"))]);
        assert_eq!(m.volumes[&id].1, claim("u2"));
    }
    // Set: the next poll leaves it alone.
    stormstorage::registry::poll_once(&state).await;
    assert_eq!(m.lock().unwrap().puts.len(), 1);
    let events = state.events.read().await;
    assert!(
        format!("{:?}", *events).contains("owner PersistentVolumeClaim default/data set on node-a's pvc-2"),
        "{:?}",
        *events
    );
}
