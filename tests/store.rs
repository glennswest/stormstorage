//! #34: state in a fleet etcd, one elected leader. Two instances share a
//! mock of etcd's v3 JSON gateway (the routes fastetcd serves): the first
//! leads and imports its state, the other follows, loads it and forwards
//! its writes to the leader; when the leader's lease goes the follower
//! takes over, and the old leader's next write is refused by the guard.

mod common;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig, StoreBackend};
use stormstorage::model::{FedState, Node, NodeSource, NodeStatus};
use stormstorage::store::{b64, unb64, Role};

#[derive(Clone)]
struct Entry {
    value: Vec<u8>,
    create: i64,
    modr: i64,
    version: i64,
    lease: i64,
}

#[derive(Default)]
struct Etcd {
    kv: BTreeMap<Vec<u8>, Entry>,
    rev: i64,
    leases: BTreeMap<i64, (i64, Instant)>,
    next_lease: i64,
    txns: usize,
}

type Mock = Arc<Mutex<Etcd>>;

impl Etcd {
    fn expire(&mut self) {
        let now = Instant::now();
        let dead: Vec<i64> = self.leases.iter().filter(|(_, (_, at))| *at <= now).map(|(id, _)| *id).collect();
        for id in dead {
            self.revoke(id);
        }
    }

    fn revoke(&mut self, id: i64) {
        self.leases.remove(&id);
        let keys: Vec<Vec<u8>> = self.kv.iter().filter(|(_, e)| e.lease == id).map(|(k, _)| k.clone()).collect();
        if !keys.is_empty() {
            self.rev += 1;
            for k in keys {
                self.kv.remove(&k);
            }
        }
    }

    fn header(&self) -> Value {
        json!({ "cluster_id": "1", "member_id": "1", "revision": self.rev.to_string(), "raft_term": "2" })
    }

    fn range(&self, req: &Value) -> Value {
        let key = unb64(req["key"].as_str().unwrap_or_default()).unwrap();
        let end = req["range_end"].as_str().map(|e| unb64(e).unwrap());
        let kvs: Vec<Value> = self
            .kv
            .iter()
            .filter(|(k, _)| match &end {
                Some(end) => **k >= key && *k < end,
                None => **k == key,
            })
            .map(|(k, e)| {
                json!({
                    "key": b64(k),
                    "value": b64(&e.value),
                    "create_revision": e.create.to_string(),
                    "mod_revision": e.modr.to_string(),
                    "version": e.version.to_string(),
                })
            })
            .collect();
        let mut r = json!({ "header": self.header(), "count": kvs.len().to_string() });
        if !kvs.is_empty() {
            r["kvs"] = json!(kvs);
        }
        r
    }

    fn compare(&self, c: &Value) -> bool {
        let key = unb64(c["key"].as_str().unwrap()).unwrap();
        let e = self.kv.get(&key);
        let n = |f: &str| c[f].as_str().map(|s| s.parse::<i64>().unwrap()).unwrap_or(0);
        assert_eq!(c["result"], "EQUAL", "the mock knows EQUAL only");
        match c["target"].as_str().unwrap() {
            "CREATE" => e.map_or(0, |e| e.create) == n("create_revision"),
            "MOD" => e.map_or(0, |e| e.modr) == n("mod_revision"),
            "VERSION" => e.map_or(0, |e| e.version) == n("version"),
            t => panic!("compare target {t}"),
        }
    }

    fn txn(&mut self, req: &Value) -> Value {
        self.txns += 1;
        let ok = req["compare"].as_array().into_iter().flatten().all(|c| self.compare(c));
        let ops = req[if ok { "success" } else { "failure" }].as_array().cloned().unwrap_or_default();
        let writes = ops.iter().any(|o| o.get("request_range").is_none());
        if writes {
            self.rev += 1;
        }
        let mut responses = Vec::new();
        for op in &ops {
            if let Some(p) = op.get("request_put") {
                let key = unb64(p["key"].as_str().unwrap()).unwrap();
                let lease = p["lease"].as_str().map(|l| l.parse().unwrap()).unwrap_or(0);
                assert!(lease == 0 || self.leases.contains_key(&lease), "put on a dead lease");
                let prev = self.kv.get(&key).cloned();
                self.kv.insert(
                    key,
                    Entry {
                        value: unb64(p["value"].as_str().unwrap_or_default()).unwrap(),
                        create: prev.as_ref().map_or(self.rev, |e| e.create),
                        modr: self.rev,
                        version: prev.map_or(1, |e| e.version + 1),
                        lease,
                    },
                );
                responses.push(json!({ "response_put": { "header": self.header() } }));
            } else if let Some(d) = op.get("request_delete_range") {
                let key = unb64(d["key"].as_str().unwrap()).unwrap();
                self.kv.remove(&key);
                responses.push(json!({ "response_delete_range": { "header": self.header() } }));
            } else if let Some(r) = op.get("request_range") {
                responses.push(json!({ "response_range": self.range(r) }));
            }
        }
        // protojson leaves `succeeded: false` out.
        let mut r = json!({ "header": self.header(), "responses": responses });
        if ok {
            r["succeeded"] = json!(true);
        }
        r
    }

    fn get(&self, key: &str) -> Option<String> {
        self.kv.get(key.as_bytes()).map(|e| String::from_utf8(e.value.clone()).unwrap())
    }
}

async fn mock_etcd(m: Mock) -> String {
    async fn handle(path: &'static str, m: Mock, body: Value) -> String {
        let mut e = m.lock().unwrap();
        e.expire();
        let out = match path {
            "range" => e.range(&body),
            "txn" => e.txn(&body),
            "grant" => {
                e.next_lease += 1;
                let id = 7_000 + e.next_lease;
                let ttl: i64 = body["TTL"].as_str().unwrap().parse().unwrap();
                e.leases.insert(id, (ttl, Instant::now() + Duration::from_secs(ttl as u64)));
                json!({ "header": e.header(), "ID": id.to_string(), "TTL": ttl.to_string() })
            }
            "keepalive" => {
                let id: i64 = body["ID"].as_str().unwrap().parse().unwrap();
                // A stream: one line per message.
                let header = e.header();
                let r = match e.leases.get_mut(&id) {
                    Some((ttl, at)) => {
                        *at = Instant::now() + Duration::from_secs(*ttl as u64);
                        json!({ "result": { "header": header, "ID": id.to_string(), "TTL": ttl.to_string() } })
                    }
                    None => json!({ "result": { "header": header, "ID": id.to_string() } }),
                };
                return format!("{r}\n");
            }
            "revoke" => {
                let id: i64 = body["ID"].as_str().unwrap().parse().unwrap();
                e.revoke(id);
                json!({ "header": e.header() })
            }
            _ => unreachable!(),
        };
        out.to_string()
    }
    let route = |p: &'static str| {
        post(move |State(m): State<Mock>, Json(b): Json<Value>| async move { handle(p, m, b).await })
    };
    let app = Router::new()
        .route("/v3/kv/range", route("range"))
        .route("/v3/kv/txn", route("txn"))
        .route("/v3/lease/grant", route("grant"))
        .route("/v3/lease/keepalive", route("keepalive"))
        .route("/v3/lease/revoke", route("revoke"))
        .with_state(m);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn registered(name: &str) -> Node {
    let config: NodeConfig =
        toml::from_str(&format!("name = \"{name}\"\nengine_url = \"http://{name}:9090\"")).unwrap();
    Node { config, status: NodeStatus::new(NodeSource::Registered) }
}

/// An instance on its own port, its state in `etcd`.
async fn instance(name: &str, etcd: &str, fed: FedState) -> (Arc<AppState>, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let mut config = Config::default();
    config.local.enabled = false;
    config.store.backend = StoreBackend::Etcd;
    config.store.endpoints = vec![
        // The first member is down: calls go on to the next.
        "http://127.0.0.1:1".into(),
        etcd.to_string(),
    ];
    config.store.lease_ttl_secs = 3;
    config.store.instance = Some(name.into());
    config.store.advertise_url = Some(url.clone());
    config.validate().unwrap();
    let state = Arc::new(AppState::new(config, fed, None));
    let app = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (state, url)
}

fn store(s: &AppState) -> &stormstorage::store::Store {
    s.store.as_deref().unwrap()
}

async fn node_names(s: &AppState) -> Vec<String> {
    s.fed.read().await.nodes.keys().cloned().collect()
}

#[tokio::test]
async fn one_leader_followers_forward_and_failover() {
    let m: Mock = Default::default();
    let etcd = mock_etcd(m.clone()).await;

    let mut fed_a = FedState::default();
    fed_a.nodes.insert("pre".into(), registered("pre"));
    fed_a.revision = 5;
    let (a, a_url) = instance("a", &etcd, fed_a).await;
    let (b, b_url) = instance("b", &etcd, FedState::default()).await;

    // A leads, and imports its state into the empty store.
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().role, Role::Leader, "{:?}", store(&a).status());
    assert!(a.leads() && a.recovery_active());
    {
        let e = m.lock().unwrap();
        assert!(e.get("/stormstorage/nodes/pre").is_some(), "imported");
        assert!(e.get("/stormstorage/meta").unwrap().contains("\"revision\":5"));
        let leader: Value = serde_json::from_str(&e.get("/stormstorage/leader").unwrap()).unwrap();
        assert_eq!(leader, json!({ "instance": "a", "url": a_url }));
    }

    // B follows and loads it.
    store(&b).step(&b).await;
    let st = store(&b).status();
    assert_eq!(st.role, Role::Follower);
    assert_eq!(st.leader.unwrap().instance, "a");
    assert!(!b.leads() && !b.recovery_active());
    assert_eq!(node_names(&b).await, vec!["pre"]);
    assert_eq!(b.fed.read().await.revision, 5);

    // A write to B is A's: forwarded, written by A, loaded by B.
    let http = reqwest::Client::new();
    let r = http
        .post(format!("{b_url}/api/v1/storage/register"))
        .json(&json!({ "node_addr": "10.0.0.9:9090", "hostname": "n9" }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.status());
    assert_eq!(r.headers()["x-stormstorage-leader"], a_url.as_str());
    assert_eq!(node_names(&a).await, vec!["n9", "pre"]);
    assert!(m.lock().unwrap().get("/stormstorage/nodes/n9").is_some());
    store(&b).step(&b).await;
    assert_eq!(node_names(&b).await, vec!["n9", "pre"]);

    // Reads stay local; the status says who leads.
    let st: Value = http.get(format!("{b_url}/api/v1/store")).send().await.unwrap().json().await.unwrap();
    assert_eq!(st["leads"], false);
    assert_eq!(st["status"]["leader"]["instance"], "a");
    let h: Value = http.get(format!("{a_url}/api/v1/health")).send().await.unwrap().json().await.unwrap();
    assert_eq!(h["store"]["role"], "leader");

    // A request forwarded once is never forwarded again.
    let r = http
        .post(format!("{b_url}/api/v1/storage/register"))
        .header(stormstorage::store::FORWARDED, "x")
        .json(&json!({ "node_addr": "10.0.0.8:9090", "hostname": "n8" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);

    // A's lease goes (A hangs, say): B takes over with the store's state.
    {
        let mut e = m.lock().unwrap();
        let ids: Vec<i64> = e.leases.keys().copied().collect();
        for id in ids {
            e.revoke(id);
        }
    }
    store(&b).step(&b).await;
    assert_eq!(store(&b).status().role, Role::Leader);
    assert_eq!(node_names(&b).await, vec!["n9", "pre"]);

    // A still thinks it leads; its next write is refused and it steps down.
    a.fed.write().await.nodes.insert("stale".into(), registered("stale"));
    a.persist().await;
    assert!(m.lock().unwrap().get("/stormstorage/nodes/stale").is_none(), "a deposed leader wrote");
    assert_eq!(store(&a).status().role, Role::Follower);
    assert!(!a.recovery_active());
    // Following again, it reloads: the store wins over its memory.
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().leader.unwrap().instance, "b");
    assert_eq!(node_names(&a).await, vec!["n9", "pre"]);

    // B renews; a write through A now lands on B.
    store(&b).step(&b).await;
    assert_eq!(store(&b).status().role, Role::Leader);
    let r = http
        .post(format!("{a_url}/api/v1/storage/register"))
        .json(&json!({ "node_addr": "10.0.0.7:9090", "hostname": "n7" }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    assert!(m.lock().unwrap().get("/stormstorage/nodes/n7").is_some());
}

#[tokio::test]
async fn a_leader_whose_lease_expired_steps_down() {
    let m: Mock = Default::default();
    let etcd = mock_etcd(m.clone()).await;
    let (a, _) = instance("a", &etcd, FedState::default()).await;
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().role, Role::Leader);
    // An empty state still writes `meta`, so followers know there is one.
    assert!(m.lock().unwrap().get("/stormstorage/meta").is_some());
    // No renewal for longer than the TTL (3 s): the lease and key go.
    tokio::time::sleep(Duration::from_millis(3200)).await;
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().role, Role::Follower);
    assert!(m.lock().unwrap().get("/stormstorage/leader").is_none());
    // And it can win again.
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().role, Role::Leader);
}

/// An instance that publishes, advertising an address nothing dials.
async fn publisher(name: &str, etcd: &str, api: &str, advertise: &str) -> Arc<AppState> {
    let mut config = Config::default();
    config.local.enabled = false;
    config.store.backend = StoreBackend::Etcd;
    config.store.endpoints = vec![etcd.to_string()];
    config.store.lease_ttl_secs = 3;
    config.store.instance = Some(name.into());
    config.store.advertise_url = Some(advertise.into());
    config.store.publish_service = Some(SERVICE.into());
    config.kubernetes.server = api.to_string();
    config.validate().unwrap();
    Arc::new(AppState::new(config, FedState::default(), None))
}

const SERVICE: &str = "stormblock-system/stormstorage";

#[tokio::test]
async fn the_leader_keeps_the_endpoints_and_a_new_leader_takes_them() {
    let m: Mock = Default::default();
    let etcd = mock_etcd(m.clone()).await;
    let api: common::ApiMock = Default::default();
    let url = common::mock_apiserver(api.clone()).await;
    let a = publisher("a", &etcd, &url, "http://192.0.2.1:9093").await;
    let b = publisher("b", &etcd, &url, "http://192.0.2.2:9093").await;

    // A follower writes nothing.
    store(&a).step(&a).await;
    store(&b).step(&b).await;
    assert_eq!(store(&b).status().role, Role::Follower);
    assert!(store(&b).status().publish.is_none());
    let want_a = Some(("192.0.2.1".to_string(), 9093, "a".to_string()));
    assert_eq!(api.lock().unwrap().says(SERVICE), want_a);
    assert_eq!(api.lock().unwrap().writes, 1, "created once");
    let st = store(&a).status().publish.unwrap();
    assert_eq!((st.address.as_deref(), st.port, st.error), (Some("192.0.2.1"), Some(9093), None));

    // Nothing changed: nothing written.
    store(&a).step(&a).await;
    assert_eq!(api.lock().unwrap().writes, 1);

    // A deposed leader's late write, or a hand edit, does not last.
    api.lock().unwrap().endpoints.get_mut(SERVICE).unwrap()["subsets"][0]["addresses"][0]["ip"] = json!("192.0.2.9");
    store(&a).step(&a).await;
    assert_eq!(api.lock().unwrap().says(SERVICE), want_a);
    assert_eq!(api.lock().unwrap().writes, 2);

    // The apiserver refuses: the leadership does not depend on it, the
    // status says why, and it heals.
    api.lock().unwrap().endpoints_status = Some(axum::http::StatusCode::FORBIDDEN);
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().role, Role::Leader);
    assert!(store(&a).status().publish.unwrap().error.unwrap().contains("403"));
    api.lock().unwrap().endpoints_status = None;
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().publish.unwrap().error, None);

    // A's lease goes: B leads and the Endpoints follow.
    tokio::time::sleep(Duration::from_millis(3200)).await;
    store(&b).step(&b).await;
    assert_eq!(store(&b).status().role, Role::Leader);
    assert_eq!(api.lock().unwrap().says(SERVICE), Some(("192.0.2.2".to_string(), 9093, "b".to_string())));
    // A finds out, and stops publishing.
    let before = api.lock().unwrap().writes;
    store(&a).step(&a).await;
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().role, Role::Follower);
    assert!(store(&a).status().publish.is_none());
    assert_eq!(api.lock().unwrap().writes, before);
}

#[tokio::test]
async fn a_loopback_is_never_published() {
    let m: Mock = Default::default();
    let etcd = mock_etcd(m.clone()).await;
    let api: common::ApiMock = Default::default();
    let url = common::mock_apiserver(api.clone()).await;
    let a = publisher("a", &etcd, &url, "http://127.0.0.1:9093").await;
    store(&a).step(&a).await;
    assert_eq!(store(&a).status().role, Role::Leader);
    let e = store(&a).status().publish.unwrap().error.unwrap();
    assert!(e.contains("not an address other nodes can reach"), "{e}");
    assert!(api.lock().unwrap().endpoints.is_empty());
}

/// A mock engine recording the bearer it is called with.
async fn bearer_engine() -> (String, Arc<Mutex<Vec<String>>>) {
    let seen: Arc<Mutex<Vec<String>>> = Default::default();
    let record = seen.clone();
    let app = Router::new()
        .route(
            "/v1/nodes/capacity",
            axum::routing::get(move |h: axum::http::HeaderMap| {
                let record = record.clone();
                async move {
                    let a = h.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("none");
                    record.lock().unwrap().push(a.to_string());
                    Json(json!([{ "node": "x", "total_bytes": 1u64 << 30, "free_bytes": 1u64 << 29 }]))
                }
            }),
        )
        .route("/api/v1/slabs", axum::routing::get(|| async { Json(json!({ "items": [], "count": 0 })) }))
        .route("/api/v1/volumes", axum::routing::get(|| async { Json(json!({ "items": [], "count": 0 })) }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr.to_string(), seen)
}

#[tokio::test]
async fn cluster_members_are_nodes_and_only_their_engines_get_the_bearer() {
    let m: Mock = Default::default();
    let etcd = mock_etcd(m.clone()).await;
    let api: common::ApiMock = Default::default();
    let url = common::mock_apiserver(api.clone()).await;
    let (member_addr, member_seen) = bearer_engine().await;
    let (stranger_addr, stranger_seen) = bearer_engine().await;

    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("members");
    std::fs::create_dir_all(&dir).unwrap();
    let token = dir.join("stormstorage.token");
    std::fs::write(&token, "sa-token\n").unwrap();

    let mut config = Config::default();
    config.local.enabled = false;
    // No engine token of any kind on this machine.
    config.local.token_file = Some(dir.join("absent").display().to_string());
    config.store.backend = StoreBackend::Etcd;
    config.store.endpoints = vec![etcd];
    config.store.instance = Some("a".into());
    config.kubernetes.server = url;
    config.kubernetes.token_file = Some(token.display().to_string());
    config.validate().unwrap();

    // A URL that only announced itself: register is open (#77).
    let mut fed = FedState::default();
    let announced: NodeConfig =
        toml::from_str(&format!("name = \"stranger\"\nengine_url = \"http://{stranger_addr}\"")).unwrap();
    fed.nodes.insert("stranger".into(), Node { config: announced, status: NodeStatus::new(NodeSource::Registered) });
    api.lock().unwrap().members = vec![
        common::member("m1", &member_addr, true),
        json!({ "metadata": { "name": "joining" }, "spec": {}, "status": { "phase": "Pending" } }),
    ];
    let a = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::poll_once(&a).await;

    {
        let fed = a.fed.read().await;
        let n = &fed.nodes["m1"];
        assert_eq!(n.status.source, NodeSource::Member);
        assert!(n.status.healthy, "{:?}", n.status);
        assert_eq!(n.config.labels["storage"], "true");
        assert_eq!(n.config.engine_url, format!("http://{member_addr}"));
        assert!(!fed.nodes.contains_key("joining"), "no address yet");
    }
    assert!(api.lock().unwrap().bearers.iter().any(|b| b == "Bearer sa-token"));
    assert!(member_seen.lock().unwrap().iter().all(|b| b == "Bearer sa-token"));
    assert!(!member_seen.lock().unwrap().is_empty());
    assert!(!stranger_seen.lock().unwrap().is_empty());
    assert!(stranger_seen.lock().unwrap().iter().all(|b| b == "none"), "the bearer stays with members");

    // Members are this instance's own, never in the store.
    a.persist().await;
    assert!(m.lock().unwrap().get("/stormstorage/nodes/m1").is_none());

    // An unreadable list changes nothing.
    api.lock().unwrap().members_status = Some(axum::http::StatusCode::FORBIDDEN);
    stormstorage::registry::poll_once(&a).await;
    assert!(a.fed.read().await.nodes.contains_key("m1"));
    let noted = a.events.read().await.since(0).iter().filter(|e| e.message.contains("not readable")).count();
    stormstorage::registry::poll_once(&a).await;
    assert_eq!(a.events.read().await.since(0).iter().filter(|e| e.message.contains("not readable")).count(), noted);
    assert_eq!(noted, 1);

    // A member that left is forgotten, and loses the bearer.
    api.lock().unwrap().members_status = None;
    api.lock().unwrap().members.clear();
    stormstorage::registry::poll_once(&a).await;
    assert!(!a.fed.read().await.nodes.contains_key("m1"));
    assert!(a.members.read().unwrap().engines.is_empty());
    assert!(a.fed.read().await.nodes.contains_key("stranger"));
}

#[tokio::test]
async fn volumes_the_store_does_not_hold_are_kept_and_reported() {
    let m: Mock = Default::default();
    let etcd = mock_etcd(m.clone()).await;
    let (a, _) = instance("a", &etcd, FedState::default()).await;
    store(&a).step(&a).await;

    let mut fed = FedState::default();
    let vol = serde_json::from_value(json!({
        "name": "mine",
        "size_bytes": 1,
        "pool": null,
        "replicas": 1,
        "rung": "node",
        "legs": [],
        "assembly": "single_leg",
        "created_at": std::time::SystemTime::now(),
    }))
    .unwrap();
    fed.volumes.insert("mine".into(), vol);

    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("displaced");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut config = Config::default();
    config.local.enabled = false;
    config.store.backend = StoreBackend::Etcd;
    config.store.endpoints = vec![etcd];
    config.store.instance = Some("b".into());
    let b = Arc::new(AppState::new(config, fed, Some(dir.join("state.json"))));
    store(&b).step(&b).await;

    assert!(b.fed.read().await.volumes.is_empty(), "the store wins");
    let d = store(&b).status().displaced.unwrap();
    assert_eq!(d.volumes, vec!["mine"]);
    let copy = d.kept_as.clone().unwrap();
    assert!(std::fs::read_to_string(&copy).unwrap().contains("\"mine\""), "{copy}");
    let c = b.conditions();
    assert_eq!(c[0]["type"], "store_displaced_volumes");
    assert_eq!(c[0]["volumes"][0], "mine");
}

#[tokio::test]
async fn no_leader_no_writes() {
    let m: Mock = Default::default();
    let etcd = mock_etcd(m.clone()).await;
    let (a, a_url) = instance("a", &etcd, FedState::default()).await;
    // Not in touch with the store yet: nobody to forward to.
    let r = reqwest::Client::new()
        .post(format!("{a_url}/api/v1/storage/register"))
        .json(&json!({ "node_addr": "10.0.0.9:9090", "hostname": "n9" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["code"], "no_leader");
    assert!(node_names(&a).await.is_empty());
    // Unchanged writes cost nothing: no transaction without a change.
    store(&a).step(&a).await;
    let before = m.lock().unwrap().txns;
    a.persist().await;
    assert_eq!(m.lock().unwrap().txns, before);
}

#[test]
fn etcd_and_peers_are_exclusive() {
    let mut c = Config::default();
    c.store.backend = StoreBackend::Etcd;
    assert!(c.validate().is_err(), "no endpoints");
    c.store.endpoints = vec!["https://e1:2379".into()];
    c.validate().unwrap();
    c.replication.peers = vec!["http://peer:9093".into()];
    assert!(c.validate().unwrap_err().to_string().contains("exclusive"));
    c.replication.peers.clear();
    c.store.endpoints = vec!["e1:2379".into()];
    assert!(c.validate().is_err(), "not a URL");
    let c: Config = toml::from_str("[store]\nbackend = \"etcd\"\nendpoints = [\"http://e:2379\"]\n").unwrap();
    assert_eq!(c.store.prefix, "/stormstorage/");
    assert!(Config::unknown_top_level_keys("[store]\nbackend = \"file\"\n").is_empty());
}
