//! A mock of etcd's v3 JSON gateway (the routes fastetcd serves), with
//! leases that expire and a txn that honours compares: the fleet store's
//! leadership in tests (#34; shared since #121).

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use stormstorage::store::{b64, unb64};

#[derive(Clone)]
pub struct Entry {
    pub value: Vec<u8>,
    pub create: i64,
    pub modr: i64,
    pub version: i64,
    pub lease: i64,
}

#[derive(Default)]
pub struct Etcd {
    pub kv: BTreeMap<Vec<u8>, Entry>,
    pub rev: i64,
    pub leases: BTreeMap<i64, (i64, Instant)>,
    pub next_lease: i64,
    pub txns: usize,
}

pub type Mock = Arc<Mutex<Etcd>>;

impl Etcd {
    pub fn expire(&mut self) {
        let now = Instant::now();
        let dead: Vec<i64> = self.leases.iter().filter(|(_, (_, at))| *at <= now).map(|(id, _)| *id).collect();
        for id in dead {
            self.revoke(id);
        }
    }

    pub fn revoke(&mut self, id: i64) {
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

    pub fn get(&self, key: &str) -> Option<String> {
        self.kv.get(key.as_bytes()).map(|e| String::from_utf8(e.value.clone()).unwrap())
    }
}

pub async fn mock_etcd(m: Mock) -> String {
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
