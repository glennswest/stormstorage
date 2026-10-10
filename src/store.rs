//! Durable state in a fleet fastetcd, and the one leader that writes it
//! (#34, phase 5).
//!
//! With `[store] backend = "etcd"` every stormstorage instance of a fleet
//! shares one etcd (a fleet fastetcd across the storage nodes, deployed by
//! stormcluster#55). It talks etcd's v3 JSON gateway (`POST /v3/kv/*`,
//! `/v3/lease/*`), which fastetcd serves on its client port, so no gRPC
//! client is needed.
//!
//! * **Election.** An instance grants itself a lease of `lease_ttl_secs`
//!   and creates `<prefix>leader` = {instance, url} bound to it, only if
//!   the key does not exist (`create_revision == 0`). The holder renews
//!   the lease every third of its TTL; one that cannot renew for two
//!   thirds of it steps down, and its key goes with the lease.
//! * **Writes.** Only the leader writes. Each durable record has its own
//!   key (`volumes/<name>`, `snapshots/<name>`, `group_snapshots/<name>`,
//!   `backups/<name>`, `nodes/<name>` for registered nodes, `orphans`,
//!   `stale_heads`); `persist()` sends what changed since the last write,
//!   plus `meta` {revision, by, at}, in a transaction guarded by
//!   `create_revision(leader) == the leader's own`, so a deposed leader's
//!   write changes nothing.
//! * **Followers** reload every record when `meta` moves, poll the engines
//!   themselves (status is local, as with peers), run no reconciler, and
//!   forward every API write to the leader's `advertise_url`
//!   ([`forward_writes`]).
//! * **Import.** The first leader of an empty store writes its own state
//!   (state.json) into it; a later leader loads the store first, and the
//!   store wins over its file.
//!
//! The data path is untouched: an outage of the store stops writes
//! through stormstorage (no leader → 503), never I/O.

use crate::api::AppState;
use crate::config::{NodeConfig, StoreConfig};
use crate::events::Severity;
use crate::model::{FedState, Node, NodeSource, NodeStatus};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Marks a request one instance forwarded to another, so a request never
/// travels twice.
pub const FORWARDED: &str = "x-stormstorage-forwarded";

/// Operations per transaction, below etcd's default `--max-txn-ops` (128)
/// with room for `meta`.
pub const TXN_OPS: usize = 100;

/// The largest request body forwarded to the leader.
const MAX_FORWARD: usize = 16 << 20;

// ---------------------------------------------------------------- base64

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding, as the gateway writes keys and values.
pub fn b64(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = chunk.iter().enumerate().fold(0u32, |acc, (i, b)| acc | ((*b as u32) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(B64[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

pub fn unb64(s: &str) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes().filter(|c| !c.is_ascii_whitespace() && *c != b'=') {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => anyhow::bail!("not base64: {s:?}"),
        };
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(out)
}

/// A 64-bit number the gateway may write as a string; absent = 0
/// (protojson leaves default values out).
fn num(v: &Value) -> i64 {
    match v {
        Value::String(s) => s.parse().unwrap_or(0),
        Value::Number(n) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

/// The end of the range holding every key that starts with `prefix`.
pub fn prefix_end(prefix: &str) -> Vec<u8> {
    let mut end = prefix.as_bytes().to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

// ------------------------------------------------------------ the client

/// One key as a range returns it.
#[derive(Debug, Clone)]
pub struct Kv {
    pub key: String,
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
}

fn kvs_of(resp: &Value) -> anyhow::Result<Vec<Kv>> {
    let Some(list) = resp.get("kvs").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    list.iter()
        .map(|kv| {
            Ok(Kv {
                key: String::from_utf8(unb64(kv["key"].as_str().unwrap_or_default())?)?,
                value: unb64(kv["value"].as_str().unwrap_or_default())?,
                create_revision: num(&kv["create_revision"]),
                mod_revision: num(&kv["mod_revision"]),
            })
        })
        .collect()
}

/// etcd's v3 JSON gateway over the configured endpoints: a call goes to
/// the last endpoint that answered, and on to the next when one does not.
pub struct Etcd {
    endpoints: Vec<String>,
    http: reqwest::Client,
    good: AtomicUsize,
}

impl Etcd {
    pub fn new(cfg: &StoreConfig) -> anyhow::Result<Self> {
        let mut b = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(3));
        if let Some(ca) = &cfg.ca_file {
            let pem = std::fs::read(ca).map_err(|e| anyhow::anyhow!("store.ca_file {ca}: {e}"))?;
            for cert in reqwest::Certificate::from_pem_bundle(&pem)? {
                b = b.add_root_certificate(cert);
            }
        }
        if let (Some(c), Some(k)) = (&cfg.cert_file, &cfg.key_file) {
            let mut pem = std::fs::read(c).map_err(|e| anyhow::anyhow!("store.cert_file {c}: {e}"))?;
            pem.push(b'\n');
            pem.extend(std::fs::read(k).map_err(|e| anyhow::anyhow!("store.key_file {k}: {e}"))?);
            b = b.identity(reqwest::Identity::from_pem(&pem)?);
        }
        Ok(Self {
            endpoints: cfg.endpoints.iter().map(|e| e.trim_end_matches('/').to_string()).collect(),
            http: b.build()?,
            good: AtomicUsize::new(0),
        })
    }

    /// POST `path` with `body`; the response, or for a streamed call
    /// (keepalive) its first message.
    pub async fn call(&self, path: &str, body: &Value) -> anyhow::Result<Value> {
        let n = self.endpoints.len();
        let start = self.good.load(Ordering::Relaxed);
        let mut errors = Vec::new();
        for i in 0..n {
            let idx = (start + i) % n;
            let url = format!("{}{path}", self.endpoints[idx]);
            match self.http.post(&url).json(body).send().await {
                Ok(r) => {
                    let status = r.status();
                    let text = r.text().await.unwrap_or_default();
                    // Unavailable (no Raft leader there, or shutting
                    // down): another member may do better.
                    if status == StatusCode::SERVICE_UNAVAILABLE {
                        errors.push(format!("{url}: 503 {}", text.trim()));
                        continue;
                    }
                    if !status.is_success() {
                        anyhow::bail!("{url}: {status} {}", text.trim());
                    }
                    self.good.store(idx, Ordering::Relaxed);
                    let line = text.lines().find(|l| !l.trim().is_empty()).unwrap_or("{}");
                    let v: Value = serde_json::from_str(line)?;
                    if let Some(e) = v.get("error") {
                        anyhow::bail!("{url}: {e}");
                    }
                    return Ok(v.get("result").cloned().unwrap_or(v));
                }
                Err(e) => errors.push(format!("{url}: {e}")),
            }
        }
        anyhow::bail!("no etcd endpoint answered: {}", errors.join("; "))
    }

    pub async fn range(&self, key: &str) -> anyhow::Result<Vec<Kv>> {
        kvs_of(&self.call("/v3/kv/range", &json!({ "key": b64(key.as_bytes()) })).await?)
    }

    pub async fn range_prefix(&self, prefix: &str) -> anyhow::Result<Vec<Kv>> {
        let body = json!({ "key": b64(prefix.as_bytes()), "range_end": b64(&prefix_end(prefix)) });
        kvs_of(&self.call("/v3/kv/range", &body).await?)
    }

    pub async fn txn(&self, body: &Value) -> anyhow::Result<Value> {
        self.call("/v3/kv/txn", body).await
    }

    pub async fn lease_grant(&self, ttl: u64) -> anyhow::Result<i64> {
        let r = self.call("/v3/lease/grant", &json!({ "TTL": ttl.to_string() })).await?;
        match num(&r["ID"]) {
            0 => anyhow::bail!("lease grant: no ID in {r}"),
            id => Ok(id),
        }
    }

    /// Renew; the TTL left (0 = the lease is gone).
    pub async fn lease_keepalive(&self, id: i64) -> anyhow::Result<i64> {
        let r = self.call("/v3/lease/keepalive", &json!({ "ID": id.to_string() })).await?;
        Ok(num(&r["TTL"]))
    }

    pub async fn lease_revoke(&self, id: i64) -> anyhow::Result<()> {
        self.call("/v3/lease/revoke", &json!({ "ID": id.to_string() })).await.map(|_| ())
    }
}

// ------------------------------------------------------------- records

/// The durable records of `fed`, by key below the prefix, as JSON.
pub fn records_of(fed: &FedState) -> BTreeMap<String, String> {
    fn put<T: Serialize>(m: &mut BTreeMap<String, String>, k: String, v: &T) {
        m.insert(k, serde_json::to_string(v).unwrap_or_default());
    }
    let mut m = BTreeMap::new();
    for (n, v) in &fed.volumes {
        put(&mut m, format!("volumes/{n}"), v);
    }
    for (n, v) in &fed.snapshots {
        put(&mut m, format!("snapshots/{n}"), v);
    }
    for (n, v) in &fed.group_snapshots {
        put(&mut m, format!("group_snapshots/{n}"), v);
    }
    for (n, v) in &fed.backups {
        put(&mut m, format!("backups/{n}"), v);
    }
    for n in fed.nodes.values().filter(|n| n.status.source == NodeSource::Registered) {
        put(&mut m, format!("nodes/{}", n.config.name), &n.config);
    }
    put(&mut m, "orphans".into(), &fed.orphans);
    put(&mut m, "stale_heads".into(), &fed.stale_heads);
    m
}

/// Replace `fed`'s durable records with `records` (keys below the
/// prefix). Static and adopted nodes are this instance's own and stay;
/// registered nodes follow the store, keeping their local poll status.
pub fn apply_records(fed: &mut FedState, records: &BTreeMap<String, String>) -> anyhow::Result<()> {
    let mut next = FedState::default();
    let mut registered = Vec::new();
    for (k, v) in records {
        let (kind, name) = k.split_once('/').unwrap_or((k.as_str(), ""));
        let bad = |e: serde_json::Error| anyhow::anyhow!("store key {k}: {e}");
        match kind {
            "volumes" => {
                next.volumes.insert(name.into(), serde_json::from_str(v).map_err(bad)?);
            }
            "snapshots" => {
                next.snapshots.insert(name.into(), serde_json::from_str(v).map_err(bad)?);
            }
            "group_snapshots" => {
                next.group_snapshots.insert(name.into(), serde_json::from_str(v).map_err(bad)?);
            }
            "backups" => {
                next.backups.insert(name.into(), serde_json::from_str(v).map_err(bad)?);
            }
            "nodes" => registered.push(serde_json::from_str::<NodeConfig>(v).map_err(bad)?),
            "orphans" => next.orphans = serde_json::from_str(v).map_err(bad)?,
            "stale_heads" => next.stale_heads = serde_json::from_str(v).map_err(bad)?,
            // leader, meta, and anything a newer version writes.
            _ => {}
        }
    }
    fed.volumes = next.volumes;
    fed.snapshots = next.snapshots;
    fed.group_snapshots = next.group_snapshots;
    fed.backups = next.backups;
    fed.orphans = next.orphans;
    fed.stale_heads = next.stale_heads;
    let names: std::collections::BTreeSet<String> = registered.iter().map(|c| c.name.clone()).collect();
    fed.nodes
        .retain(|n, node| node.status.source != NodeSource::Registered || names.contains(n));
    for nc in registered {
        match fed.nodes.get_mut(&nc.name) {
            Some(n) if n.status.source == NodeSource::Registered => n.config = nc,
            // This instance configures it itself.
            Some(_) => {}
            None => {
                fed.nodes.insert(
                    nc.name.clone(),
                    Node { config: nc, status: NodeStatus::new(NodeSource::Registered) },
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(String, String),
    Delete(String),
}

/// What turns `old` into `new`.
pub fn diff(old: &BTreeMap<String, String>, new: &BTreeMap<String, String>) -> Vec<Op> {
    let mut ops: Vec<Op> = new
        .iter()
        .filter(|(k, v)| old.get(*k) != Some(*v))
        .map(|(k, v)| Op::Put(k.clone(), v.clone()))
        .collect();
    ops.extend(old.keys().filter(|k| !new.contains_key(*k)).map(|k| Op::Delete(k.clone())));
    ops
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderInfo {
    pub instance: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    revision: u64,
    by: String,
    at: SystemTime,
}

// ------------------------------------------------------------- the store

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Not yet in touch with the store.
    #[default]
    Starting,
    Leader,
    Follower,
}

/// What `GET /api/v1/store` shows.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StoreStatus {
    pub role: Role,
    pub leader: Option<LeaderInfo>,
    /// This instance's lease while it leads.
    pub lease_id: Option<String>,
    /// `create_revision` of the leader key this instance holds: every
    /// write is guarded on it.
    pub leader_revision: Option<i64>,
    /// `mod_revision` of `meta` this instance's memory reflects.
    pub meta_revision: i64,
    pub last_load: Option<SystemTime>,
    pub last_write: Option<SystemTime>,
    pub writes: u64,
    pub loads: u64,
    pub last_error: Option<String>,
    #[serde(skip)]
    pub last_renewal: Option<Instant>,
}

pub struct Store {
    pub etcd: Etcd,
    pub prefix: String,
    pub instance: String,
    pub advertise_url: String,
    pub ttl: u64,
    status: std::sync::RwLock<StoreStatus>,
    /// The records as last written or loaded; also serializes writes.
    written: tokio::sync::Mutex<BTreeMap<String, String>>,
    forward: reqwest::Client,
}

impl Store {
    pub fn new(cfg: &StoreConfig, listen_addr: &str) -> anyhow::Result<Self> {
        let host = crate::registry::hostname().unwrap_or_else(|| "localhost".into());
        let port = listen_addr.rsplit(':').next().unwrap_or("9093");
        Ok(Self {
            etcd: Etcd::new(cfg)?,
            prefix: cfg.prefix.clone(),
            instance: cfg.instance.clone().unwrap_or_else(|| host.clone()),
            advertise_url: cfg
                .advertise_url
                .clone()
                .unwrap_or_else(|| format!("http://{host}:{port}"))
                .trim_end_matches('/')
                .to_string(),
            ttl: cfg.lease_ttl_secs,
            status: Default::default(),
            written: Default::default(),
            // Writes can take the engine's time (array create: minutes).
            forward: reqwest::Client::builder()
                .timeout(Duration::from_secs(900))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    fn key(&self, k: &str) -> String {
        format!("{}{k}", self.prefix)
    }

    pub fn status(&self) -> StoreStatus {
        self.status.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn update<R>(&self, f: impl FnOnce(&mut StoreStatus) -> R) -> R {
        f(&mut self.status.write().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn is_leader(&self) -> bool {
        self.status().role == Role::Leader
    }

    /// Where writes go: the leader's API, when another instance leads.
    pub fn leader_url(&self) -> Option<String> {
        self.status().leader.map(|l| l.url)
    }

    /// The guard on every write: the leader key is still the one this
    /// instance created.
    fn guard(&self, rev: i64) -> Value {
        json!({
            "key": b64(self.key("leader").as_bytes()),
            "target": "CREATE",
            "result": "EQUAL",
            "create_revision": rev.to_string(),
        })
    }

    fn put_op(&self, k: &str, v: &str, lease: Option<i64>) -> Value {
        let mut p = json!({ "key": b64(self.key(k).as_bytes()), "value": b64(v.as_bytes()) });
        if let Some(l) = lease {
            p["lease"] = json!(l.to_string());
        }
        json!({ "request_put": p })
    }

    fn delete_op(&self, k: &str) -> Value {
        json!({ "request_delete_range": { "key": b64(self.key(k).as_bytes()) } })
    }

    /// Write what changed in `fed` since the last write, if this instance
    /// leads. A failed guard means another instance leads now: this one
    /// steps down.
    pub async fn sync(&self, state: &AppState) -> anyhow::Result<()> {
        let Some(rev) = self.status().leader_revision.filter(|_| self.is_leader()) else {
            return Ok(());
        };
        let mut written = self.written.lock().await;
        let (records, revision) = {
            let fed = state.fed.read().await;
            (records_of(&fed), fed.revision)
        };
        let ops = diff(&written, &records);
        if ops.is_empty() {
            return Ok(());
        }
        let meta = serde_json::to_string(&Meta { revision, by: self.instance.clone(), at: SystemTime::now() })?;
        for chunk in ops.chunks(TXN_OPS) {
            let mut success: Vec<Value> = chunk
                .iter()
                .map(|op| match op {
                    Op::Put(k, v) => self.put_op(k, v, None),
                    Op::Delete(k) => self.delete_op(k),
                })
                .collect();
            success.push(self.put_op("meta", &meta, None));
            let r = self
                .etcd
                .txn(&json!({ "compare": [self.guard(rev)], "success": success }))
                .await;
            let r = match r {
                Ok(r) => r,
                Err(e) => {
                    self.update(|s| s.last_error = Some(format!("write: {e:#}")));
                    return Err(e);
                }
            };
            if !r["succeeded"].as_bool().unwrap_or(false) {
                drop(written);
                self.step_down(state, "the leader key is no longer this instance's: a write was refused").await;
                anyhow::bail!("not the leader any more");
            }
            for op in chunk {
                match op {
                    Op::Put(k, v) => written.insert(k.clone(), v.clone()),
                    Op::Delete(k) => written.remove(k),
                };
            }
            let meta_rev = num(&r["header"]["revision"]);
            self.update(|s| {
                s.meta_revision = meta_rev;
                s.writes += 1;
                s.last_write = Some(SystemTime::now());
                s.last_error = None;
            });
        }
        Ok(())
    }

    /// Replace memory with the store's records. Returns whether the store
    /// held any state.
    async fn load(&self, state: &AppState) -> anyhow::Result<bool> {
        let mut written = self.written.lock().await;
        let kvs = self.etcd.range_prefix(&self.prefix).await?;
        let mut records = BTreeMap::new();
        let mut meta: Option<(Meta, i64)> = None;
        for kv in kvs {
            let Some(k) = kv.key.strip_prefix(&self.prefix) else { continue };
            let v = String::from_utf8(kv.value)?;
            match k {
                "leader" => {}
                "meta" => meta = Some((serde_json::from_str(&v)?, kv.mod_revision)),
                _ => {
                    records.insert(k.to_string(), v);
                }
            }
        }
        let Some((meta, meta_rev)) = meta else {
            return Ok(false);
        };
        {
            let mut fed = state.fed.write().await;
            apply_records(&mut fed, &records)?;
            fed.revision = meta.revision;
        }
        *written = records;
        self.update(|s| {
            s.meta_revision = meta_rev;
            s.loads += 1;
            s.last_load = Some(SystemTime::now());
        });
        Ok(true)
    }

    async fn step_down(&self, state: &AppState, why: &str) {
        let was = self.update(|s| {
            let was = s.role == Role::Leader;
            s.role = Role::Follower;
            s.lease_id = None;
            s.leader_revision = None;
            s.leader = None;
            // Reload from the store on the next step.
            s.meta_revision = 0;
            s.last_error = Some(why.to_string());
            was
        });
        if was {
            tracing::warn!(instance = %self.instance, "stepped down as stormstorage leader: {why}");
            state.events.write().await.push(None, Severity::Warning, "store", format!("{} stepped down as leader: {why}", self.instance));
        }
    }

    /// One round: renew or campaign, then follow. Run every third of the
    /// lease's TTL by [`run`]; tests call it directly.
    pub async fn step(&self, state: &AppState) {
        if let Err(e) = self.try_step(state).await {
            tracing::warn!("store: {e:#}");
            self.update(|s| s.last_error = Some(format!("{e:#}")));
        }
    }

    async fn try_step(&self, state: &AppState) -> anyhow::Result<()> {
        let st = self.status();
        if st.role == Role::Leader {
            let id = st.lease_id.as_deref().and_then(|i| i.parse().ok()).unwrap_or(0);
            match self.etcd.lease_keepalive(id).await {
                Ok(ttl) if ttl > 0 => {
                    self.update(|s| s.last_renewal = Some(Instant::now()));
                }
                Ok(_) => self.step_down(state, "its lease expired").await,
                Err(e) => {
                    let since = st.last_renewal.map(|t| t.elapsed()).unwrap_or_default();
                    if since >= Duration::from_secs(self.ttl * 2 / 3) {
                        self.step_down(state, &format!("could not renew its lease for {}s: {e:#}", since.as_secs()))
                            .await;
                    }
                    return Err(e);
                }
            }
            return Ok(());
        }
        self.campaign(state).await?;
        if !self.is_leader() {
            self.follow(state).await?;
        }
        Ok(())
    }

    async fn campaign(&self, state: &AppState) -> anyhow::Result<()> {
        // Someone holds it: no lease to grant.
        if let Some(kv) = self.etcd.range(&self.key("leader")).await?.into_iter().next() {
            let holder: Option<LeaderInfo> = serde_json::from_slice(&kv.value).ok();
            self.update(|s| {
                s.role = Role::Follower;
                s.leader = holder;
            });
            return Ok(());
        }
        let lease = self.etcd.lease_grant(self.ttl).await?;
        let me = serde_json::to_string(&LeaderInfo { instance: self.instance.clone(), url: self.advertise_url.clone() })?;
        let r = self
            .etcd
            .txn(&json!({
                "compare": [self.guard(0)],
                "success": [self.put_op("leader", &me, Some(lease))],
                "failure": [{ "request_range": { "key": b64(self.key("leader").as_bytes()) } }],
            }))
            .await;
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                let _ = self.etcd.lease_revoke(lease).await;
                return Err(e);
            }
        };
        if !r["succeeded"].as_bool().unwrap_or(false) {
            let _ = self.etcd.lease_revoke(lease).await;
            let holder = kvs_of(&r["responses"][0]["response_range"])
                .ok()
                .and_then(|k| k.into_iter().next())
                .and_then(|kv| serde_json::from_slice(&kv.value).ok());
            self.update(|s| {
                s.role = Role::Follower;
                s.leader = holder;
            });
            return Ok(());
        }
        let created = num(&r["header"]["revision"]);
        // Take the store's state before acting on it; an empty store
        // takes ours.
        let loaded = match self.load(state).await {
            Ok(l) => l,
            Err(e) => {
                let _ = self.etcd.lease_revoke(lease).await;
                return Err(e.context("loading the store after winning the election"));
            }
        };
        self.update(|s| {
            s.role = Role::Leader;
            s.lease_id = Some(lease.to_string());
            s.leader_revision = Some(created);
            s.leader = Some(LeaderInfo { instance: self.instance.clone(), url: self.advertise_url.clone() });
            s.last_renewal = Some(Instant::now());
            s.last_error = None;
        });
        let how = if loaded { "loaded the store" } else { "imported its state into an empty store" };
        tracing::info!(instance = %self.instance, "stormstorage leader: {how}");
        state.events.write().await.push(
            None,
            Severity::Info,
            "store",
            format!("{} is the leader ({how})", self.instance),
        );
        if !loaded {
            // Written by the guarded sync below, `meta` included.
            self.written.lock().await.clear();
        }
        // An import writes every record (and `meta`), so followers load.
        self.sync(state).await?;
        state.persist_file().await;
        Ok(())
    }

    /// A follower reloads when `meta` moved.
    async fn follow(&self, state: &AppState) -> anyhow::Result<()> {
        let meta_rev = self
            .etcd
            .range(&self.key("meta"))
            .await?
            .into_iter()
            .next()
            .map(|kv| kv.mod_revision)
            .unwrap_or(0);
        if meta_rev != 0 && meta_rev != self.status().meta_revision {
            self.load(state).await?;
            state.persist_file().await;
        }
        Ok(())
    }
}

/// Keep the leadership round going.
pub async fn run(state: Arc<AppState>) {
    let Some(store) = state.store.clone() else { return };
    let every = Duration::from_millis(store.ttl * 1000 / 3);
    loop {
        store.step(&state).await;
        tokio::time::sleep(every).await;
    }
}

/// Middleware: on a follower, every API write goes to the leader, with
/// its headers (the token) and body; reads are answered here.
pub async fn forward_writes(State(s): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let Some(store) = s.store.clone() else {
        return next.run(req).await;
    };
    if matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS)
        || !req.uri().path().starts_with("/api/")
        || store.is_leader()
    {
        return next.run(req).await;
    }
    let unavailable = |code: &str, m: String| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": m, "code": code }))).into_response()
    };
    if req.headers().contains_key(FORWARDED) {
        return unavailable("not_leader", format!("{} does not lead any more; retry", store.instance));
    }
    let Some(leader) = store.leader_url() else {
        return unavailable("no_leader", "no stormstorage leader is elected (store unreachable?); retry".into());
    };
    let path = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_default();
    let (parts, body) = req.into_parts();
    let body = match axum::body::to_bytes(body, MAX_FORWARD).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response(),
    };
    let mut fwd = store.forward.request(parts.method.clone(), format!("{leader}{path}")).header(FORWARDED, &store.instance);
    for h in [axum::http::header::AUTHORIZATION, axum::http::header::CONTENT_TYPE, axum::http::header::ACCEPT] {
        if let Some(v) = parts.headers.get(&h) {
            fwd = fwd.header(h, v);
        }
    }
    match fwd.body(body).send().await {
        Ok(r) => {
            let status = r.status();
            let ctype = r.headers().get(axum::http::header::CONTENT_TYPE).cloned();
            let bytes = r.bytes().await.unwrap_or_default();
            let mut resp = Response::new(Body::from(bytes));
            *resp.status_mut() = status;
            if let Some(c) = ctype {
                resp.headers_mut().insert(axum::http::header::CONTENT_TYPE, c);
            }
            if let Ok(v) = HeaderValue::from_str(&leader) {
                resp.headers_mut().insert("x-stormstorage-leader", v);
            }
            resp
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": format!("forwarding to the leader {leader}: {e}"), "code": "leader_unreachable" })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_roundtrip() {
        for (raw, enc) in [("", ""), ("f", "Zg=="), ("fo", "Zm8="), ("foo", "Zm9v"), ("/registry/", "L3JlZ2lzdHJ5Lw==")] {
            assert_eq!(b64(raw.as_bytes()), enc);
            assert_eq!(unb64(enc).unwrap(), raw.as_bytes());
        }
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(unb64(&b64(&all)).unwrap(), all);
        assert!(unb64("a*b").is_err());
    }

    #[test]
    fn prefix_range_end() {
        assert_eq!(prefix_end("/stormstorage/"), b"/stormstorage0");
        assert_eq!(prefix_end("a\u{7f}"), b"b");
    }

    #[test]
    fn numbers_as_strings() {
        assert_eq!(num(&json!("42")), 42);
        assert_eq!(num(&json!(7)), 7);
        assert_eq!(num(&Value::Null), 0, "protojson leaves 0 out");
    }

    #[test]
    fn diff_puts_changes_and_deletes_gone() {
        let m = |kv: &[(&str, &str)]| kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let old = m(&[("volumes/a", "1"), ("volumes/b", "2"), ("orphans", "[]")]);
        let new = m(&[("volumes/a", "1"), ("volumes/b", "3"), ("volumes/c", "4"), ("orphans", "[]")]);
        let mut ops = diff(&old, &new);
        ops.sort_by_key(|o| format!("{o:?}"));
        assert_eq!(ops, vec![Op::Put("volumes/b".into(), "3".into()), Op::Put("volumes/c".into(), "4".into())]);
        assert_eq!(diff(&new, &old), vec![Op::Put("volumes/b".into(), "2".into()), Op::Delete("volumes/c".into())]);
        assert!(diff(&new, &new).is_empty());
    }

    fn nc(name: &str) -> NodeConfig {
        toml::from_str(&format!("name = \"{name}\"\nengine_url = \"http://{name}:9090\"")).unwrap()
    }

    #[test]
    fn records_roundtrip_keeps_own_nodes() {
        let mut fed = FedState::default();
        fed.apply_config_nodes(&[nc("static")]);
        fed.nodes.insert("reg".into(), Node { config: nc("reg"), status: NodeStatus::new(NodeSource::Registered) });
        let records = records_of(&fed);
        assert!(records.contains_key("nodes/reg"));
        assert!(!records.contains_key("nodes/static"), "static nodes are each instance's own");
        assert!(records.contains_key("orphans") && records.contains_key("stale_heads"));

        // A follower with its own static node and a registered node the
        // store no longer has.
        let mut other = FedState::default();
        other.apply_config_nodes(&[nc("mine")]);
        other.nodes.insert("gone".into(), Node { config: nc("gone"), status: NodeStatus::new(NodeSource::Registered) });
        apply_records(&mut other, &records).unwrap();
        let names: Vec<&str> = other.nodes.keys().map(String::as_str).collect();
        assert_eq!(names, vec!["mine", "reg"]);
        assert_eq!(records_of(&other), records);
    }

    #[test]
    fn chunks_stay_under_the_txn_limit() {
        let ops: Vec<Op> = (0..250).map(|i| Op::Delete(format!("volumes/v{i}"))).collect();
        let sizes: Vec<usize> = ops.chunks(TXN_OPS).map(|c| c.len() + 1).collect();
        assert_eq!(sizes, vec![101, 101, 51]);
        assert!(sizes.iter().all(|n| *n <= 128));
    }
}
