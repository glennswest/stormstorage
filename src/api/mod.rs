//! REST API on :9093, the embedded UI, the stormd card, and the
//! stormblock-compatible self-registration endpoints.

pub mod auth;

use crate::config::{Config, NodeConfig, PoolConfig};
use crate::engine::Engine;
use crate::events::{EventLog, Severity};
use crate::model::{AssemblyState, DistVolume, FedState, Leg, LegState, Node, NodeSource, NodeStatus};
use crate::placement::{self, Candidate};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use crate::inventory::NodeInventory;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

const INDEX_HTML: &str = include_str!("../ui/index.html");

pub struct AppState {
    pub config: Config,
    pub fed: RwLock<FedState>,
    pub events: RwLock<EventLog>,
    pub state_path: Option<PathBuf>,
    /// Each node's slabs and volumes as last observed (#9). Not persisted.
    pub inventory: RwLock<BTreeMap<String, NodeInventory>>,
    /// Volumes with a leg replacement in flight in this process (#1).
    pub replacing: std::sync::Mutex<std::collections::BTreeSet<String>>,
    /// Engines refusing our token, backed off (#38).
    pub refusals: crate::refusal::Refusals,
    /// Every stormblock PV/PVC the apiserver holds, last read (#28).
    pub kube: RwLock<crate::kube::KubeView>,
    /// Each assembled volume's array as last read on its head (#33). Not
    /// persisted, not replicated.
    pub heads: RwLock<BTreeMap<String, crate::head::ArrayReading>>,
    /// Each volume's last *live* reading of its head, kept while the head
    /// does not answer: the bar a reading from superblocks must meet (#48).
    pub head_live: RwLock<BTreeMap<String, crate::head::ArrayReading>>,
    /// Conditions already reported once (keys), so a poll does not repeat
    /// the same error event.
    pub noted: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

impl AppState {
    pub fn new(config: Config, fed: FedState, state_path: Option<PathBuf>) -> Self {
        Self {
            config,
            fed: RwLock::new(fed),
            events: RwLock::new(EventLog::new(4096)),
            state_path,
            inventory: RwLock::new(BTreeMap::new()),
            replacing: std::sync::Mutex::new(Default::default()),
            refusals: Default::default(),
            kube: Default::default(),
            heads: Default::default(),
            head_live: Default::default(),
            noted: Default::default(),
        }
    }
}

impl AppState {
    pub async fn persist(&self) {
        let Some(path) = &self.state_path else {
            return;
        };
        let fed = self.fed.read().await;
        if let Err(e) = fed.save(path) {
            tracing::error!("state persist failed: {e:#}");
        }
    }

    pub(crate) fn engine_for(&self, node: &Node) -> Engine {
        Engine::new(&node.config.engine_url, self.engine_token(&node.config))
            .with_admin(self.admin_credential(&node.config.engine_url))
    }

    /// What to present for the engine's destructive verbs (#47,
    /// stormblock#274): its admin token (`$STORMBLOCK_ADMIN_TOKEN`, or
    /// `[local] admin_token_file` for this machine's engine), else the
    /// `[kubernetes]` bearer, which the engine reviews with a
    /// SubjectAccessReview against `storage-admin`.
    pub fn admin_credential(&self, engine_url: &str) -> Option<String> {
        self.config
            .local
            .admin_token_for(engine_url)
            .or_else(|| self.config.kubernetes.token())
    }

    /// The token to present to a node's engine: its own when the config
    /// gives one, else the configured engine token (`$STORMBLOCK_API_TOKEN`,
    /// `[local] token_file`), read now so a re-minted file is picked up.
    /// Nodes stormblock registered and nodes adopted carry none (#38).
    pub fn engine_token(&self, node: &NodeConfig) -> Option<String> {
        self.engine_token_source(node).0
    }

    /// [`Self::engine_token`] and where it came from (#12): the node's
    /// `api_token`, its `token_file`, then the `[local]` rule — the shared
    /// token for any engine, the minted one only for this machine's.
    pub fn engine_token_source(&self, node: &NodeConfig) -> (Option<String>, String) {
        if let Some(t) = node.api_token.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            return (Some(t.to_string()), format!("{}: api_token", node.name));
        }
        if let Some(f) = node.token_file.as_deref() {
            let found = crate::config::token_search(None, &[f.to_string()]);
            if found.0.is_some() {
                return found;
            }
        }
        self.config.local.token_for(&node.engine_url)
    }
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn not_found(m: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, code: "not_found", message: m.into() }
    }
    fn bad_request(m: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, code: "bad_request", message: m.into() }
    }
    fn conflict(m: impl Into<String>) -> Self {
        Self { status: StatusCode::CONFLICT, code: "conflict", message: m.into() }
    }
    fn upstream(m: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_GATEWAY, code: "engine", message: m.into() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message, "code": self.code }))).into_response()
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(ui_index))
        .route("/ui", get(ui_index))
        .route("/ui/", get(ui_index))
        .route("/api/v1/health", get(health))
        .route("/api/v1/components", get(components_feed))
        .route("/ws/components", get(ws_components))
        .route("/api/v1/replicate", post(receive_replication))
        .route("/api/v1/replication/status", get(replication_status))
        .route("/api/v1/nodes", get(list_nodes))
        .route("/api/v1/nodes/{name}/inventory", get(node_inventory))
        .route("/api/v1/topology", get(topology))
        .route("/api/v1/pools", get(list_pools))
        .route("/api/v1/pools/{name}/rebalance", get(pool_rebalance))
        .route("/api/v1/placement/plan", post(plan_dry_run))
        .route("/api/v1/volumes", get(list_volumes).post(create_volume))
        .route("/api/v1/volumes/{name}", get(get_volume).delete(delete_volume))
        .route("/api/v1/volumes/{name}/move", post(move_volume_leg))
        .route("/api/v1/volumes/{name}/migrate", post(migrate_volume).delete(cancel_migration))
        .route("/api/v1/volumes/{name}/export", post(export_volume))
        .route("/api/v1/volumes/{name}/export/hosts", post(serve_export_host))
        .route("/api/v1/volumes/{name}/export/hosts/{host_nqn}", delete(withdraw_export_host))
        .route("/api/v1/volumes/{name}/assemble", post(assemble_volume))
        .route("/api/v1/volumes/{name}/replicas", get(volume_replicas))
        .route("/api/v1/volumes/{name}/fence", post(fence_volume))
        .route("/api/v1/volumes/{name}/promote", post(promote_volume))
        .route("/api/v1/volumes/{name}/prestage", post(prestage_volume))
        .route("/api/v1/volumes/{name}/bandwidth-class", put(put_bandwidth_class))
        .route("/api/v1/volumes/{name}/dual-attach", post(open_dual_attach))
        .route("/api/v1/volumes/{name}/dual-attach/close", post(close_dual_attach))
        .route("/api/v1/stale-heads", get(list_stale_heads))
        .route("/api/v1/orphans", get(list_orphans))
        .route("/api/v1/events", get(list_events))
        .route("/api/v1/summary", get(summary))
        .route("/api/v1/storage/register", post(register_node))
        .route("/api/v1/storage/deregister", post(deregister_node))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth::require_token))
        .with_state(state)
}

async fn ui_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "version": crate::VERSION }))
}

async fn components_feed(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let feed = crate::components::collect(&s).await;
    Json(serde_json::to_value(feed).unwrap_or_default())
}

/// Full-snapshot pushes, stormd-style: every 2 s, send when changed.
async fn ws_components(
    ws: axum::extract::ws::WebSocketUpgrade,
    State(s): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |mut sock| async move {
        let mut last = String::new();
        loop {
            let feed = crate::components::collect(&s).await;
            let json = serde_json::to_string(&feed).unwrap_or_default();
            if json != last {
                if sock
                    .send(axum::extract::ws::Message::Text(json.clone().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                last = json;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    })
}

async fn receive_replication(
    State(s): State<Arc<AppState>>,
    Json(payload): Json<crate::replicate::Payload>,
) -> Json<serde_json::Value> {
    let incoming = payload.revision;
    let applied = crate::replicate::apply(&s, payload).await;
    let local = s.fed.read().await.revision;
    Json(json!({ "applied": applied, "incoming": incoming, "revision": local }))
}

async fn replication_status(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    Json(json!({
        "revision": fed.revision,
        "peers": s.config.replication.peers,
    }))
}

async fn list_nodes(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    let nodes: Vec<serde_json::Value> = fed
        .nodes
        .values()
        .map(|n| {
            json!({
                "name": n.config.name,
                "engine_url": n.config.engine_url,
                "tier": n.config.tier,
                "labels": n.labels(),
                "status": n.status,
            })
        })
        .collect();
    Json(json!({ "nodes": nodes }))
}

async fn topology(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    let rungs = &s.config.federation.rungs;
    let nodes: Vec<serde_json::Value> = fed
        .nodes
        .values()
        .map(|n| {
            let labels = n.labels();
            let chain: Vec<serde_json::Value> = rungs
                .iter()
                .filter_map(|r| labels.get(r).map(|v| json!({ "rung": r, "value": v })))
                .collect();
            json!({
                "name": n.config.name,
                "chain": chain,
                "tier": n.config.tier,
                "healthy": n.status.healthy,
            })
        })
        .collect();
    Json(json!({ "rungs": rungs, "nodes": nodes }))
}

fn pool_rollup(pool: &PoolConfig, fed: &FedState) -> serde_json::Value {
    let mut total = 0u64;
    let mut free = 0u64;
    let mut matched = 0u32;
    let mut healthy = 0u32;
    let mut names = Vec::new();
    for n in fed.nodes.values() {
        if pool.selector.matches(&n.config) {
            matched += 1;
            names.push(n.config.name.clone());
            if n.status.healthy {
                healthy += 1;
                total += n.status.total_bytes;
                free += n.status.free_bytes;
            }
        }
    }
    json!({
        "name": pool.name,
        "kind": "policy",
        "replicas": pool.replicas,
        "rung": pool.rung,
        "nodes": names,
        "matched": matched,
        "healthy": healthy,
        "total_bytes": total,
        "free_bytes": free,
    })
}

/// Policy pools from the config, then every node's slabs (a node's own
/// pools), then the slabs summed per tier across nodes (#9).
/// What rebalancing would do in a pool now (#30): the moves it would
/// start and, if none, why. A dry run: nothing is moved.
async fn pool_rebalance(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let pool = s
        .config
        .pools
        .iter()
        .find(|p| p.name == name)
        .cloned()
        .ok_or_else(|| ApiError::not_found(format!("pool {name:?}")))?;
    let fed = s.fed.read().await;
    let p = crate::rebalance::plan(
        &fed,
        std::slice::from_ref(&pool),
        &s.config.federation.rungs,
        s.config.placement.io_weight,
        &|n| s.replacing.lock().expect("replacing lock").contains(n),
        SystemTime::now(),
    );
    Ok(Json(json!({
        "pool": name,
        "enabled": pool.high_watermark.is_some() && pool.low_watermark.is_some(),
        "high_watermark": pool.high_watermark,
        "low_watermark": pool.low_watermark,
        "max_moves": pool.max_moves,
        "moves": p.moves,
        "held": p.held.into_iter().map(|h| h.why).collect::<Vec<_>>(),
    })))
}

async fn list_pools(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    let mut pools: Vec<serde_json::Value> =
        s.config.pools.iter().map(|p| pool_rollup(p, &fed)).collect();
    drop(fed);
    let inv = s.inventory.read().await;
    for (node, ni) in inv.iter() {
        for slab in &ni.slabs {
            pools.push(json!({
                "name": format!("{node}/{}", slab.id),
                "kind": "slab",
                "node": node,
                "slab": slab.id,
                "tier": slab.tier,
                "role": slab.role,
                "domain": slab.domain,
                "drive": slab.drive,
                "total_bytes": slab.total_bytes,
                "free_bytes": slab.free_bytes,
                "allocated_bytes": slab.allocated_bytes(),
                "volumes": crate::inventory::volumes_on(ni, &slab.id).len(),
            }));
        }
    }
    for t in crate::inventory::tiers(&inv) {
        pools.push(json!({
            "name": format!("tier:{}", t.tier),
            "kind": "tier",
            "tier": t.tier,
            "nodes": t.nodes,
            "slabs": t.slabs,
            "total_bytes": t.total_bytes,
            "free_bytes": t.free_bytes,
            "allocated_bytes": t.allocated_bytes,
            "volumes": t.volumes,
        }));
    }
    Json(json!({ "pools": pools }))
}

/// A node's slabs and its engine volumes, each with the slab(s) it is on.
async fn node_inventory(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !s.fed.read().await.nodes.contains_key(&name) {
        return Err(ApiError::not_found(format!("node {name:?}")));
    }
    let inv = s.inventory.read().await;
    let ni = inv.get(&name).cloned().unwrap_or_default();
    Ok(Json(serde_json::to_value(ni).unwrap_or_default()))
}

#[derive(Deserialize)]
struct PlanRequest {
    #[serde(default)]
    pool: Option<String>,
    size_bytes: u64,
    #[serde(default)]
    replicas: Option<u32>,
    #[serde(default)]
    rung: Option<String>,
    #[serde(default)]
    tier: Option<String>,
    /// Soft: put the first leg — the head — on this node when it fits (#50).
    #[serde(default)]
    prefer_node: Option<String>,
}

struct ResolvedPlan {
    replicas: u32,
    rung: String,
    candidates: Vec<Candidate>,
}

async fn resolve_plan(s: &AppState, req: &PlanRequest) -> Result<ResolvedPlan, ApiError> {
    let pool = match &req.pool {
        Some(name) => Some(
            s.config
                .pool(name)
                .ok_or_else(|| ApiError::not_found(format!("pool {name:?}")))?,
        ),
        None => None,
    };
    let replicas = req
        .replicas
        .or(pool.map(|p| p.replicas))
        .unwrap_or(1)
        .max(1);
    let rung = req
        .rung
        .clone()
        .or_else(|| pool.map(|p| p.rung.clone()))
        .unwrap_or_else(|| "node".into());
    if !s.config.federation.rungs.contains(&rung) {
        return Err(ApiError::bad_request(format!(
            "rung {rung:?} not in federation.rungs"
        )));
    }
    let fed = s.fed.read().await;
    let candidates: Vec<Candidate> = fed
        .nodes
        .values()
        .filter(|n| n.status.healthy)
        .filter(|n| pool.map(|p| p.selector.matches(&n.config)).unwrap_or(true))
        .filter(|n| match &req.tier {
            Some(t) => n.config.tier.as_deref() == Some(t.as_str()),
            None => true,
        })
        .map(|n| Candidate {
            name: n.config.name.clone(),
            labels: n.labels(),
            free_bytes: n.status.free_bytes,
            total_bytes: n.status.total_bytes,
            io_busy: n.status.io.map(|i| i.busy),
        })
        .collect();
    Ok(ResolvedPlan {
        replicas,
        rung,
        candidates,
    })
}

async fn plan_dry_run(
    State(s): State<Arc<AppState>>,
    Json(req): Json<PlanRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let rp = resolve_plan(&s, &req).await?;
    let (picks, honored) = placement::plan_preferring(
        &rp.candidates,
        &s.config.federation.rungs,
        &rp.rung,
        rp.replicas,
        req.size_bytes,
        s.config.placement.io_weight,
        req.prefer_node.as_deref(),
    )
    .map_err(ApiError::conflict)?;
    Ok(Json(json!({
        "replicas": rp.replicas, "rung": rp.rung, "legs": picks,
        "prefer_node_honored": req.prefer_node.as_ref().map(|_| honored),
    })))
}

#[derive(Deserialize)]
struct CreateVolumeRequest {
    name: String,
    size_bytes: u64,
    #[serde(default)]
    pool: Option<String>,
    #[serde(default)]
    replicas: Option<u32>,
    #[serde(default)]
    rung: Option<String>,
    #[serde(default)]
    tier: Option<String>,
    /// Resync rate class for later legs (#33).
    #[serde(default)]
    bandwidth_class: Option<crate::model::BandwidthClass>,
    /// Soft: put the first leg — the head, or the only leg — on this node
    /// when it fits the pool and has room (#50, WaitForFirstConsumer).
    #[serde(default)]
    prefer_node: Option<String>,
    /// Consumer hosts to serve it to (#51/#53), each from its own
    /// subsystem; none = the shared subsystem, as before.
    #[serde(default)]
    hosts: Vec<HostRequest>,
    /// Extent size of every leg, bytes (#59, stormblock#156): a
    /// StorageClass's `extentSize`. Absent = each node chooses.
    #[serde(default)]
    extent_size_bytes: Option<u64>,
}

/// A consumer host to serve a volume to (#51).
#[derive(Deserialize)]
struct HostRequest {
    host_nqn: String,
    /// Ask the engine for a DH-HMAC-CHAP secret for this host.
    #[serde(default)]
    dhchap: bool,
}

fn host_records(hosts: &[HostRequest]) -> Result<Vec<crate::model::HostServe>, ApiError> {
    let mut out: Vec<crate::model::HostServe> = Vec::new();
    for h in hosts {
        let nqn = h.host_nqn.trim();
        if !crate::orchestrate::valid_host_nqn(nqn) {
            return Err(ApiError::bad_request(format!("{nqn:?} is not a host NQN (nqn.…, at most 223 bytes)")));
        }
        match out.iter_mut().find(|x| x.host_nqn == nqn) {
            Some(x) => x.dhchap |= h.dhchap,
            None => out.push(crate::model::HostServe { host_nqn: nqn.to_string(), dhchap: h.dhchap, ..Default::default() }),
        }
    }
    Ok(out)
}

async fn create_volume(
    State(s): State<Arc<AppState>>,
    Json(req): Json<CreateVolumeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if req.name.is_empty() || req.size_bytes == 0 {
        return Err(ApiError::bad_request("name and size_bytes required"));
    }
    let hosts = host_records(&req.hosts)?;
    if let Some(e) = req.extent_size_bytes {
        if !crate::engine::Engine::valid_extent_size(e) {
            return Err(ApiError::bad_request(format!(
                "extent_size_bytes {e}: a power of two of 4096 or more"
            )));
        }
    }
    if req.name.ends_with(crate::orchestrate::MIRROR_SUFFIX) {
        // The head names the consumer volume `<name>-mirror`; a leg of that
        // name would be answered as the consumer volume (name-idempotent).
        return Err(ApiError::bad_request(format!(
            "names ending in {:?} are reserved for served mirrors",
            crate::orchestrate::MIRROR_SUFFIX
        )));
    }
    {
        let fed = s.fed.read().await;
        if fed.volumes.contains_key(&req.name) {
            return Err(ApiError::conflict(format!("volume {:?} exists", req.name)));
        }
    }
    let plan_req = PlanRequest {
        pool: req.pool.clone(),
        size_bytes: req.size_bytes,
        replicas: req.replicas,
        rung: req.rung.clone(),
        tier: req.tier.clone(),
        prefer_node: req.prefer_node.clone(),
    };
    let rp = resolve_plan(&s, &plan_req).await?;
    let (picks, honored) = placement::plan_preferring(
        &rp.candidates,
        &s.config.federation.rungs,
        &rp.rung,
        rp.replicas,
        req.size_bytes,
        s.config.placement.io_weight,
        req.prefer_node.as_deref(),
    )
    .map_err(ApiError::conflict)?;

    // Create one ordinary volume per leg node via /v1 (name-idempotent).
    let mut legs: Vec<Leg> = Vec::new();
    let mut failure: Option<String> = None;
    for node_name in &picks {
        let engine = {
            let fed = s.fed.read().await;
            let node = fed
                .nodes
                .get(node_name)
                .ok_or_else(|| ApiError::not_found(format!("node {node_name:?}")))?;
            s.engine_for(node)
        };
        match engine.create_volume(&req.name, req.size_bytes, req.extent_size_bytes).await {
            Ok(v) => legs.push(Leg {
                node: node_name.clone(),
                volume_id: v.get("id").and_then(|i| i.as_str()).map(|x| x.to_string()),
                state: LegState::Created,
                message: None,
                master_node: crate::engine::Engine::master_node_of(&v),
                export: None,
                drive_uuid: None,
                member_uuid: None,
                epoch: None,
            }),
            Err(e) => {
                failure = Some(format!("{node_name}: {e:#}"));
                break;
            }
        }
    }
    if let Some(why) = failure {
        // Roll back what was created — a half-placed volume is worse than
        // a failed request.
        for leg in &legs {
            if let Some(id) = &leg.volume_id {
                let engine = {
                    let fed = s.fed.read().await;
                    fed.nodes.get(&leg.node).map(|n| s.engine_for(n))
                };
                if let Some(engine) = engine {
                    let _ = engine.delete_volume(id).await;
                }
            }
        }
        return Err(ApiError::upstream(format!("leg create failed: {why}")));
    }

    let assembly = if legs.len() <= 1 {
        AssemblyState::SingleLeg
    } else {
        AssemblyState::PendingEngineSupport
    };
    let multi_leg = legs.len() > 1;
    let vol = DistVolume {
        name: req.name.clone(),
        size_bytes: req.size_bytes,
        pool: req.pool.clone(),
        replicas: rp.replicas,
        rung: rp.rung.clone(),
        legs,
        assembly,
        head: None,
        array_id: None,
        created_at: SystemTime::now(),
        replacing: None,
        next_releg_after: None,
        next_assemble_after: None,
        export: crate::model::Export { per_host: !hosts.is_empty(), hosts, ..Default::default() },
        epoch: 1,
        fenced: false,
        bandwidth_class: req.bandwidth_class.unwrap_or_default(),
        extent_size_bytes: req.extent_size_bytes,
        rate_pending: false,
        dual_attach: None,
        migration: None,
    };
    {
        let mut fed = s.fed.write().await;
        fed.volumes.insert(req.name.clone(), vol);
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(s.clone());
    s.events.write().await.push(
        Some(req.name.clone()),
        Severity::Info,
        "volume",
        format!(
            "{}: created, {} leg(s) across rung {:?} on [{}]{}",
            req.name,
            rp.replicas,
            rp.rung,
            picks.join(", "),
            match (&req.prefer_node, honored) {
                (Some(p), false) => format!(" — preferred node {p} not used (unhealthy, outside the pool or tier, or no room)"),
                _ => String::new(),
            }
        ),
    );
    s.persist().await;

    // Wire the legs into a mirror. Failure leaves the volume stored with
    // assembly pending (partial export progress persisted) plus an error
    // event — the legs and their data are never rolled back for this.
    let assembled = if multi_leg {
        match crate::orchestrate::assemble(&s, &req.name).await {
            Ok(()) => true,
            Err(e) => {
                s.events.write().await.push(
                    Some(req.name.clone()),
                    Severity::Error,
                    "assemble",
                    format!(
                        "{}: assembly failed, legs kept; retried automatically once every leg's node is healthy \
                         (after {}s), or POST /api/v1/volumes/{}/assemble: {e:#}",
                        req.name, s.config.recovery.cooldown_secs, req.name
                    ),
                );
                false
            }
        }
    } else {
        true
    };
    // Serve it (#2). A failure is recorded on the volume and in the events;
    // `POST /api/v1/volumes/{name}/export` retries.
    if assembled {
        let _ = crate::orchestrate::publish(&s, &req.name).await;
    }
    let mut response = {
        let fed = s.fed.read().await;
        serde_json::to_value(fed.volumes.get(&req.name)).unwrap_or_default()
    };
    if req.prefer_node.is_some() {
        if let Some(o) = response.as_object_mut() {
            o.insert("prefer_node_honored".into(), json!(honored));
        }
    }
    Ok(Json(response))
}

async fn list_volumes(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    let heads = s.heads.read().await;
    let volumes: Vec<serde_json::Value> = fed
        .volumes
        .values()
        .map(|v| crate::head::view(v, heads.get(&v.name)))
        .collect();
    Json(json!({ "volumes": volumes }))
}

async fn get_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let fed = s.fed.read().await;
    let v = fed
        .volumes
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("volume {name:?}")))?;
    Ok(Json(crate::head::view(v, s.heads.read().await.get(&name))))
}

/// A refused replication call, in /v1's error envelope (`{code, message,
/// current_epoch?}`) plus this API's `error`.
fn refusal(r: crate::head::Refusal) -> Response {
    use crate::head::Refusal::*;
    let (status, code, current) = match &r {
        NotFound(_) => (StatusCode::NOT_FOUND, "not_found", None),
        StaleEpoch(c) => (StatusCode::PRECONDITION_FAILED, "stale_epoch", Some(*c)),
        Conflict(_) => (StatusCode::CONFLICT, "conflict", None),
        Upstream(_) => (StatusCode::BAD_GATEWAY, "engine", None),
    };
    let msg = r.to_string();
    let mut body = json!({ "error": msg, "message": msg, "code": code });
    if let Some(c) = current {
        body["current_epoch"] = json!(c);
    }
    (status, Json(body)).into_response()
}

/// The volume in /v1's replica shape (#33): what stormblock-csi reads.
async fn volume_replicas(State(s): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
    let fed = s.fed.read().await;
    let Some(v) = fed.volumes.get(&name) else {
        return refusal(crate::head::Refusal::NotFound(format!("volume {name:?}")));
    };
    let heads = s.heads.read().await;
    let reading = heads.get(&name);
    let reps = crate::head::replicas(v, reading);
    Json(json!({
        "id": v.name,
        "name": v.name,
        "size_bytes": v.size_bytes,
        "epoch": v.epoch,
        "fenced": v.fenced,
        "health": crate::head::health(v, &reps),
        "replicas": reps,
        "bandwidth_class": v.bandwidth_class,
        "extent_size_bytes": v.extent_size_bytes,
        "head": v.head,
        "dual_attach": v.dual_attach,
        "sync_read_at": reading.map(|r| r.read_at),
        "sync_source": reading.map(|r| r.source),
        "head_read_at": reading.and_then(|r| r.head_read_at),
        "rebuild_bytes_per_sec": reading.and_then(|r| r.rebuild_bytes_per_sec),
    }))
    .into_response()
}

#[derive(Deserialize)]
struct FenceBody {
    expected_epoch: u64,
}

async fn fence_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(b): Json<FenceBody>,
) -> Response {
    match crate::head::fence(&s, &name, b.expected_epoch).await {
        Ok(f) => Json(json!({
            "epoch": f.epoch,
            "legs_fenced": f.legs.iter().map(|(n, e)| json!({"node": n, "epoch": e})).collect::<Vec<_>>(),
            "legs_not_fenced": f.not_fenced.iter().map(|(n, e)| json!({"node": n, "error": e})).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(r) => refusal(r),
    }
}

#[derive(Deserialize)]
struct PromoteBody {
    target_node: String,
    fenced_epoch: u64,
}

async fn promote_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(b): Json<PromoteBody>,
) -> Response {
    match crate::head::promote(&s, &name, &b.target_node, b.fenced_epoch).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => refusal(r),
    }
}

#[derive(Deserialize, Default)]
struct PrestageBody {
    /// Where the new slave goes; placement picks when absent.
    #[serde(default)]
    node: Option<String>,
    /// The slave it replaces; the lost one, or the only one, when absent.
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    bandwidth_class: Option<crate::model::BandwidthClass>,
}

async fn prestage_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    body: Option<Json<PrestageBody>>,
) -> Response {
    let b = body.map(|b| b.0).unwrap_or_default();
    match crate::head::prestage(&s, &name, b.node, b.from, b.bandwidth_class).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => refusal(r),
    }
}

#[derive(Deserialize)]
struct BandwidthClassBody {
    bandwidth_class: crate::model::BandwidthClass,
}

/// Change a volume's bandwidth class after create (#60).
async fn put_bandwidth_class(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(b): Json<BandwidthClassBody>,
) -> Response {
    match crate::head::set_bandwidth_class(&s, &name, b.bandwidth_class).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => refusal(r),
    }
}

#[derive(Deserialize)]
struct DualAttachBody {
    target_node: String,
    ttl_secs: u64,
}

async fn open_dual_attach(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(b): Json<DualAttachBody>,
) -> Response {
    match crate::head::open_window(&s, &name, &b.target_node, b.ttl_secs).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => refusal(r),
    }
}

#[derive(Deserialize)]
struct CloseBody {
    epoch: u64,
    outcome: crate::head::Outcome,
}

async fn close_dual_attach(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(b): Json<CloseBody>,
) -> Response {
    match crate::head::close_window(&s, &name, b.epoch, b.outcome).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => refusal(r),
    }
}

async fn list_stale_heads(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    Json(json!({ "stale_heads": fed.stale_heads }))
}

async fn delete_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let vol = {
        let fed = s.fed.read().await;
        fed.volumes
            .get(&name)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("volume {name:?}")))?
    };
    // Stop serving first (#2): consumers lose the volume before any leg
    // does, and the head's array cannot go while a volume is pinned to it.
    if let Err(e) = crate::orchestrate::revoke(&s, &vol).await {
        return Err(ApiError::upstream(format!(
            "export not revoked: {e:#} — volume record kept"
        )));
    }
    // Unwire the mirror (array, head drives, exports) — best-effort;
    // a half-torn assembly must not block deleting the legs.
    let mut errors = crate::orchestrate::teardown(&s, &vol).await;
    if !errors.is_empty() {
        s.events.write().await.push(
            Some(name.clone()),
            Severity::Warning,
            "volume",
            format!("{name}: teardown issues: {}", errors.join("; ")),
        );
        errors.clear();
    }
    // Every leg, and the new leg of a replacement in flight. A leg whose
    // node is unreachable cannot be deleted now: it is recorded as an
    // orphan and reaped when the node answers (#1), rather than blocking
    // the delete forever.
    let mut orphans = Vec::new();
    for leg in vol.legs.iter().chain(vol.replacing.as_ref().map(|r| &r.leg)) {
        let Some(id) = &leg.volume_id else { continue };
        let engine = {
            let fed = s.fed.read().await;
            fed.nodes
                .get(&leg.node)
                .map(|n| (s.engine_for(n), n.status.healthy))
        };
        match engine {
            Some((_, false)) => orphans.push(crate::model::Orphan {
                node: leg.node.clone(),
                volume_id: id.clone(),
                master_node: leg.master_node.clone(),
                of_volume: name.clone(),
                reason: "volume deleted while its node was unreachable".into(),
                since: SystemTime::now(),
            }),
            Some((engine, true)) => {
                if let Err(e) = engine.delete_volume(id).await {
                    errors.push(format!("{}: {e:#}", leg.node));
                }
            }
            None => errors.push(format!("{}: node no longer known", leg.node)),
        }
    }
    if !errors.is_empty() {
        return Err(ApiError::upstream(format!(
            "legs not deleted: {} — volume record kept",
            errors.join("; ")
        )));
    }
    let orphaned = orphans.len();
    {
        let mut fed = s.fed.write().await;
        fed.volumes.remove(&name);
        fed.orphans.extend(orphans);
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(s.clone());
    s.events.write().await.push(
        Some(name.clone()),
        Severity::Info,
        "volume",
        if orphaned == 0 {
            format!("{name}: deleted ({} legs)", vol.legs.len())
        } else {
            format!(
                "{name}: deleted ({} legs; {orphaned} on unreachable nodes left to reap)",
                vol.legs.len()
            )
        },
    );
    s.persist().await;
    Ok(Json(json!({ "deleted": name })))
}

#[derive(Deserialize)]
struct MoveBody {
    /// Node whose leg leaves.
    from: String,
    /// Target node; omitted = placement picks (distinct domain, emptiest).
    #[serde(default)]
    to: Option<String>,
}

/// Move one leg of an assembled volume: new leg + rebuild in the
/// foreground of the mirror, old leg retired by a background task once
/// the new member reports active. Progress lands in the event feed.
async fn move_volume_leg(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<MoveBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let target = crate::orchestrate::move_leg(&s, &name, &body.from, body.to)
        .await
        .map_err(|e| ApiError::conflict(format!("{e:#}")))?;
    Ok(Json(json!({ "moving": body.from, "to": target, "status": "rebuilding" })))
}

#[derive(Deserialize, Default)]
struct ExportBody {
    /// Consumer hosts to serve it to as well (#53); added to the recorded
    /// ones. Secrets are returned by `…/export/hosts`, not here.
    #[serde(default)]
    hosts: Vec<HostRequest>,
    /// Its served volume is gone (#40): serve a new, EMPTY one.
    #[serde(default)]
    recreate: bool,
}

#[derive(Deserialize)]
struct MigrateBody {
    pool: String,
}

fn migrate_refusal(r: crate::migrate::Refusal) -> ApiError {
    match r {
        crate::migrate::Refusal::NotFound(m) => ApiError::not_found(m),
        crate::migrate::Refusal::Conflict(m) => ApiError::conflict(m),
    }
}

/// Move every leg of the volume into another pool (#32). Recorded here; the
/// reconciler moves one leg at a time. Returns the migration.
async fn migrate_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<MigrateBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let m = crate::migrate::start(&s, &name, &body.pool).await.map_err(migrate_refusal)?;
    Ok(Json(serde_json::to_value(m).unwrap_or_default()))
}

/// Stop a tier migration (#32); legs already moved stay.
async fn cancel_migration(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let had = crate::migrate::cancel(&s, &name).await.map_err(migrate_refusal)?;
    Ok(Json(json!({ "cancelled": had })))
}

/// Publish the volume to consumers, or republish it and report whether the
/// coordinates changed (#2). Returns the export record. An optional body
/// `{hosts: [{host_nqn, dhchap?}]}` adds consumer hosts first (#53);
/// `{recreate: true}` replaces a served volume that is gone with a new,
/// empty one (#40) — never done without being asked.
async fn export_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let body: ExportBody = if body.iter().all(u8::is_ascii_whitespace) {
        ExportBody::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::bad_request(format!("body: {e}")))?
    };
    let add = host_records(&body.hosts)?;
    {
        let mut fed = s.fed.write().await;
        let v = fed
            .volumes
            .get_mut(&name)
            .ok_or_else(|| ApiError::not_found(format!("volume {name:?}")))?;
        v.export.per_host |= !add.is_empty();
        for h in add {
            match v.export.hosts.iter_mut().find(|x| x.host_nqn == h.host_nqn) {
                Some(x) => x.dhchap |= h.dhchap,
                None => v.export.hosts.push(h),
            }
        }
    }
    let ex = if body.recreate {
        crate::orchestrate::recreate_export(&s, &name).await
    } else {
        crate::orchestrate::publish(&s, &name).await
    }
    .map_err(|e| ApiError::conflict(format!("{e:#}")))?;
    Ok(Json(serde_json::to_value(ex).unwrap_or_default()))
}

/// Serve a volume to one consumer host (#51): that host's coordinates (its
/// own subsystem on the serving engine) and, with `dhchap`, its secret.
/// The secret is in this answer only. Idempotent.
async fn serve_export_host(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<HostRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !s.fed.read().await.volumes.contains_key(&name) {
        return Err(ApiError::not_found(format!("volume {name:?}")));
    }
    if !crate::orchestrate::valid_host_nqn(&req.host_nqn) {
        return Err(ApiError::bad_request(format!(
            "{:?} is not a host NQN (nqn.…, at most 223 bytes)",
            req.host_nqn
        )));
    }
    let (rec, secret) = crate::orchestrate::serve_host(&s, &name, &req.host_nqn, req.dhchap)
        .await
        .map_err(|e| ApiError::conflict(format!("{e:#}")))?;
    let mut out = serde_json::to_value(&rec).unwrap_or_default();
    if let Some(sec) = secret {
        out["dhchap_secret"] = json!(sec);
    }
    Ok(Json(out))
}

/// Stop serving a volume to one consumer host (#51). `withdrawn`: `done`,
/// or `pending` when the serving engine does not answer (withdrawn there
/// when it does), or `nothing_served`.
async fn withdraw_export_host(
    State(s): State<Arc<AppState>>,
    Path((name, host_nqn)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !s.fed.read().await.volumes.contains_key(&name) {
        return Err(ApiError::not_found(format!("volume {name:?}")));
    }
    let w = crate::orchestrate::withdraw_host(&s, &name, &host_nqn)
        .await
        .map_err(|e| ApiError::conflict(format!("{e:#}")))?;
    Ok(Json(json!({ "host_nqn": host_nqn.trim(), "withdrawn": w })))
}

/// Retry a pending volume's assembly (#7), then serve it. Resumes from
/// what the failed attempt got done; an array the head already built over
/// these legs is adopted, not built twice.
async fn assemble_volume(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    {
        let fed = s.fed.read().await;
        let v = fed
            .volumes
            .get(&name)
            .ok_or_else(|| ApiError::not_found(format!("volume {name:?}")))?;
        if v.assembly != AssemblyState::PendingEngineSupport {
            return Err(ApiError::conflict(format!(
                "{name}: not pending (assembly {:?}) — nothing to assemble",
                v.assembly
            )));
        }
        if v.legs.len() < 2 {
            return Err(ApiError::conflict(format!("{name}: single leg — nothing to assemble")));
        }
    }
    let r = crate::orchestrate::assemble(&s, &name).await;
    if let Err(e) = &r {
        let msg = format!("{e:#}");
        if msg.contains("busy") {
            return Err(ApiError::conflict(msg));
        }
        s.events.write().await.push(
            Some(name.clone()),
            Severity::Error,
            "assemble",
            format!("{name}: assembly retry failed: {msg}"),
        );
        return Err(ApiError::upstream(msg));
    }
    let _ = crate::orchestrate::publish(&s, &name).await;
    let fed = s.fed.read().await;
    Ok(Json(serde_json::to_value(fed.volumes.get(&name)).unwrap_or_default()))
}

/// stormblock's own registration heartbeat shape (stormblock
/// src/stormfs.rs VolumeAnnouncement) — implemented verbatim so a node
/// with `[stormfs] metadata_url` pointed here enrolls with zero engine
/// changes.
#[derive(Deserialize)]
struct Announce {
    node_addr: String,
    hostname: String,
    #[serde(default)]
    volumes: Vec<serde_json::Value>,
}

pub(crate) fn engine_url_from(node_addr: &str) -> String {
    if node_addr.contains("://") {
        node_addr.trim_end_matches('/').to_string()
    } else if node_addr.contains(':') {
        format!("http://{node_addr}")
    } else {
        format!("http://{node_addr}:9090")
    }
}

async fn register_node(
    State(s): State<Arc<AppState>>,
    Json(a): Json<Announce>,
) -> Json<serde_json::Value> {
    let url = engine_url_from(&a.node_addr);
    let mut fed = s.fed.write().await;
    let is_new = !fed.nodes.contains_key(&a.hostname);
    let node = fed.nodes.entry(a.hostname.clone()).or_insert_with(|| Node {
        config: NodeConfig {
            name: a.hostname.clone(),
            engine_url: url.clone(),
            api_token: None,
            token_file: None,
            labels: Default::default(),
            tier: None,
        },
        status: NodeStatus::new(NodeSource::Registered),
    });
    if node.status.source == NodeSource::Registered {
        node.config.engine_url = url;
    }
    node.status.volumes = a.volumes.len() as u64;
    node.status.last_ok = Some(SystemTime::now());
    node.status.healthy = true;
    node.status.consecutive_failures = 0;
    if is_new {
        fed.revision += 1;
    }
    drop(fed);
    if is_new {
        crate::replicate::push_to_peers(s.clone());
        s.events.write().await.push(
            Some(a.hostname.clone()),
            Severity::Info,
            "register",
            format!("{}: self-registered from {}", a.hostname, a.node_addr),
        );
        s.persist().await;
    }
    Json(json!({ "accepted": true, "message": "registered with stormstorage" }))
}

#[derive(Deserialize)]
struct Deregister {
    node_addr: String,
}

async fn deregister_node(
    State(s): State<Arc<AppState>>,
    Json(d): Json<Deregister>,
) -> Json<serde_json::Value> {
    let url = engine_url_from(&d.node_addr);
    let mut fed = s.fed.write().await;
    let mut name = None;
    for n in fed.nodes.values_mut() {
        if n.config.engine_url == url {
            n.status.healthy = false;
            name = Some(n.config.name.clone());
        }
    }
    drop(fed);
    if let Some(name) = name {
        s.events.write().await.push(
            Some(name.clone()),
            Severity::Warning,
            "register",
            format!("{name}: deregistered (clean shutdown)"),
        );
        s.persist().await;
    }
    Json(json!({ "accepted": true }))
}

/// Leg volumes left on unreachable nodes, waiting to be reaped (#1).
async fn list_orphans(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    Json(json!({ "orphans": fed.orphans }))
}

#[derive(Deserialize)]
struct SinceQuery {
    #[serde(default)]
    since: u64,
}

async fn list_events(
    State(s): State<Arc<AppState>>,
    Query(q): Query<SinceQuery>,
) -> Json<serde_json::Value> {
    let log = s.events.read().await;
    Json(json!({ "latest_seq": log.latest_seq(), "events": log.since(q.since) }))
}

async fn summary(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    let total_nodes = fed.nodes.len();
    let healthy = fed.nodes.values().filter(|n| n.status.healthy).count();
    let free: u64 = fed
        .nodes
        .values()
        .filter(|n| n.status.healthy)
        .map(|n| n.status.free_bytes)
        .sum();
    let cap: u64 = fed
        .nodes
        .values()
        .filter(|n| n.status.healthy)
        .map(|n| n.status.total_bytes)
        .sum();
    let volumes = fed.volumes.len();
    let (engine_volumes, slabs) = {
        let inv = s.inventory.read().await;
        (
            inv.values().map(|i| i.volumes.len()).sum::<usize>(),
            inv.values().map(|i| i.slabs.len()).sum::<usize>(),
        )
    };
    let pending = fed
        .volumes
        .values()
        .filter(|v| v.assembly == AssemblyState::PendingEngineSupport)
        .count();
    let degraded = fed
        .volumes
        .values()
        .filter(|v| v.assembly == AssemblyState::Degraded)
        .count();
    let health = if total_nodes == 0 {
        "idle"
    } else if healthy < total_nodes {
        if healthy == 0 {
            "error"
        } else {
            "warn"
        }
    } else if degraded > 0 {
        "warn"
    } else {
        "ok"
    };
    let detail = format!(
        "{healthy}/{total_nodes} nodes, {slabs} slab pools, {engine_volumes} node volumes, \
         {volumes} distributed ({pending} pending assembly, {degraded} degraded), {} free",
        human(free)
    );
    Json(json!({
        "health": health,
        "detail": detail,
        "metrics": [
            { "label": "Nodes", "value": format!("{healthy}/{total_nodes}"),
              "tone": if healthy < total_nodes { "warn" } else { "ok" } },
            { "label": "Pools", "value": slabs.to_string() },
            { "label": "Volumes", "value": (engine_volumes).to_string(), "tone": "accent" },
            { "label": "Distributed", "value": volumes.to_string() },
            { "label": "Free", "value": human(free) },
            { "label": "Capacity", "value": human(cap), "tone": "muted" },
        ]
    }))
}

fn human(b: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < units.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{:.1} {}", v, units[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_url_derivation() {
        assert_eq!(engine_url_from("10.0.0.1:9090"), "http://10.0.0.1:9090");
        assert_eq!(engine_url_from("10.0.0.1"), "http://10.0.0.1:9090");
        assert_eq!(
            engine_url_from("http://x:9090/"),
            "http://x:9090"
        );
    }

    #[test]
    fn human_sizes() {
        assert_eq!(human(0), "0 B");
        assert_eq!(human(1024), "1.0 KiB");
        assert_eq!(human(10 * 1024 * 1024 * 1024), "10.0 GiB");
    }
}
