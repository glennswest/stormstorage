//! REST API on :9093, the embedded UI, the stormd card, and the
//! stormblock-compatible self-registration endpoints.

use crate::config::{Config, NodeConfig, PoolConfig};
use crate::engine::Engine;
use crate::events::{EventLog, Severity};
use crate::model::{AssemblyState, DistVolume, FedState, Leg, LegState, Node, NodeSource, NodeStatus};
use crate::placement::{self, Candidate};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
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

    fn engine_for(&self, node: &Node) -> Engine {
        Engine::new(&node.config.engine_url, node.config.api_token.clone())
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
        .route("/api/v1/nodes", get(list_nodes))
        .route("/api/v1/topology", get(topology))
        .route("/api/v1/pools", get(list_pools))
        .route("/api/v1/placement/plan", post(plan_dry_run))
        .route("/api/v1/volumes", get(list_volumes).post(create_volume))
        .route("/api/v1/volumes/{name}", get(get_volume).delete(delete_volume))
        .route("/api/v1/events", get(list_events))
        .route("/api/v1/summary", get(summary))
        .route("/api/v1/storage/register", post(register_node))
        .route("/api/v1/storage/deregister", post(deregister_node))
        .with_state(state)
}

async fn ui_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "version": crate::VERSION }))
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
        "replicas": pool.replicas,
        "rung": pool.rung,
        "nodes": names,
        "matched": matched,
        "healthy": healthy,
        "total_bytes": total,
        "free_bytes": free,
    })
}

async fn list_pools(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    let pools: Vec<serde_json::Value> =
        s.config.pools.iter().map(|p| pool_rollup(p, &fed)).collect();
    Json(json!({ "pools": pools }))
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
    let picks = placement::plan(
        &rp.candidates,
        &s.config.federation.rungs,
        &rp.rung,
        rp.replicas,
        req.size_bytes,
    )
    .map_err(ApiError::conflict)?;
    Ok(Json(json!({ "replicas": rp.replicas, "rung": rp.rung, "legs": picks })))
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
}

async fn create_volume(
    State(s): State<Arc<AppState>>,
    Json(req): Json<CreateVolumeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if req.name.is_empty() || req.size_bytes == 0 {
        return Err(ApiError::bad_request("name and size_bytes required"));
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
    };
    let rp = resolve_plan(&s, &plan_req).await?;
    let picks = placement::plan(
        &rp.candidates,
        &s.config.federation.rungs,
        &rp.rung,
        rp.replicas,
        req.size_bytes,
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
        match engine.create_volume(&req.name, req.size_bytes).await {
            Ok(v) => legs.push(Leg {
                node: node_name.clone(),
                volume_id: v.get("id").and_then(|i| i.as_str()).map(|x| x.to_string()),
                state: LegState::Created,
                message: None,
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
    let vol = DistVolume {
        name: req.name.clone(),
        size_bytes: req.size_bytes,
        pool: req.pool.clone(),
        replicas: rp.replicas,
        rung: rp.rung.clone(),
        legs,
        assembly,
        created_at: SystemTime::now(),
    };
    let response = serde_json::to_value(&vol).unwrap_or_default();
    s.fed.write().await.volumes.insert(req.name.clone(), vol);
    s.events.write().await.push(
        Some(req.name.clone()),
        Severity::Info,
        "volume",
        format!(
            "{}: created, {} leg(s) across rung {:?} on [{}]",
            req.name,
            rp.replicas,
            rp.rung,
            picks.join(", ")
        ),
    );
    s.persist().await;
    Ok(Json(response))
}

async fn list_volumes(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let fed = s.fed.read().await;
    let volumes: Vec<&DistVolume> = fed.volumes.values().collect();
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
    Ok(Json(serde_json::to_value(v).unwrap_or_default()))
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
    let mut errors = Vec::new();
    for leg in &vol.legs {
        let Some(id) = &leg.volume_id else { continue };
        let engine = {
            let fed = s.fed.read().await;
            fed.nodes.get(&leg.node).map(|n| s.engine_for(n))
        };
        match engine {
            Some(engine) => {
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
    s.fed.write().await.volumes.remove(&name);
    s.events.write().await.push(
        Some(name.clone()),
        Severity::Info,
        "volume",
        format!("{name}: deleted ({} legs)", vol.legs.len()),
    );
    s.persist().await;
    Ok(Json(json!({ "deleted": name })))
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

fn engine_url_from(node_addr: &str) -> String {
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
    drop(fed);
    if is_new {
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
    let pending = fed
        .volumes
        .values()
        .filter(|v| v.assembly == AssemblyState::PendingEngineSupport)
        .count();
    let health = if total_nodes == 0 {
        "idle"
    } else if healthy < total_nodes {
        if healthy == 0 {
            "error"
        } else {
            "warn"
        }
    } else {
        "ok"
    };
    let detail = format!(
        "{healthy}/{total_nodes} nodes, {volumes} volumes ({pending} pending assembly), {} free",
        human(free)
    );
    Json(json!({
        "health": health,
        "detail": detail,
        "metrics": [
            { "label": "Nodes", "value": format!("{healthy}/{total_nodes}"),
              "tone": if healthy < total_nodes { "warn" } else { "ok" } },
            { "label": "Volumes", "value": volumes.to_string(), "tone": "accent" },
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
