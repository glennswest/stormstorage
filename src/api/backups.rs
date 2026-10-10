//! The backup API (#46): policy, state and points, restore, purge.

use super::{ApiError, AppState};
use crate::backup::{drop_snapshot, now_ms};
use crate::events::Severity;
use crate::model::{AssemblyState, Backup, BackupPolicy, BackupState, DistVolume, Leg, LegState};
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::SystemTime;

async fn commit(s: &Arc<AppState>, subject: &str, sev: Severity, msg: String) {
    crate::replicate::push_to_peers(s.clone());
    s.persist().await;
    s.events.write().await.push(Some(subject.to_string()), sev, "backup", msg);
}

#[derive(Deserialize)]
pub(super) struct PolicyBody {
    #[serde(default)]
    pool: Option<String>,
    #[serde(default)]
    tier: Option<String>,
    #[serde(default)]
    node: Option<String>,
    #[serde(default)]
    interval_secs: Option<u64>,
    #[serde(default)]
    keep: Option<u32>,
}

/// PUT /api/v1/volumes/{name}/backup — start or change the volume's
/// backups. The first point is due at once. A changed destination is
/// refused once a backup node is chosen (purge first): its points are
/// there.
pub(super) async fn put_policy(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<PolicyBody>,
) -> Result<Json<Value>, ApiError> {
    let policy = BackupPolicy {
        pool: body.pool,
        tier: body.tier,
        node: body.node,
        interval_secs: body.interval_secs.unwrap_or(3600),
        keep: body.keep.unwrap_or(7),
    };
    if policy.interval_secs < 60 {
        return Err(ApiError::bad_request("interval_secs: at least 60"));
    }
    if policy.keep == 0 {
        return Err(ApiError::bad_request("keep: at least 1"));
    }
    if let Some(p) = &policy.pool {
        if s.config.pool(p).is_none() {
            return Err(ApiError::not_found(format!("pool {p:?}")));
        }
    }
    let view = {
        let mut fed = s.fed.write().await;
        if let Some(n) = &policy.node {
            if !fed.nodes.contains_key(n) {
                return Err(ApiError::not_found(format!("node {n:?}")));
            }
        }
        let vol = fed.volumes.get(&name).ok_or_else(|| ApiError::not_found(format!("volume {name:?}")))?;
        if vol.assembly == AssemblyState::Clone {
            return Err(ApiError::conflict(format!(
                "{name} is a clone on its parent's data; back up the parent"
            )));
        }
        let size = vol.size_bytes;
        let b = fed.backups.entry(name.clone()).or_insert_with(|| Backup {
            volume: name.clone(),
            size_bytes: size,
            ..Default::default()
        });
        if b.node.is_some() {
            if let Some(old) = &b.policy {
                if (old.pool.as_ref(), old.tier.as_ref(), old.node.as_ref())
                    != (policy.pool.as_ref(), policy.tier.as_ref(), policy.node.as_ref())
                {
                    return Err(ApiError::conflict(format!(
                        "{name}: its backups are on {}; purge them (DELETE …/backup?purge=true) to move them",
                        b.node.as_deref().unwrap_or("?")
                    )));
                }
            }
        }
        if b.policy.is_none() {
            b.next_at_ms = now_ms(SystemTime::now());
        }
        b.policy = Some(policy.clone());
        let v = serde_json::to_value(&*b).unwrap_or_default();
        fed.revision += 1;
        v
    };
    commit(
        &s,
        &name,
        Severity::Info,
        format!("{name}: backups every {} s, {} points kept", policy.interval_secs, policy.keep),
    )
    .await;
    Ok(Json(view))
}

pub(super) async fn get_backup(State(s): State<Arc<AppState>>, Path(name): Path<String>) -> Result<Json<Backup>, ApiError> {
    s.fed
        .read()
        .await
        .backups
        .get(&name)
        .cloned()
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("no backups of {name:?}")))
}

/// GET /api/v1/backups — every backup, including those of deleted volumes.
pub(super) async fn list_backups(State(s): State<Arc<AppState>>) -> Json<Value> {
    let fed = s.fed.read().await;
    Json(json!({ "backups": fed.backups.values().collect::<Vec<_>>() }))
}

#[derive(Deserialize)]
pub(super) struct StopQuery {
    #[serde(default)]
    purge: bool,
}

/// DELETE /api/v1/volumes/{name}/backup — stop; the points stay. With
/// `?purge=true`, also delete them, the backup volume and the base.
pub(super) async fn stop(
    State(s): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(q): Query<StopQuery>,
) -> Result<Json<Value>, ApiError> {
    let b = {
        let mut fed = s.fed.write().await;
        let Some(b) = fed.backups.get_mut(&name) else {
            return Ok(Json(json!({})));
        };
        b.policy = None;
        b.state = BackupState::Idle;
        b.message = None;
        let out = b.clone();
        fed.revision += 1;
        out
    };
    if !q.purge {
        commit(&s, &name, Severity::Info, format!("{name}: backups stopped; {} points kept", b.points.len())).await;
        return Ok(Json(json!({ "stopped": name, "points": b.points.len() })));
    }
    purge(&s, &b).await?;
    {
        let mut fed = s.fed.write().await;
        fed.backups.remove(&name);
        fed.revision += 1;
    }
    commit(&s, &name, Severity::Info, format!("{name}: backups purged ({} points)", b.points.len())).await;
    Ok(Json(json!({ "purged": name })))
}

/// Delete what a backup holds: its points and volume on the backup node,
/// and the base (and a job's snapshot) on the serving node.
async fn purge(s: &Arc<AppState>, b: &Backup) -> Result<(), ApiError> {
    let engine_of = |fed: &crate::model::FedState, node: &str| -> Result<crate::engine::Engine, ApiError> {
        let n = fed.nodes.get(node).ok_or_else(|| ApiError::conflict(format!("node {node:?} not in registry")))?;
        if !n.status.healthy {
            return Err(ApiError::conflict(format!("{node} is unreachable; purge when it answers")));
        }
        Ok(s.engine_for(n))
    };
    let (backup_engine, serving) = {
        let fed = s.fed.read().await;
        let be = match &b.node {
            Some(n) => Some(engine_of(&fed, n)?),
            None => None,
        };
        let serving: Vec<(crate::engine::Engine, crate::model::ShipSnapshot)> = b
            .base
            .iter()
            .chain(b.job.as_ref().map(|j| &j.snapshot))
            .filter_map(|x| fed.nodes.get(&x.node).filter(|n| n.status.healthy).map(|n| (s.engine_for(n), x.clone())))
            .collect();
        (be, serving)
    };
    for (e, snap) in serving {
        let _ = drop_snapshot(&e, &snap.engine_name, snap.engine_id.as_deref()).await;
    }
    if let Some(be) = backup_engine {
        for p in &b.points {
            drop_snapshot(&be, &p.name, p.engine_id.as_deref())
                .await
                .map_err(|e| ApiError::upstream(format!("point {}: {e:#}", p.name)))?;
        }
        if let Some(t) = &b.target_volume_id {
            if let Some(m) = &b.target_master {
                let _ = be.detach_volume(t, m).await;
            }
            be.delete_volume(t).await.map_err(|e| ApiError::upstream(format!("backup volume: {e:#}")))?;
        }
    }
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct RestoreBody {
    name: String,
}

/// POST /api/v1/volumes/{name}/backup/points/{point}/restore {name} — a new
/// volume holding that point: a copy-on-write clone of it on the backup
/// node, served from there as a single-leg volume. `{name}` in the path is
/// the backed-up volume, which may have been deleted.
pub(super) async fn restore(
    State(s): State<Arc<AppState>>,
    Path((volume, point)): Path<(String, String)>,
    Json(body): Json<RestoreBody>,
) -> Result<Json<Value>, ApiError> {
    let (b, engine) = {
        let fed = s.fed.read().await;
        let b = fed.backups.get(&volume).cloned().ok_or_else(|| ApiError::not_found(format!("no backups of {volume:?}")))?;
        if !b.points.iter().any(|p| p.name == point) {
            return Err(ApiError::not_found(format!("point {point:?} of {volume:?}")));
        }
        if body.name.is_empty() || body.name.ends_with(crate::orchestrate::MIRROR_SUFFIX) {
            return Err(ApiError::bad_request("a name, not ending in -mirror"));
        }
        if fed.volumes.contains_key(&body.name) || fed.snapshots.contains_key(&body.name) {
            return Err(ApiError::conflict(format!("{:?} exists", body.name)));
        }
        if fed.backups.get(&body.name).is_some_and(|x| x.source_deleted) {
            return Err(ApiError::conflict(format!("{:?} names a deleted volume's backups", body.name)));
        }
        let node = b.node.clone().ok_or_else(|| ApiError::conflict("no backup node"))?;
        let n = fed.nodes.get(&node).filter(|n| n.status.healthy).ok_or_else(|| {
            ApiError::conflict(format!("{node} holds the backups and is unreachable"))
        })?;
        (b, s.engine_for(n))
    };
    let node = b.node.clone().expect("checked");
    let master = b.target_master.clone().unwrap_or_else(|| node.clone());
    let r = engine
        .create_clone(&body.name, b.size_bytes, &master, &point)
        .await
        .map_err(|e| ApiError::upstream(format!("{node}: restore: {e:#}")))?;
    let id = r.get("id").and_then(|x| x.as_str()).ok_or_else(|| ApiError::upstream(format!("{node}: no id in {r}")))?;
    let rung = s.config.federation.rungs.first().cloned().unwrap_or_else(|| "node".into());
    let vol = DistVolume {
        name: body.name.clone(),
        size_bytes: b.size_bytes,
        pool: None,
        replicas: 1,
        rung,
        legs: vec![Leg {
            node: node.clone(),
            volume_id: Some(id.to_string()),
            state: LegState::Created,
            message: None,
            master_node: crate::engine::Engine::master_node_of(&r).or(Some(master)),
            export: None,
            drive_uuid: None,
            member_uuid: None,
            epoch: None,
        }],
        assembly: AssemblyState::SingleLeg,
        head: None,
        array_id: None,
        created_at: SystemTime::now(),
        replacing: None,
        next_releg_after: None,
        next_assemble_after: None,
        export: Default::default(),
        epoch: crate::model::first_epoch(),
        fenced: false,
        bandwidth_class: Default::default(),
        extent_size_bytes: None,
        rate_pending: false,
        dual_attach: None,
        migration: None,
        clone_of: None,
    };
    {
        let mut fed = s.fed.write().await;
        fed.volumes.insert(body.name.clone(), vol);
        fed.revision += 1;
    }
    commit(&s, &body.name, Severity::Info, format!("{}: restored from {volume}'s point {point} on {node}", body.name)).await;
    let _ = crate::orchestrate::publish(&s, &body.name).await;
    let fed = s.fed.read().await;
    let heads = s.heads.read().await;
    Ok(Json(fed.volumes.get(&body.name).map(|v| crate::head::view_in(&fed, &heads, v)).unwrap_or_default()))
}

/// DELETE /api/v1/volumes/{name}/backup/points/{point}.
pub(super) async fn delete_point(
    State(s): State<Arc<AppState>>,
    Path((volume, point)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let (p, engine) = {
        let fed = s.fed.read().await;
        let Some(b) = fed.backups.get(&volume) else { return Ok(Json(json!({}))) };
        let Some(p) = b.points.iter().find(|x| x.name == point).cloned() else { return Ok(Json(json!({}))) };
        let node = b.node.clone().unwrap_or_default();
        let n = fed.nodes.get(&node).filter(|n| n.status.healthy).ok_or_else(|| {
            ApiError::conflict(format!("{node} holds the backups and is unreachable"))
        })?;
        (p, s.engine_for(n))
    };
    drop_snapshot(&engine, &p.name, p.engine_id.as_deref())
        .await
        .map_err(|e| ApiError::upstream(format!("{e:#}")))?;
    {
        let mut fed = s.fed.write().await;
        if let Some(b) = fed.backups.get_mut(&volume) {
            b.points.retain(|x| x.name != point);
        }
        fed.revision += 1;
    }
    commit(&s, &volume, Severity::Info, format!("{volume}: backup point {point} deleted")).await;
    Ok(Json(json!({})))
}

/// Before a volume with backups is deleted: stop them, keep the points,
/// and drop the base and a job's snapshot on the serving node (they sit on
/// the volume's array, which goes next). Fails when one cannot be dropped.
pub(super) async fn on_volume_delete(s: &Arc<AppState>, name: &str) -> Result<(), ApiError> {
    let (b, serving) = {
        let fed = s.fed.read().await;
        let Some(b) = fed.backups.get(name).cloned() else { return Ok(()) };
        let serving: Vec<(Option<crate::engine::Engine>, crate::model::ShipSnapshot)> = b
            .base
            .iter()
            .chain(b.job.as_ref().map(|j| &j.snapshot))
            .map(|x| (fed.nodes.get(&x.node).filter(|n| n.status.healthy).map(|n| s.engine_for(n)), x.clone()))
            .collect();
        (b, serving)
    };
    for (e, snap) in serving {
        let e = e.ok_or_else(|| ApiError::conflict(format!("{} holds {name}'s backup base and is unreachable", snap.node)))?;
        drop_snapshot(&e, &snap.engine_name, snap.engine_id.as_deref())
            .await
            .map_err(|e| ApiError::upstream(format!("{name}: backup base {}: {e:#}", snap.engine_name)))?;
    }
    {
        let mut fed = s.fed.write().await;
        if let Some(r) = fed.backups.get_mut(name) {
            r.policy = None;
            r.base = None;
            r.job = None;
            r.state = BackupState::Idle;
            r.source_deleted = true;
            r.message = Some("the volume was deleted; its points are kept until purged".into());
        }
        fed.revision += 1;
    }
    s.events.write().await.push(
        Some(name.to_string()),
        Severity::Info,
        "backup",
        format!("{name}: deleted; its {} backup points are kept (restore or purge them)", b.points.len()),
    );
    Ok(())
}
