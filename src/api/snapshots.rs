//! Snapshots, group snapshots and copy-on-write clones (#49) — what
//! stormblock-csi's controller asks of stormstorage for CreateSnapshot,
//! the group-snapshot service and CloneVolume/restore.
//!
//! Owner (2026-10-09): clones are copy-on-write on the source's array (full
//! copies onto new legs wait on stormblock#295); group snapshots only for
//! volumes served from one node. Everything here runs on the node that
//! serves the volume's data — the head's array, or a single leg — through
//! that engine's `/v1` snapshots, group snapshots and create-with-source,
//! so a snapshot is one point in time across the mirror.

use super::{ApiError, AppState};
use crate::events::Severity;
use crate::model::{
    AssemblyState, CloneOf, DistVolume, Export, GroupSnapshot, HostServe, Snapshot, VolumeSource,
};
use crate::orchestrate::{data_at, MIRROR_SUFFIX};
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Volumes and snapshots share one namespace: each is an engine volume of
/// that name on the node that holds it, beside the legs named after their
/// volumes.
fn name_free(fed: &crate::model::FedState, name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::bad_request("name required"));
    }
    if name.ends_with(MIRROR_SUFFIX) {
        return Err(ApiError::bad_request(format!(
            "names ending in {MIRROR_SUFFIX:?} are reserved for served mirrors"
        )));
    }
    if fed.volumes.contains_key(name) {
        return Err(ApiError::conflict(format!("{name:?} is a volume")));
    }
    if fed.snapshots.contains_key(name) {
        return Err(ApiError::conflict(format!("{name:?} is a snapshot")));
    }
    Ok(())
}

async fn healthy(s: &AppState, node: &str) -> Result<(), ApiError> {
    let ok = s.fed.read().await.nodes.get(node).map(|n| n.status.healthy).unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(ApiError::conflict(format!("{node} is unreachable — it holds the data; try again when it answers")))
    }
}

async fn commit(s: &Arc<AppState>, subject: &str, msg: String) {
    crate::replicate::push_to_peers(s.clone());
    s.persist().await;
    s.events.write().await.push(Some(subject.to_string()), Severity::Info, "snapshot", msg);
}

#[derive(Deserialize)]
pub(super) struct CreateSnapshot {
    name: String,
    volume_id: String,
}

/// POST /api/v1/snapshots {name, volume_id} — idempotent on `name` for the
/// same volume (409 for another).
pub(super) async fn create_snapshot(
    State(s): State<Arc<AppState>>,
    Json(req): Json<CreateSnapshot>,
) -> Result<Json<Snapshot>, ApiError> {
    let at = {
        let fed = s.fed.read().await;
        if let Some(existing) = fed.snapshots.get(&req.name) {
            if existing.source_volume_id == req.volume_id {
                return Ok(Json(existing.clone()));
            }
            return Err(ApiError::conflict(format!(
                "snapshot {:?} exists for volume {:?}",
                req.name, existing.source_volume_id
            )));
        }
        name_free(&fed, &req.name)?;
        let vol = fed
            .volumes
            .get(&req.volume_id)
            .ok_or_else(|| ApiError::not_found(format!("volume {:?}", req.volume_id)))?;
        data_at(&s, &fed, vol).map_err(|e| ApiError::conflict(format!("{e:#}")))?
    };
    healthy(&s, &at.node).await?;
    let size = s.fed.read().await.volumes.get(&req.volume_id).map(|v| v.size_bytes).unwrap_or(0);
    let r = at
        .engine
        .create_snapshot(&req.name, &at.engine_name)
        .await
        .map_err(|e| ApiError::upstream(format!("{}: snapshot: {e:#}", at.node)))?;
    let snap = Snapshot {
        id: req.name.clone(),
        name: req.name.clone(),
        source_volume_id: req.volume_id.clone(),
        size_bytes: r.get("size_bytes").and_then(|x| x.as_u64()).unwrap_or(size),
        ready: r.get("ready").and_then(|x| x.as_bool()).unwrap_or(true),
        created_at_ms: r.get("created_at_ms").and_then(|x| x.as_i64()).unwrap_or_else(now_ms),
        group_snapshot_id: None,
        parent: at.parent.clone(),
        node: at.node.clone(),
        engine_name: req.name.clone(),
        engine_id: r.get("id").and_then(|x| x.as_str()).map(str::to_string),
    };
    {
        let mut fed = s.fed.write().await;
        fed.snapshots.insert(snap.name.clone(), snap.clone());
        fed.revision += 1;
    }
    commit(
        &s,
        &req.volume_id,
        format!("{}: snapshot {} taken on {}", req.volume_id, snap.name, at.node),
    )
    .await;
    Ok(Json(snap))
}

#[derive(Deserialize)]
pub(super) struct SnapshotFilter {
    name: Option<String>,
    source_volume: Option<String>,
}

/// GET /api/v1/snapshots[?name=][&source_volume=] — a bare array, as /v1.
pub(super) async fn list_snapshots(
    State(s): State<Arc<AppState>>,
    Query(q): Query<SnapshotFilter>,
) -> Json<Vec<Snapshot>> {
    let fed = s.fed.read().await;
    Json(
        fed.snapshots
            .values()
            .filter(|x| q.name.as_deref().map(|n| x.name == n).unwrap_or(true))
            .filter(|x| q.source_volume.as_deref().map(|v| x.source_volume_id == v).unwrap_or(true))
            .cloned()
            .collect(),
    )
}

pub(super) async fn get_snapshot(
    State(s): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Snapshot>, ApiError> {
    s.fed
        .read()
        .await
        .snapshots
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("snapshot {id:?}")))
}

/// Where a snapshot's data is now: the node that serves its parent (moved
/// by a promote, it is found by name there).
fn snapshot_home(
    s: &Arc<AppState>,
    fed: &crate::model::FedState,
    snap: &Snapshot,
) -> Result<crate::orchestrate::DataAt, ApiError> {
    let parent = fed
        .volumes
        .get(&snap.parent)
        .ok_or_else(|| ApiError::conflict(format!("{}: its volume {} is gone", snap.name, snap.parent)))?;
    data_at(s, fed, parent).map_err(|e| ApiError::conflict(format!("{e:#}")))
}

/// Delete engine volumes by name on `at` (after a promote, no /v1 record).
async fn delete_by_name(at: &crate::orchestrate::DataAt, names: &[String]) -> Result<(), ApiError> {
    for n in names {
        match at.engine.local_volume_id(n).await {
            Ok(id) => at
                .engine
                .delete_any_volume(&id)
                .await
                .map_err(|e| ApiError::upstream(format!("{}: delete {n}: {e:#}", at.node)))?,
            Err(e) if crate::engine::is_not_found(&e) => {}
            Err(e) => return Err(ApiError::upstream(format!("{}: {e:#}", at.node))),
        }
    }
    Ok(())
}

/// DELETE /api/v1/snapshots/{id}. A group's member goes with its group.
/// Clones made from it keep their data (copy-on-write).
pub(super) async fn delete_snapshot(
    State(s): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let (snap, at) = {
        let fed = s.fed.read().await;
        let Some(snap) = fed.snapshots.get(&id).cloned() else {
            return Ok(Json(json!({})));
        };
        if let Some(g) = &snap.group_snapshot_id {
            return Err(ApiError::conflict(format!(
                "snapshot {id:?} belongs to group snapshot {g:?}; delete the group"
            )));
        }
        let at = snapshot_home(&s, &fed, &snap)?;
        (snap, at)
    };
    healthy(&s, &at.node).await?;
    match (&snap.engine_id, at.node == snap.node) {
        (Some(eid), true) => at
            .engine
            .delete_v1(&format!("/v1/snapshots/{eid}"))
            .await
            .map_err(|e| ApiError::upstream(format!("{}: {e:#}", at.node)))?,
        _ => delete_by_name(&at, std::slice::from_ref(&snap.engine_name)).await?,
    }
    {
        let mut fed = s.fed.write().await;
        fed.snapshots.remove(&id);
        fed.revision += 1;
    }
    commit(&s, &snap.source_volume_id, format!("{}: snapshot {id} deleted", snap.source_volume_id)).await;
    Ok(Json(json!({})))
}

#[derive(Deserialize)]
pub(super) struct CreateGroup {
    name: String,
    volume_ids: Vec<String>,
}

/// /v1's `GroupSnapshot` shape: the members in full.
fn group_view(fed: &crate::model::FedState, g: &GroupSnapshot) -> Value {
    let snaps: Vec<&Snapshot> = g.members.iter().filter_map(|m| fed.snapshots.get(m)).collect();
    json!({
        "id": g.id, "name": g.name, "snapshots": snaps, "ready": g.ready,
        "created_at_ms": g.created_at_ms, "node": g.node,
    })
}

/// POST /api/v1/group-snapshots {name, volume_ids} — one consistency point
/// across volumes served from one node (owner, #49); volumes on different
/// nodes are refused with where each is. Idempotent on `name` for the same
/// volumes.
pub(super) async fn create_group(
    State(s): State<Arc<AppState>>,
    Json(req): Json<CreateGroup>,
) -> Result<Json<Value>, ApiError> {
    if req.volume_ids.is_empty() {
        return Err(ApiError::bad_request("volume_ids required"));
    }
    let mut ids = req.volume_ids.clone();
    ids.sort();
    ids.dedup();
    if ids.len() != req.volume_ids.len() {
        return Err(ApiError::bad_request("volume_ids repeats a volume"));
    }
    let member = |v: &str| format!("{}-{v}", req.name);
    let ats = {
        let fed = s.fed.read().await;
        if let Some(g) = fed.group_snapshots.get(&req.name) {
            let mut same: Vec<String> = req.volume_ids.iter().map(|v| member(v)).collect();
            same.sort();
            let mut have = g.members.clone();
            have.sort();
            if same == have {
                return Ok(Json(group_view(&fed, g)));
            }
            return Err(ApiError::conflict(format!("group snapshot {:?} exists for other volumes", req.name)));
        }
        let mut ats = Vec::new();
        for v in &req.volume_ids {
            name_free(&fed, &member(v))?;
            let vol = fed.volumes.get(v).ok_or_else(|| ApiError::not_found(format!("volume {v:?}")))?;
            ats.push(data_at(&s, &fed, vol).map_err(|e| ApiError::conflict(format!("{e:#}")))?);
        }
        ats
    };
    let node = ats[0].node.clone();
    if ats.iter().any(|a| a.node != node) {
        let wh: Vec<String> = req.volume_ids.iter().zip(&ats).map(|(v, a)| format!("{v} on {}", a.node)).collect();
        return Err(ApiError::conflict(format!(
            "a group snapshot is one point in time on one node; these are served from different nodes: {}",
            wh.join(", ")
        )));
    }
    healthy(&s, &node).await?;
    let engine_names: Vec<String> = ats.iter().map(|a| a.engine_name.clone()).collect();
    let r = ats[0]
        .engine
        .create_group_snapshot(&req.name, &engine_names)
        .await
        .map_err(|e| ApiError::upstream(format!("{node}: group snapshot: {e:#}")))?;
    let created = r.get("created_at_ms").and_then(|x| x.as_i64()).unwrap_or_else(now_ms);
    let engine_snaps = r.get("snapshots").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let sizes: Vec<u64> = {
        let fed = s.fed.read().await;
        req.volume_ids.iter().map(|v| fed.volumes.get(v).map(|x| x.size_bytes).unwrap_or(0)).collect()
    };
    let mut members = Vec::new();
    for (i, (v, a)) in req.volume_ids.iter().zip(&ats).enumerate() {
        let en = engine_snaps.get(i);
        members.push(Snapshot {
            id: member(v),
            name: member(v),
            source_volume_id: v.clone(),
            size_bytes: en.and_then(|x| x.get("size_bytes")).and_then(|x| x.as_u64()).unwrap_or(sizes[i]),
            ready: en.and_then(|x| x.get("ready")).and_then(|x| x.as_bool()).unwrap_or(true),
            created_at_ms: created,
            group_snapshot_id: Some(req.name.clone()),
            parent: a.parent.clone(),
            node: node.clone(),
            // The engine names each member `<group>-<volume id it was given>`.
            engine_name: format!("{}-{}", req.name, a.engine_name),
            engine_id: en.and_then(|x| x.get("id")).and_then(|x| x.as_str()).map(str::to_string),
        });
    }
    let g = GroupSnapshot {
        id: req.name.clone(),
        name: req.name.clone(),
        members: members.iter().map(|m| m.name.clone()).collect(),
        ready: members.iter().all(|m| m.ready),
        created_at_ms: created,
        node: node.clone(),
        engine_id: r.get("id").and_then(|x| x.as_str()).map(str::to_string),
    };
    let view = {
        let mut fed = s.fed.write().await;
        for m in members {
            fed.snapshots.insert(m.name.clone(), m);
        }
        fed.group_snapshots.insert(g.name.clone(), g.clone());
        fed.revision += 1;
        group_view(&fed, &g)
    };
    commit(
        &s,
        &req.name,
        format!("group snapshot {} of {} taken on {node}", req.name, req.volume_ids.join(", ")),
    )
    .await;
    Ok(Json(view))
}

#[derive(Deserialize)]
pub(super) struct NameFilter {
    name: Option<String>,
}

pub(super) async fn list_groups(State(s): State<Arc<AppState>>, Query(q): Query<NameFilter>) -> Json<Vec<Value>> {
    let fed = s.fed.read().await;
    Json(
        fed.group_snapshots
            .values()
            .filter(|g| q.name.as_deref().map(|n| g.name == n).unwrap_or(true))
            .map(|g| group_view(&fed, g))
            .collect(),
    )
}

pub(super) async fn get_group(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let fed = s.fed.read().await;
    fed.group_snapshots
        .get(&id)
        .map(|g| Json(group_view(&fed, g)))
        .ok_or_else(|| ApiError::not_found(format!("group snapshot {id:?}")))
}

/// DELETE /api/v1/group-snapshots/{id} — the group and every member.
pub(super) async fn delete_group(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let (g, members, at) = {
        let fed = s.fed.read().await;
        let Some(g) = fed.group_snapshots.get(&id).cloned() else {
            return Ok(Json(json!({})));
        };
        let members: Vec<Snapshot> = g.members.iter().filter_map(|m| fed.snapshots.get(m).cloned()).collect();
        let at = match members.first() {
            Some(m) => Some(snapshot_home(&s, &fed, m)?),
            None => None,
        };
        (g, members, at)
    };
    if let Some(at) = &at {
        healthy(&s, &at.node).await?;
        match (&g.engine_id, at.node == g.node) {
            (Some(eid), true) => at
                .engine
                .delete_v1(&format!("/v1/group-snapshots/{eid}"))
                .await
                .map_err(|e| ApiError::upstream(format!("{}: {e:#}", at.node)))?,
            _ => {
                let names: Vec<String> = members.iter().map(|m| m.engine_name.clone()).collect();
                delete_by_name(at, &names).await?
            }
        }
    }
    {
        let mut fed = s.fed.write().await;
        for m in &g.members {
            fed.snapshots.remove(m);
        }
        fed.group_snapshots.remove(&id);
        fed.revision += 1;
    }
    commit(&s, &id, format!("group snapshot {id} deleted ({} members)", members.len())).await;
    Ok(Json(json!({})))
}

/// A clone (#49): `POST /api/v1/volumes` with `source`. Copy-on-write on
/// the node that holds the source's data (owner: the clone shares the
/// source's head and legs, and lives as long as they do — the parent
/// cannot be deleted under it). Served as itself from that node.
pub(super) async fn create_clone(
    s: &Arc<AppState>,
    name: &str,
    size_bytes: u64,
    source: VolumeSource,
    hosts: Vec<HostServe>,
) -> Result<Value, ApiError> {
    let (at, parent, source_engine) = {
        let fed = s.fed.read().await;
        name_free(&fed, name)?;
        let (vol, engine_name, min) = match &source {
            VolumeSource::Volume(id) => {
                let v = fed.volumes.get(id).ok_or_else(|| ApiError::not_found(format!("volume {id:?}")))?;
                (v, None, v.size_bytes)
            }
            VolumeSource::Snapshot(id) => {
                let sn = fed.snapshots.get(id).ok_or_else(|| ApiError::not_found(format!("snapshot {id:?}")))?;
                let v = fed
                    .volumes
                    .get(&sn.parent)
                    .ok_or_else(|| ApiError::conflict(format!("snapshot {id:?}: its volume {} is gone", sn.parent)))?;
                (v, Some(sn.engine_name.clone()), sn.size_bytes)
            }
        };
        if size_bytes < min {
            return Err(ApiError::bad_request(format!(
                "a clone is at least its source's size ({min} bytes), not {size_bytes}"
            )));
        }
        let at = data_at(s, &fed, vol).map_err(|e| ApiError::conflict(format!("{e:#}")))?;
        let parent = fed.volumes.get(&at.parent).cloned().expect("data_at found it");
        let source_engine = engine_name.unwrap_or_else(|| at.engine_name.clone());
        (at, parent, source_engine)
    };
    healthy(s, &at.node).await?;
    let master = at.master_node.clone().ok_or_else(|| {
        ApiError::conflict(format!("{}: no /v1 node name known for {} — cannot place a clone there", parent.name, at.node))
    })?;
    let r = at
        .engine
        .create_clone(name, size_bytes, &master, &source_engine)
        .await
        .map_err(|e| ApiError::upstream(format!("{}: clone: {e:#}", at.node)))?;
    let id = r
        .get("id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ApiError::upstream(format!("{}: clone answered without an id: {r}", at.node)))?
        .to_string();
    let per_host = !hosts.is_empty();
    let vol = DistVolume {
        name: name.to_string(),
        size_bytes,
        legs: Vec::new(),
        assembly: AssemblyState::Clone,
        head: None,
        array_id: None,
        created_at: SystemTime::now(),
        replacing: None,
        next_releg_after: None,
        next_assemble_after: None,
        export: Export {
            node: Some(at.node.clone()),
            volume_id: Some(id),
            master_node: crate::engine::Engine::master_node_of(&r).or(Some(master)),
            hosts,
            per_host,
            ..Default::default()
        },
        epoch: crate::model::first_epoch(),
        fenced: false,
        rate_pending: false,
        dual_attach: None,
        migration: None,
        clone_of: Some(CloneOf { source: source.clone(), parent: parent.name.clone() }),
        ..parent
    };
    {
        let mut fed = s.fed.write().await;
        fed.volumes.insert(name.to_string(), vol);
        fed.revision += 1;
    }
    let what = match &source {
        VolumeSource::Volume(v) => format!("volume {v}"),
        VolumeSource::Snapshot(sn) => format!("snapshot {sn}"),
    };
    commit(s, name, format!("{name}: cloned from {what} on {} (copy-on-write on {}'s data)", at.node, parent.name)).await;
    let _ = crate::orchestrate::publish(s, name).await;
    let fed = s.fed.read().await;
    let heads = s.heads.read().await;
    Ok(fed
        .volumes
        .get(name)
        .map(|v| crate::head::view_in(&fed, &heads, v))
        .unwrap_or_default())
}
