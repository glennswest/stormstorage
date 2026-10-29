//! Point-in-time backups to the backup tier (#46; owner decision B,
//! 2026-10-09).
//!
//! Every `interval_secs`, the served volume is snapshotted on the node that
//! serves it (the head's array, or a single leg). The engine ships the
//! extents changed since the last shipped snapshot to `<volume>-backup`, a
//! thin volume on a backup node, as a background job (stormblock#400, over
//! the snapshot diff of stormblock#399). Once the job is done, the backup
//! volume is snapshotted on the backup node: that is a point in time, and
//! `keep` of them are kept. stormstorage drives the cycle; the copy is
//! engine to engine.
//!
//! Until an engine has the ship route, a cycle ends `waiting_engine`, and
//! the snapshot it took is dropped.

use crate::api::AppState;
use crate::events::Severity;
use crate::model::{Backup, BackupPoint, BackupState, FedState, ShipJob, ShipSnapshot};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn claim_key(volume: &str) -> String {
    format!("backup:{volume}")
}

async fn event(state: &Arc<AppState>, subject: &str, sev: Severity, msg: String) {
    state.events.write().await.push(Some(subject.to_string()), sev, "backup", msg);
}

/// The nodes a backup may go to: healthy, matching the policy (pool
/// selector, tier, or the named node), never one holding a leg of the
/// volume. Pure.
pub fn candidates(
    fed: &FedState,
    config: &crate::config::Config,
    b: &Backup,
) -> Vec<crate::placement::Candidate> {
    let Some(p) = &b.policy else { return Vec::new() };
    let legs: Vec<&str> = fed
        .volumes
        .get(&b.volume)
        .map(|v| v.legs.iter().map(|l| l.node.as_str()).collect())
        .unwrap_or_default();
    let pool = p.pool.as_deref().and_then(|n| config.pool(n));
    fed.nodes
        .values()
        .filter(|n| n.status.healthy)
        .filter(|n| !legs.contains(&n.config.name.as_str()))
        .filter(|n| pool.map(|pc| pc.selector.matches(&n.config)).unwrap_or(true))
        .filter(|n| p.tier.as_deref().map(|t| n.config.tier.as_deref() == Some(t)).unwrap_or(true))
        .filter(|n| p.node.as_deref().map(|x| n.config.name == x).unwrap_or(true))
        .map(|n| crate::placement::Candidate {
            name: n.config.name.clone(),
            labels: n.labels(),
            free_bytes: n.status.free_bytes,
            total_bytes: n.status.total_bytes,
            io_busy: n.status.io.map(|i| i.busy),
        })
        .collect()
}

/// Points beyond `keep`, oldest first: the ones to drop. Pure.
pub fn prune(points: &[BackupPoint], keep: u32) -> Vec<BackupPoint> {
    let mut sorted = points.to_vec();
    sorted.sort_by_key(|p| p.taken_at_ms);
    let excess = sorted.len().saturating_sub(keep.max(1) as usize);
    sorted.into_iter().take(excess).collect()
}

/// One backup step for every volume with a policy, on the instance that
/// acts on recovery. Each step does at most one thing per volume and
/// records it, so a restart resumes where it was.
pub async fn run(state: &Arc<AppState>, now: SystemTime) {
    if !state.recovery_active() {
        return;
    }
    let names: Vec<String> = {
        let fed = state.fed.read().await;
        fed.backups
            .values()
            .filter(|b| b.policy.is_some() && !b.source_deleted)
            .map(|b| b.volume.clone())
            .collect()
    };
    for name in names {
        if !crate::orchestrate::claim(state, &claim_key(&name)) {
            continue;
        }
        let r = step(state, &name, now).await;
        crate::orchestrate::release(state, &claim_key(&name));
        if let Err(e) = r {
            let interval = {
                let fed = state.fed.read().await;
                fed.backups.get(&name).and_then(|b| b.policy.as_ref()).map(|p| p.interval_secs).unwrap_or(3600)
            };
            let changed = update(state, &name, |b| {
                let msg = format!("{e:#}");
                let changed = b.state != BackupState::Failed || b.message.as_deref() != Some(msg.as_str());
                b.state = BackupState::Failed;
                b.message = Some(msg);
                b.next_at_ms = now_ms(now) + (interval as i64) * 1000;
                changed
            })
            .await;
            if changed == Some(true) {
                event(state, &name, Severity::Error, format!("{name}: backup: {e:#}")).await;
            }
        }
    }
}

/// Apply `f` to the backup record and persist; None when it is gone.
async fn update<T>(state: &Arc<AppState>, name: &str, f: impl FnOnce(&mut Backup) -> T) -> Option<T> {
    let out = {
        let mut fed = state.fed.write().await;
        let b = fed.backups.get_mut(name)?;
        let out = f(b);
        fed.revision += 1;
        out
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    Some(out)
}

fn engine_on(state: &Arc<AppState>, fed: &FedState, node: &str) -> anyhow::Result<crate::engine::Engine> {
    let n = fed.nodes.get(node).ok_or_else(|| anyhow::anyhow!("node {node:?} not in registry"))?;
    if !n.status.healthy {
        anyhow::bail!("{node} is unreachable");
    }
    Ok(state.engine_for(n))
}

/// Delete a snapshot on `engine`: by its /v1 id when known, else its
/// engine volume by name (after a promote only the names came across).
pub async fn drop_snapshot(engine: &crate::engine::Engine, name: &str, id: Option<&str>) -> anyhow::Result<()> {
    if let Some(id) = id {
        engine.delete_v1(&format!("/v1/snapshots/{id}")).await?;
    }
    match engine.local_volume_id(name).await {
        Ok(local) => engine.delete_any_volume(&local).await,
        Err(e) if crate::engine::is_not_found(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn step(state: &Arc<AppState>, name: &str, now: SystemTime) -> anyhow::Result<()> {
    let (b, at, vol_bytes) = {
        let fed = state.fed.read().await;
        let Some(b) = fed.backups.get(name).cloned() else { return Ok(()) };
        let vol = fed.volumes.get(name).ok_or_else(|| anyhow::anyhow!("{name}: volume gone"))?;
        (b, crate::orchestrate::data_at(state, &fed, vol)?, vol.size_bytes)
    };
    let policy = b.policy.clone().expect("filtered");

    // 1. The backup node.
    let node = match &b.node {
        Some(n) => n.clone(),
        None => {
            let pick = {
                let fed = state.fed.read().await;
                let cands = candidates(&fed, &state.config, &b);
                crate::placement::plan(&cands, &state.config.federation.rungs, "node", 1, b.size_bytes, state.config.placement.io_weight)
                    .map_err(|e| anyhow::anyhow!("no backup node: {e}"))?
                    .remove(0)
            };
            update(state, name, |r| r.node = Some(pick.clone())).await;
            event(state, name, Severity::Info, format!("{name}: backups go to {pick}")).await;
            return Ok(());
        }
    };
    let backup_engine = {
        let fed = state.fed.read().await;
        engine_on(state, &fed, &node)?
    };

    // 2. Its volume there: thin, so a first copy of allocated extents only.
    let Some(target) = b.target_volume_id.clone() else {
        let v = backup_engine
            .create_volume(&crate::model::backup_target_name(name), b.size_bytes, None)
            .await
            .map_err(|e| anyhow::anyhow!("{node}: backup volume: {e:#}"))?;
        let id = v.get("id").and_then(|x| x.as_str()).ok_or_else(|| anyhow::anyhow!("{node}: no id in {v}"))?.to_string();
        let master = crate::engine::Engine::master_node_of(&v);
        update(state, name, |r| {
            r.target_volume_id = Some(id);
            r.target_master = master;
        })
        .await;
        return Ok(());
    };

    // 3. Served to the node that ships to it (again after a promote).
    if b.attached_for.as_deref() != Some(at.node.as_str()) || b.target_attach.is_none() {
        let master = b.target_master.clone().unwrap_or_else(|| node.clone());
        let host = state.config.legs.host_nqn_for(&at.node);
        let a = backup_engine
            .attach_leg(&target, &master, None, Some(&host))
            .await
            .map_err(|e| anyhow::anyhow!("{node}: attach backup volume: {e:#}"))?;
        let for_node = at.node.clone();
        update(state, name, |r| {
            r.target_attach = Some(a);
            r.attached_for = Some(for_node);
        })
        .await;
        return Ok(());
    }

    // 3b. The volume was expanded (#73): its backup volume grows before
    // the next ship, so a ship never runs past its end.
    if b.job.is_none() && vol_bytes > b.size_bytes {
        backup_engine
            .expand_v1(&target, vol_bytes)
            .await
            .map_err(|e| anyhow::anyhow!("{node}: expand backup volume: {e:#}"))?;
        update(state, name, |r| r.size_bytes = vol_bytes).await;
        event(state, name, Severity::Info, format!("{name}: backup volume on {node} grown to {vol_bytes} bytes")).await;
        return Ok(());
    }

    // 4. A job in flight.
    if let Some(job) = &b.job {
        if job.snapshot.node != at.node {
            // The array moved under it: the copy is lost; take a new point.
            update(state, name, |r| {
                r.job = None;
                r.state = BackupState::Idle;
                r.next_at_ms = now_ms(now);
            })
            .await;
            anyhow::bail!("serving node changed from {} to {} during a ship", job.snapshot.node, at.node);
        }
        let read = at.engine.ship_job(&job.id).await?;
        let st = read.as_ref().and_then(|j| j.get("state")).and_then(|x| x.as_str()).unwrap_or("lost").to_string();
        match st.as_str() {
            "copying" => {
                let j = read.unwrap_or_default();
                let (t, d) = (
                    j.get("bytes_total").and_then(|x| x.as_u64()).unwrap_or(0),
                    j.get("bytes_done").and_then(|x| x.as_u64()).unwrap_or(0),
                );
                update(state, name, |r| {
                    if let Some(jj) = &mut r.job {
                        jj.bytes_total = t;
                        jj.bytes_done = d;
                    }
                })
                .await;
            }
            "done" => finish(state, name, &b, job, &backup_engine, &at.engine, policy.keep, now).await?,
            other => {
                let msg = read
                    .as_ref()
                    .and_then(|j| j.get("message"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("the engine lost the job")
                    .to_string();
                let _ = drop_snapshot(&at.engine, &job.snapshot.engine_name, job.snapshot.engine_id.as_deref()).await;
                update(state, name, |r| r.job = None).await;
                anyhow::bail!("ship {} {other}: {msg}", job.id);
            }
        }
        return Ok(());
    }

    // 5. Due: snapshot the served volume and ship it.
    if now_ms(now) < b.next_at_ms {
        return Ok(());
    }
    let taken = now_ms(now);
    let snap_name = format!("{name}-ship-{taken}");
    let snap = at
        .engine
        .create_snapshot(&snap_name, &at.engine_name)
        .await
        .map_err(|e| anyhow::anyhow!("{}: snapshot: {e:#}", at.node))?;
    let snapshot = ShipSnapshot {
        node: at.node.clone(),
        engine_name: snap_name.clone(),
        engine_id: snap.get("id").and_then(|x| x.as_str()).map(str::to_string),
        taken_at_ms: taken,
    };
    let uri = b.target_attach.as_ref().expect("checked").drive_uri();
    let rate = {
        let fed = state.fed.read().await;
        fed.volumes.get(name).map(|v| state.config.recovery.rate(v.bandwidth_class)).unwrap_or(0)
    };
    let base = b.base.as_ref().map(|x| x.engine_name.as_str());
    match at.engine.ship_snapshot(&snap_name, &uri, base, rate).await {
        Ok(j) => {
            let id = j.get("id").and_then(|x| x.as_str()).ok_or_else(|| anyhow::anyhow!("ship answered without a job id: {j}"))?;
            let job = ShipJob {
                id: id.to_string(),
                snapshot,
                bytes_total: j.get("bytes_total").and_then(|x| x.as_u64()).unwrap_or(0),
                bytes_done: j.get("bytes_done").and_then(|x| x.as_u64()).unwrap_or(0),
            };
            let what = if base.is_some() { "changes since the last point" } else { "a first full copy (allocated extents)" };
            update(state, name, |r| {
                r.job = Some(job);
                r.state = BackupState::Shipping;
                r.message = None;
            })
            .await;
            event(state, name, Severity::Info, format!("{name}: shipping {what} from {} to {node}", at.node)).await;
            Ok(())
        }
        Err(e) if crate::engine::is_not_found(&e) => {
            let _ = drop_snapshot(&at.engine, &snapshot.engine_name, snapshot.engine_id.as_deref()).await;
            let msg = format!("{} cannot ship snapshots yet (stormblock#400); retried every interval", at.node);
            let first = update(state, name, |r| {
                let first = r.state != BackupState::WaitingEngine;
                r.state = BackupState::WaitingEngine;
                r.message = Some(msg.clone());
                r.next_at_ms = taken + (policy.interval_secs as i64) * 1000;
                first
            })
            .await;
            if first == Some(true) {
                event(state, name, Severity::Warning, format!("{name}: backup waiting: {msg}")).await;
            }
            Ok(())
        }
        Err(e) => {
            let _ = drop_snapshot(&at.engine, &snapshot.engine_name, snapshot.engine_id.as_deref()).await;
            Err(anyhow::anyhow!("{}: ship: {e:#}", at.node))
        }
    }
}

/// A ship is done: the backup volume now holds that point. Snapshot it on
/// the backup node, drop points beyond `keep` and the old base.
#[allow(clippy::too_many_arguments)]
async fn finish(
    state: &Arc<AppState>,
    name: &str,
    b: &Backup,
    job: &ShipJob,
    backup_engine: &crate::engine::Engine,
    serving: &crate::engine::Engine,
    keep: u32,
    now: SystemTime,
) -> anyhow::Result<()> {
    let point_name = format!("{name}-bk-{}", job.snapshot.taken_at_ms);
    let p = backup_engine
        .create_snapshot(&point_name, &crate::model::backup_target_name(name))
        .await
        .map_err(|e| anyhow::anyhow!("{}: point: {e:#}", b.node.as_deref().unwrap_or("?")))?;
    let point = BackupPoint {
        name: point_name.clone(),
        taken_at_ms: job.snapshot.taken_at_ms,
        engine_id: p.get("id").and_then(|x| x.as_str()).map(str::to_string),
    };
    let mut points = b.points.clone();
    points.push(point);
    let dropped = prune(&points, keep);
    let mut kept_failed = Vec::new();
    for d in &dropped {
        if drop_snapshot(backup_engine, &d.name, d.engine_id.as_deref()).await.is_err() {
            kept_failed.push(d.name.clone());
        }
    }
    points.retain(|p| !dropped.iter().any(|d| d.name == p.name) || kept_failed.contains(&p.name));
    if let Some(old) = &b.base {
        let _ = drop_snapshot(serving, &old.engine_name, old.engine_id.as_deref()).await;
    }
    let interval = b.policy.as_ref().map(|p| p.interval_secs).unwrap_or(3600) as i64;
    let new_base = job.snapshot.clone();
    let n_points = points.len();
    update(state, name, |r| {
        r.points = points;
        r.base = Some(new_base);
        r.job = None;
        r.state = BackupState::Idle;
        r.message = None;
        r.next_at_ms = now_ms(now).max(job.snapshot.taken_at_ms + interval * 1000);
    })
    .await;
    event(
        state,
        name,
        Severity::Info,
        format!("{name}: point {point_name} kept on {} ({n_points} kept)", b.node.as_deref().unwrap_or("?")),
    )
    .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(n: &str, t: i64) -> BackupPoint {
        BackupPoint { name: n.into(), taken_at_ms: t, engine_id: None }
    }

    #[test]
    fn prune_drops_the_oldest_beyond_keep() {
        let ps = vec![pt("c", 3), pt("a", 1), pt("b", 2)];
        let d: Vec<String> = prune(&ps, 2).into_iter().map(|p| p.name).collect();
        assert_eq!(d, ["a"]);
        assert!(prune(&ps, 3).is_empty());
        // keep 0 is read as 1: a backup always holds its latest point.
        assert_eq!(prune(&ps, 0).len(), 2);
    }
}
