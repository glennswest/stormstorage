//! Live migration of an in-use node volume — a VM's disk — from node S to
//! node T (#44; owner on rustkube-node#159: "raid it to destination and let
//! it catchup, and then you can start").
//!
//! A VM disk is a plain local engine volume on S, attached over ublk to a
//! running guest. Only S's engine sees its writes, so the copy is S's
//! (stormblock#295, v23.11.0). stormstorage drives it:
//!
//! 1. a fresh thin volume of the same size on T (`/v1`, named like S's),
//!    served to S's host NQN over NVMe/TCP (`[legs] host_nqn` of S, which
//!    the drive URI's `hostnqn=` makes S present);
//! 2. `POST /api/v1/volumes/{id}/mirror {target}` on S: every write lands
//!    on both, and a background copy fills T; `mirror.state` goes
//!    `copying` → `synced`;
//! 3. at the switch-over (guest paused) `DELETE …/mirror`: the writes in
//!    flight land, T is flushed, the mirror is dropped. T's volume is then
//!    T's own, and the kubelet attaches it there; S's attach on T is
//!    withdrawn;
//! 4. complete retires S's volume; abort stops the mirror and deletes T's,
//!    leaving S as it was.
//!
//! Each step is recorded before the next, so a restart resumes. The mirror
//! itself does not survive an engine restart (it reads `failed`); `retry`
//! starts it again.

use crate::api::AppState;
use anyhow::Context;
use crate::events::Severity;
use crate::model::{VmMigration, VmMigrationState};
use serde_json::Value;
use std::sync::Arc;
use std::time::SystemTime;

/// Terminal records are dropped after this long.
const KEEP_TERMINAL_MS: i64 = 24 * 3600 * 1000;

/// A step refused for a reason the caller can act on (a 409 at the API).
#[derive(Debug)]
pub struct Refused(pub String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

fn refuse<T>(m: impl Into<String>) -> anyhow::Result<T> {
    Err(Refused(m.into()).into())
}

pub fn claim_key(id: &str) -> String {
    format!("vmmig:{id}")
}

fn now_ms() -> i64 {
    crate::backup::now_ms(SystemTime::now())
}

async fn event(state: &Arc<AppState>, m: &VmMigration, sev: Severity, msg: String) {
    let subject = format!("{}/{}", m.source_node, m.source_name);
    state.events.write().await.push(Some(subject), sev, "vm-migration", msg);
}

/// Apply `f` to the record and persist; None when it is gone.
pub async fn update<T>(state: &Arc<AppState>, id: &str, f: impl FnOnce(&mut VmMigration) -> T) -> Option<T> {
    let out = {
        let mut fed = state.fed.write().await;
        let m = fed.vm_migrations.get_mut(id)?;
        let out = f(m);
        m.updated_at_ms = now_ms();
        fed.revision += 1;
        out
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    Some(out)
}

async fn get(state: &Arc<AppState>, id: &str) -> anyhow::Result<VmMigration> {
    match state.fed.read().await.vm_migrations.get(id) {
        Some(m) => Ok(m.clone()),
        None => Err(crate::engine::HttpStatus {
            path: format!("migration {id}"),
            status: reqwest::StatusCode::NOT_FOUND,
            message: "no such migration".into(),
        }
        .into()),
    }
}

async fn engine(state: &Arc<AppState>, node: &str) -> anyhow::Result<crate::engine::Engine> {
    let fed = state.fed.read().await;
    crate::backup::engine_on(state, &fed, node)
}

async fn healthy(state: &Arc<AppState>, node: &str) -> bool {
    state.fed.read().await.nodes.get(node).is_some_and(|n| n.status.healthy)
}

/// S's mirror as its engine reports it (`mirror` on `GET
/// /api/v1/volumes/{id}`).
#[derive(Debug, Clone, PartialEq)]
pub struct MirrorReading {
    pub target: String,
    pub state: String,
    pub bytes_total: u64,
    pub bytes_remaining: u64,
    pub message: Option<String>,
}

impl MirrorReading {
    pub fn of(v: &Value) -> Option<Self> {
        let m = v.get("mirror").filter(|m| m.is_object()).unwrap_or(v);
        Some(Self {
            target: m.get("target")?.as_str()?.to_string(),
            state: m.get("state")?.as_str()?.to_string(),
            bytes_total: m.get("bytes_total").and_then(|x| x.as_u64()).unwrap_or(0),
            bytes_remaining: m.get("bytes_remaining").and_then(|x| x.as_u64()).unwrap_or(0),
            message: m.get("message").and_then(|x| x.as_str()).map(str::to_string),
        })
    }
}

/// Fold a reading of S's mirror into a copying/synced record. Pure.
/// Returns the new state when it changed. A record in any other state is
/// left alone: the reading belongs to a later step's caller.
pub fn apply_reading(m: &mut VmMigration, reading: Option<&MirrorReading>) -> Option<VmMigrationState> {
    if !matches!(m.state, VmMigrationState::Copying | VmMigrationState::Synced) || m.aborting {
        return None;
    }
    let before = m.state;
    match reading {
        None => {
            m.state = VmMigrationState::Failed;
            m.message = Some(format!("{} holds no mirror of {} any more", m.source_node, m.source_name));
        }
        Some(r) if Some(&r.target) != m.mirror_target.as_ref() => {
            m.state = VmMigrationState::Failed;
            m.message = Some(format!("{} mirrors {} to another target ({})", m.source_node, m.source_name, r.target));
        }
        Some(r) => {
            m.bytes_total = r.bytes_total;
            m.bytes_remaining = r.bytes_remaining;
            match r.state.as_str() {
                "synced" => {
                    m.state = VmMigrationState::Synced;
                    m.message = None;
                }
                "copying" => {
                    m.state = VmMigrationState::Copying;
                    m.message = None;
                }
                other => {
                    m.state = VmMigrationState::Failed;
                    m.message = Some(r.message.clone().unwrap_or_else(|| format!("mirror {other}")));
                }
            }
        }
    }
    (m.state != before).then_some(m.state)
}

/// Read S's mirror of the migration's volume. `Ok(None)`: no mirror.
pub async fn read_mirror(state: &Arc<AppState>, m: &VmMigration) -> anyhow::Result<Option<MirrorReading>> {
    let s = engine(state, &m.source_node).await?;
    let v = s.get_any_volume(&m.source_volume_id).await?;
    Ok(MirrorReading::of(&v))
}

/// The record with a live reading of S's mirror folded in (not recorded):
/// what `GET` answers, so a caller waiting for `synced` need not wait for
/// the next poll. S not answering: the record as it is.
pub async fn live(state: &Arc<AppState>, mut m: VmMigration) -> VmMigration {
    if matches!(m.state, VmMigrationState::Copying | VmMigrationState::Synced) && !m.aborting {
        if let Ok(r) = read_mirror(state, &m).await {
            apply_reading(&mut m, r.as_ref());
        }
    }
    m
}

/// Take the starting steps not yet recorded: T's volume, its engine id,
/// its attach for S, S's mirror. Idempotent; each step is recorded.
pub async fn advance(state: &Arc<AppState>, id: &str) -> anyhow::Result<()> {
    let m = get(state, id).await?;
    if m.state != VmMigrationState::Starting || m.aborting {
        return Ok(());
    }
    let t = engine(state, &m.target_node).await?;
    let tn = &m.target_node;
    if m.target_volume_id.is_none() {
        let v = t
            .create_volume(&m.target_name, m.size_bytes, None)
            .await
            .with_context(|| format!("{tn}: create {}", m.target_name))?;
        let vid = v
            .get("id")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow::anyhow!("{tn}: create answered no id: {v}"))?
            .to_string();
        update(state, id, |r| r.target_volume_id = Some(vid)).await;
    }
    let local = match m.target_local_id.clone() {
        Some(l) => l,
        None => {
            let l = t
                .local_volume_id(&m.target_name)
                .await
                .with_context(|| format!("{tn}: find {}", m.target_name))?;
            update(state, id, |r| r.target_local_id = Some(l.clone())).await;
            l
        }
    };
    let attach = match m.target_attach.clone() {
        Some(a) => a,
        None => {
            let host = state.config.legs.host_nqn_for(&m.source_node);
            let (a, _) = t
                .attach_for_host(&local, &host, false)
                .await
                .with_context(|| format!("{tn}: serve {} to {}", m.target_name, m.source_node))?;
            update(state, id, |r| r.target_attach = Some(a.clone())).await;
            a
        }
    };
    let uri = attach.drive_uri();
    let s = engine(state, &m.source_node).await?;
    let sn = &m.source_node;
    // Started before a restart and not recorded: take it as it is.
    let current = s.get_any_volume(&m.source_volume_id).await.ok().and_then(|v| MirrorReading::of(&v));
    let reading = match current {
        Some(r) if r.target == uri && r.state != "failed" => r,
        _ => {
            let v = s
                .start_mirror(&m.source_volume_id, &uri)
                .await
                .with_context(|| format!("{sn}: mirror {} to {tn}", m.source_name))?;
            MirrorReading::of(&v).unwrap_or(MirrorReading {
                target: uri.clone(),
                state: "copying".into(),
                bytes_total: m.size_bytes,
                bytes_remaining: m.size_bytes,
                message: None,
            })
        }
    };
    let rec = update(state, id, |r| {
        r.mirror_target = Some(uri.clone());
        r.state = VmMigrationState::Copying;
        r.message = None;
        apply_reading(r, Some(&reading));
        r.clone()
    })
    .await;
    if let Some(r) = rec {
        event(state, &r, Severity::Info, format!("{}: mirroring {sn} → {tn} ({})", r.id, r.source_name)).await;
    }
    Ok(())
}

/// Record S's mirror as it is now (the reconciler, each poll).
pub async fn refresh(state: &Arc<AppState>, id: &str) -> anyhow::Result<()> {
    let m = get(state, id).await?;
    let reading = read_mirror(state, &m).await?;
    let mut next = m.clone();
    let changed = apply_reading(&mut next, reading.as_ref());
    if next != m {
        let rec = update(state, id, |r| {
            r.state = next.state;
            r.bytes_total = next.bytes_total;
            r.bytes_remaining = next.bytes_remaining;
            r.message = next.message.clone();
            r.clone()
        })
        .await;
        if let (Some(st), Some(r)) = (changed, rec) {
            let (sev, what) = match st {
                VmMigrationState::Synced => (Severity::Info, "synced: ready to cut over".to_string()),
                VmMigrationState::Failed => {
                    (Severity::Error, format!("failed: {}", r.message.as_deref().unwrap_or("?")))
                }
                other => (Severity::Info, format!("{other:?}").to_lowercase()),
            };
            event(state, &r, sev, format!("{}: {} {what}", r.id, r.source_name)).await;
        }
    }
    Ok(())
}

/// Stop serving T's volume to S. A failure is kept on the record and
/// retried by the reconciler.
async fn withdraw(state: &Arc<AppState>, m: &VmMigration) -> anyhow::Result<()> {
    let (Some(a), Some(local)) = (&m.target_attach, &m.target_local_id) else {
        return Ok(());
    };
    let host = a.host_nqn.clone().unwrap_or_else(|| state.config.legs.host_nqn_for(&m.source_node));
    let t = engine(state, &m.target_node).await?;
    t.withdraw_host(local, &host).await?;
    update(state, &m.id, |r| r.target_attach = None).await;
    Ok(())
}

/// The switch-over, with the guest paused on S: needs `synced`. Stops S's
/// mirror, which must answer synced and flushed (else T is not exact: the
/// migration fails, and the guest goes on on S). Then withdraws S's attach
/// on T. Idempotent once cut over.
pub async fn cutover(state: &Arc<AppState>, id: &str) -> anyhow::Result<VmMigration> {
    let m = get(state, id).await?;
    if m.aborting {
        return refuse(format!("{id} is being aborted"));
    }
    match m.state {
        VmMigrationState::CutOver | VmMigrationState::Completed => {
            if let Err(e) = withdraw(state, &m).await {
                tracing::warn!("{id}: withdraw {} from {}: {e:#}", m.target_name, m.source_node);
            }
            return get(state, id).await;
        }
        VmMigrationState::Copying | VmMigrationState::Synced => {}
        other => return refuse(format!("{id} is {other:?}, not synced")),
    }
    let s = engine(state, &m.source_node).await?;
    let now = live(state, m.clone()).await;
    if now.state != VmMigrationState::Synced {
        return refuse(format!(
            "{id} is not synced yet ({:?}, {} of {} bytes to go)",
            now.state, now.bytes_remaining, now.bytes_total
        ));
    }
    let stopped = s
        .stop_mirror(&m.source_volume_id)
        .await
        .with_context(|| format!("{}: stop the mirror of {}", m.source_node, m.source_name))?;
    let exact = stopped.as_ref().is_some_and(|v| {
        v.get("state").and_then(|x| x.as_str()) == Some("synced") && v.get("flushed").and_then(|x| x.as_bool()) == Some(true)
    });
    if !exact {
        let why = match &stopped {
            None => "S held no mirror at the switch-over".to_string(),
            Some(v) => format!(
                "the mirror stopped {} (flushed: {}){}",
                v.get("state").and_then(|x| x.as_str()).unwrap_or("?"),
                v.get("flushed").and_then(|x| x.as_bool()).unwrap_or(false),
                v.get("message").and_then(|x| x.as_str()).map(|x| format!(": {x}")).unwrap_or_default()
            ),
        };
        let msg = format!("switch-over: {why}; {} is not exact — resume on {} and abort or retry", m.target_node, m.source_node);
        let rec = update(state, id, |r| {
            r.state = VmMigrationState::Failed;
            r.message = Some(msg.clone());
            r.clone()
        })
        .await;
        if let Some(r) = rec {
            event(state, &r, Severity::Error, format!("{id}: {msg}")).await;
        }
        return refuse(msg);
    }
    let rec = update(state, id, |r| {
        r.state = VmMigrationState::CutOver;
        r.bytes_remaining = 0;
        r.message = None;
        r.clone()
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("{id} gone"))?;
    event(
        state,
        &rec,
        Severity::Info,
        format!("{id}: cut over: {} is {}'s own now ({} kept until complete)", rec.target_name, rec.target_node, rec.source_node),
    )
    .await;
    if let Err(e) = withdraw(state, &rec).await {
        let msg = format!("{} still served to {}: {e:#} (retried)", rec.target_name, rec.source_node);
        update(state, id, |r| r.message = Some(msg)).await;
    }
    get(state, id).await
}

/// After a cut-over: retire S's volume.
pub async fn complete(state: &Arc<AppState>, id: &str) -> anyhow::Result<VmMigration> {
    let m = get(state, id).await?;
    match m.state {
        VmMigrationState::Completed => return Ok(m),
        VmMigrationState::CutOver => {}
        other => return refuse(format!("{id} is {other:?}: cut over first")),
    }
    if m.aborting {
        return refuse(format!("{id} is being aborted"));
    }
    let s = engine(state, &m.source_node).await?;
    if let Err(e) = s.delete_any_volume(&m.source_volume_id).await {
        let msg = format!("{}: retire {}: {e:#}", m.source_node, m.source_name);
        update(state, id, |r| r.message = Some(msg.clone())).await;
        return Err(crate::engine::HttpStatus {
            path: format!("/api/v1/volumes/{}", m.source_volume_id),
            status: crate::engine::status_of(&e).unwrap_or(reqwest::StatusCode::BAD_GATEWAY),
            message: msg,
        }
        .into());
    }
    let _ = withdraw(state, &m).await;
    let rec = update(state, id, |r| {
        r.state = VmMigrationState::Completed;
        r.message = None;
        r.clone()
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("{id} gone"))?;
    event(state, &rec, Severity::Info, format!("{id}: completed: {} retired on {}", rec.source_name, rec.source_node)).await;
    Ok(rec)
}

/// Abort: S's mirror stopped (when S answers), T's volume withdrawn and
/// deleted. S's volume is never touched. A step that fails leaves
/// `aborting` set; the reconciler finishes it.
pub async fn abort(state: &Arc<AppState>, id: &str) -> anyhow::Result<VmMigration> {
    let m = get(state, id).await?;
    match m.state {
        VmMigrationState::Aborted => return Ok(m),
        VmMigrationState::Completed => {
            return refuse(format!("{id} is completed: {} is retired, {} holds the disk", m.source_name, m.target_node))
        }
        _ => {}
    }
    if !m.aborting {
        update(state, id, |r| r.aborting = true).await;
        event(state, &m, Severity::Warning, format!("{id}: aborting; {} stays on {}", m.source_name, m.source_node)).await;
    }
    let r = abort_steps(state, &m).await;
    if let Err(e) = &r {
        update(state, id, |r| r.message = Some(format!("abort: {e:#} (retried)"))).await;
    }
    r?;
    let rec = update(state, id, |r| {
        r.state = VmMigrationState::Aborted;
        r.aborting = false;
        r.message = None;
        r.target_attach = None;
        r.clone()
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("{id} gone"))?;
    event(state, &rec, Severity::Info, format!("{id}: aborted; {} deleted on {}", rec.target_name, rec.target_node)).await;
    Ok(rec)
}

async fn abort_steps(state: &Arc<AppState>, m: &VmMigration) -> anyhow::Result<()> {
    // S: our mirror only. S unreachable: its mirror dies with its engine
    // (a restart reports it failed) or with T's volume below.
    if m.state != VmMigrationState::CutOver && healthy(state, &m.source_node).await {
        let s = engine(state, &m.source_node).await?;
        let ours = match (&m.mirror_target, &m.target_attach) {
            (Some(t), _) => Some(t.clone()),
            (None, Some(a)) => Some(a.drive_uri()),
            _ => None,
        };
        let reading = s.get_any_volume(&m.source_volume_id).await.ok().and_then(|v| MirrorReading::of(&v));
        if let (Some(r), Some(ours)) = (reading, ours) {
            if r.target == ours {
                s.stop_mirror(&m.source_volume_id)
                    .await
                    .with_context(|| format!("{}: stop the mirror", m.source_node))?;
            }
        }
    }
    undo_target(state, m).await
}

/// Withdraw and delete T's volume (abort; a first start that failed).
pub async fn undo_target(state: &Arc<AppState>, m: &VmMigration) -> anyhow::Result<()> {
    if m.target_volume_id.is_none() && m.target_local_id.is_none() {
        return Ok(());
    }
    let t = engine(state, &m.target_node).await?;
    if let (Some(a), Some(local)) = (&m.target_attach, &m.target_local_id) {
        let host = a.host_nqn.clone().unwrap_or_else(|| state.config.legs.host_nqn_for(&m.source_node));
        t.withdraw_host(local, &host).await?;
    }
    if let Some(v1) = &m.target_volume_id {
        t.delete_volume(v1).await?;
    }
    if let Some(local) = &m.target_local_id {
        t.delete_any_volume(local).await?;
    }
    Ok(())
}

/// A failed migration, before the cut-over: forget S's stale mirror and
/// start it again on the same target volume (the copy starts over).
pub async fn retry(state: &Arc<AppState>, id: &str) -> anyhow::Result<VmMigration> {
    let m = get(state, id).await?;
    if m.aborting {
        return refuse(format!("{id} is being aborted"));
    }
    if m.state != VmMigrationState::Failed {
        return refuse(format!("{id} is {:?}: only a failed migration is retried", m.state));
    }
    let s = engine(state, &m.source_node).await?;
    let reading = s.get_any_volume(&m.source_volume_id).await?;
    if let Some(r) = MirrorReading::of(&reading) {
        if Some(&r.target) == m.mirror_target.as_ref() || r.state == "failed" {
            s.stop_mirror(&m.source_volume_id).await?;
        } else {
            return refuse(format!("{} mirrors {} to {} now: not ours", m.source_node, m.source_name, r.target));
        }
    }
    update(state, id, |r| {
        r.state = VmMigrationState::Starting;
        r.message = None;
        r.mirror_target = None;
    })
    .await;
    if let Err(e) = advance(state, id).await {
        fail(state, id, &e).await;
        return Err(e);
    }
    get(state, id).await
}

/// Record a failed step.
pub async fn fail(state: &Arc<AppState>, id: &str, e: &anyhow::Error) {
    let msg = format!("{e:#}");
    let rec = update(state, id, |r| {
        let changed = r.state != VmMigrationState::Failed || r.message.as_deref() != Some(msg.as_str());
        r.state = VmMigrationState::Failed;
        r.message = Some(msg.clone());
        changed.then(|| r.clone())
    })
    .await
    .flatten();
    if let Some(r) = rec {
        event(state, &r, Severity::Error, format!("{id}: failed: {msg}")).await;
    }
}

/// The reconciler's part (the instance that acts on recovery): resume a
/// start a restart interrupted, record each mirror's progress, finish
/// aborts and withdrawals, drop old terminal records.
pub async fn run(state: &Arc<AppState>) {
    if !state.recovery_active() {
        return;
    }
    let all: Vec<VmMigration> = state.fed.read().await.vm_migrations.values().cloned().collect();
    let now = now_ms();
    for m in all {
        if m.state.terminal() && m.target_attach.is_none() && now - m.updated_at_ms > KEEP_TERMINAL_MS {
            {
                let mut fed = state.fed.write().await;
                fed.vm_migrations.remove(&m.id);
                fed.revision += 1;
            }
            crate::replicate::push_to_peers(state.clone());
            state.persist().await;
            continue;
        }
        let key = claim_key(&m.id);
        if !crate::orchestrate::claim(state, &key) {
            continue;
        }
        let (s_ok, t_ok) = (healthy(state, &m.source_node).await, healthy(state, &m.target_node).await);
        let r = if m.aborting {
            abort(state, &m.id).await.map(|_| ())
        } else {
            match m.state {
                VmMigrationState::Starting => {
                    let r = advance(state, &m.id).await;
                    if let Err(e) = &r {
                        fail(state, &m.id, e).await;
                    }
                    r
                }
                VmMigrationState::Copying | VmMigrationState::Synced if s_ok => {
                    refresh(state, &m.id).await
                }
                VmMigrationState::CutOver | VmMigrationState::Completed
                    if m.target_attach.is_some() && t_ok =>
                {
                    withdraw(state, &m).await
                }
                _ => Ok(()),
            }
        };
        crate::orchestrate::release(state, &key);
        if let Err(e) = r {
            tracing::warn!("vm migration {}: {e:#}", m.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(state: VmMigrationState) -> VmMigration {
        VmMigration {
            id: "vm-1".into(),
            source_node: "s".into(),
            source_name: "ns.vm-disk".into(),
            target_node: "t".into(),
            mirror_target: Some("nvme-tcp://t:4420/n?nsid=1".into()),
            state,
            ..Default::default()
        }
    }

    fn reading(state: &str, remaining: u64) -> MirrorReading {
        MirrorReading {
            target: "nvme-tcp://t:4420/n?nsid=1".into(),
            state: state.into(),
            bytes_total: 100,
            bytes_remaining: remaining,
            message: None,
        }
    }

    #[test]
    fn copying_then_synced() {
        let mut m = rec(VmMigrationState::Copying);
        assert_eq!(apply_reading(&mut m, Some(&reading("copying", 40))), None);
        assert_eq!(m.bytes_remaining, 40);
        assert_eq!(apply_reading(&mut m, Some(&reading("synced", 0))), Some(VmMigrationState::Synced));
        assert_eq!((m.bytes_total, m.bytes_remaining), (100, 0));
    }

    #[test]
    fn a_failed_or_missing_or_foreign_mirror_fails_the_migration() {
        let mut m = rec(VmMigrationState::Synced);
        let mut r = reading("failed", 10);
        r.message = Some("the engine restarted".into());
        assert_eq!(apply_reading(&mut m, Some(&r)), Some(VmMigrationState::Failed));
        assert_eq!(m.message.as_deref(), Some("the engine restarted"));

        let mut m = rec(VmMigrationState::Copying);
        assert_eq!(apply_reading(&mut m, None), Some(VmMigrationState::Failed));

        let mut m = rec(VmMigrationState::Copying);
        let mut r = reading("copying", 1);
        r.target = "nvme-tcp://elsewhere:4420/n?nsid=1".into();
        assert_eq!(apply_reading(&mut m, Some(&r)), Some(VmMigrationState::Failed));
        assert!(m.message.unwrap().contains("another target"));
    }

    #[test]
    fn readings_never_move_other_states() {
        for st in [VmMigrationState::Starting, VmMigrationState::CutOver, VmMigrationState::Failed, VmMigrationState::Aborted] {
            let mut m = rec(st);
            assert_eq!(apply_reading(&mut m, None), None);
            assert_eq!(m.state, st);
        }
        let mut m = rec(VmMigrationState::Copying);
        m.aborting = true;
        assert_eq!(apply_reading(&mut m, None), None);
    }

    #[test]
    fn reading_parses_the_volume_or_the_mirror_itself() {
        let v = serde_json::json!({"id": "u", "mirror": {"target": "x", "state": "copying", "bytes_total": 5, "bytes_remaining": 2}});
        assert_eq!(MirrorReading::of(&v).unwrap().bytes_remaining, 2);
        let v = serde_json::json!({"target": "x", "state": "synced"});
        assert_eq!(MirrorReading::of(&v).unwrap().state, "synced");
        assert!(MirrorReading::of(&serde_json::json!({"id": "u"})).is_none());
    }
}
