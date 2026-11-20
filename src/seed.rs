//! A mirrored volume filled from a golden (#113, #116; owner: "no copy at
//! create, unless it has a spec for one").
//!
//! A RAID1 member keeps its first MiB for the array's superblock, and what a
//! consumer is served is a new volume carved on the head's array, so legs
//! cloned from a golden would still serve a blank disk. The golden is copied
//! once, into the carve, with what the engine already has:
//!
//! 1. the head clones the golden (`<name>-seed`, copy-on-write: nothing is
//!    copied, and the clone maps every extent the golden holds);
//! 2. the carve is served to the head's own host NQN over NVMe/TCP;
//! 3. `POST /api/v1/volumes/{clone}/mirror {target: the carve}`
//!    (stormblock#295) copies the clone's mapped extents into the carve,
//!    skipping what reads as zeros, through the RAID1 onto every leg;
//! 4. at `synced` the mirror is stopped (flushed), the head's own serve of
//!    the carve withdrawn and the clone deleted; the seed is `done` and the
//!    volume is published to its consumers.
//!
//! Until then the volume is served to no one (`orchestrate::publish_inner`
//! holds it), so nothing reads a half-copied disk. Each step is recorded, so
//! a restart resumes; a failure undoes what the seed made on the engine and
//! starts again after the recovery cooldown.

use crate::api::AppState;
use crate::events::Severity;
use crate::model::{AssemblyState, Seed, SeedState};
use crate::vmmigrate::MirrorReading;
use anyhow::Context;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// The name of a volume's temporary clone of its golden.
pub fn clone_name(volume: &str) -> String {
    format!("{volume}-seed")
}

/// What a reading of the clone's mirror means for the seed. Pure.
#[derive(Debug, Clone, PartialEq)]
pub enum Reading {
    Copying { total: u64, remaining: u64 },
    Synced { total: u64 },
    Failed(String),
}

pub fn read(seed: &Seed, reading: Option<&MirrorReading>) -> Reading {
    match reading {
        None => Reading::Failed("the head holds no mirror of the clone any more (its engine restarted?)".into()),
        Some(r) if Some(&r.target) != seed.mirror_target.as_ref() => {
            Reading::Failed(format!("the clone is mirrored to another target ({})", r.target))
        }
        Some(r) => match r.state.as_str() {
            "synced" => Reading::Synced { total: r.bytes_total },
            "copying" => Reading::Copying { total: r.bytes_total, remaining: r.bytes_remaining },
            other => Reading::Failed(r.message.clone().unwrap_or_else(|| format!("mirror {other}"))),
        },
    }
}

/// Why a golden cannot be copied into a carve, or None. Pure: the golden's
/// and the carve's `GET /api/v1/volumes/{id}` answers.
pub fn mismatch(golden: &serde_json::Value, carve: &serde_json::Value) -> Option<String> {
    let size = |v: &serde_json::Value| v.get("virtual_size_bytes").and_then(|x| x.as_u64()).unwrap_or(0);
    let lba = |v: &serde_json::Value| v.get("lba").and_then(|x| x.as_u64()).unwrap_or(4096);
    if golden.get("secret").and_then(|x| x.as_bool()) == Some(true) {
        return Some("the golden is a secret volume: its bytes never leave it".into());
    }
    if size(carve) < size(golden) {
        return Some(format!("the served volume is {} bytes, the golden {}", size(carve), size(golden)));
    }
    // A partition table is laid in blocks: a disk laid at 512 is not found
    // on one presented at 4096.
    if lba(carve) != lba(golden) {
        return Some(format!(
            "the golden is laid in {}-byte blocks and the served volume is presented at {}",
            lba(golden),
            lba(carve)
        ));
    }
    None
}

async fn event(state: &Arc<AppState>, name: &str, sev: Severity, msg: String) {
    state.events.write().await.push(Some(name.to_string()), sev, "seed", msg);
}

async fn update(state: &Arc<AppState>, name: &str, f: impl FnOnce(&mut Seed)) {
    {
        let mut fed = state.fed.write().await;
        let Some(s) = fed.volumes.get_mut(name).and_then(|v| v.seed.as_mut()) else { return };
        f(s);
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
}

async fn engine(state: &Arc<AppState>, node: &str) -> anyhow::Result<crate::engine::Engine> {
    let fed = state.fed.read().await;
    let n = fed.nodes.get(node).ok_or_else(|| anyhow::anyhow!("node {node:?} not registered"))?;
    if !n.status.healthy {
        anyhow::bail!("{node} is unreachable");
    }
    Ok(state.engine_for(n))
}

/// The carve's engine-local id on the head.
async fn carve_local(e: &crate::engine::Engine, vol: &crate::model::DistVolume) -> anyhow::Result<String> {
    match &vol.export.local_id {
        Some(l) => Ok(l.clone()),
        None => e.local_volume_id(&crate::orchestrate::served_name(vol)).await,
    }
}

/// One step of a volume's seed. Ok(true) when it changed something.
pub async fn step(state: &Arc<AppState>, name: &str) -> anyhow::Result<bool> {
    let vol = match state.fed.read().await.volumes.get(name).cloned() {
        Some(v) => v,
        None => return Ok(false),
    };
    let Some(seed) = vol.seed.clone() else { return Ok(false) };
    match seed.state {
        SeedState::Done => return Ok(false),
        SeedState::Failed => {
            if seed.retry_after.is_some_and(|t| SystemTime::now() < t) {
                return Ok(false);
            }
            update(state, name, |s| {
                s.state = SeedState::Pending;
                s.retry_after = None;
            })
            .await;
            return Ok(true);
        }
        SeedState::Pending | SeedState::Copying => {}
    }
    if !matches!(vol.assembly, AssemblyState::Assembled | AssemblyState::Degraded) {
        return Ok(false);
    }
    let head = vol.head.clone().ok_or_else(|| anyhow::anyhow!("{name}: no head"))?;
    // The carve: made by a publish, which holds it from consumers.
    if vol.export.volume_id.is_none() || vol.export.node.as_deref() != Some(head.as_str()) {
        crate::orchestrate::publish(state, name).await.context("making the served volume")?;
        return Ok(true);
    }
    let e = engine(state, &head).await?;
    match seed.state {
        SeedState::Pending => start(state, name, &vol, &seed, &head, &e).await.map(|_| true),
        SeedState::Copying => refresh(state, name, &vol, &seed, &head, &e).await,
        _ => Ok(false),
    }
}

async fn start(
    state: &Arc<AppState>,
    name: &str,
    vol: &crate::model::DistVolume,
    seed: &Seed,
    head: &str,
    e: &crate::engine::Engine,
) -> anyhow::Result<()> {
    let golden = e
        .get_any_volume(&seed.golden)
        .await
        .with_context(|| format!("{head}: golden {}", seed.golden))?;
    let carve_id = carve_local(e, vol).await.with_context(|| format!("{head}: the served volume"))?;
    let carve = e.get_any_volume(&carve_id).await.with_context(|| format!("{head}: the served volume"))?;
    if let Some(why) = mismatch(&golden, &carve) {
        anyhow::bail!("golden {} cannot fill {name}: {why}", seed.golden);
    }
    let golden_bytes = golden.get("virtual_size_bytes").and_then(|x| x.as_u64()).unwrap_or(vol.size_bytes);
    let clone = match seed.clone_id.clone().filter(|_| seed.node.as_deref() == Some(head)) {
        Some(c) => c,
        None => {
            let master = vol
                .legs
                .iter()
                .find(|l| l.node == head)
                .and_then(|l| l.master_node.clone())
                .ok_or_else(|| anyhow::anyhow!("{head}: no /v1 node name known — cannot place the clone there"))?;
            let cn = clone_name(name);
            e.create_clone(&cn, golden_bytes, &master, &seed.golden, vol.owner.as_ref())
                .await
                .with_context(|| format!("{head}: clone {}", seed.golden))?;
            let id = e.local_volume_id(&cn).await.with_context(|| format!("{head}: find {cn}"))?;
            let (h, i) = (head.to_string(), id.clone());
            update(state, name, |s| {
                s.node = Some(h);
                s.clone_id = Some(i);
            })
            .await;
            id
        }
    };
    let host = state.config.legs.host_nqn_for(head);
    let (att, _) = e
        .attach_for_host(&carve_id, &host, false)
        .await
        .with_context(|| format!("{head}: serve the served volume to itself"))?;
    let uri = att.drive_uri();
    // Started before a restart and not recorded: taken as it is.
    let current = e.get_any_volume(&clone).await.ok().and_then(|v| MirrorReading::of(&v));
    let reading = match current {
        Some(r) if r.target == uri && r.state != "failed" => r,
        other => {
            // Another target, or a mirror a restart left failed: stopped
            // first, or the engine refuses a second one.
            if other.is_some() {
                let _ = e.stop_mirror(&clone).await;
            }
            let v = e
                .start_mirror(&clone, &uri)
                .await
                .with_context(|| format!("{head}: mirror the golden's clone into the served volume"))?;
            MirrorReading::of(&v).unwrap_or(MirrorReading {
                target: uri.clone(),
                state: "copying".into(),
                bytes_total: golden_bytes,
                bytes_remaining: golden_bytes,
                message: None,
            })
        }
    };
    update(state, name, |s| {
        s.mirror_target = Some(uri.clone());
        s.state = SeedState::Copying;
        s.bytes_total = reading.bytes_total;
        s.bytes_remaining = reading.bytes_remaining;
        s.message = None;
    })
    .await;
    event(
        state,
        name,
        Severity::Info,
        format!("{name}: copying golden {} into the served volume on {head} ({} bytes to walk)", seed.golden, reading.bytes_total),
    )
    .await;
    Ok(())
}

async fn refresh(
    state: &Arc<AppState>,
    name: &str,
    vol: &crate::model::DistVolume,
    seed: &Seed,
    head: &str,
    e: &crate::engine::Engine,
) -> anyhow::Result<bool> {
    if seed.node.as_deref() != Some(head) {
        anyhow::bail!("the head moved to {head} while the copy ran on {}", seed.node.as_deref().unwrap_or("?"));
    }
    let clone = seed.clone_id.clone().ok_or_else(|| anyhow::anyhow!("no clone recorded"))?;
    let v = e.get_any_volume(&clone).await.with_context(|| format!("{head}: the golden's clone"))?;
    match read(seed, MirrorReading::of(&v).as_ref()) {
        Reading::Failed(m) => anyhow::bail!("{m}"),
        Reading::Copying { total, remaining } => {
            if (total, remaining) != (seed.bytes_total, seed.bytes_remaining) {
                update(state, name, |s| {
                    s.bytes_total = total;
                    s.bytes_remaining = remaining;
                })
                .await;
                return Ok(true);
            }
            Ok(false)
        }
        Reading::Synced { total } => {
            let stopped = e.stop_mirror(&clone).await.with_context(|| format!("{head}: stop the mirror"))?;
            let exact = stopped.as_ref().is_some_and(|v| {
                v.get("state").and_then(|x| x.as_str()) == Some("synced")
                    && v.get("flushed").and_then(|x| x.as_bool()) == Some(true)
            });
            if !exact {
                anyhow::bail!("the mirror stopped without a flushed, synced copy: {}", stopped.unwrap_or_default());
            }
            undo(state, vol, seed).await?;
            let now = SystemTime::now();
            let secs = now.duration_since(seed.started_at).unwrap_or_default().as_secs_f64();
            update(state, name, |s| {
                s.state = SeedState::Done;
                s.bytes_total = total;
                s.bytes_remaining = 0;
                s.clone_id = None;
                s.mirror_target = None;
                s.finished_at = Some(now);
                s.secs = Some(secs);
                s.message = None;
            })
            .await;
            event(
                state,
                name,
                Severity::Info,
                format!("{name}: filled from golden {} on {head}: {total} bytes walked in {secs:.1}s; serving it", seed.golden),
            )
            .await;
            let _ = crate::orchestrate::publish(state, name).await;
            Ok(true)
        }
    }
}

/// Take down what a seed made on its engine: the mirror, the head's own
/// serve of the carve, the clone. Idempotent; an unreachable node is an
/// error (retried).
pub async fn undo(state: &Arc<AppState>, vol: &crate::model::DistVolume, seed: &Seed) -> anyhow::Result<()> {
    let Some(node) = seed.node.clone() else { return Ok(()) };
    let e = engine(state, &node).await?;
    if let Some(c) = &seed.clone_id {
        let _ = e.stop_mirror(c).await;
    }
    if vol.export.node.as_deref() == Some(node.as_str()) {
        if let Ok(carve) = carve_local(&e, vol).await {
            e.withdraw_host(&carve, &state.config.legs.host_nqn_for(&node))
                .await
                .with_context(|| format!("{node}: withdraw the served volume from itself"))?;
        }
    }
    if let Some(c) = &seed.clone_id {
        e.delete_any_volume(c).await.with_context(|| format!("{node}: delete the golden's clone"))?;
    }
    Ok(())
}

async fn fail(state: &Arc<AppState>, name: &str, e: &anyhow::Error) {
    let vol = state.fed.read().await.volumes.get(name).cloned();
    let Some(vol) = vol else { return };
    let Some(seed) = vol.seed.clone() else { return };
    let cleaned = undo(state, &vol, &seed).await;
    let cooldown = state.config.recovery.cooldown_secs;
    let msg = format!("{e:#}");
    let kept = cleaned.is_err();
    let m = msg.clone();
    update(state, name, |s| {
        s.state = SeedState::Failed;
        s.message = Some(m);
        s.retry_after = Some(SystemTime::now() + Duration::from_secs(cooldown));
        if !kept {
            s.clone_id = None;
            s.node = None;
        }
        s.mirror_target = None;
    })
    .await;
    let left = match cleaned {
        Ok(()) => String::new(),
        Err(c) => format!(" (its clone is kept for the retry: {c:#})"),
    };
    event(state, name, Severity::Error, format!("{name}: filling from golden {} failed, retry in {cooldown}s: {msg}{left}", seed.golden))
        .await;
}

/// The reconciler's part: advance every seed not done.
pub async fn run(state: &Arc<AppState>) {
    if !state.recovery_active() {
        return;
    }
    let names: Vec<String> = {
        let fed = state.fed.read().await;
        fed.volumes.values().filter(|v| crate::model::seeding(v)).map(|v| v.name.clone()).collect()
    };
    for name in names {
        if !crate::orchestrate::claim(state, &name) {
            continue;
        }
        let r = step(state, &name).await;
        crate::orchestrate::release(state, &name);
        if let Err(e) = r {
            fail(state, &name, &e).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn seed() -> Seed {
        let mut s = Seed::new("golden-x", SystemTime::UNIX_EPOCH);
        s.mirror_target = Some("nvme-tcp://h:4420/n?nsid=1&hostnqn=q".into());
        s
    }

    fn reading(state: &str, total: u64, remaining: u64) -> MirrorReading {
        MirrorReading {
            target: "nvme-tcp://h:4420/n?nsid=1&hostnqn=q".into(),
            state: state.into(),
            bytes_total: total,
            bytes_remaining: remaining,
            message: None,
        }
    }

    #[test]
    fn a_reading_is_copying_synced_or_failed() {
        let s = seed();
        assert_eq!(read(&s, Some(&reading("copying", 10, 4))), Reading::Copying { total: 10, remaining: 4 });
        assert_eq!(read(&s, Some(&reading("synced", 10, 0))), Reading::Synced { total: 10 });
        assert!(matches!(read(&s, Some(&reading("failed", 10, 4))), Reading::Failed(_)));
        assert!(matches!(read(&s, None), Reading::Failed(_)));
        let mut other = reading("synced", 10, 0);
        other.target = "nvme-tcp://elsewhere:4420/n?nsid=1".into();
        assert!(matches!(read(&s, Some(&other)), Reading::Failed(m) if m.contains("another target")));
    }

    #[test]
    fn a_golden_fits_a_carve_at_least_its_size_and_block_size() {
        let g = json!({"virtual_size_bytes": 100, "lba": 512});
        assert_eq!(mismatch(&g, &json!({"virtual_size_bytes": 100, "lba": 512})), None);
        assert_eq!(mismatch(&g, &json!({"virtual_size_bytes": 200, "lba": 512})), None);
        assert!(mismatch(&g, &json!({"virtual_size_bytes": 99, "lba": 512})).unwrap().contains("99 bytes"));
        assert!(mismatch(&g, &json!({"virtual_size_bytes": 100, "lba": 4096})).unwrap().contains("512-byte"));
        let secret = json!({"virtual_size_bytes": 1, "lba": 4096, "secret": true});
        assert!(mismatch(&secret, &json!({"virtual_size_bytes": 1})).unwrap().contains("secret"));
    }

    #[test]
    fn only_a_seed_not_done_holds_the_volume() {
        let mut s = seed();
        assert!(!s.done());
        s.state = SeedState::Failed;
        assert!(!s.done());
        s.state = SeedState::Done;
        assert!(s.done());
    }
}
