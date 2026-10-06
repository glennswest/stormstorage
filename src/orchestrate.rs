//! Leg wiring: assemble a DistVolume's legs into a RAID1 on the head node,
//! tear it down again, and move a leg between nodes.
//!
//! Assembly (per docs/architecture.md): every leg is attached via
//! `/v1/volumes/{id}/attach` (hot-adds an NVMe-TCP namespace, returns
//! nqn/addr/nsid), the head opens each as an `nvme-tcp://` drive —
//! including its own leg over loopback, uniformity first, local fast-path
//! later — and assembles RAID1 via `/api/v1/arrays`.
//!
//! A leg move is the same machinery run forward: new leg → attach →
//! add_member → poll until the member is active (rebuild done) →
//! remove_member → drop the old drive and volume. The same sequence is
//! failure recovery and evacuation, driven by a different trigger.

use crate::api::AppState;
use crate::events::Severity;
use crate::model::{AssemblyState, DistVolume, Leg, LegState, Orphan, Replacement};
use crate::placement::domain_at;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn engine_of(state: &Arc<AppState>, fed: &crate::model::FedState, node: &str) -> anyhow::Result<crate::engine::Engine> {
    fed.nodes
        .get(node)
        .map(|n| state.engine_for(n))
        .ok_or_else(|| anyhow::anyhow!("node {node:?} not in registry"))
}

async fn event(state: &Arc<AppState>, subject: &str, sev: Severity, msg: String) {
    state
        .events
        .write()
        .await
        .push(Some(subject.to_string()), sev, "assemble", msg);
}

/// Attach every leg and assemble the RAID1 on the head (legs[0]'s node).
/// Mutates the stored volume as it goes; on error the partial progress is
/// persisted, assembly stays pending, and `next_assemble_after` holds off
/// the automatic retry for `recovery.cooldown_secs` (#7). Retrying resumes:
/// exports already made are kept, drive opens are idempotent, and an array
/// the head already holds over exactly these legs is adopted.
///
/// Takes the volume's claim, so it never runs beside a re-leg or another
/// assembly of the same volume; a busy volume is an error.
pub async fn assemble(state: &Arc<AppState>, name: &str) -> anyhow::Result<()> {
    if !claim(state, name) {
        anyhow::bail!("{name}: busy — an assembly or leg move is in flight");
    }
    let r = assemble_claimed(state, name).await;
    release(state, name);
    r
}

async fn assemble_claimed(state: &Arc<AppState>, name: &str) -> anyhow::Result<()> {
    let (mut vol, engines) = {
        let fed = state.fed.read().await;
        let vol = fed
            .volumes
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("volume {name:?} not found"))?;
        if vol.legs.len() < 2 {
            anyhow::bail!("{name}: single leg — nothing to assemble");
        }
        if vol.legs.iter().any(|l| l.state != LegState::Created || l.volume_id.is_none()) {
            anyhow::bail!("{name}: not every leg is created");
        }
        let mut engines = std::collections::BTreeMap::new();
        for l in &vol.legs {
            engines.insert(l.node.clone(), engine_of(state, &fed, &l.node)?);
        }
        (vol, engines)
    };
    let head = vol.legs[0].node.clone();
    let head_engine = engines.get(&head).expect("head engine").clone();

    let result = assemble_inner(state, &mut vol, &engines, &head, &head_engine).await;
    let ok = result.is_ok();
    vol.next_assemble_after = if ok {
        None
    } else {
        Some(SystemTime::now() + Duration::from_secs(state.config.recovery.cooldown_secs))
    };
    {
        let mut fed = state.fed.write().await;
        fed.volumes.insert(name.to_string(), vol);
        if ok {
            fed.revision += 1;
        }
    }
    if ok {
        crate::replicate::push_to_peers(state.clone());
    }
    state.persist().await;
    result
}

async fn assemble_inner(
    state: &Arc<AppState>,
    vol: &mut DistVolume,
    engines: &std::collections::BTreeMap<String, crate::engine::Engine>,
    head: &str,
    head_engine: &crate::engine::Engine,
) -> anyhow::Result<()> {
    // 1. Export every leg to the head alone (idempotent on the engine side:
    //    attach re-returns the namespace it already hot-added).
    let host = state.config.legs.host_nqn_for(head);
    for leg in vol.legs.iter_mut() {
        // Kept: an export for this head, or one from before #27 (no host;
        // its URI is what an array made then holds). One made for another
        // head is attached again for this one.
        if leg.export.as_ref().is_some_and(|x| x.host_nqn.as_deref().map_or(true, |h| h == host)) {
            continue;
        }
        let engine = engines.get(&leg.node).expect("leg engine");
        let vid = leg.volume_id.as_deref().expect("checked created");
        let master = leg.master_node.clone().unwrap_or_else(|| "localhost".into());
        let att = engine
            .attach_leg(vid, &master, leg.epoch, Some(&host))
            .await
            .map_err(|e| anyhow::anyhow!("{}: attach: {e:#}", leg.node))?;
        leg.export = Some(att);
    }
    // 2. Head opens each export as a drive.
    let mut drive_uuids = Vec::new();
    for leg in vol.legs.iter_mut() {
        let uri = leg.export.as_ref().expect("just exported").drive_uri();
        let uuid = head_engine
            .add_drive_idempotent(&uri)
            .await
            .map_err(|e| anyhow::anyhow!("{head}: open {uri}: {e:#}"))?;
        leg.drive_uuid = Some(uuid.clone());
        drive_uuids.push(uuid);
    }
    // 3. RAID1 across the legs — unless the head already holds it: a
    //    create whose response never came back (a timeout) did happen, and
    //    the engine would format a second array over the same members
    //    (stormblock#215).
    let uris: Vec<String> = vol
        .legs
        .iter()
        .map(|l| l.export.as_ref().expect("exported").drive_uri())
        .collect();
    let existing = head_engine
        .list_arrays()
        .await
        .map_err(|e| anyhow::anyhow!("{head}: list arrays: {e:#}"))?;
    let arr = match match_array(&existing, &uris) {
        ArrayMatch::Exact(arr) => {
            event(
                state,
                &vol.name,
                Severity::Info,
                format!(
                    "{}: adopted array {} on {head}, already built over these legs",
                    vol.name,
                    arr.get("id").and_then(|x| x.as_str()).unwrap_or("?")
                ),
            )
            .await;
            arr
        }
        ArrayMatch::Conflict { array, uri } => anyhow::bail!(
            "{head}: {uri} is already a member of array {array}, which is not this volume's; \
             not building a second array over it"
        ),
        ArrayMatch::None => head_engine
            .create_raid1(&drive_uuids)
            .await
            .map_err(|e| anyhow::anyhow!("{head}: create raid1: {e:#}"))?,
    };
    let array_id = arr
        .get("id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("{head}: array response without id: {arr}"))?
        .to_string();
    // Match members to legs by drive path; on a create, member order ==
    // drive_uuids order == leg order is the fallback.
    if let Some(members) = arr.get("members").and_then(|m| m.as_array()) {
        for (i, m) in members.iter().enumerate() {
            let Some(uuid) = m.get("uuid").and_then(|u| u.as_str()) else {
                continue;
            };
            let by_path = m
                .get("device_path")
                .and_then(|p| p.as_str())
                .and_then(|p| uris.iter().position(|u| u == p));
            if let Some(leg) = vol.legs.get_mut(by_path.unwrap_or(i)) {
                leg.member_uuid = Some(uuid.to_string());
            }
        }
    }
    vol.head = Some(head.to_string());
    vol.array_id = Some(array_id.clone());
    vol.assembly = AssemblyState::Assembled;
    // Resyncs onto later legs run at the volume's bandwidth class (#33).
    let _ = head_engine
        .set_rebuild_rate(&array_id, state.config.recovery.rate(vol.bandwidth_class))
        .await;
    event(
        state,
        &vol.name,
        Severity::Info,
        format!(
            "{}: RAID1 {array_id} on {head}, {} legs [{}]",
            vol.name,
            vol.legs.len(),
            vol.legs
                .iter()
                .map(|l| l.node.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
    .await;
    Ok(())
}

/// What the head already holds over a volume's leg drives.
#[derive(Debug, PartialEq)]
pub enum ArrayMatch {
    /// No array uses any of them: create one.
    None,
    /// An array whose members are exactly these drives: adopt it.
    Exact(serde_json::Value),
    /// An array holds one of them among other members: refuse.
    Conflict { array: String, uri: String },
}

/// Compare the head's arrays (`GET /api/v1/arrays` items) with the drive
/// URIs of a volume's legs, by each member's `device_path`.
pub fn match_array(arrays: &[serde_json::Value], uris: &[String]) -> ArrayMatch {
    let want: std::collections::BTreeSet<&str> = uris.iter().map(|s| s.as_str()).collect();
    for a in arrays {
        let paths: std::collections::BTreeSet<&str> = a
            .get("members")
            .and_then(|m| m.as_array())
            .map(|ms| ms.iter().filter_map(|m| m.get("device_path")?.as_str()).collect())
            .unwrap_or_default();
        if paths == want {
            return ArrayMatch::Exact(a.clone());
        }
        if let Some(uri) = paths.intersection(&want).next() {
            return ArrayMatch::Conflict {
                array: a.get("id").and_then(|x| x.as_str()).unwrap_or("?").to_string(),
                uri: uri.to_string(),
            };
        }
    }
    ArrayMatch::None
}

/// Tear down the head-side assembly (array + attached drives) and detach
/// leg exports. Best-effort; returns the error strings it hit. Leg volume
/// deletion stays with the caller.
pub async fn teardown(state: &Arc<AppState>, vol: &DistVolume) -> Vec<String> {
    let mut errors = Vec::new();
    let fed = state.fed.read().await;
    let head_engine = vol
        .head
        .as_ref()
        .and_then(|h| fed.nodes.get(h))
        .map(|n| state.engine_for(n));
    if let (Some(engine), Some(array_id)) = (&head_engine, &vol.array_id) {
        if let Err(e) = engine.delete_array(array_id).await {
            errors.push(format!("array {array_id}: {e:#}"));
        }
        for leg in vol.legs.iter().chain(vol.replacing.as_ref().map(|r| &r.leg)) {
            if let Some(uri) = leg.export.as_ref().map(|x| x.drive_uri()) {
                if let Err(e) = engine.delete_drive(&uri, true).await {
                    errors.push(format!("head drive {uri}: {e:#}"));
                }
            }
        }
    }
    for leg in vol.legs.iter().chain(vol.replacing.as_ref().map(|r| &r.leg)) {
        let (Some(vid), Some(_)) = (&leg.volume_id, &leg.export) else {
            continue;
        };
        if let Some(n) = fed.nodes.get(&leg.node).filter(|n| n.status.healthy) {
            let engine = state.engine_for(n);
            let master = leg.master_node.clone().unwrap_or_else(|| "localhost".into());
            if let Err(e) = engine.detach_volume(vid, &master).await {
                errors.push(format!("{} detach: {e:#}", leg.node));
            }
        }
    }
    errors
}

/// Where a moved leg may go: healthy, not already carrying a leg, and in a
/// distinct failure domain from every *staying* leg at the volume's rung.
pub fn move_target_candidates(
    fed: &crate::model::FedState,
    rungs: &[String],
    vol: &DistVolume,
    from: &str,
) -> Vec<crate::placement::Candidate> {
    let staying_domains: Vec<String> = vol
        .legs
        .iter()
        .filter(|l| l.node != from)
        .filter_map(|l| fed.nodes.get(&l.node))
        .map(|n| domain_at(&n.labels(), rungs, &vol.rung))
        .collect();
    fed.nodes
        .values()
        .filter(|n| n.status.healthy)
        .filter(|n| vol.legs.iter().all(|l| l.node != n.config.name))
        .filter(|n| {
            let d = domain_at(&n.labels(), rungs, &vol.rung);
            !staying_domains.contains(&d)
        })
        .map(|n| crate::placement::Candidate {
            name: n.config.name.clone(),
            labels: n.labels(),
            free_bytes: n.status.free_bytes,
            total_bytes: n.status.total_bytes,
        })
        .collect()
}

/// Claim the one-replacement-per-volume slot. The API, the reconciler and
/// a resumed wait all go through it, so a replacement is never started
/// twice.
pub(crate) fn claim(state: &AppState, name: &str) -> bool {
    state
        .replacing
        .lock()
        .expect("replacing lock")
        .insert(name.to_string())
}

pub(crate) fn release(state: &AppState, name: &str) {
    state.replacing.lock().expect("replacing lock").remove(name);
}

fn busy(state: &AppState, name: &str) -> bool {
    state.replacing.lock().expect("replacing lock").contains(name)
}

/// Move one leg on request (`POST /api/v1/volumes/{name}/move`).
pub async fn move_leg(
    state: &Arc<AppState>,
    name: &str,
    from: &str,
    to: Option<String>,
) -> anyhow::Result<String> {
    start_replacement(state, name, from, to, "operator move").await
}

/// Replace the leg on `from`: create + attach a new leg, add it as a RAID
/// member on the head, record the replacement on the volume, then hand off
/// to a background task that waits for the rebuild and retires the old
/// leg. Returns the target node. Operator moves and node-loss re-legs are
/// the same sequence; only the trigger and the reason differ.
pub async fn start_replacement(
    state: &Arc<AppState>,
    name: &str,
    from: &str,
    to: Option<String>,
    reason: &str,
) -> anyhow::Result<String> {
    if !claim(state, name) {
        anyhow::bail!("{name}: a leg replacement is already in progress");
    }
    match build_replacement(state, name, from, to, reason).await {
        Ok(target) => {
            let st = state.clone();
            let n = name.to_string();
            tokio::spawn(async move { finish_replacement(st, n).await });
            Ok(target)
        }
        Err(e) => {
            release(state, name);
            Err(e)
        }
    }
}

async fn build_replacement(
    state: &Arc<AppState>,
    name: &str,
    from: &str,
    to: Option<String>,
    reason: &str,
) -> anyhow::Result<String> {
    let (vol, target, target_engine, head_engine) = {
        let fed = state.fed.read().await;
        let vol = fed
            .volumes
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("volume {name:?} not found"))?;
        if !matches!(vol.assembly, AssemblyState::Assembled | AssemblyState::Degraded) {
            anyhow::bail!("{name}: not assembled — only assembled volumes move legs");
        }
        if vol.replacing.is_some() {
            anyhow::bail!("{name}: a leg replacement is already in progress");
        }
        if !vol.legs.iter().any(|l| l.node == from) {
            anyhow::bail!("{name}: no leg on {from:?}");
        }
        let head = vol.head.clone().ok_or_else(|| anyhow::anyhow!("{name}: no head"))?;
        let head_ok = fed.nodes.get(&head).map(|n| n.status.healthy).unwrap_or(false);
        if !head_ok {
            anyhow::bail!(
                "{name}: head {head} is unreachable — the array lives there; promote a surviving leg's node first (fence, then promote)"
            );
        }
        let rungs = &state.config.federation.rungs;
        let target = match to {
            Some(t) => {
                let n = fed
                    .nodes
                    .get(&t)
                    .ok_or_else(|| anyhow::anyhow!("target node {t:?} not in registry"))?;
                if !n.status.healthy {
                    anyhow::bail!("target node {t:?} is unhealthy");
                }
                if vol.legs.iter().any(|l| l.node == t) {
                    anyhow::bail!("target node {t:?} already carries a leg");
                }
                t
            }
            None => {
                let cands = move_target_candidates(&fed, rungs, &vol, from);
                crate::placement::plan(&cands, rungs, &vol.rung, 1, vol.size_bytes)
                    .map_err(|e| anyhow::anyhow!("no target: {e}"))?
                    .remove(0)
            }
        };
        let target_engine = engine_of(state, &fed, &target)?;
        let head_engine = engine_of(state, &fed, &head)?;
        (vol, target, target_engine, head_engine)
    };
    let head = vol.head.clone().expect("checked");
    let array_id = vol
        .array_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("{name}: no array id"))?;

    // New leg: volume, export, head drive, member. Undo what was done if a
    // later step fails, so a failed attempt leaves nothing behind.
    let created = target_engine
        .create_volume(&vol.name, vol.size_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("{target}: create leg: {e:#}"))?;
    let vid = created
        .get("id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("{target}: create returned no id"))?
        .to_string();
    let master =
        crate::engine::Engine::master_node_of(&created).unwrap_or_else(|| "localhost".into());
    let mut leg = Leg {
        node: target.clone(),
        volume_id: Some(vid),
        state: LegState::Created,
        message: Some("rebuilding".into()),
        master_node: Some(master),
        export: None,
        drive_uuid: None,
        member_uuid: None,
        epoch: None,
    };
    let wired: anyhow::Result<()> = async {
        let vid = leg.volume_id.clone().expect("set");
        let master = leg.master_node.clone().expect("set");
        let host = state.config.legs.host_nqn_for(&head);
        let att = target_engine
            .attach_leg(&vid, &master, None, Some(&host))
            .await
            .map_err(|e| anyhow::anyhow!("{target}: attach: {e:#}"))?;
        let uri = att.drive_uri();
        leg.export = Some(att);
        let drive = head_engine
            .add_drive_idempotent(&uri)
            .await
            .map_err(|e| anyhow::anyhow!("{head}: open leg drive: {e:#}"))?;
        leg.drive_uuid = Some(drive.clone());
        let member = head_engine
            .array_add_member(&array_id, &drive)
            .await
            .map_err(|e| anyhow::anyhow!("{head}: add member: {e:#}"))?;
        leg.member_uuid = Some(member);
        Ok(())
    }
    .await;
    if let Err(e) = wired {
        undo_leg(&head_engine, &array_id, &target_engine, &leg).await;
        return Err(e);
    }

    {
        let mut fed = state.fed.write().await;
        let Some(v) = fed.volumes.get_mut(name) else {
            drop(fed);
            undo_leg(&head_engine, &array_id, &target_engine, &leg).await;
            anyhow::bail!("{name}: deleted while its new leg was being built");
        };
        v.replacing = Some(Replacement {
            from: from.to_string(),
            leg,
            reason: reason.to_string(),
            started_at: SystemTime::now(),
        });
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    event(
        state,
        name,
        Severity::Info,
        format!("{name}: replacing leg {from} → {target} ({reason}); rebuild started on {head}"),
    )
    .await;
    Ok(target)
}

/// Best-effort removal of a new leg that is not going to be kept.
async fn undo_leg(
    head: &crate::engine::Engine,
    array_id: &str,
    target: &crate::engine::Engine,
    leg: &Leg,
) {
    if let Some(m) = &leg.member_uuid {
        let _ = head.array_remove_member(array_id, m).await;
    }
    if let Some(uri) = leg.export.as_ref().map(|x| x.drive_uri()) {
        let _ = head.delete_drive(&uri, true).await;
    }
    if let (Some(vid), Some(master)) = (&leg.volume_id, &leg.master_node) {
        if leg.export.is_some() {
            let _ = target.detach_volume(vid, master).await;
        }
        let _ = target.delete_volume(vid).await;
    }
}

/// Wait for the recorded replacement's member to rebuild, then retire the
/// old leg — or, if it never converges, undo the new one. Holds the
/// volume's claim (taken by the caller) and releases it at the end.
async fn finish_replacement(state: Arc<AppState>, name: String) {
    finish_inner(&state, &name).await;
    release(&state, &name);
}

async fn finish_inner(state: &Arc<AppState>, name: &str) {
    let (rep, head_engine, target_engine, array_id, head) = {
        let fed = state.fed.read().await;
        let Some(vol) = fed.volumes.get(name) else { return };
        let Some(rep) = vol.replacing.clone() else { return };
        let (Some(head), Some(array_id)) = (vol.head.clone(), vol.array_id.clone()) else {
            return;
        };
        let (Ok(he), Ok(te)) = (engine_of(state, &fed, &head), engine_of(state, &fed, &rep.leg.node))
        else {
            return;
        };
        (rep, he, te, array_id, head)
    };
    let member = rep.leg.member_uuid.clone().unwrap_or_default();
    let timeout = Duration::from_secs(state.config.recovery.rebuild_timeout_secs.max(1));
    let ok = wait_member_active(&head_engine, &array_id, &member, timeout).await;
    let from = rep.from.clone();
    let to = rep.leg.node.clone();

    if !ok {
        undo_leg(&head_engine, &array_id, &target_engine, &rep.leg).await;
        {
            let mut fed = state.fed.write().await;
            if let Some(v) = fed.volumes.get_mut(name) {
                v.replacing = None;
                v.next_releg_after = Some(
                    SystemTime::now() + Duration::from_secs(state.config.recovery.cooldown_secs),
                );
            }
            fed.revision += 1;
        }
        crate::replicate::push_to_peers(state.clone());
        state.persist().await;
        event(
            state,
            name,
            Severity::Error,
            format!(
                "{name}: new leg on {to} did not rebuild on {head} — undone, leg on {from} kept ({})",
                rep.reason
            ),
        )
        .await;
        return;
    }

    // Retire the old leg. Its node may be dead: every step is allowed to
    // fail, and a leg volume that cannot be deleted is recorded as an
    // orphan to reap when the node answers — the rebuild never waits on it.
    let old = {
        let fed = state.fed.read().await;
        fed.volumes
            .get(name)
            .and_then(|v| v.legs.iter().find(|l| l.node == from).cloned())
    };
    let mut notes = Vec::new();
    let mut orphan = None;
    if let Some(old) = &old {
        if let Some(mu) = &old.member_uuid {
            if let Err(e) = head_engine.array_remove_member(&array_id, mu).await {
                notes.push(format!("remove member: {e:#}"));
            }
        }
        if let Some(uri) = old.export.as_ref().map(|x| x.drive_uri()) {
            if let Err(e) = head_engine.delete_drive(&uri, true).await {
                notes.push(format!("head drive: {e:#}"));
            }
        }
        if let Some(vid) = &old.volume_id {
            let engine = {
                let fed = state.fed.read().await;
                fed.nodes
                    .get(&from)
                    .filter(|n| n.status.healthy)
                    .map(|n| state.engine_for(n))
            };
            let deleted = match engine {
                Some(engine) => {
                    let master = old.master_node.clone().unwrap_or_else(|| "localhost".into());
                    let _ = engine.detach_volume(vid, &master).await;
                    match engine.delete_volume(vid).await {
                        Ok(()) => true,
                        Err(e) => {
                            notes.push(format!("{from} delete volume: {e:#}"));
                            false
                        }
                    }
                }
                None => {
                    notes.push(format!("{from} unreachable — leg volume left to reap"));
                    false
                }
            };
            if !deleted {
                orphan = Some(Orphan {
                    node: from.clone(),
                    volume_id: vid.clone(),
                    master_node: old.master_node.clone(),
                    of_volume: name.to_string(),
                    reason: rep.reason.clone(),
                    since: SystemTime::now(),
                });
            }
        }
    }
    {
        let mut fed = state.fed.write().await;
        if let Some(o) = orphan {
            fed.orphans.push(o);
        }
        if let Some(v) = fed.volumes.get_mut(name) {
            let mut nl = rep.leg.clone();
            nl.message = None;
            match v.legs.iter_mut().find(|l| l.node == from) {
                Some(slot) => *slot = nl,
                None => v.legs.push(nl),
            }
            v.replacing = None;
            v.next_releg_after = None;
            v.assembly = if v.legs.iter().any(|l| l.state == LegState::Lost) {
                AssemblyState::Degraded
            } else {
                AssemblyState::Assembled
            };
        }
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    event(
        state,
        name,
        if notes.is_empty() { Severity::Info } else { Severity::Warning },
        if notes.is_empty() {
            format!("{name}: leg {from} → {to} complete ({})", rep.reason)
        } else {
            format!(
                "{name}: leg {from} → {to} complete ({}); cleanup pending: {}",
                rep.reason,
                notes.join("; ")
            )
        },
    )
    .await;
}

/// What the reconciler decided for one poll (pure, so it is testable).
#[derive(Debug, Default, PartialEq)]
pub struct Plan {
    /// (volume, node) legs newly marked lost.
    pub lost: Vec<(String, String)>,
    /// (volume, head) volumes whose head is lost: reported, not recovered.
    pub head_lost: Vec<(String, String)>,
    /// (volume, from) re-legs to start.
    pub start: Vec<(String, String)>,
    /// Volumes with a recorded replacement to resume waiting on.
    pub resume: Vec<String>,
    /// Pending volumes to retry assembling: every leg created, every leg
    /// node healthy, past `next_assemble_after` (#7).
    pub assemble: Vec<String>,
}

/// Mark legs on unhealthy nodes lost, degrade their volumes, and decide
/// which re-legs to start or resume. Mutates `fed`; returns the plan.
pub fn plan_recovery(
    fed: &mut crate::model::FedState,
    enabled: bool,
    now: SystemTime,
    busy: &dyn Fn(&str) -> bool,
) -> Plan {
    let mut plan = Plan::default();
    let healthy: std::collections::BTreeMap<String, bool> = fed
        .nodes
        .iter()
        .map(|(k, n)| (k.clone(), n.status.healthy))
        .collect();
    let mut changed = false;
    for vol in fed.volumes.values_mut() {
        if vol.assembly == AssemblyState::PendingEngineSupport {
            let ready = vol.legs.len() >= 2
                && vol.legs.iter().all(|l| {
                    l.state == LegState::Created
                        && l.volume_id.is_some()
                        && healthy.get(&l.node) == Some(&true)
                });
            if enabled
                && ready
                && !busy(&vol.name)
                && !vol.next_assemble_after.map(|t| now < t).unwrap_or(false)
            {
                plan.assemble.push(vol.name.clone());
            }
            continue;
        }
        if !matches!(vol.assembly, AssemblyState::Assembled | AssemblyState::Degraded) {
            continue;
        }
        for leg in vol.legs.iter_mut() {
            if leg.state == LegState::Created && healthy.get(&leg.node) != Some(&true) {
                leg.state = LegState::Lost;
                leg.message = Some(format!("node {} unreachable", leg.node));
                plan.lost.push((vol.name.clone(), leg.node.clone()));
                if vol.head.as_deref() == Some(leg.node.as_str()) {
                    plan.head_lost.push((vol.name.clone(), leg.node.clone()));
                }
                changed = true;
            }
        }
        let lost: Vec<String> = vol
            .legs
            .iter()
            .filter(|l| l.state == LegState::Lost)
            .map(|l| l.node.clone())
            .collect();
        if !lost.is_empty() && vol.assembly == AssemblyState::Assembled {
            vol.assembly = AssemblyState::Degraded;
            changed = true;
        }
        if vol.replacing.is_some() {
            if enabled && !busy(&vol.name) {
                plan.resume.push(vol.name.clone());
            }
            continue;
        }
        if !enabled || lost.is_empty() || busy(&vol.name) {
            continue;
        }
        let head_lost = match &vol.head {
            Some(h) => lost.contains(h) || healthy.get(h) != Some(&true),
            None => true,
        };
        if head_lost {
            continue;
        }
        if vol.next_releg_after.map(|t| now < t).unwrap_or(false) {
            continue;
        }
        plan.start.push((vol.name.clone(), lost[0].clone()));
    }
    if changed {
        fed.revision += 1;
    }
    plan
}

/// One reconcile pass, run after every poll: leg loss, re-legs, and
/// reaping orphans whose node answers again (#1).
pub async fn reconcile(state: &Arc<AppState>) {
    let now = SystemTime::now();
    let (plan, changed) = {
        let mut fed = state.fed.write().await;
        let before = fed.revision;
        let active = state
            .config
            .recovery
            .active(!state.config.replication.peers.is_empty());
        let plan = plan_recovery(&mut fed, active, now, &|n| {
            busy(state, n)
        });
        (plan, fed.revision != before)
    };
    if changed {
        crate::replicate::push_to_peers(state.clone());
        state.persist().await;
    }
    for (vol, node) in &plan.lost {
        event(
            state,
            vol,
            Severity::Warning,
            format!("{vol}: leg on {node} lost — node unreachable; volume degraded"),
        )
        .await;
    }
    for (vol, head) in &plan.head_lost {
        event(
            state,
            vol,
            Severity::Error,
            format!(
                "{vol}: head {head} lost — the array lives there; not re-legged — fence it and promote a surviving leg's node (automatic re-head is #14)"
            ),
        )
        .await;
    }
    for name in plan.resume {
        if claim(state, &name) {
            let st = state.clone();
            tokio::spawn(async move { finish_replacement(st, name).await });
        }
    }
    for (name, from) in plan.start {
        if let Err(e) = start_replacement(state, &name, &from, None, "node lost").await {
            let cooldown = state.config.recovery.cooldown_secs;
            {
                let mut fed = state.fed.write().await;
                if let Some(v) = fed.volumes.get_mut(&name) {
                    v.next_releg_after = Some(now + Duration::from_secs(cooldown));
                }
            }
            state.persist().await;
            event(
                state,
                &name,
                Severity::Error,
                format!("{name}: re-leg of lost leg on {from} failed, retry in {cooldown}s: {e:#}"),
            )
            .await;
        }
    }
    for name in plan.assemble {
        if claim(state, &name) {
            let st = state.clone();
            tokio::spawn(async move {
                let r = assemble_claimed(&st, &name).await;
                release(&st, &name);
                after_assembly_retry(&st, &name, r).await;
            });
        }
    }
    rejoin_heads(state).await;
    // Host withdrawals an engine has not taken yet (#51).
    let pending: std::collections::BTreeSet<String> = {
        let fed = state.fed.read().await;
        fed.volumes
            .values()
            .flat_map(|v| v.export.withdrawing.iter().map(|w| w.node.clone()))
            .filter(|n| fed.nodes.get(n).is_some_and(|x| x.status.healthy))
            .collect()
    };
    for node in pending {
        withdraw_pending_on(state, &node).await;
    }
    let closed = {
        let mut fed = state.fed.write().await;
        crate::head::expire_windows(&mut fed, now)
    };
    if !closed.is_empty() {
        crate::replicate::push_to_peers(state.clone());
        state.persist().await;
    }
    for (vol, target) in closed {
        event(
            state,
            &vol,
            Severity::Warning,
            format!("{vol}: dual-attach window for {target} expired — aborted, head unchanged"),
        )
        .await;
    }
    crate::head::reap_stale_heads(state).await;
    reap_orphans(state).await;
}

/// What reading the head again found for a volume whose head leg is
/// lost (#26).
#[derive(Debug, Clone, PartialEq)]
pub enum HeadReturn {
    /// The head's member is active: its leg is back. `assembled` when no
    /// other leg is still lost.
    Restored { assembled: bool },
    /// The array is there but the head's member is not active (its state,
    /// or "missing").
    NotActive(String),
    /// The head answers but no longer holds the array (#15).
    ArrayGone,
}

/// Volumes whose head leg is lost while the head answers again: degraded,
/// not fenced (a fence hands the head role on, #33), not busy. Returns
/// (volume, head, array id).
pub fn head_rejoin_candidates(
    fed: &crate::model::FedState,
    busy: &dyn Fn(&str) -> bool,
) -> Vec<(String, String, String)> {
    fed.volumes
        .values()
        .filter(|v| v.assembly == AssemblyState::Degraded && !v.fenced && !busy(&v.name))
        .filter_map(|v| {
            let head = v.head.clone()?;
            let array = v.array_id.clone()?;
            fed.nodes.get(&head).filter(|n| n.status.healthy)?;
            v.legs
                .iter()
                .any(|l| l.node == head && l.state == LegState::Lost)
                .then_some((v.name.clone(), head, array))
        })
        .collect()
}

/// Apply a fresh reading of the head's array (`None`: the head answered
/// 404) to a volume whose head leg is lost. Returns what was found and
/// whether the volume's record changed — a finding that repeats the last
/// one changes nothing, so it is reported once.
pub fn apply_head_reading(
    vol: &mut DistVolume,
    reading: Option<&crate::head::ArrayReading>,
) -> Option<(HeadReturn, bool)> {
    let head = vol.head.clone()?;
    let array_id = vol.array_id.clone()?;
    let idx = vol
        .legs
        .iter()
        .position(|l| l.node == head && l.state == LegState::Lost)?;
    let (found, message) = match reading {
        None => (
            HeadReturn::ArrayGone,
            format!("head {head} answers but no longer holds array {array_id} (engine restart, #15)"),
        ),
        Some(r) if r.head != head || r.array_id != array_id => return None,
        Some(r) => match crate::head::member_of(&vol.legs[idx], r) {
            Some(m) if m.state == "active" => {
                let leg = &mut vol.legs[idx];
                leg.state = LegState::Created;
                leg.message = None;
                let assembled = !vol.legs.iter().any(|l| l.state == LegState::Lost);
                if assembled {
                    vol.assembly = AssemblyState::Assembled;
                }
                return Some((HeadReturn::Restored { assembled }, true));
            }
            m => {
                let st = m.map(|m| m.state.clone()).unwrap_or_else(|| "missing".into());
                (
                    HeadReturn::NotActive(st.clone()),
                    format!("head {head} answers; its member of array {array_id} is {st}"),
                )
            }
        },
    };
    let leg = &mut vol.legs[idx];
    let changed = leg.message.as_deref() != Some(message.as_str());
    leg.message = Some(message);
    Some((found, changed))
}

/// Bring a lost head leg back when its head answers again and still holds
/// the array with the head's member active (#26). A head that stalled past
/// `poll.fail_threshold` otherwise left its volume degraded for good: the
/// head leg is never re-legged (re-head is #14).
async fn rejoin_heads(state: &Arc<AppState>) {
    let due: Vec<(String, String, String, crate::engine::Engine)> = {
        let fed = state.fed.read().await;
        head_rejoin_candidates(&fed, &|n| busy(state, n))
            .into_iter()
            .filter_map(|(v, h, a)| {
                let e = engine_of(state, &fed, &h).ok()?;
                Some((v, h, a, e))
            })
            .collect()
    };
    for (name, head, array_id, engine) in due {
        // This poll's reading of the head, else read it here — a failed
        // read in the poll could be a 404, which is a finding.
        let polled = state.heads.read().await.get(&name).cloned();
        let reading = match polled.filter(|r| {
            r.source == crate::head::SyncSource::Head && r.head == head && r.array_id == array_id
        }) {
            Some(r) => Some(r),
            None => match engine.find_array(&array_id).await {
                Ok(v) => v.map(|v| crate::head::parse_array(&head, &array_id, &v)),
                Err(_) => continue,
            },
        };
        let outcome = {
            let mut fed = state.fed.write().await;
            let out = fed
                .volumes
                .get_mut(&name)
                .and_then(|v| apply_head_reading(v, reading.as_ref()));
            if matches!(out, Some((_, true))) {
                fed.revision += 1;
            }
            out
        };
        let Some((found, true)) = outcome else { continue };
        crate::replicate::push_to_peers(state.clone());
        state.persist().await;
        let (sev, msg) = match found {
            HeadReturn::Restored { assembled: true } => (
                Severity::Info,
                format!("{name}: head {head} answers again and its member of array {array_id} is active — leg back, volume assembled"),
            ),
            HeadReturn::Restored { assembled: false } => (
                Severity::Info,
                format!("{name}: head {head} answers again and its member of array {array_id} is active — leg back; another leg is still lost"),
            ),
            HeadReturn::NotActive(st) => (
                Severity::Warning,
                format!("{name}: head {head} answers again but its member of array {array_id} is {st} — still degraded"),
            ),
            HeadReturn::ArrayGone => (
                Severity::Warning,
                format!("{name}: head {head} answers again but no longer holds array {array_id} (engine restart, #15) — still degraded"),
            ),
        };
        event(state, &name, sev, msg).await;
    }
}

/// Report an assembly retry, and serve the volume once it is assembled.
pub async fn after_assembly_retry(state: &Arc<AppState>, name: &str, r: anyhow::Result<()>) {
    match r {
        Ok(()) => {
            let _ = publish(state, name).await;
        }
        Err(e) => {
            let cooldown = state.config.recovery.cooldown_secs;
            event(
                state,
                name,
                Severity::Error,
                format!(
                    "{name}: assembly retry failed, next in {cooldown}s \
                     (or POST /api/v1/volumes/{name}/assemble): {e:#}"
                ),
            )
            .await;
        }
    }
}

/// Delete orphaned leg volumes whose node answers again.
pub async fn reap_orphans(state: &Arc<AppState>) {
    let due: Vec<(Orphan, crate::engine::Engine)> = {
        let fed = state.fed.read().await;
        fed.orphans
            .iter()
            .filter_map(|o| {
                fed.nodes
                    .get(&o.node)
                    .filter(|n| n.status.healthy)
                    .map(|n| (o.clone(), state.engine_for(n)))
            })
            .collect()
    };
    for (o, engine) in due {
        let master = o.master_node.clone().unwrap_or_else(|| "localhost".into());
        let _ = engine.detach_volume(&o.volume_id, &master).await;
        if engine.delete_volume(&o.volume_id).await.is_ok() {
            {
                let mut fed = state.fed.write().await;
                fed.orphans
                    .retain(|x| !(x.node == o.node && x.volume_id == o.volume_id));
                fed.revision += 1;
            }
            crate::replicate::push_to_peers(state.clone());
            state.persist().await;
            event(
                state,
                &o.of_volume,
                Severity::Info,
                format!(
                    "{}: reaped orphaned leg {} on {} (left by {})",
                    o.of_volume, o.volume_id, o.node, o.reason
                ),
            )
            .await;
        }
    }
}

/// Suffix of the consumer volume carved on an assembled volume's array.
/// Distinct from the leg name, because a /v1 create is name-idempotent and
/// the head carries a leg named after the volume itself.
pub const MIRROR_SUFFIX: &str = "-mirror";

/// Where a volume is served and which engine volume that is: the only leg
/// for a single-leg volume, the pinned consumer volume on the head for an
/// assembled one (`None`: not created yet).
struct ServeAt {
    node: String,
    engine: crate::engine::Engine,
    volume_id: Option<String>,
    master_node: Option<String>,
    array_id: Option<String>,
}

fn serve_at(
    state: &Arc<AppState>,
    fed: &crate::model::FedState,
    vol: &DistVolume,
) -> anyhow::Result<ServeAt> {
    let name = &vol.name;
    match vol.assembly {
        AssemblyState::SingleLeg => {
            let leg = vol
                .legs
                .first()
                .filter(|l| l.state == LegState::Created && l.volume_id.is_some())
                .ok_or_else(|| anyhow::anyhow!("{name}: its leg is not created"))?;
            Ok(ServeAt {
                node: leg.node.clone(),
                engine: engine_of(state, fed, &leg.node)?,
                volume_id: leg.volume_id.clone(),
                master_node: leg.master_node.clone(),
                array_id: None,
            })
        }
        AssemblyState::Assembled | AssemblyState::Degraded => {
            let head = vol.head.clone().ok_or_else(|| anyhow::anyhow!("{name}: no head"))?;
            let array_id = vol
                .array_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("{name}: no array id"))?;
            // A consumer volume already made on this head is reused.
            let (volume_id, master_node) = if vol.export.node.as_deref() == Some(head.as_str()) {
                (vol.export.volume_id.clone(), vol.export.master_node.clone())
            } else {
                (None, None)
            };
            Ok(ServeAt {
                engine: engine_of(state, fed, &head)?,
                node: head,
                volume_id,
                master_node,
                array_id: Some(array_id),
            })
        }
        AssemblyState::PendingEngineSupport => {
            anyhow::bail!("{name}: not assembled — nothing to serve until the mirror exists")
        }
    }
}

/// Publish a volume to consumers, or republish it (#2): make sure the
/// served volume exists, attach it for NVMe-TCP, and record the
/// coordinates. Idempotent — the engine re-returns an attach it already
/// made — so it is also how a republish learns whether the coordinates
/// changed. A failure is recorded on the volume (`export.state = failed`)
/// and returned.
///
/// With consumer hosts named (#51), the volume is served to each of them
/// from its own subsystem and not on the shared one (a closed engine,
/// stormblock#210, admits no host there); every re-serve (move, promote,
/// recovery) serves them all again.
pub async fn publish(state: &Arc<AppState>, name: &str) -> anyhow::Result<crate::model::Export> {
    publish_inner(state, name).await.map(|(ex, _)| ex)
}

/// The served volume's engine-local id on `at`: what the per-host attach
/// and withdraw take. Recorded once known.
async fn served_local_id(at: &ServeAt, vol: &DistVolume, volume_id: &str, adopted: bool) -> anyhow::Result<String> {
    if adopted {
        // Came across with the array: no /v1 record, the id is the engine's.
        return Ok(volume_id.to_string());
    }
    if vol.export.node.as_deref() == Some(at.node.as_str())
        && vol.export.volume_id.as_deref() == Some(volume_id)
    {
        if let Some(l) = &vol.export.local_id {
            return Ok(l.clone());
        }
    }
    at.engine
        .local_volume_id(&served_name(vol))
        .await
        .map_err(|e| anyhow::anyhow!("{}: {e:#}", at.node))
}

/// The served volume's name on its engine: the mirror, or the only leg.
fn served_name(vol: &DistVolume) -> String {
    match vol.assembly {
        AssemblyState::SingleLeg => vol.name.clone(),
        _ => format!("{}{MIRROR_SUFFIX}", vol.name),
    }
}

/// [`publish`], plus each host's DH-HMAC-CHAP secret as the engine
/// answered it — for the caller that asked for a host, never stored.
async fn publish_inner(
    state: &Arc<AppState>,
    name: &str,
) -> anyhow::Result<(crate::model::Export, BTreeMap<String, String>)> {
    let ready = {
        let fed = state.fed.read().await;
        let vol = fed
            .volumes
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("volume {name:?} not found"))?;
        // A delete began taking the served volume down (#24): the engine
        // may no longer have it, and attaching it would 404 for ever.
        if vol.export.state == crate::model::ExportState::Revoking {
            anyhow::bail!(
                "{name}: being deleted — a delete stopped while revoking its export ({}); DELETE it again",
                vol.export.message.as_deref().unwrap_or("no message")
            );
        }
        // Nothing to serve yet (not assembled): refused, record untouched.
        let at = serve_at(state, &fed, &vol)?;
        if fed.nodes.get(&at.node).map(|n| n.status.healthy).unwrap_or(false) {
            Ok((vol, at))
        } else {
            Err((
                at.node.clone(),
                anyhow::anyhow!("{name}: {} is unreachable — cannot serve from it", at.node),
            ))
        }
    };
    let (vol, at) = match ready {
        Ok(r) => r,
        Err((node, e)) => {
            // Recorded like any other failure, so the create response and
            // the events say why, and the node's recovery retries it.
            let recorded = {
                let mut fed = state.fed.write().await;
                match fed.volumes.get_mut(name) {
                    Some(v) => {
                        v.export.state = crate::model::ExportState::Failed;
                        v.export.message = Some(format!("{e:#}"));
                        v.export.node = Some(node);
                        fed.revision += 1;
                        true
                    }
                    None => false,
                }
            };
            if recorded {
                crate::replicate::push_to_peers(state.clone());
                state.persist().await;
                event(state, name, Severity::Error, format!("{name}: export failed: {e:#}")).await;
            }
            return Err(e);
        }
    };
    let mut volume_id = at.volume_id.clone();
    let mut master_node = at.master_node.clone();
    // Served volume that came across with the array on a promote (#33): no
    // /v1 record on this head, and never replaced by a new, empty one.
    let adopted = vol.export.adopted && vol.export.node.as_deref() == Some(at.node.as_str());
    let hosts = vol.export.hosts.clone();
    let per_host = vol.export.per_host || !hosts.is_empty();
    // The shared attach, when no host is named; `None` when serving hosts.
    let mut result: anyhow::Result<Option<crate::engine::AttachedLeg>> = async {
        if adopted {
            let vid = volume_id.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "{}: the served volume did not come across with the array — not creating an empty one",
                    at.node
                )
            })?;
            if per_host {
                return Ok(None);
            }
            return at.engine.attach_any(&vid).await.map(Some).map_err(|e| anyhow::anyhow!("{}: {e:#}", at.node));
        }
        if volume_id.is_none() {
            let array_id = at.array_id.as_deref().expect("assembled");
            let created = at
                .engine
                .create_pinned_volume(&format!("{name}{MIRROR_SUFFIX}"), vol.size_bytes, array_id)
                .await
                .map_err(|e| anyhow::anyhow!("{}: {e:#}", at.node))?;
            let id = created
                .get("id")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("{}: create returned no id", at.node))?
                .to_string();
            if vol.legs.iter().any(|l| l.volume_id.as_deref() == Some(id.as_str())) {
                anyhow::bail!("{}: the engine answered with a leg ({id}), not a volume on the array", at.node);
            }
            volume_id = Some(id);
            master_node = crate::engine::Engine::master_node_of(&created);
        }
        if per_host {
            return Ok(None);
        }
        let vid = volume_id.clone().expect("set");
        let master = master_node.clone().unwrap_or_else(|| "localhost".into());
        at.engine
            .attach_volume(&vid, &master)
            .await
            .map(Some)
            .map_err(|e| anyhow::anyhow!("{}: {e:#}", at.node))
    }
    .await;

    // Serve each named host (#51).
    let mut local_id = None;
    let mut served: BTreeMap<String, crate::model::HostServe> = BTreeMap::new();
    let mut secrets = BTreeMap::new();
    let mut host_failures = Vec::new();
    if result.is_ok() && !hosts.is_empty() {
        let vid = volume_id.clone().expect("set");
        match served_local_id(&at, &vol, &vid, adopted).await {
            Err(e) => result = Err(e),
            Ok(lid) => {
                for h in &hosts {
                    let mut rec = h.clone();
                    match at.engine.attach_for_host(&lid, &h.host_nqn, h.dhchap).await {
                        Ok((coords, secret)) => {
                            rec.coordinates_changed = h.coordinates.as_ref().is_some_and(|c| *c != coords);
                            rec.coordinates = Some(coords);
                            rec.served_at = Some(SystemTime::now());
                            rec.message = None;
                            if let Some(sec) = secret {
                                secrets.insert(h.host_nqn.clone(), sec);
                            }
                        }
                        Err(e) => {
                            let m = format!("{}: {e:#}", at.node);
                            host_failures.push(format!("{}: {m}", h.host_nqn));
                            rec.message = Some(m);
                        }
                    }
                    served.insert(h.host_nqn.clone(), rec);
                }
                local_id = Some(lid);
            }
        }
    }
    if result.is_ok() && !host_failures.is_empty() {
        result = Err(anyhow::anyhow!("serving {}", host_failures.join("; ")));
    }

    let (export, previous) = {
        let mut fed = state.fed.write().await;
        let Some(v) = fed.volumes.get_mut(name) else {
            anyhow::bail!("{name}: deleted while it was being published");
        };
        let previous = v.export.clone();
        // Hosts as they are now (one may have been asked for or withdrawn
        // while this ran), with what this serve found for each.
        let hosts_now: Vec<crate::model::HostServe> = previous
            .hosts
            .iter()
            .map(|h| match served.get(&h.host_nqn) {
                Some(r) => crate::model::HostServe { dhchap: h.dhchap || r.dhchap, ..r.clone() },
                None => h.clone(),
            })
            .collect();
        let same_served = previous.node.as_deref() == Some(at.node.as_str()) && previous.volume_id == volume_id;
        let mut ex = crate::model::Export {
            volume_id: volume_id.clone(),
            node: Some(at.node.clone()),
            master_node: master_node.clone(),
            adopted,
            per_host: per_host || previous.per_host,
            local_id: local_id.or_else(|| previous.local_id.clone().filter(|_| same_served)),
            withdrawing: previous.withdrawing.clone(),
            ..Default::default()
        };
        match &result {
            Ok(Some(coords)) => {
                ex.state = crate::model::ExportState::Published;
                // Against the last coordinates handed out, published or not:
                // a consumer may still hold them after a failed attempt.
                ex.coordinates_changed =
                    previous.coordinates.as_ref().is_some_and(|c| c != coords);
                ex.coordinates = Some(coords.clone());
                ex.published_at = Some(SystemTime::now());
                if v.assembly == AssemblyState::SingleLeg {
                    if let Some(leg) = v.legs.first_mut() {
                        leg.export = Some(coords.clone());
                    }
                }
            }
            Ok(None) => {
                // Served per host: no shared coordinates to hand out.
                ex.state = crate::model::ExportState::Published;
                ex.coordinates_changed = hosts_now.iter().any(|h| h.coordinates_changed);
                ex.published_at = Some(SystemTime::now());
            }
            Err(e) => {
                ex.state = crate::model::ExportState::Failed;
                ex.message = Some(format!("{e:#}"));
                ex.coordinates = previous.coordinates.clone();
            }
        }
        ex.hosts = hosts_now;
        v.export = ex.clone();
        fed.revision += 1;
        (ex, previous)
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    let what = |changed: bool| {
        if changed {
            "republished with NEW coordinates — consumers must reconnect"
        } else if previous.state != crate::model::ExportState::Published {
            "published"
        } else {
            "republished, coordinates unchanged"
        }
    };
    match &result {
        Ok(Some(c)) => {
            let w = what(export.coordinates_changed);
            event(state, name, Severity::Info, format!("{name}: {w} on {} at {}", at.node, c.drive_uri())).await;
        }
        Ok(None) => {
            let w = what(export.coordinates_changed);
            let to: Vec<&str> = export.hosts.iter().map(|h| h.host_nqn.as_str()).collect();
            let to = if to.is_empty() { "no host".to_string() } else { to.join(", ") };
            event(state, name, Severity::Info, format!("{name}: {w} on {} to {to}", at.node)).await;
        }
        Err(_) => {
            let e = export.message.clone().unwrap_or_default();
            event(state, name, Severity::Error, format!("{name}: export failed: {e}")).await;
        }
    }
    result.map(|_| (export, secrets))
}

/// A host NQN as stormblock takes one (`nqn.…`, at most 223 bytes).
pub fn valid_host_nqn(h: &str) -> bool {
    let h = h.trim();
    h.starts_with("nqn.") && h.len() <= 223 && !h.chars().any(char::is_whitespace)
}

/// Serve a volume to one consumer host (#51): record the host, then serve
/// every recorded host (the publish). Returns that host's record and its
/// DH-HMAC-CHAP secret when it has one. Idempotent; `dhchap` once asked for
/// stays on (the engine never drops a host's secret either).
pub async fn serve_host(
    state: &Arc<AppState>,
    name: &str,
    host_nqn: &str,
    dhchap: bool,
) -> anyhow::Result<(crate::model::HostServe, Option<String>)> {
    let host_nqn = host_nqn.trim();
    if !valid_host_nqn(host_nqn) {
        anyhow::bail!("{host_nqn:?} is not a host NQN (nqn.…, at most 223 bytes)");
    }
    {
        let mut fed = state.fed.write().await;
        let v = fed
            .volumes
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("volume {name:?} not found"))?;
        match v.export.hosts.iter_mut().find(|h| h.host_nqn == host_nqn) {
            Some(h) => h.dhchap |= dhchap,
            None => v.export.hosts.push(crate::model::HostServe {
                host_nqn: host_nqn.to_string(),
                dhchap,
                ..Default::default()
            }),
        }
        v.export.per_host = true;
        // Asked for again: no withdrawal of it is pending any more.
        v.export.withdrawing.retain(|w| w.host_nqn != host_nqn);
        fed.revision += 1;
    }
    let (ex, mut secrets) = match publish_inner(state, name).await {
        Ok(r) => r,
        Err(e) => {
            // This host's own failure says more than the whole export's.
            let why = state
                .fed
                .read()
                .await
                .volumes
                .get(name)
                .and_then(|v| v.export.hosts.iter().find(|h| h.host_nqn == host_nqn).and_then(|h| h.message.clone()));
            return Err(match why {
                Some(m) => anyhow::anyhow!("{name}: serving {host_nqn}: {m}"),
                None => e,
            });
        }
    };
    let rec = ex
        .hosts
        .iter()
        .find(|h| h.host_nqn == host_nqn)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{name}: {host_nqn} was withdrawn while it was being served"))?;
    Ok((rec, secrets.remove(host_nqn)))
}

/// What a withdrawal did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Withdrawn {
    /// The serving engine no longer serves it to that host.
    Done,
    /// The engine does not answer: withdrawn there when it does.
    Pending,
    /// Nothing was served there (no export yet).
    NothingServed,
}

/// Stop serving a volume to one consumer host (#51). The host leaves the
/// record at once, so no re-serve brings it back; the engine withdraws it
/// now, or when it answers again.
pub async fn withdraw_host(state: &Arc<AppState>, name: &str, host_nqn: &str) -> anyhow::Result<Withdrawn> {
    let host_nqn = host_nqn.trim().to_string();
    let (node, local_id, served) = {
        let mut fed = state.fed.write().await;
        let v = fed
            .volumes
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("volume {name:?} not found"))?;
        v.export.hosts.retain(|h| h.host_nqn != host_nqn);
        let out = (v.export.node.clone(), v.export.local_id.clone(), served_name(v));
        fed.revision += 1;
        out
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    let Some(node) = node else {
        return Ok(Withdrawn::NothingServed);
    };
    let pending = crate::model::PendingWithdraw { node: node.clone(), local_id, name: served, host_nqn: host_nqn.clone() };
    let outcome = withdraw_on_engine(state, &pending).await;
    let result = match &outcome {
        Ok(()) => Withdrawn::Done,
        Err(_) => {
            let mut fed = state.fed.write().await;
            if let Some(v) = fed.volumes.get_mut(name) {
                if !v.export.withdrawing.contains(&pending) {
                    v.export.withdrawing.push(pending.clone());
                }
            }
            fed.revision += 1;
            Withdrawn::Pending
        }
    };
    if result == Withdrawn::Pending {
        crate::replicate::push_to_peers(state.clone());
        state.persist().await;
    }
    let msg = match &outcome {
        Ok(()) => format!("{name}: no longer served to {host_nqn} on {node}"),
        Err(e) => format!("{name}: withdrawal of {host_nqn} on {node} pending until it answers: {e:#}"),
    };
    let sev = if outcome.is_ok() { Severity::Info } else { Severity::Warning };
    event(state, name, sev, msg).await;
    Ok(result)
}

async fn withdraw_on_engine(state: &Arc<AppState>, w: &crate::model::PendingWithdraw) -> anyhow::Result<()> {
    let engine = {
        let fed = state.fed.read().await;
        fed.nodes
            .get(&w.node)
            .filter(|n| n.status.healthy)
            .map(|n| state.engine_for(n))
            .ok_or_else(|| anyhow::anyhow!("{} is unreachable", w.node))?
    };
    let id = match &w.local_id {
        Some(id) => id.clone(),
        None => match engine.local_volume_id(&w.name).await {
            Ok(id) => id,
            // The served volume is gone from that engine: nothing to withdraw.
            Err(e) if e.to_string().starts_with("no engine volume") => return Ok(()),
            Err(e) => return Err(e),
        },
    };
    engine.withdraw_host(&id, &w.host_nqn).await
}

/// Withdrawals waiting for `node` (#51), done now that it answers.
async fn withdraw_pending_on(state: &Arc<AppState>, node: &str) {
    let due: Vec<(String, crate::model::PendingWithdraw)> = {
        let fed = state.fed.read().await;
        fed.volumes
            .values()
            .flat_map(|v| v.export.withdrawing.iter().filter(|w| w.node == node).map(|w| (v.name.clone(), w.clone())))
            .collect()
    };
    for (name, w) in due {
        if withdraw_on_engine(state, &w).await.is_ok() {
            {
                let mut fed = state.fed.write().await;
                if let Some(v) = fed.volumes.get_mut(&name) {
                    v.export.withdrawing.retain(|x| x != &w);
                }
                fed.revision += 1;
            }
            crate::replicate::push_to_peers(state.clone());
            state.persist().await;
            event(state, &name, Severity::Info, format!("{name}: no longer served to {} on {node}", w.host_nqn)).await;
        }
    }
}

/// Stop serving a volume before it is torn down (#2): detach the consumer
/// volume and delete it, so the head's array can go (a dedicated array
/// refuses deletion while a volume is pinned to it). A single-leg volume's
/// export is its leg, which the leg teardown detaches. A head that cannot
/// be reached is not an error — its array goes with its legs.
pub async fn revoke(state: &Arc<AppState>, vol: &DistVolume) -> anyhow::Result<()> {
    let (Some(node), Some(vid)) = (&vol.export.node, &vol.export.volume_id) else {
        return Ok(());
    };
    if vol.legs.iter().any(|l| l.volume_id.as_ref() == Some(vid)) {
        return Ok(());
    }
    let engine = {
        let fed = state.fed.read().await;
        match fed.nodes.get(node).filter(|n| n.status.healthy) {
            Some(n) => state.engine_for(n),
            None => return Ok(()),
        }
    };
    // From here the engine may delete the served volume even if its answer
    // never comes back (#24): recorded first, so nothing attaches it again.
    set_export_state(state, &vol.name, crate::model::ExportState::Revoking, Some("delete in progress".into())).await;
    let r = async {
        if vol.export.adopted {
            let _ = engine.detach_any(vid).await;
            return engine.delete_any_volume(vid).await;
        }
        let master = vol.export.master_node.clone().unwrap_or_else(|| "localhost".into());
        if vol.export.coordinates.is_some() || !vol.export.hosts.is_empty() {
            // Best-effort: the delete below is what must succeed.
            let _ = engine.detach_volume(vid, &master).await;
        }
        engine.delete_volume(vid).await
    }
    .await
    .map_err(|e| anyhow::anyhow!("{node}: consumer volume {vid}: {e:#}"));
    if let Err(e) = &r {
        set_export_state(state, &vol.name, crate::model::ExportState::Revoking, Some(format!("{e:#}"))).await;
    }
    r
}

async fn set_export_state(state: &Arc<AppState>, name: &str, st: crate::model::ExportState, message: Option<String>) {
    {
        let mut fed = state.fed.write().await;
        let Some(v) = fed.volumes.get_mut(name) else { return };
        v.export.state = st;
        v.export.message = message;
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
}

/// Republish every published or failed export served from `node` — run
/// when the node's engine answers again after being unreachable, since a
/// restarted engine may hand out different coordinates, and a publish that
/// failed while the node was unreachable can now succeed.
pub async fn republish_on(state: &Arc<AppState>, node: &str) {
    withdraw_pending_on(state, node).await;
    let names: Vec<String> = {
        let fed = state.fed.read().await;
        fed.volumes
            .values()
            .filter(|v| {
                matches!(
                    v.export.state,
                    crate::model::ExportState::Published | crate::model::ExportState::Failed
                ) && v.export.node.as_deref() == Some(node)
            })
            .map(|v| v.name.clone())
            .collect()
    };
    for name in names {
        let _ = publish(state, &name).await;
    }
}

/// Poll the head's array until the member reports active. False on timeout
/// or persistent errors.
async fn wait_member_active(
    engine: &crate::engine::Engine,
    array_id: &str,
    member_uuid: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        if let Ok(arr) = engine.get_array(array_id).await {
            let st = arr
                .get("members")
                .and_then(|m| m.as_array())
                .and_then(|ms| {
                    ms.iter()
                        .find(|m| m.get("uuid").and_then(|u| u.as_str()) == Some(member_uuid))
                })
                .and_then(|m| m.get("state").and_then(|s| s.as_str()).map(|s| s.to_lowercase()));
            match st.as_deref() {
                Some("active") => return true,
                Some("failed") => return false,
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::model::{FedState, Node, NodeSource, NodeStatus};
    use std::time::SystemTime;

    fn node(name: &str, rack: &str, free: u64) -> Node {
        let mut config: NodeConfig = toml::from_str(&format!(
            r#"name = "{name}"
               engine_url = "http://{name}:9090""#
        ))
        .unwrap();
        config.labels.insert("rack".into(), rack.into());
        let mut status = NodeStatus::new(NodeSource::Static);
        status.healthy = true;
        status.free_bytes = free;
        status.total_bytes = 100;
        Node { config, status }
    }

    fn vol(legs: &[&str], rung: &str) -> DistVolume {
        DistVolume {
            name: "v".into(),
            size_bytes: 10,
            pool: None,
            replicas: legs.len() as u32,
            rung: rung.into(),
            legs: legs
                .iter()
                .map(|n| Leg {
                    node: n.to_string(),
                    volume_id: Some("vol-x".into()),
                    state: LegState::Created,
                    message: None,
                    master_node: None,
                    export: None,
                    drive_uuid: None,
                    member_uuid: None,
                    epoch: None,
                })
                .collect(),
            assembly: AssemblyState::Assembled,
            head: Some(legs[0].to_string()),
            array_id: Some("arr".into()),
            created_at: SystemTime::now(),
            replacing: None,
            next_releg_after: None,
            next_assemble_after: None,
            export: Default::default(),
            epoch: 1,
            fenced: false,
            bandwidth_class: Default::default(),
            dual_attach: None,
        }
    }

    #[test]
    fn move_targets_exclude_leg_nodes_and_staying_domains() {
        let rungs: Vec<String> = ["rack", "node"].iter().map(|s| s.to_string()).collect();
        let mut fed = FedState::default();
        for (n, r) in [("a", "r1"), ("b", "r2"), ("c", "r2"), ("d", "r3"), ("e", "r3")] {
            fed.nodes.insert(n.into(), node(n, r, 50));
        }
        fed.nodes.get_mut("e").unwrap().status.healthy = false;

        let v = vol(&["a", "b"], "rack");
        // Moving the leg off b: staying leg is a (r1). c shares b's old rack
        // r2 — allowed (b is leaving). d (r3) allowed. e unhealthy. a carries
        // a leg already.
        let c = move_target_candidates(&fed, &rungs, &v, "b");
        let names: Vec<&str> = c.iter().map(|x| x.name.as_str()).collect();
        assert!(names.contains(&"c"));
        assert!(names.contains(&"d"));
        assert!(!names.contains(&"a"), "already carries a leg");
        assert!(!names.contains(&"e"), "unhealthy");

        // Moving off a instead: staying leg is b (r2) — c (r2) now collides.
        let c2 = move_target_candidates(&fed, &rungs, &v, "a");
        let names2: Vec<&str> = c2.iter().map(|x| x.name.as_str()).collect();
        assert!(!names2.contains(&"c"), "same rack as the staying leg");
        assert!(names2.contains(&"d"));
    }

    fn fed_with(vol_legs: &[&str], unhealthy: &[&str]) -> FedState {
        let mut fed = FedState::default();
        for (n, r) in [("a", "r1"), ("b", "r2"), ("c", "r3")] {
            let mut nd = node(n, r, 50);
            nd.status.healthy = !unhealthy.contains(&n);
            fed.nodes.insert(n.into(), nd);
        }
        fed.volumes.insert("v".into(), vol(vol_legs, "rack"));
        fed
    }

    #[test]
    fn lost_leg_degrades_and_starts_one_releg() {
        let mut fed = fed_with(&["a", "b"], &["b"]);
        let now = SystemTime::now();
        let p = plan_recovery(&mut fed, true, now, &|_| false);
        assert_eq!(p.lost, vec![("v".to_string(), "b".to_string())]);
        assert_eq!(p.start, vec![("v".to_string(), "b".to_string())]);
        assert!(p.head_lost.is_empty());
        let v = &fed.volumes["v"];
        assert_eq!(v.assembly, AssemblyState::Degraded);
        assert_eq!(v.legs[1].state, LegState::Lost);
        assert_eq!(fed.revision, 1);

        // Next tick while the replacement is being built: nothing new.
        let p = plan_recovery(&mut fed, true, now, &|_| true);
        assert_eq!(p, Plan::default());
        assert_eq!(fed.revision, 1, "no change, no revision bump");

        // The node comes back (flap): the leg stays lost, so it is still
        // replaced once rather than toggling.
        fed.nodes.get_mut("b").unwrap().status.healthy = true;
        let p = plan_recovery(&mut fed, true, now, &|_| true);
        assert!(p.lost.is_empty() && p.start.is_empty());
        assert_eq!(fed.volumes["v"].legs[1].state, LegState::Lost);
    }

    #[test]
    fn cooldown_and_disabled_hold_off() {
        let mut fed = fed_with(&["a", "b"], &["b"]);
        let now = SystemTime::now();
        fed.volumes.get_mut("v").unwrap().next_releg_after = Some(now + Duration::from_secs(60));
        let p = plan_recovery(&mut fed, true, now, &|_| false);
        assert_eq!(p.lost.len(), 1, "still marked lost");
        assert!(p.start.is_empty(), "cooling down");
        let p = plan_recovery(&mut fed, true, now + Duration::from_secs(61), &|_| false);
        assert_eq!(p.start.len(), 1, "after the cooldown");

        let mut fed = fed_with(&["a", "b"], &["b"]);
        let p = plan_recovery(&mut fed, false, now, &|_| false);
        assert_eq!(p.lost.len(), 1);
        assert!(p.start.is_empty(), "recovery off: marked, not acted on");
        assert_eq!(fed.volumes["v"].assembly, AssemblyState::Degraded);
    }

    #[test]
    fn head_loss_is_reported_not_relegged() {
        let mut fed = fed_with(&["a", "b"], &["a"]);
        let p = plan_recovery(&mut fed, true, SystemTime::now(), &|_| false);
        assert_eq!(p.head_lost, vec![("v".to_string(), "a".to_string())]);
        assert!(p.start.is_empty());
        assert_eq!(fed.volumes["v"].assembly, AssemblyState::Degraded);
    }

    fn reading(head: &str, members: &[(&str, &str)]) -> crate::head::ArrayReading {
        crate::head::parse_array(
            head,
            "arr",
            &serde_json::json!({
                "members": members.iter()
                    .map(|(u, st)| serde_json::json!({"uuid": u, "state": st}))
                    .collect::<Vec<_>>(),
                "status": {"state": "clean"},
            }),
        )
    }

    /// A head stalled past fail_threshold, then answers again (#26).
    fn fed_head_back() -> FedState {
        let mut fed = fed_with(&["a", "b"], &["a"]);
        for (i, l) in fed.volumes.get_mut("v").unwrap().legs.iter_mut().enumerate() {
            l.member_uuid = Some(format!("m{i}"));
        }
        plan_recovery(&mut fed, true, SystemTime::now(), &|_| false);
        assert_eq!(fed.volumes["v"].legs[0].state, LegState::Lost);
        fed.nodes.get_mut("a").unwrap().status.healthy = true;
        fed
    }

    #[test]
    fn head_back_with_active_member_restores_assembled() {
        let mut fed = fed_head_back();
        assert_eq!(
            head_rejoin_candidates(&fed, &|_| false),
            vec![("v".to_string(), "a".to_string(), "arr".to_string())]
        );
        assert!(head_rejoin_candidates(&fed, &|_| true).is_empty(), "busy");
        let v = fed.volumes.get_mut("v").unwrap();
        let r = reading("a", &[("m0", "active"), ("m1", "active")]);
        assert_eq!(
            apply_head_reading(v, Some(&r)),
            Some((HeadReturn::Restored { assembled: true }, true))
        );
        assert_eq!(v.legs[0].state, LegState::Created);
        assert_eq!(v.legs[0].message, None);
        assert_eq!(v.assembly, AssemblyState::Assembled);
        assert!(head_rejoin_candidates(&fed, &|_| false).is_empty());
        // Next poll: nothing lost, nothing to do.
        let p = plan_recovery(&mut fed, true, SystemTime::now(), &|_| false);
        assert_eq!(p, Plan::default());
    }

    #[test]
    fn head_back_with_another_leg_lost_stays_degraded() {
        let mut fed = fed_head_back();
        let v = fed.volumes.get_mut("v").unwrap();
        v.legs[1].state = LegState::Lost;
        let r = reading("a", &[("m0", "active"), ("m1", "failed")]);
        assert_eq!(
            apply_head_reading(v, Some(&r)),
            Some((HeadReturn::Restored { assembled: false }, true))
        );
        assert_eq!(v.assembly, AssemblyState::Degraded);
        // The head is back, so the other lost leg is now re-legged.
        let p = plan_recovery(&mut fed, true, SystemTime::now(), &|_| false);
        assert_eq!(p.start, vec![("v".to_string(), "b".to_string())]);
    }

    #[test]
    fn head_back_without_its_array_is_reported_once() {
        let mut fed = fed_head_back();
        let v = fed.volumes.get_mut("v").unwrap();
        assert_eq!(apply_head_reading(v, None), Some((HeadReturn::ArrayGone, true)));
        assert_eq!(apply_head_reading(v, None), Some((HeadReturn::ArrayGone, false)), "once");
        assert_eq!(v.legs[0].state, LegState::Lost);
        assert_eq!(v.assembly, AssemblyState::Degraded);
        assert!(v.legs[0].message.as_deref().unwrap().contains("#15"));

        // Member not active: still lost, reported on the change.
        let r = reading("a", &[("m0", "failed"), ("m1", "active")]);
        assert_eq!(
            apply_head_reading(v, Some(&r)),
            Some((HeadReturn::NotActive("failed".into()), true))
        );
        let r = reading("a", &[("m1", "active")]);
        assert_eq!(
            apply_head_reading(v, Some(&r)),
            Some((HeadReturn::NotActive("missing".into()), true))
        );
        assert_eq!(v.legs[0].state, LegState::Lost);

        // A reading of another array is not evidence.
        let mut other = reading("a", &[("m0", "active")]);
        other.array_id = "other".into();
        assert_eq!(apply_head_reading(v, Some(&other)), None);
        assert_eq!(v.legs[0].state, LegState::Lost);
    }

    #[test]
    fn fenced_or_unhealthy_head_is_not_a_rejoin_candidate() {
        let mut fed = fed_head_back();
        fed.volumes.get_mut("v").unwrap().fenced = true;
        assert!(head_rejoin_candidates(&fed, &|_| false).is_empty(), "fenced");
        let mut fed = fed_head_back();
        fed.nodes.get_mut("a").unwrap().status.healthy = false;
        assert!(head_rejoin_candidates(&fed, &|_| false).is_empty(), "still down");
    }

    #[test]
    fn recorded_replacement_resumes_once() {
        let mut fed = fed_with(&["a", "b"], &["b"]);
        let leg = fed.volumes["v"].legs[1].clone();
        fed.volumes.get_mut("v").unwrap().replacing = Some(Replacement {
            from: "b".into(),
            leg: Leg { node: "c".into(), ..leg },
            reason: "node lost".into(),
            started_at: SystemTime::now(),
        });
        let p = plan_recovery(&mut fed, true, SystemTime::now(), &|_| false);
        assert_eq!(p.resume, vec!["v".to_string()]);
        assert!(p.start.is_empty(), "never a second replacement");
        let p = plan_recovery(&mut fed, true, SystemTime::now(), &|_| true);
        assert!(p.resume.is_empty(), "already being waited on");
    }

    #[test]
    fn unassembled_volumes_are_left_alone() {
        let mut fed = fed_with(&["a", "b"], &["b"]);
        fed.volumes.get_mut("v").unwrap().assembly = AssemblyState::PendingEngineSupport;
        let p = plan_recovery(&mut fed, true, SystemTime::now(), &|_| false);
        assert_eq!(p, Plan::default());
        assert_eq!(fed.volumes["v"].legs[1].state, LegState::Created);
    }

    #[test]
    fn pending_volume_is_retried_once_every_leg_node_is_healthy() {
        let now = SystemTime::now();
        let pending = |fed: &mut FedState| {
            let v = fed.volumes.get_mut("v").unwrap();
            v.assembly = AssemblyState::PendingEngineSupport;
            v.head = None;
            v.array_id = None;
        };
        let mut fed = fed_with(&["a", "b"], &[]);
        pending(&mut fed);
        let p = plan_recovery(&mut fed, true, now, &|_| false);
        assert_eq!(p.assemble, vec!["v".to_string()]);
        assert!(p.lost.is_empty() && p.start.is_empty(), "a pending volume is never re-legged");

        assert!(plan_recovery(&mut fed, true, now, &|_| true).assemble.is_empty(), "busy");
        assert!(plan_recovery(&mut fed, false, now, &|_| false).assemble.is_empty(), "recovery off");
        fed.volumes.get_mut("v").unwrap().next_assemble_after = Some(now + Duration::from_secs(60));
        assert!(plan_recovery(&mut fed, true, now, &|_| false).assemble.is_empty(), "cooling down");
        let p = plan_recovery(&mut fed, true, now + Duration::from_secs(61), &|_| false);
        assert_eq!(p.assemble.len(), 1, "after the cooldown");

        let mut fed = fed_with(&["a", "b"], &["b"]);
        pending(&mut fed);
        assert!(plan_recovery(&mut fed, true, now, &|_| false).assemble.is_empty(), "a leg node is down");

        let mut fed = fed_with(&["a"], &[]);
        pending(&mut fed);
        assert!(plan_recovery(&mut fed, true, now, &|_| false).assemble.is_empty(), "single leg");
    }

    #[test]
    fn array_match_adopts_exact_and_refuses_overlap() {
        let arr = |id: &str, paths: &[&str]| {
            serde_json::json!({"id": id, "members": paths.iter().enumerate()
                .map(|(i, p)| serde_json::json!({"index": i, "uuid": format!("m{i}"), "device_path": p}))
                .collect::<Vec<_>>()})
        };
        let uris = vec!["nvme-tcp://a/x?nsid=1".to_string(), "nvme-tcp://b/y?nsid=1".to_string()];
        assert_eq!(match_array(&[], &uris), ArrayMatch::None);
        let other = arr("o", &["/dev/sdz", "/dev/sdy"]);
        assert_eq!(match_array(std::slice::from_ref(&other), &uris), ArrayMatch::None);
        // Same members, other order: the one the lost create made.
        let mine = arr("m", &["nvme-tcp://b/y?nsid=1", "nvme-tcp://a/x?nsid=1"]);
        assert_eq!(match_array(&[other.clone(), mine.clone()], &uris), ArrayMatch::Exact(mine));
        let overlap = arr("x", &["nvme-tcp://a/x?nsid=1", "/dev/sdq"]);
        assert_eq!(
            match_array(&[overlap], &uris),
            ArrayMatch::Conflict { array: "x".into(), uri: "nvme-tcp://a/x?nsid=1".into() }
        );
    }
}
