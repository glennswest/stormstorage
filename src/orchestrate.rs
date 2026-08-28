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
use crate::model::{AssemblyState, DistVolume, Leg, LegState};
use crate::placement::domain_at;
use std::sync::Arc;
use std::time::Duration;

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
/// persisted and assembly stays pending for a retry.
pub async fn assemble(state: &Arc<AppState>, name: &str) -> anyhow::Result<()> {
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
    // 1. Export every leg (idempotent on the engine side: attach re-returns
    //    the namespace it already hot-added).
    for leg in vol.legs.iter_mut() {
        if leg.export.is_some() {
            continue;
        }
        let engine = engines.get(&leg.node).expect("leg engine");
        let vid = leg.volume_id.as_deref().expect("checked created");
        let master = leg.master_node.clone().unwrap_or_else(|| "localhost".into());
        let att = engine
            .attach_volume(vid, &master)
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
    // 3. RAID1 across the legs.
    let arr = head_engine
        .create_raid1(&drive_uuids)
        .await
        .map_err(|e| anyhow::anyhow!("{head}: create raid1: {e:#}"))?;
    let array_id = arr
        .get("id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("{head}: array response without id: {arr}"))?
        .to_string();
    // Member order == drive_uuids order == leg order.
    if let Some(members) = arr.get("members").and_then(|m| m.as_array()) {
        for (i, m) in members.iter().enumerate() {
            if let (Some(leg), Some(uuid)) =
                (vol.legs.get_mut(i), m.get("uuid").and_then(|u| u.as_str()))
            {
                leg.member_uuid = Some(uuid.to_string());
            }
        }
    }
    vol.head = Some(head.to_string());
    vol.array_id = Some(array_id.clone());
    vol.assembly = AssemblyState::Assembled;
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
        for leg in &vol.legs {
            if let Some(uri) = leg.export.as_ref().map(|x| x.drive_uri()) {
                if let Err(e) = engine.delete_drive(&uri, true).await {
                    errors.push(format!("head drive {uri}: {e:#}"));
                }
            }
        }
    }
    for leg in &vol.legs {
        let (Some(vid), Some(_)) = (&leg.volume_id, &leg.export) else {
            continue;
        };
        if let Some(n) = fed.nodes.get(&leg.node) {
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

/// Move one leg: create + attach on the target, add as a RAID member on
/// the head, then hand off to a background task that waits for the rebuild
/// and retires the old leg. Returns the target node.
pub async fn move_leg(
    state: &Arc<AppState>,
    name: &str,
    from: &str,
    to: Option<String>,
) -> anyhow::Result<String> {
    let (vol, target, rungs) = {
        let fed = state.fed.read().await;
        let vol = fed
            .volumes
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("volume {name:?} not found"))?;
        if vol.assembly != AssemblyState::Assembled {
            anyhow::bail!("{name}: not assembled — only assembled volumes move legs");
        }
        if !vol.legs.iter().any(|l| l.node == from) {
            anyhow::bail!("{name}: no leg on {from:?}");
        }
        let rungs = state.config.federation.rungs.clone();
        let target = match to {
            Some(t) => {
                let n = fed
                    .nodes
                    .get(&t)
                    .ok_or_else(|| anyhow::anyhow!("target node {t:?} not in registry"))?;
                if !n.status.healthy {
                    anyhow::bail!("target node {t:?} is unhealthy");
                }
                t
            }
            None => {
                let cands = move_target_candidates(&fed, &rungs, &vol, from);
                crate::placement::plan(&cands, &rungs, &vol.rung, 1, vol.size_bytes)
                    .map_err(|e| anyhow::anyhow!("no move target: {e}"))?
                    .remove(0)
            }
        };
        (vol, target, rungs)
    };
    let _ = rungs;
    let head = vol.head.clone().expect("assembled has head");
    let array_id = vol.array_id.clone().expect("assembled has array");
    let old_leg = vol.legs.iter().find(|l| l.node == from).cloned().expect("checked");

    // Build the new leg synchronously: volume, export, head drive, member.
    let (target_engine, head_engine) = {
        let fed = state.fed.read().await;
        (
            engine_of(state, &fed, &target)?,
            engine_of(state, &fed, &head)?,
        )
    };
    let created = target_engine
        .create_volume(&vol.name, vol.size_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("{target}: create leg: {e:#}"))?;
    let vid = created
        .get("id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("{target}: create returned no id"))?
        .to_string();
    let master = crate::engine::Engine::master_node_of(&created).unwrap_or_else(|| "localhost".into());
    let att = target_engine
        .attach_volume(&vid, &master)
        .await
        .map_err(|e| anyhow::anyhow!("{target}: attach: {e:#}"))?;
    let drive_uuid = head_engine
        .add_drive_idempotent(&att.drive_uri())
        .await
        .map_err(|e| anyhow::anyhow!("{head}: open leg drive: {e:#}"))?;
    let member_uuid = head_engine
        .array_add_member(&array_id, &drive_uuid)
        .await
        .map_err(|e| anyhow::anyhow!("{head}: add member: {e:#}"))?;

    let new_leg = Leg {
        node: target.clone(),
        volume_id: Some(vid),
        state: LegState::Created,
        message: Some("rebuilding".into()),
        master_node: Some(master),
        export: Some(att),
        drive_uuid: Some(drive_uuid),
        member_uuid: Some(member_uuid.clone()),
    };
    event(
        state,
        name,
        Severity::Info,
        format!("{name}: moving leg {from} → {target}; rebuild started on {head}"),
    )
    .await;

    // Background: wait for rebuild, then retire the old leg.
    let st = state.clone();
    let name_owned = name.to_string();
    let from_owned = from.to_string();
    tokio::spawn(async move {
        let ok = wait_member_active(&head_engine, &array_id, &member_uuid, Duration::from_secs(3600)).await;
        if !ok {
            event(
                &st,
                &name_owned,
                Severity::Error,
                format!(
                    "{name_owned}: rebuild of new leg on {} did not reach active — old leg on {from_owned} kept",
                    new_leg.node
                ),
            )
            .await;
            return;
        }
        // Retire the old leg: member out, head drive out, volume gone.
        let mut errs = Vec::new();
        if let Some(mu) = &old_leg.member_uuid {
            if let Err(e) = head_engine.array_remove_member(&array_id, mu).await {
                errs.push(format!("remove member: {e:#}"));
            }
        }
        if let Some(uri) = old_leg.export.as_ref().map(|x| x.drive_uri()) {
            if let Err(e) = head_engine.delete_drive(&uri, true).await {
                errs.push(format!("head drive: {e:#}"));
            }
        }
        {
            let fed = st.fed.read().await;
            if let (Some(n), Some(vid)) = (fed.nodes.get(&from_owned), &old_leg.volume_id) {
                let engine = st.engine_for(n);
                let master = old_leg.master_node.clone().unwrap_or_else(|| "localhost".into());
                let _ = engine.detach_volume(vid, &master).await;
                if let Err(e) = engine.delete_volume(vid).await {
                    errs.push(format!("{from_owned} delete volume: {e:#}"));
                }
            }
        }
        // Swap the leg record.
        {
            let mut fed = st.fed.write().await;
            if let Some(v) = fed.volumes.get_mut(&name_owned) {
                if let Some(slot) = v.legs.iter_mut().find(|l| l.node == from_owned) {
                    let mut nl = new_leg.clone();
                    nl.message = None;
                    *slot = nl;
                }
            }
            fed.revision += 1;
        }
        crate::replicate::push_to_peers(st.clone());
        st.persist().await;
        let sev = if errs.is_empty() { Severity::Info } else { Severity::Warning };
        event(
            &st,
            &name_owned,
            sev,
            if errs.is_empty() {
                format!("{name_owned}: leg move {from_owned} → {} complete", new_leg.node)
            } else {
                format!(
                    "{name_owned}: leg move {from_owned} → {} complete; cleanup issues: {}",
                    new_leg.node,
                    errs.join("; ")
                )
            },
        )
        .await;
    });
    Ok(target)
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
                })
                .collect(),
            assembly: AssemblyState::Assembled,
            head: Some(legs[0].to_string()),
            array_id: Some("arr".into()),
            created_at: SystemTime::now(),
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
}
