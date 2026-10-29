//! Expand a volume (#73, stormblock#318): `POST /api/v1/volumes/{name}/expand
//! {size_bytes}` — stormblock-csi's ExpandVolume.
//!
//! An assembled volume grows bottom up: every leg on its own engine (`/v1`
//! expand), then the head's RAID1 array and its dedicated slab
//! (`arrays/{id}/grow`, which refuses while a leg has not grown), then the
//! served `<name>-mirror`. A single-leg volume is its leg; a clone is its
//! own served volume on its parent's array, so it grows only as far as the
//! parent already has. The target is recorded first (`expanding`) and every
//! step is grow-only, so a retry of the same call resumes where one stopped.

use crate::api::AppState;
use crate::engine::Engine;
use crate::events::Severity;
use crate::head::Refusal;
use crate::model::{AssemblyState, DistVolume, ExportState, Expansion, LegState};
use std::sync::Arc;
use std::time::SystemTime;

async fn event(state: &Arc<AppState>, subject: &str, sev: Severity, msg: String) {
    state.events.write().await.push(Some(subject.to_string()), sev, "expand", msg);
}

/// Why `vol` cannot be expanded to `to` now, or None. Pure.
pub fn refuse(fed: &crate::model::FedState, vol: &DistVolume, to: u64) -> Option<Refusal> {
    let name = &vol.name;
    let c = |m: String| Some(Refusal::Conflict(m));
    if to < vol.size_bytes {
        return c(format!(
            "{name}: a volume grows only; it is {} bytes, not {to} (a smaller one is a copy, not a resize)",
            vol.size_bytes
        ));
    }
    if vol.fenced {
        return c(format!("{name}: fenced — promote a head first"));
    }
    if vol.replacing.is_some() {
        return c(format!("{name}: a leg replacement is in flight"));
    }
    if vol.dual_attach.is_some() {
        return c(format!("{name}: a dual-attach window is open"));
    }
    if vol.migration.is_some() {
        return c(format!("{name}: a tier migration is in flight"));
    }
    if vol.export.state == ExportState::Revoking {
        return c(format!("{name}: being deleted"));
    }
    if vol.export.gone {
        return c(format!("{name}: its served volume is gone (#40); recreate it first"));
    }
    match vol.assembly {
        AssemblyState::SingleLeg | AssemblyState::Assembled => {}
        AssemblyState::Degraded => return c(format!("{name}: degraded — a lost leg cannot grow; expand once it is replaced")),
        AssemblyState::PendingEngineSupport => return c(format!("{name}: not assembled yet")),
        AssemblyState::Clone => {
            let parent = fed.parent_of(vol).filter(|p| p.clone_of.is_none());
            return match parent {
                None => c(format!("{name}: its parent volume is gone")),
                Some(p) if to > p.size_bytes => c(format!(
                    "{name}: a clone lives on {}'s array ({} bytes); expand {} first",
                    p.name, p.size_bytes, p.name
                )),
                Some(_) => None,
            };
        }
    }
    if let Some(l) = vol.legs.iter().find(|l| l.state != LegState::Created || l.volume_id.is_none()) {
        return c(format!("{name}: leg on {} is {:?}", l.node, l.state));
    }
    let mut nodes: Vec<&str> = vol.legs.iter().map(|l| l.node.as_str()).collect();
    nodes.extend(vol.head.as_deref());
    if let Some(n) = nodes.iter().find(|n| !fed.nodes.get(**n).is_some_and(|x| x.status.healthy)) {
        return c(format!("{name}: {n} is not answering"));
    }
    None
}

/// The engine of a registered node.
fn engine(state: &Arc<AppState>, fed: &crate::model::FedState, node: &str) -> Result<Engine, Refusal> {
    fed.nodes
        .get(node)
        .map(|n| state.engine_for(n))
        .ok_or_else(|| Refusal::Conflict(format!("node {node:?} not in registry")))
}

/// What one try works on, read under the lock.
struct Plan {
    vol: DistVolume,
    /// (node, engine, /v1 id) of every leg.
    legs: Vec<(String, Engine, String)>,
    head: Option<(String, Engine, String)>,
    /// (node, engine, id, engine-side name, adopted) of the served volume.
    served: Option<(String, Engine, String, String, bool)>,
}

/// `POST /api/v1/volumes/{name}/expand {size_bytes}`.
pub async fn expand(state: &Arc<AppState>, name: &str, to: u64) -> Result<serde_json::Value, Refusal> {
    if !crate::orchestrate::claim(state, name) {
        return Err(Refusal::Conflict(format!("{name}: busy — an assembly, leg move or expand is in flight")));
    }
    let r = expand_claimed(state, name, to).await;
    crate::orchestrate::release(state, name);
    if let Err(e) = &r {
        let mut fed = state.fed.write().await;
        if let Some(x) = fed.volumes.get_mut(name).and_then(|v| v.expanding.as_mut()) {
            x.message = Some(e.to_string());
            fed.revision += 1;
        }
        drop(fed);
        state.persist().await;
    }
    r?;
    let fed = state.fed.read().await;
    let heads = state.heads.read().await;
    Ok(fed.volumes.get(name).map(|v| crate::head::view_in(&fed, &heads, v)).unwrap_or_default())
}

async fn expand_claimed(state: &Arc<AppState>, name: &str, to: u64) -> Result<(), Refusal> {
    let plan = {
        let mut fed = state.fed.write().await;
        let vol = fed.volumes.get(name).ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        // A smaller size than one already in flight would leave grown legs
        // under a record that says less.
        let to = vol.expanding.as_ref().map_or(to, |x| to.max(x.to_bytes));
        if to == vol.size_bytes && vol.expanding.is_none() {
            return Ok(());
        }
        if let Some(r) = refuse(&fed, vol, to) {
            return Err(r);
        }
        let legs = vol
            .legs
            .iter()
            .map(|l| Ok((l.node.clone(), engine(state, &fed, &l.node)?, l.volume_id.clone().unwrap_or_default())))
            .collect::<Result<Vec<_>, Refusal>>()?;
        let head = match (vol.assembly, &vol.head, &vol.array_id) {
            (AssemblyState::Assembled, Some(h), Some(a)) => Some((h.clone(), engine(state, &fed, h)?, a.clone())),
            (AssemblyState::Assembled, _, _) => return Err(Refusal::Conflict(format!("{name}: no head array recorded"))),
            _ => None,
        };
        let served = match (&vol.export.node, &vol.export.volume_id) {
            (Some(n), Some(id)) => Some((
                n.clone(),
                engine(state, &fed, n)?,
                id.clone(),
                vol.export.local_id.clone().unwrap_or_else(|| crate::orchestrate::served_name(vol)),
                vol.export.adopted,
            )),
            _ => None,
        };
        let v = fed.volumes.get_mut(name).expect("read above");
        match &mut v.expanding {
            Some(x) => {
                x.to_bytes = to;
                x.message = None;
            }
            None => {
                v.expanding = Some(Expansion {
                    to_bytes: to,
                    started_at: SystemTime::now(),
                    array_bytes_before: None,
                    array_grown: false,
                    slab_short: None,
                    message: None,
                })
            }
        }
        fed.revision += 1;
        let vol = fed.volumes.get(name).cloned().expect("read above");
        Plan { vol, legs, head, served }
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    let x = plan.vol.expanding.clone().expect("set above");
    let to = x.to_bytes;
    if let Some(short) = &x.slab_short {
        return Err(Refusal::Conflict(format!(
            "{name}: the head array's slab cannot reach the new room ({short}); its volumes need a new array"
        )));
    }

    // 1. Every leg on its own engine.
    for (node, e, id) in &plan.legs {
        e.expand_v1(id, to)
            .await
            .map_err(|e| Refusal::Upstream(format!("{name}: leg on {node}: expand: {e:#}")))?;
    }

    // 2. The head's array and its slab.
    if let (Some((node, e, array)), false) = (&plan.head, x.array_grown) {
        let before = match x.array_bytes_before {
            Some(b) => b,
            None => {
                let b = array_bytes(e, array).await.map_err(|e| Refusal::Upstream(format!("{name}: head {node}: {e:#}")))?;
                record(state, name, |x| x.array_bytes_before = Some(b)).await;
                b
            }
        };
        let short = match e.grow_array(array).await {
            Ok(r) => r.get("slab_short").and_then(|s| s.as_str()).map(str::to_string),
            Err(err) if crate::engine::status_of(&err) == Some(reqwest::StatusCode::CONFLICT) => {
                // Grown by an earlier try whose answer was lost, or a real
                // refusal (a leg not grown, a rebuild running).
                let now = array_bytes(e, array).await.unwrap_or(before);
                if now <= before {
                    return Err(Refusal::Conflict(format!("{name}: head {node}: grow array: {err:#}")));
                }
                None
            }
            Err(err) => return Err(Refusal::Upstream(format!("{name}: head {node}: grow array: {err:#}"))),
        };
        record(state, name, |x| {
            x.array_grown = true;
            x.slab_short = short.clone();
        })
        .await;
        if let Some(short) = short {
            event(state, name, Severity::Error, format!("{name}: array grown on {node}, its slab short: {short}")).await;
            return Err(Refusal::Conflict(format!(
                "{name}: the head array grew but its slab cannot reach the new room ({short}); its volumes need a new array"
            )));
        }
    }

    // 3. The served volume (for a single leg this is the leg, grown in 1).
    if let Some((node, e, id, engine_name, adopted)) = &plan.served {
        let grown = if *adopted { e.resize_any(id, to).await } else { e.expand_v1(id, to).await };
        grown.map_err(|e| Refusal::Upstream(format!("{name}: served volume on {node}: expand: {e:#}")))?;
        let size = e
            .served_size(engine_name)
            .await
            .map_err(|e| Refusal::Upstream(format!("{name}: served volume on {node}: {e:#}")))?;
        if size < to {
            return Err(Refusal::Upstream(format!(
                "{name}: served volume on {node} is {size} bytes after its expand to {to}: the engine did not grow it"
            )));
        }
    }

    // 4. The record.
    let from = {
        let mut fed = state.fed.write().await;
        let v = fed.volumes.get_mut(name).ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        let from = v.size_bytes;
        v.size_bytes = to;
        v.expanding = None;
        fed.revision += 1;
        from
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    event(state, name, Severity::Info, format!("{name}: expanded {from} → {to} bytes")).await;
    Ok(())
}

async fn array_bytes(e: &Engine, array: &str) -> anyhow::Result<u64> {
    let a = e.get_array(array).await?;
    a.get("capacity_bytes")
        .and_then(|x| x.as_u64())
        .ok_or_else(|| anyhow::anyhow!("array {array}: no capacity_bytes in {a}"))
}

async fn record(state: &Arc<AppState>, name: &str, f: impl FnOnce(&mut Expansion)) {
    {
        let mut fed = state.fed.write().await;
        let Some(x) = fed.volumes.get_mut(name).and_then(|v| v.expanding.as_mut()) else { return };
        f(x);
        fed.revision += 1;
    }
    state.persist().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FedState, Leg};

    fn vol(size: u64) -> DistVolume {
        serde_json::from_value(serde_json::json!({
            "name": "v", "size_bytes": size, "pool": null, "replicas": 2, "rung": "node",
            "legs": [], "assembly": "single_leg",
            "created_at": {"secs_since_epoch": 0, "nanos_since_epoch": 0},
        }))
        .unwrap()
    }

    #[test]
    fn a_volume_only_grows() {
        let f = FedState::default();
        assert!(matches!(refuse(&f, &vol(100), 50), Some(Refusal::Conflict(_))));
        assert!(refuse(&f, &vol(100), 200).is_none());
    }

    #[test]
    fn a_lost_leg_or_a_fence_refuses() {
        let f = FedState::default();
        let mut v = vol(100);
        v.fenced = true;
        assert!(refuse(&f, &v, 200).is_some());
        let mut v = vol(100);
        v.assembly = AssemblyState::Degraded;
        assert!(refuse(&f, &v, 200).is_some());
        let mut v = vol(100);
        v.legs.push(Leg {
            node: "a".into(),
            volume_id: None,
            state: LegState::Lost,
            message: None,
            master_node: None,
            export: None,
            drive_uuid: None,
            member_uuid: None,
            epoch: None,
        });
        assert!(refuse(&f, &v, 200).is_some());
    }

    #[test]
    fn a_clone_grows_only_as_far_as_its_parent() {
        let mut f = FedState::default();
        f.volumes.insert("v".into(), vol(100));
        let mut c = vol(100);
        c.name = "c".into();
        c.assembly = AssemblyState::Clone;
        c.clone_of = Some(crate::model::CloneOf {
            source: crate::model::VolumeSource::Volume("v".into()),
            parent: "v".into(),
        });
        assert!(refuse(&f, &c, 100).is_none());
        assert!(matches!(refuse(&f, &c, 200), Some(Refusal::Conflict(m)) if m.contains("expand v first")));
    }

    #[test]
    fn a_leg_is_made_at_an_expand_target() {
        let mut v = vol(100);
        assert_eq!(v.leg_bytes(), 100);
        v.expanding = Some(Expansion {
            to_bytes: 300,
            started_at: SystemTime::UNIX_EPOCH,
            array_bytes_before: None,
            array_grown: false,
            slab_short: None,
            message: None,
        });
        assert_eq!(v.leg_bytes(), 300);
    }
}
