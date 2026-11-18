//! A volume's owner on the engines (#109, stormblock#115).
//!
//! `POST /api/v1/volumes` records what a volume belongs to (a claim, a pod,
//! a VM) on the [`DistVolume`] and sends it on every engine create it makes.
//! What is on the engines can still drift from the record: a promote puts
//! the served `<name>-mirror` on another head (adopted, never created with
//! an owner), a re-export or re-leg makes a new engine volume, an engine
//! older than stormblock#649 drops the field on create. So each poll the
//! leader compares every leg and the served volume, as the inventory last
//! saw them, with the record, and sets any that differ with
//! `PUT /api/v1/volumes/{id}/owner`. Level-triggered: nothing to remember.
//!
//! Engine volumes are matched by name on their node (the leg is `vol.name`,
//! the served volume [`crate::orchestrate::served_name`]); the route takes
//! the engine-local id, which a /v1 id is not. A name the node holds twice
//! is left alone rather than guessed.

use crate::api::AppState;
use crate::events::Severity;
use crate::inventory::{NodeInventory, Owner};
use crate::model::{DistVolume, FedState};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// One engine volume whose owner is not the record's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerFix {
    /// The stormstorage volume.
    pub volume: String,
    pub node: String,
    /// The engine volume's name and engine-local id.
    pub engine_name: String,
    pub engine_id: String,
    pub owner: Owner,
}

/// Every engine volume `vol` is made of: (node, name) of each leg and of
/// the served volume. A single leg is its own served volume (listed once).
pub fn engine_volumes(vol: &DistVolume) -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    for leg in &vol.legs {
        if leg.volume_id.is_some() {
            out.insert((leg.node.clone(), vol.name.clone()));
        }
    }
    if let (Some(node), Some(_)) = (&vol.export.node, &vol.export.volume_id) {
        out.insert((node.clone(), crate::orchestrate::served_name(vol)));
    }
    out
}

/// What to set (pure): each engine volume of an owned volume, on a healthy
/// node whose inventory holds exactly one volume of that name, whose owner
/// is not the record's.
pub fn plan(fed: &FedState, inventory: &BTreeMap<String, NodeInventory>) -> Vec<OwnerFix> {
    let mut out = Vec::new();
    for vol in fed.volumes.values() {
        let Some(owner) = &vol.owner else { continue };
        for (node, name) in engine_volumes(vol) {
            if !fed.nodes.get(&node).is_some_and(|n| n.status.healthy) {
                continue;
            }
            let Some(inv) = inventory.get(&node) else { continue };
            let mut named = inv.volumes.iter().filter(|p| p.volume.name == name);
            let (Some(one), None) = (named.next(), named.next()) else { continue };
            if one.volume.owner.as_ref() != Some(owner) {
                out.push(OwnerFix {
                    volume: vol.name.clone(),
                    node,
                    engine_name: name,
                    engine_id: one.volume.id.clone(),
                    owner: owner.clone(),
                });
            }
        }
    }
    out
}

/// Put each volume's recorded owner on the engine volumes that read
/// otherwise. A failure is reported once (and again after it has worked);
/// it is retried every poll, the call being one small PUT.
pub async fn sync(state: &Arc<AppState>) {
    let fixes = {
        let fed = state.fed.read().await;
        let inv = state.inventory.read().await;
        plan(&fed, &inv)
    };
    for f in fixes {
        let engine = {
            let fed = state.fed.read().await;
            match fed.nodes.get(&f.node) {
                Some(n) => state.engine_for(n),
                None => continue,
            }
        };
        let failed_key = format!("owner-failed:{}:{}", f.node, f.engine_id);
        match engine.set_owner(&f.engine_id, Some(&f.owner)).await {
            Ok(()) => {
                // Seen at once, not only after the next inventory read.
                if let Some(inv) = state.inventory.write().await.get_mut(&f.node) {
                    for p in inv.volumes.iter_mut().filter(|p| p.volume.id == f.engine_id) {
                        p.volume.owner = Some(f.owner.clone());
                    }
                }
                state.noted.lock().expect("noted").remove(&failed_key);
                state.events.write().await.push(
                    Some(f.volume.clone()),
                    Severity::Info,
                    "owner",
                    format!("{}: owner {} set on {}'s {}", f.volume, f.owner.describe(), f.node, f.engine_name),
                );
            }
            Err(e) => {
                if state.noted.lock().expect("noted").insert(failed_key) {
                    state.events.write().await.push(
                        Some(f.volume.clone()),
                        Severity::Warning,
                        "owner",
                        format!(
                            "{}: owner {} not set on {}'s {} (retried each poll): {e:#}",
                            f.volume,
                            f.owner.describe(),
                            f.node,
                            f.engine_name
                        ),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{EngineVolume, PlacedBy, PlacedVolume};
    use crate::model::{AssemblyState, Leg, LegState};

    fn owner(uid: &str) -> Owner {
        Owner { kind: "PersistentVolumeClaim".into(), namespace: "ns".into(), name: "claim".into(), uid: Some(uid.into()) }
    }

    fn leg(node: &str) -> Leg {
        Leg {
            node: node.into(),
            volume_id: Some(format!("v1-{node}")),
            state: LegState::Created,
            message: None,
            master_node: None,
            export: None,
            drive_uuid: None,
            member_uuid: None,
            epoch: None,
        }
    }

    fn placed(id: &str, name: &str, owner: Option<Owner>) -> PlacedVolume {
        PlacedVolume {
            volume: EngineVolume { id: id.into(), name: name.into(), owner, ..Default::default() },
            slabs: Vec::new(),
            placed_by: PlacedBy::Unknown,
            pv: None,
        }
    }

    fn fed_with(vol: DistVolume, nodes: &[&str]) -> FedState {
        let mut fed = FedState::default();
        let cfg: Vec<crate::config::NodeConfig> = nodes
            .iter()
            .map(|n| toml::from_str(&format!("name = \"{n}\"\nengine_url = \"http://{n}\"")).unwrap())
            .collect();
        fed.apply_config_nodes(&cfg);
        for n in fed.nodes.values_mut() {
            n.status.healthy = true;
        }
        fed.volumes.insert(vol.name.clone(), vol);
        fed
    }

    fn mirrored(owner: Option<Owner>) -> DistVolume {
        let mut v: DistVolume = serde_json::from_value(serde_json::json!({
            "name": "pvc-1", "size_bytes": 1u64 << 30, "pool": null, "replicas": 2, "rung": "node",
            "legs": [], "assembly": "assembled", "created_at": {"secs_since_epoch": 0, "nanos_since_epoch": 0}
        }))
        .unwrap();
        v.legs = vec![leg("a"), leg("b")];
        v.assembly = AssemblyState::Assembled;
        v.head = Some("a".into());
        v.export.node = Some("a".into());
        v.export.volume_id = Some("v1-mirror".into());
        v.owner = owner;
        v
    }

    #[test]
    fn every_leg_and_the_served_mirror_is_an_engine_volume_of_it() {
        let got = engine_volumes(&mirrored(None));
        let want: BTreeSet<(String, String)> = [("a", "pvc-1"), ("b", "pvc-1"), ("a", "pvc-1-mirror")]
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn a_single_leg_is_its_own_served_volume() {
        let mut v = mirrored(None);
        v.legs.truncate(1);
        v.assembly = AssemblyState::SingleLeg;
        assert_eq!(engine_volumes(&v).len(), 1);
    }

    #[test]
    fn plan_sets_only_what_differs_and_only_where_unambiguous() {
        let fed = fed_with(mirrored(Some(owner("u1"))), &["a", "b"]);
        let mut inv = BTreeMap::new();
        inv.insert(
            "a".to_string(),
            NodeInventory {
                volumes: vec![
                    placed("ea-leg", "pvc-1", Some(owner("u1"))),
                    // Adopted on a promote: no owner.
                    placed("ea-mirror", "pvc-1-mirror", None),
                ],
                ..Default::default()
            },
        );
        inv.insert(
            "b".to_string(),
            NodeInventory {
                // A stale owner: a claim recreated under the same name.
                volumes: vec![placed("eb-leg", "pvc-1", Some(owner("u0")))],
                ..Default::default()
            },
        );
        let mut got: Vec<(String, String)> = plan(&fed, &inv).into_iter().map(|f| (f.node, f.engine_id)).collect();
        got.sort();
        assert_eq!(got, vec![("a".into(), "ea-mirror".into()), ("b".into(), "eb-leg".into())]);

        // Two of that name on b: left alone.
        inv.get_mut("b").unwrap().volumes.push(placed("eb-other", "pvc-1", None));
        assert_eq!(plan(&fed, &inv).len(), 1);
    }

    #[test]
    fn plan_skips_unowned_volumes_and_unhealthy_nodes() {
        let mut inv = BTreeMap::new();
        inv.insert(
            "a".to_string(),
            NodeInventory { volumes: vec![placed("ea-leg", "pvc-1", None)], ..Default::default() },
        );
        assert!(plan(&fed_with(mirrored(None), &["a", "b"]), &inv).is_empty());
        let mut fed = fed_with(mirrored(Some(owner("u1"))), &["a", "b"]);
        fed.nodes.get_mut("a").unwrap().status.healthy = false;
        assert!(plan(&fed, &inv).is_empty());
    }
}
