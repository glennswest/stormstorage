//! What each node's engine holds, as observed: its slabs (a node's pools)
//! and its volumes, each placed on the slab(s) it lives on (#9).
//!
//! Observed state, refreshed every poll and never persisted or replicated —
//! the engine is the source of truth and a restart re-reads it.
//!
//! Placement comes from the slabs' slot tables (`GET /api/v1/slabs/{id}/slots`
//! names the volume owning each slot). A volume that owns no slot — a fresh
//! copy-on-write clone reads entirely through its parent — is placed with its
//! parent, and failing that on the only slab of its role on the node. What is
//! still ambiguous is left unplaced rather than guessed: stormblock#136 is the
//! engine reporting placement per volume, which replaces all of this.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

/// One slab as `GET /api/v1/slabs` reports it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Slab {
    pub id: String,
    /// hot | warm | cool | cold.
    pub tier: String,
    /// system | data.
    pub role: String,
    /// Failure domain, e.g. `drive=…` (stormblock#136: a drive identity).
    pub domain: String,
    pub slot_size: u64,
    pub total_slots: u64,
    pub free_slots: u64,
    pub allocated_slots: u64,
    pub total_bytes: u64,
    pub free_bytes: u64,
}

impl Slab {
    pub fn allocated_bytes(&self) -> u64 {
        self.allocated_slots.saturating_mul(self.slot_size)
    }
}

/// What a volume belongs to (stormblock volume metadata `owner`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Owner {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub uid: Option<String>,
}

impl Owner {
    pub fn describe(&self) -> String {
        if self.namespace.is_empty() {
            format!("{} {}", self.kind, self.name)
        } else {
            format!("{} {}/{}", self.kind, self.namespace, self.name)
        }
    }
}

/// One volume as `GET /api/v1/volumes` reports it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineVolume {
    pub id: String,
    pub name: String,
    pub virtual_size_bytes: u64,
    pub allocated_bytes: u64,
    pub shared_bytes: u64,
    pub parent: Option<String>,
    pub sealed: bool,
    /// system | data — which half of the node's storage it lives in.
    pub role: String,
    pub health: String,
    pub redundancy: String,
    pub array_id: Option<String>,
    pub owner: Option<Owner>,
}

/// How a volume's slabs were worked out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacedBy {
    /// It owns slots on these slabs.
    Slots,
    /// It owns none; its parent's (a clone reads through it).
    Parent,
    /// The only slab of its role on the node.
    Role,
    /// Not determinable from the engine today (stormblock#136).
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacedVolume {
    #[serde(flatten)]
    pub volume: EngineVolume,
    /// Slab ids, sorted.
    pub slabs: Vec<String>,
    pub placed_by: PlacedBy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeInventory {
    pub slabs: Vec<Slab>,
    pub volumes: Vec<PlacedVolume>,
    pub fetched_at: Option<SystemTime>,
    /// Why the last refresh failed, if it did (the previous inventory is
    /// kept until the node is marked unhealthy).
    pub error: Option<String>,
}

/// Place every volume on its slab(s). `slot_owners` maps a volume id to the
/// slabs holding slots it owns.
pub fn place(
    slabs: &[Slab],
    volumes: Vec<EngineVolume>,
    slot_owners: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<PlacedVolume> {
    let by_id: BTreeMap<&str, &EngineVolume> =
        volumes.iter().map(|v| (v.id.as_str(), v)).collect();
    let mut placed = Vec::with_capacity(volumes.len());
    for v in &volumes {
        let (slabs_of, placed_by) = resolve(v, &by_id, slabs, slot_owners);
        placed.push(PlacedVolume {
            volume: v.clone(),
            slabs: slabs_of,
            placed_by,
        });
    }
    placed
}

fn resolve(
    v: &EngineVolume,
    by_id: &BTreeMap<&str, &EngineVolume>,
    slabs: &[Slab],
    slot_owners: &BTreeMap<String, BTreeSet<String>>,
) -> (Vec<String>, PlacedBy) {
    if let Some(s) = slot_owners.get(&v.id).filter(|s| !s.is_empty()) {
        return (s.iter().cloned().collect(), PlacedBy::Slots);
    }
    // Walk up the parent chain to the first ancestor that owns slots.
    // Bounded, so a cycle in bad data cannot hang the poller.
    let mut cur = v.parent.as_deref();
    for _ in 0..64 {
        let Some(pid) = cur else { break };
        if let Some(s) = slot_owners.get(pid).filter(|s| !s.is_empty()) {
            return (s.iter().cloned().collect(), PlacedBy::Parent);
        }
        cur = by_id.get(pid).and_then(|p| p.parent.as_deref());
    }
    if !v.role.is_empty() {
        let same_role: Vec<&Slab> = slabs.iter().filter(|s| s.role == v.role).collect();
        if same_role.len() == 1 {
            return (vec![same_role[0].id.clone()], PlacedBy::Role);
        }
    }
    (Vec::new(), PlacedBy::Unknown)
}

/// Slabs of one tier, summed across nodes.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct TierRollup {
    pub tier: String,
    /// `node/slab-id` of every slab in the tier.
    pub slabs: Vec<String>,
    pub nodes: Vec<String>,
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub allocated_bytes: u64,
    pub volumes: usize,
}

/// Volumes placed on a slab.
pub fn volumes_on<'a>(inv: &'a NodeInventory, slab_id: &str) -> Vec<&'a PlacedVolume> {
    inv.volumes
        .iter()
        .filter(|v| v.slabs.iter().any(|s| s == slab_id))
        .collect()
}

/// Aggregate every node's slabs per tier.
pub fn tiers(all: &BTreeMap<String, NodeInventory>) -> Vec<TierRollup> {
    let mut out: BTreeMap<String, TierRollup> = BTreeMap::new();
    for (node, inv) in all {
        for slab in &inv.slabs {
            let t = out.entry(slab.tier.clone()).or_insert_with(|| TierRollup {
                tier: slab.tier.clone(),
                ..Default::default()
            });
            t.slabs.push(format!("{node}/{}", slab.id));
            if !t.nodes.contains(node) {
                t.nodes.push(node.clone());
            }
            t.total_bytes += slab.total_bytes;
            t.free_bytes += slab.free_bytes;
            t.allocated_bytes += slab.allocated_bytes();
        }
        // A volume spanning two slabs of one tier counts once.
        let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
        for v in &inv.volumes {
            for sid in &v.slabs {
                if let Some(slab) = inv.slabs.iter().find(|s| &s.id == sid) {
                    if seen.insert((slab.tier.as_str(), v.volume.id.as_str())) {
                        if let Some(t) = out.get_mut(&slab.tier) {
                            t.volumes += 1;
                        }
                    }
                }
            }
        }
    }
    out.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slab(id: &str, role: &str) -> Slab {
        Slab {
            id: id.into(),
            tier: "hot".into(),
            role: role.into(),
            ..Default::default()
        }
    }

    fn vol(id: &str, role: &str, parent: Option<&str>) -> EngineVolume {
        EngineVolume {
            id: id.into(),
            name: id.into(),
            role: role.into(),
            parent: parent.map(|p| p.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn placement_prefers_slots_then_parent_then_role() {
        let slabs = vec![slab("sys", "system"), slab("d1", "data"), slab("d2", "data")];
        let mut owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        owners.insert("golden".into(), ["sys".to_string()].into());
        owners.insert("big".into(), ["d1".to_string(), "d2".to_string()].into());
        let vols = vec![
            vol("golden", "system", None),
            vol("clone", "system", Some("golden")),
            vol("clone2", "system", Some("clone")),
            vol("big", "data", None),
            vol("fresh-sys", "system", None),
            vol("fresh-data", "data", None),
        ];
        let p = place(&slabs, vols, &owners);
        let get = |id: &str| p.iter().find(|v| v.volume.id == id).unwrap();
        assert_eq!(get("golden").placed_by, PlacedBy::Slots);
        assert_eq!(get("clone").placed_by, PlacedBy::Parent);
        assert_eq!(get("clone").slabs, vec!["sys".to_string()]);
        assert_eq!(get("clone2").placed_by, PlacedBy::Parent, "grandparent walk");
        assert_eq!(get("big").slabs, vec!["d1".to_string(), "d2".to_string()]);
        assert_eq!(get("fresh-sys").placed_by, PlacedBy::Role);
        assert_eq!(
            get("fresh-data").placed_by,
            PlacedBy::Unknown,
            "two data slabs: ambiguous, not guessed"
        );
    }

    #[test]
    fn tiers_aggregate_across_nodes() {
        let mut a = slab("s1", "system");
        a.total_bytes = 100;
        a.free_bytes = 40;
        a.slot_size = 1;
        a.allocated_slots = 60;
        let mut b = slab("s2", "data");
        b.tier = "cold".into();
        b.total_bytes = 1000;
        let mut c = slab("s3", "data");
        c.total_bytes = 50;
        c.free_bytes = 50;
        let mut owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        owners.insert("v".into(), ["s1".to_string()].into());
        let inv1 = NodeInventory {
            slabs: vec![a.clone(), b],
            volumes: place(&[a], vec![vol("v", "system", None)], &owners),
            ..Default::default()
        };
        let inv2 = NodeInventory {
            slabs: vec![c],
            ..Default::default()
        };
        let all: BTreeMap<String, NodeInventory> =
            [("n1".to_string(), inv1.clone()), ("n2".to_string(), inv2)].into();
        let t = tiers(&all);
        assert_eq!(t.len(), 2);
        let hot = t.iter().find(|t| t.tier == "hot").unwrap();
        assert_eq!(hot.nodes, vec!["n1".to_string(), "n2".to_string()]);
        assert_eq!((hot.total_bytes, hot.free_bytes, hot.allocated_bytes), (150, 90, 60));
        assert_eq!(hot.volumes, 1);
        assert_eq!(volumes_on(&inv1, "s1").len(), 1);
        assert!(volumes_on(&inv1, "s2").is_empty());
    }

    #[test]
    fn parent_cycle_terminates() {
        let vols = vec![vol("a", "", Some("b")), vol("b", "", Some("a"))];
        let p = place(&[], vols, &BTreeMap::new());
        assert!(p.iter().all(|v| v.placed_by == PlacedBy::Unknown));
    }

    #[test]
    fn engine_shapes_parse() {
        let s: Slab = serde_json::from_value(serde_json::json!({
            "id": "5d1c", "tier": "hot", "role": "data", "domain": "drive=file+x",
            "slot_size": 1048576, "total_slots": 10, "free_slots": 4,
            "allocated_slots": 6, "total_bytes": 10485760,
            "total_bytes_human": "10 MiB", "free_bytes": 4194304,
            "free_bytes_human": "4 MiB"
        }))
        .unwrap();
        assert_eq!(s.allocated_bytes(), 6 * 1048576);
        let v: EngineVolume = serde_json::from_value(serde_json::json!({
            "id": "9f0e", "name": "fastetcd-data", "virtual_size_bytes": 1,
            "virtual_size_human": "1 B", "allocated_bytes": 0, "allocated_human": "0 B",
            "shared_bytes": 0, "shared_human": "0 B", "array_id": null,
            "redundancy": "none", "health": "healthy", "physical_bytes": 0,
            "sealed": false, "access": "rw", "writable": true, "role": "data",
            "owner": {"kind": "PersistentVolumeClaim", "namespace": "kube-system", "name": "etcd"}
        }))
        .unwrap();
        assert_eq!(
            v.owner.unwrap().describe(),
            "PersistentVolumeClaim kube-system/etcd"
        );
    }
}
