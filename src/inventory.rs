//! What each node's engine holds, as observed: its slabs (a node's pools)
//! and its volumes, each placed on the slab(s) it lives on (#9).
//!
//! Observed state, refreshed every poll and never persisted or replicated —
//! the engine is the source of truth and a restart re-reads it.
//!
//! Placement comes from the engine: stormblock ≥ v17.1.0 reports each
//! volume's `placement` (slabs, drives, legs, RAID partners; stormblock#136)
//! on `GET /api/v1/volumes?placement=true`, and every slab names its `drive`
//! by the identity stormdrive uses. From v18.1.0 each volume also says its
//! `kind`, whether it is `in_use`, its `attachments` and its `consumer`
//! (stormblock#138).
//!
//! For an older engine that sends no placement, the volume is placed from
//! the slabs' slot tables (`GET /api/v1/slabs/{id}/slots` names the volume
//! owning each slot); a volume that owns no slot (a fresh copy-on-write
//! clone reads entirely through its parent) is placed with its parent, and
//! failing that on the only slab of its role on the node. What is still
//! ambiguous is left unplaced rather than guessed.

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
    /// Failure domain, e.g. `drive=…`.
    pub domain: String,
    /// The drive the slab is on (stormblock ≥ v17.1.0).
    pub drive: Option<DriveRef>,
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

/// A drive, named the way stormdrive names it (stormblock `DriveRef`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DriveRef {
    pub serial: String,
    pub wwn: String,
    pub model: String,
    pub path: String,
}

impl DriveRef {
    /// Serial, else WWN, else path: the shortest stable name.
    pub fn describe(&self) -> String {
        [&self.serial, &self.wwn, &self.path]
            .into_iter()
            .find(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| "?".into())
    }
}

/// One way the engine is serving a volume right now (stormblock#138).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Attachment {
    /// `ublk`, `nvme-tcp` or `iscsi`.
    pub transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mounted_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lun: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nsid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

/// Where a volume lives, as the engine reports it (stormblock#136).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Placement {
    pub slabs: Vec<SlabPlacement>,
    pub drives: Vec<DrivePlacement>,
    pub legs: LegTotals,
    /// `none`, `needed`, `queued` or `running`.
    pub rebuild: String,
    /// Drive-level RAID under the volume's slabs, with each partner.
    pub arrays: Vec<ArrayPlacement>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SlabPlacement {
    pub id: String,
    pub role: String,
    pub tier: String,
    pub domain: String,
    pub drive: DriveRef,
    /// This node, or the host a fabric drive is served from.
    pub node: String,
    /// `ok`, `failed`, `quarantined`, `draining` or `missing`.
    pub state: String,
    pub array_id: Option<String>,
    pub legs: u64,
    pub shared_legs: u64,
    pub parity_legs: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DrivePlacement {
    pub drive: DriveRef,
    pub node: String,
    pub slabs: usize,
    pub legs: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LegTotals {
    pub policy: String,
    pub health: String,
    pub extents: usize,
    pub expected: usize,
    pub missing: usize,
    pub unreadable: usize,
    pub failed_slabs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ArrayPlacement {
    pub id: String,
    pub level: String,
    pub members: Vec<ArrayMember>,
}

/// A RAID partner: `active`, `degraded`, `spare`, `failed` or `rebuilding`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ArrayMember {
    pub index: usize,
    pub state: String,
    pub drive: DriveRef,
    pub node: String,
}

impl Placement {
    /// Slabs this volume stopped trusting, or that are gone.
    pub fn bad_slabs(&self) -> usize {
        self.slabs.iter().filter(|s| matches!(s.state.as_str(), "failed" | "missing")).count()
    }
    /// RAID partners that are not active.
    pub fn partners_down(&self) -> usize {
        self.arrays
            .iter()
            .flat_map(|a| &a.members)
            .filter(|m| m.state != "active" && m.state != "spare")
            .count()
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
    /// `volume`, `golden`, `blank`, `media`, `snapshot` or `template`
    /// (stormblock ≥ v18.1.0).
    pub kind: Option<String>,
    pub in_use: Option<bool>,
    pub attachments: Vec<Attachment>,
    /// The owner, else the mount a ublk device carries (`kind: Mount`).
    pub consumer: Option<Owner>,
    /// Where it lives (stormblock ≥ v17.1.0, `?placement=true`).
    pub placement: Option<Placement>,
}

impl EngineVolume {
    /// Who uses it: the engine's consumer, else its owner.
    pub fn consumer(&self) -> Option<&Owner> {
        self.consumer.as_ref().or(self.owner.as_ref())
    }
}

/// How a volume's slabs were worked out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacedBy {
    /// The engine said so (`placement`, stormblock#136).
    Engine,
    /// It owns slots on these slabs.
    Slots,
    /// It owns none; its parent's (a clone reads through it).
    Parent,
    /// The only slab of its role on the node.
    Role,
    /// Not determinable (an engine older than stormblock v17.1.0).
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacedVolume {
    #[serde(flatten)]
    pub volume: EngineVolume,
    /// Slab ids, sorted.
    pub slabs: Vec<String>,
    pub placed_by: PlacedBy,
    /// Its PV and the claim bound to it, from the apiserver (#28).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pv: Option<crate::kube::VolumeClaim>,
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
            pv: None,
        });
    }
    placed
}

/// Does placing these volumes need the slot tables? Only when an engine
/// reports no placement for some volume.
pub fn needs_slot_scan(volumes: &[EngineVolume]) -> bool {
    volumes.iter().any(|v| v.placement.is_none())
}

fn resolve(
    v: &EngineVolume,
    by_id: &BTreeMap<&str, &EngineVolume>,
    slabs: &[Slab],
    slot_owners: &BTreeMap<String, BTreeSet<String>>,
) -> (Vec<String>, PlacedBy) {
    if let Some(p) = v.placement.as_ref().filter(|p| !p.slabs.is_empty()) {
        let ids: BTreeSet<String> = p.slabs.iter().map(|s| s.id.clone()).collect();
        return (ids.into_iter().collect(), PlacedBy::Engine);
    }
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

    /// The shapes stormblock v19.4.0 sends (src/mgmt/api/{placement,usage,slabs}.rs).
    fn engine_volume_with_placement() -> EngineVolume {
        serde_json::from_value(serde_json::json!({
            "id": "v1", "name": "db", "virtual_size_bytes": 1073741824, "role": "data",
            "health": "degraded", "redundancy": "mirror:2", "sealed": false,
            "kind": "volume", "in_use": true,
            "attachments": [{"transport": "nvme-tcp", "target": "nqn.2024.io.stormblock:n1", "port": 4420, "nsid": 3}],
            "consumer": {"kind": "PersistentVolumeClaim", "namespace": "shop", "name": "db"},
            "owner": {"kind": "PersistentVolumeClaim", "namespace": "shop", "name": "db"},
            "placement": {
                "slabs": [
                    {"id": "d2", "role": "data", "tier": "hot", "domain": "drive=WD1",
                     "drive": {"serial": "WD1", "wwn": "naa.5001", "model": "WD", "path": "/dev/sda"},
                     "node": "n1", "state": "ok", "legs": 3, "shared_legs": 0, "parity_legs": 0, "bytes": 3145728},
                    {"id": "d1", "role": "data", "tier": "hot", "domain": "drive=S2",
                     "drive": {"serial": "S2", "model": "Samsung", "path": "/dev/nvme0n1"},
                     "node": "10.0.0.2", "state": "failed", "legs": 3, "shared_legs": 0, "parity_legs": 0,
                     "bytes": 3145728, "drain": {"state": "running", "moved": 1, "remaining": 2, "failed": 0}}
                ],
                "drives": [{"drive": {"serial": "WD1", "model": "WD", "path": "/dev/sda"}, "node": "n1", "slabs": 1, "legs": 3, "bytes": 3145728}],
                "legs": {"policy": "mirror:2", "health": "degraded", "extents": 3, "expected": 6,
                         "missing": 3, "unreadable": 0, "failed_slabs": ["d1"]},
                "rebuild": "needed",
                "arrays": [{"id": "a-1", "level": "raid1", "members": [
                    {"index": 0, "state": "active", "drive": {"serial": "WD1", "model": "", "path": ""}, "node": "n1"},
                    {"index": 1, "state": "rebuilding", "drive": {"serial": "S3", "model": "", "path": ""}, "node": "n2"}
                ]}]
            }
        }))
        .unwrap()
    }

    #[test]
    fn engine_placement_wins_and_skips_the_slot_scan() {
        let v = engine_volume_with_placement();
        assert!(!needs_slot_scan(std::slice::from_ref(&v)));
        assert!(needs_slot_scan(&[vol("old", "data", None)]), "older engine: no placement");
        // The slot table disagrees; the engine's word wins.
        let mut owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        owners.insert("v1".into(), ["elsewhere".to_string()].into());
        let slabs = vec![slab("d1", "data"), slab("d2", "data")];
        let p = place(&slabs, vec![v], &owners);
        assert_eq!(p[0].placed_by, PlacedBy::Engine);
        assert_eq!(p[0].slabs, vec!["d1".to_string(), "d2".to_string()]);
        let pl = p[0].volume.placement.as_ref().unwrap();
        assert_eq!(pl.bad_slabs(), 1);
        assert_eq!(pl.partners_down(), 1);
        assert_eq!(pl.rebuild, "needed");
        assert_eq!(pl.slabs[0].drive.describe(), "WD1");
        assert_eq!(p[0].volume.consumer().unwrap().describe(), "PersistentVolumeClaim shop/db");
        assert_eq!(p[0].volume.attachments[0].nsid, Some(3));
        assert_eq!(p[0].volume.in_use, Some(true));
    }

    #[test]
    fn slab_names_its_drive() {
        let s: Slab = serde_json::from_value(serde_json::json!({
            "id": "5d1c", "tier": "hot", "role": "data", "domain": "drive=WD-WX11",
            "drive": {"serial": "WD-WX11D28JFS6T", "wwn": "naa.50014ee", "model": "WDC WD20", "path": "/dev/sdd"}
        }))
        .unwrap();
        assert_eq!(s.drive.unwrap().describe(), "WD-WX11D28JFS6T");
        let old: Slab = serde_json::from_value(serde_json::json!({"id": "x"})).unwrap();
        assert!(old.drive.is_none());
    }
}
