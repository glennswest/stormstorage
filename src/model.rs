//! Federation state: nodes, distributed volumes. Persisted to
//! `<data_dir>/state.json` with atomic writes; rebuildable in principle
//! from the registry plus each engine's /v1/volumes.

use crate::config::NodeConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeSource {
    /// From stormstorage.toml.
    Static,
    /// Announced itself via POST /api/v1/storage/register.
    Registered,
    /// Adopted by this instance: the engine on its own machine, or a peer
    /// in that engine's stormblock cluster (`[local]`, #9). Not replicated.
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub healthy: bool,
    pub last_ok: Option<SystemTime>,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub free_bytes: u64,
    /// Topology labels the engine reports (merged under config labels —
    /// config wins on conflict).
    #[serde(default)]
    pub engine_topology: BTreeMap<String, String>,
    #[serde(default)]
    pub volumes: u64,
    pub source: NodeSource,
}

impl NodeStatus {
    pub fn new(source: NodeSource) -> Self {
        Self {
            healthy: false,
            last_ok: None,
            consecutive_failures: 0,
            total_bytes: 0,
            free_bytes: 0,
            engine_topology: BTreeMap::new(),
            volumes: 0,
            source,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub config: NodeConfig,
    pub status: NodeStatus,
}

impl Node {
    /// Effective labels: engine topology, overridden by config labels,
    /// with the SNO defaults (node/cluster = name).
    pub fn labels(&self) -> BTreeMap<String, String> {
        let mut l = self.status.engine_topology.clone();
        for (k, v) in self.config.effective_labels() {
            l.insert(k, v);
        }
        l
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegState {
    /// Volume exists on the node.
    Created,
    /// Creation failed; message in the leg.
    Failed,
    /// Was created, and its node has since crossed the unhealthy
    /// threshold. The reconciler replaces it (#1).
    Lost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Leg {
    pub node: String,
    pub volume_id: Option<String>,
    pub state: LegState,
    #[serde(default)]
    pub message: Option<String>,
    /// The engine's own node name — /v1 read-write attach is gated on it.
    #[serde(default)]
    pub master_node: Option<String>,
    /// Attach coordinates once the leg is exported over NVMe-TCP.
    #[serde(default)]
    pub export: Option<crate::engine::AttachedLeg>,
    /// The head engine's drive uuid for this leg (POST /api/v1/drives).
    #[serde(default)]
    pub drive_uuid: Option<String>,
    /// The head array's member uuid for this leg.
    #[serde(default)]
    pub member_uuid: Option<String>,
    /// The leg volume's own /v1 epoch on its engine after the last fence
    /// (#33). The head presents it on the leg attach once stormblock#6
    /// enforces it.
    #[serde(default)]
    pub epoch: Option<u64>,
}

/// Whether a volume's legs are a mirrored whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyState {
    /// Single leg — nothing to assemble.
    SingleLeg,
    /// Legs exist but the RAID is not (yet) assembled — either mid-flight
    /// or a failed assembly awaiting retry (see the volume's events).
    /// (Historic records carry this from before stormblock#73 landed.)
    PendingEngineSupport,
    /// RAID1 assembled on the head across every leg over NVMe-TCP.
    Assembled,
    /// Assembled, but at least one leg is lost: the array runs short of
    /// the redundancy asked for until the re-leg converges (#1).
    Degraded,
}

/// A leg being replaced: the new leg is a member of the head's array and
/// rebuilding; the old one leaves when the new one reports active.
/// Recorded so a restart resumes the wait instead of adding another member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replacement {
    /// Node whose leg is leaving.
    pub from: String,
    pub leg: Leg,
    /// Why: "operator move" or "node lost".
    pub reason: String,
    pub started_at: SystemTime,
}

/// A leg volume left on a node that could not be reached to delete it.
/// Reaped when the node answers again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Orphan {
    pub node: String,
    pub volume_id: String,
    #[serde(default)]
    pub master_node: Option<String>,
    /// The distributed volume it was a leg of.
    pub of_volume: String,
    pub reason: String,
    pub since: SystemTime,
}

/// Whether a distributed volume is served to consumers (#2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportState {
    /// Not served: not assembled yet, revoked, or created before #2.
    #[default]
    None,
    /// Consumers attach `coordinates`.
    Published,
    /// The last publish failed; `message` says why.
    Failed,
}

/// What a consumer attaches: the mirror, never one side of it. For an
/// assembled volume it is a /v1 volume pinned to the head's array
/// (`<name>-mirror`); for a single-leg volume it is that leg's own export.
/// The volume id is kept beside the coordinates because an NSID can be
/// reused (stormblock#96).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Export {
    #[serde(default)]
    pub state: ExportState,
    /// The served volume's id on `node`'s engine.
    #[serde(default)]
    pub volume_id: Option<String>,
    /// Where it is served: the head, or the only leg's node.
    #[serde(default)]
    pub node: Option<String>,
    /// That engine's own node name — the attach and detach are asked as it.
    #[serde(default)]
    pub master_node: Option<String>,
    #[serde(default)]
    pub coordinates: Option<crate::engine::AttachedLeg>,
    #[serde(default)]
    pub published_at: Option<SystemTime>,
    /// The last republish returned different coordinates than before, so
    /// consumers must reconnect to the new ones.
    #[serde(default)]
    pub coordinates_changed: bool,
    #[serde(default)]
    pub message: Option<String>,
    /// The served volume came across with the array when the head moved
    /// (promote, #33): the engine has no /v1 record of it, so it is
    /// attached, detached and deleted through `/api/v1/volumes/{id}`.
    #[serde(default)]
    pub adopted: bool,
    /// Consumer hosts it is served to (#51), each from its own subsystem
    /// on the serving engine. With any named, the shared subsystem is not
    /// used: a closed engine (stormblock#210) admits no host there.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<HostServe>,
    /// Served per host from the first host named on: with every host
    /// withdrawn it is served to none, never back on the shared subsystem.
    #[serde(default)]
    pub per_host: bool,
    /// The served volume's engine-local id, which the per-host attach and
    /// withdraw take (a /v1 id is not one). Found by name when first needed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_id: Option<String>,
    /// Hosts withdrawn while the serving engine did not answer: withdrawn
    /// there when it does (`republish_on`), so a node that no longer runs
    /// the workload cannot keep reaching it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub withdrawing: Vec<PendingWithdraw>,
}

/// A host withdrawal waiting for its engine (#51).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingWithdraw {
    pub node: String,
    /// The served volume's engine-local id; `None` when it was never
    /// learned (found by `name` on that engine then).
    pub local_id: Option<String>,
    /// The served volume's name on that engine.
    pub name: String,
    pub host_nqn: String,
}

/// One consumer host a volume is served to (#51). The DH-HMAC-CHAP secret
/// is never kept here: it is returned to the caller that asks for the host
/// and nowhere else (not persisted, not replicated, not in events).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HostServe {
    pub host_nqn: String,
    #[serde(default)]
    pub dhchap: bool,
    /// What that host connects to: the per-host subsystem's NQN, the
    /// address and the NSID, with `host_nqn` (`--hostnqn`).
    #[serde(default)]
    pub coordinates: Option<crate::engine::AttachedLeg>,
    /// The last re-serve (move, promote, recovery) answered different
    /// coordinates: the host must reconnect, and with `dhchap` ask again
    /// for its secret.
    #[serde(default)]
    pub coordinates_changed: bool,
    #[serde(default)]
    pub served_at: Option<SystemTime>,
    /// Why the last serve to this host failed, if it did.
    #[serde(default)]
    pub message: Option<String>,
}

/// How fast a resync onto a new leg may run (#33, stormblock-csi's
/// `bandwidth_class`). Applied as the head array's rebuild rate cap
/// (`PUT /api/v1/arrays/{id}/rebuild`); the rates are `[recovery]` keys.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BandwidthClass {
    Low,
    #[default]
    Normal,
    High,
    Unthrottled,
}

/// A bounded window in which a second consumer (a live migration's
/// target) may attach the served volume (#33, was stormblock#7). Closed by
/// commit (fence + promote the target), abort, or expiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualAttach {
    pub target_node: String,
    /// The volume's epoch when the window opened; close must present it.
    pub epoch: u64,
    pub opened_at: SystemTime,
    pub expires_at: SystemTime,
}

/// A head that lost its role (promote, #33) and still holds the array and
/// drives of the legs, as far as we know. Reaped when its engine answers:
/// the leg drives go first, then the array with its superblocks kept —
/// never wiped, the legs belong to the new head.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleHead {
    pub node: String,
    pub array_id: String,
    pub drive_uris: Vec<String>,
    pub of_volume: String,
    /// The epoch the volume was fenced at.
    pub epoch: u64,
    pub since: SystemTime,
}

pub fn first_epoch() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistVolume {
    pub name: String,
    pub size_bytes: u64,
    pub pool: Option<String>,
    pub replicas: u32,
    pub rung: String,
    pub legs: Vec<Leg>,
    pub assembly: AssemblyState,
    /// Node whose engine holds the assembled array.
    #[serde(default)]
    pub head: Option<String>,
    /// Array id on the head engine.
    #[serde(default)]
    pub array_id: Option<String>,
    pub created_at: SystemTime,
    /// The leg replacement in flight, if any (#1).
    #[serde(default)]
    pub replacing: Option<Replacement>,
    /// No automatic re-leg before this — set after a failed attempt so a
    /// failure is not retried every poll.
    #[serde(default)]
    pub next_releg_after: Option<SystemTime>,
    /// No automatic assembly retry before this — set after a failed
    /// assembly (#7).
    #[serde(default)]
    pub next_assemble_after: Option<SystemTime>,
    /// How consumers attach this volume (#2).
    #[serde(default)]
    pub export: Export,
    /// Fencing epoch (#33): bumped by every fence; a promote must present
    /// the epoch the fence returned. Starts at 1, like /v1.
    #[serde(default = "first_epoch")]
    pub epoch: u64,
    /// Set by a fence and cleared by the promote that follows it: the head
    /// at this epoch has lost its writer role and none has taken it yet.
    #[serde(default)]
    pub fenced: bool,
    #[serde(default)]
    pub bandwidth_class: BandwidthClass,
    #[serde(default)]
    pub dual_attach: Option<DualAttach>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FedState {
    /// Bumped on every *durable-intent* mutation (volumes, registered
    /// nodes) — the replication watermark. Poll status never bumps it, so
    /// peers each observing the engines cannot ping-pong overwrites.
    #[serde(default)]
    pub revision: u64,
    pub nodes: BTreeMap<String, Node>,
    pub volumes: BTreeMap<String, DistVolume>,
    /// Leg volumes to delete once their node answers (#1).
    #[serde(default)]
    pub orphans: Vec<Orphan>,
    /// Former heads to clean up once they answer (#33).
    #[serde(default)]
    pub stale_heads: Vec<StaleHead>,
}

impl FedState {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => Ok(serde_json::from_str(&s)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = PathBuf::from(format!("{}.tmp", path.display()));
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Overlay static config nodes: config is authoritative for
    /// engine_url/labels/tier of nodes it names; persisted status is kept.
    pub fn apply_config_nodes(&mut self, configured: &[NodeConfig]) {
        for nc in configured {
            match self.nodes.get_mut(&nc.name) {
                Some(n) => {
                    n.config = nc.clone();
                    n.status.source = NodeSource::Static;
                }
                None => {
                    self.nodes.insert(
                        nc.name.clone(),
                        Node {
                            config: nc.clone(),
                            status: NodeStatus::new(NodeSource::Static),
                        },
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nc(name: &str) -> NodeConfig {
        toml::from_str(&format!(
            r#"name = "{name}"
               engine_url = "http://{name}:9090""#
        ))
        .unwrap()
    }

    #[test]
    fn state_roundtrip_and_config_overlay() {
        let dir = std::env::temp_dir().join(format!("ss-state-{}", std::process::id()));
        let path = dir.join("state.json");
        let mut st = FedState::default();
        st.apply_config_nodes(&[nc("a"), nc("b")]);
        st.nodes.get_mut("a").unwrap().status.healthy = true;
        st.save(&path).unwrap();

        let mut loaded = FedState::load(&path).unwrap();
        assert_eq!(loaded.nodes.len(), 2);
        assert!(loaded.nodes["a"].status.healthy, "status persisted");
        // Config re-applied on startup: url changes take effect, status kept.
        let mut a2 = nc("a");
        a2.engine_url = "http://a:9999".into();
        loaded.apply_config_nodes(&[a2]);
        assert_eq!(loaded.nodes["a"].config.engine_url, "http://a:9999");
        assert!(loaded.nodes["a"].status.healthy);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn node_labels_prefer_config_over_engine() {
        let mut n = Node {
            config: nc("n1"),
            status: NodeStatus::new(NodeSource::Static),
        };
        n.status
            .engine_topology
            .insert("rack".into(), "engine-says-r9".into());
        n.config.labels.insert("rack".into(), "r1".into());
        let l = n.labels();
        assert_eq!(l["rack"], "r1");
        assert_eq!(l["node"], "n1");
        assert_eq!(l["cluster"], "n1");
    }
}
