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
