//! stormstorage.toml parsing. Missing file = defaults; CLI overrides file.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub listen_addr: String,
    pub data_dir: Option<String>,
    pub federation: FederationConfig,
    pub poll: PollConfig,
    pub api: ApiConfig,
    pub nodes: Vec<NodeConfig>,
    pub pools: Vec<PoolConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9093".into(),
            data_dir: None,
            federation: FederationConfig::default(),
            poll: PollConfig::default(),
            api: ApiConfig::default(),
            nodes: Vec::new(),
            pools: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FederationConfig {
    /// The physical/logical rung order, top-down. Spreading "at rung R"
    /// means distinct label-chain prefixes down to R.
    pub rungs: Vec<String>,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            rungs: [
                "site",
                "building",
                "room",
                "row",
                "rack",
                "multicluster",
                "cluster",
                "node",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PollConfig {
    pub interval_secs: u64,
    /// Consecutive poll failures before a node is marked unhealthy.
    pub fail_threshold: u32,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            interval_secs: 15,
            fail_threshold: 3,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    /// Empty = no auth (family posture); present from day one.
    pub api_token: String,
}

/// A storage node: an SNO stormblock cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub name: String,
    /// stormblock management base URL, e.g. "http://192.168.8.150:9090".
    pub engine_url: String,
    #[serde(default)]
    pub api_token: Option<String>,
    /// Failure-domain labels, rung → value (site/rack/cluster/…).
    /// "node" defaults to `name`; "cluster" defaults to `name` too (SNO).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Cluster-level tier role: "high" | "medium" | "backup" | free-form.
    #[serde(default)]
    pub tier: Option<String>,
}

impl NodeConfig {
    /// Labels with SNO defaults applied and `node` always present.
    pub fn effective_labels(&self) -> BTreeMap<String, String> {
        let mut l = self.labels.clone();
        l.entry("node".into()).or_insert_with(|| self.name.clone());
        l.entry("cluster".into())
            .or_insert_with(|| self.name.clone());
        l
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub name: String,
    #[serde(default)]
    pub selector: Selector,
    /// Default leg count for volumes created in this pool.
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    /// Default spread rung.
    #[serde(default = "default_rung")]
    pub rung: String,
}

fn default_replicas() -> u32 {
    2
}
fn default_rung() -> String {
    "node".into()
}

/// Which nodes a pool draws from. All present conditions must hold;
/// an empty selector matches every node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Selector {
    pub tier: Option<String>,
    /// Every listed label must match the node's effective labels.
    pub labels: BTreeMap<String, String>,
    /// Explicit node names; empty = no restriction.
    pub nodes: Vec<String>,
}

impl Selector {
    pub fn matches(&self, node: &NodeConfig) -> bool {
        if let Some(t) = &self.tier {
            if node.tier.as_deref() != Some(t.as_str()) {
                return false;
            }
        }
        let eff = node.effective_labels();
        for (k, v) in &self.labels {
            if eff.get(k) != Some(v) {
                return false;
            }
        }
        if !self.nodes.is_empty() && !self.nodes.contains(&node.name) {
            return false;
        }
        true
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => Ok(toml::from_str(&s)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(?path, "no config file, using defaults");
                Ok(Self::default())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.listen_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| anyhow::anyhow!("listen_addr {:?}: {e}", self.listen_addr))?;
        let mut names = std::collections::HashSet::new();
        for n in &self.nodes {
            if !names.insert(&n.name) {
                anyhow::bail!("duplicate node name {:?}", n.name);
            }
        }
        for p in &self.pools {
            if !self.federation.rungs.contains(&p.rung) {
                anyhow::bail!(
                    "pool {:?}: rung {:?} not in federation.rungs {:?}",
                    p.name,
                    p.rung,
                    self.federation.rungs
                );
            }
        }
        if self.poll.interval_secs == 0 {
            anyhow::bail!("poll.interval_secs must be non-zero");
        }
        Ok(())
    }

    pub fn pool(&self, name: &str) -> Option<&PoolConfig> {
        self.pools.iter().find(|p| p.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn parses_nodes_and_pools() {
        let c: Config = toml::from_str(
            r#"
            [[nodes]]
            name = "shelf25"
            engine_url = "http://10.0.0.1:9090"
            tier = "high"
            labels = { site = "gw", rack = "r1" }

            [[nodes]]
            name = "shelf35"
            engine_url = "http://10.0.0.2:9090"
            tier = "medium"

            [[pools]]
            name = "fast"
            selector = { tier = "high" }
            replicas = 2
            rung = "cluster"
            "#,
        )
        .unwrap();
        c.validate().unwrap();
        assert_eq!(c.nodes.len(), 2);
        let p = c.pool("fast").unwrap();
        assert!(p.selector.matches(&c.nodes[0]));
        assert!(!p.selector.matches(&c.nodes[1]));
        let eff = c.nodes[1].effective_labels();
        assert_eq!(eff["node"], "shelf35");
        assert_eq!(eff["cluster"], "shelf35", "SNO: cluster defaults to node name");
    }

    #[test]
    fn pool_rung_must_be_known() {
        let c: Config = toml::from_str(
            r#"
            [[pools]]
            name = "x"
            rung = "warehouse"
            "#,
        )
        .unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn selector_conditions_compose() {
        let node: NodeConfig = toml::from_str(
            r#"name = "n1"
               engine_url = "http://x:9090"
               tier = "high"
               labels = { site = "gw" }"#,
        )
        .unwrap();
        let mut s = Selector::default();
        assert!(s.matches(&node), "empty selector matches all");
        s.labels.insert("site".into(), "gw".into());
        assert!(s.matches(&node));
        s.labels.insert("site".into(), "g8".into());
        assert!(!s.matches(&node));
        let s = Selector {
            nodes: vec!["other".into()],
            ..Default::default()
        };
        assert!(!s.matches(&node));
    }
}
