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
    pub replication: ReplicationConfig,
    pub local: LocalConfig,
    pub recovery: RecoveryConfig,
    pub legs: LegsConfig,
    pub kubernetes: KubeConfig,
    pub nodes: Vec<NodeConfig>,
    pub pools: Vec<PoolConfig>,
}

/// Re-legging a distributed volume when a node carrying a leg is lost
/// (#1). A leg counts as lost once its node crosses `poll.fail_threshold`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RecoveryConfig {
    /// Replace lost legs automatically. Off: legs are still marked lost
    /// and the volume degraded, and a move is left to the operator.
    /// Unset: on for a lone instance, off when `[replication] peers` is
    /// set — every peer sees the same loss, so exactly one of them must
    /// be told to act (`enabled = true` there).
    pub enabled: Option<bool>,
    /// After a failed re-leg attempt (no target, engine error, rebuild
    /// that never converged), wait this long before the next one.
    pub cooldown_secs: u64,
    /// How long a new member may take to rebuild before the replacement
    /// is abandoned and rolled back.
    pub rebuild_timeout_secs: u64,
    /// Resync rate caps per `bandwidth_class` (#33), bytes a second of
    /// member data, applied to the head array's rebuilds. 0 = unlimited;
    /// `unthrottled` is always 0.
    pub rate_low: u64,
    pub rate_normal: u64,
    pub rate_high: u64,
    /// Longest dual-attach window a caller may open (#33).
    pub max_dual_attach_secs: u64,
    /// Re-head a volume automatically when its head is lost (#14): fence
    /// it and promote a surviving leg that reads `in_sync`. **Off by
    /// default** (owner, 2026-10-06): turn it on only once a head is fenced
    /// through cluster membership/quorum (stormcluster), not merely
    /// unreachable from here — a partitioned head that keeps writing to
    /// the legs is split-brain. Until then failover is the consumer's
    /// tiebreaker's or an operator's (promote).
    pub rehead: bool,
    /// How long the head must have failed its polls before a re-head.
    pub rehead_after_secs: u64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            cooldown_secs: 300,
            rebuild_timeout_secs: 3600,
            rate_low: 50 << 20,
            rate_normal: 200 << 20,
            rate_high: 1 << 30,
            max_dual_attach_secs: 3600,
            rehead: false,
            rehead_after_secs: 120,
        }
    }
}

impl RecoveryConfig {
    /// The rebuild cap for a class, bytes a second (0 = unlimited).
    pub fn rate(&self, class: crate::model::BandwidthClass) -> u64 {
        use crate::model::BandwidthClass::*;
        match class {
            Low => self.rate_low,
            Normal => self.rate_normal,
            High => self.rate_high,
            Unthrottled => 0,
        }
    }

    /// Whether this instance replaces lost legs itself.
    pub fn active(&self, has_peers: bool) -> bool {
        self.enabled.unwrap_or(!has_peers)
    }
}

/// Adopting the stormblock on the machine stormstorage runs on (#9). On a
/// node nothing else tells it where storage is, so by default it looks at
/// the engine on loopback and, once that answers, registers it and the
/// live peers of its stormblock cluster. Nodes found this way are local to
/// this instance and never replicated — every instance finds its own.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LocalConfig {
    pub enabled: bool,
    /// The local engine's management API.
    pub engine_url: String,
    /// Node name; unset = the engine's own name (its discovery
    /// `local_node`), else this machine's hostname.
    pub name: Option<String>,
    /// Also adopt the live peers in the local engine's stormblock cluster.
    pub cluster_peers: bool,
    /// Bearer token file for the engine(s): `$STORMBLOCK_API_TOKEN` wins;
    /// otherwise the first readable of this file, `$STORMBLOCK_TOKEN_FILE`,
    /// and [`DEFAULT_TOKEN_FILES`].
    pub token_file: Option<String>,
    /// A file holding the cluster's *shared* engine token (stormblock's
    /// `management.api_token`), presented to every engine, peers too
    /// (#12). The files above hold a token the engine minted for itself,
    /// which is presented only to an engine on this machine.
    pub shared_token_file: Option<String>,
    /// The engine's **admin** token file, for its destructive verbs
    /// (array create/delete/members, drive close; stormblock#274, #47).
    /// Presented only to an engine on this machine, like a minted token.
    /// No default: stormblock keeps it out of what services mount
    /// (`/run/stormblock-admin/admin_token`); without it the `[kubernetes]`
    /// bearer is presented, which the engine reviews against
    /// `storage-admin`.
    pub admin_token_file: Option<String>,
    /// Cluster-level tier role given to adopted nodes.
    pub tier: Option<String>,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            engine_url: "http://127.0.0.1:9090".into(),
            name: None,
            cluster_peers: true,
            token_file: None,
            shared_token_file: None,
            admin_token_file: None,
            tier: None,
        }
    }
}

impl LocalConfig {
    /// The engine token, if one is available to this process. Read on
    /// every call: stormblock mints the file at start (#38).
    pub fn token(&self) -> Option<String> {
        self.token_source().0
    }

    /// The engine token and where it came from — or, without one, why
    /// not. The reason goes into the log when an engine refuses (#38).
    ///
    /// The family order (stormdrive#14, #42): `$STORMBLOCK_API_TOKEN`,
    /// then the first *readable*, non-empty file of `token_file`,
    /// `$STORMBLOCK_TOKEN_FILE`, and the default paths — the last one is
    /// where a stormcos unit mounts the engine's minted token (stormcos#104).
    pub fn token_source(&self) -> (Option<String>, String) {
        self.token_for(&self.engine_url)
    }

    /// The token to present to the engine at `url`, and where it came from
    /// — stormblock's own rule (`mgmt::auth::token_for`, #107; #12): the
    /// cluster's *shared* token for any engine; else the token this
    /// machine's engine minted, but only for an engine on this machine. A
    /// minted token means nothing to a peer, which minted its own.
    pub fn token_for(&self, url: &str) -> (Option<String>, String) {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let shared = token_search(env("STORMBLOCK_API_TOKEN"), &self.shared_token_file.iter().cloned().collect::<Vec<_>>());
        if shared.0.is_some() {
            return shared;
        }
        if !is_this_machine(url) {
            return (
                None,
                format!(
                    "no shared token for an engine on another machine ({}) — a minted token means nothing \
                     to a peer: set $STORMBLOCK_API_TOKEN or [local] shared_token_file to the cluster's \
                     api_token, or the node's api_token/token_file",
                    shared.1
                ),
            );
        }
        token_search(None, &token_files(self.token_file.as_deref(), env("STORMBLOCK_TOKEN_FILE").as_deref()))
    }
}

impl LocalConfig {
    /// The admin token for the engine at `url` (#47): `$STORMBLOCK_ADMIN_TOKEN`
    /// for any engine, else `admin_token_file` for an engine on this machine.
    pub fn admin_token_for(&self, url: &str) -> Option<String> {
        if let Some(t) = std::env::var("STORMBLOCK_ADMIN_TOKEN").ok().filter(|v| !v.trim().is_empty()) {
            return Some(t.trim().to_string());
        }
        let file = self.admin_token_file.as_deref()?;
        if !is_this_machine(url) {
            return None;
        }
        token_search(None, &[file.to_string()]).0
    }
}

/// The host part of an engine URL, without port or brackets.
fn url_host(url: &str) -> &str {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let hostport = rest.split(['/', '?']).next().unwrap_or("");
    if let Some(v6) = hostport.strip_prefix('[') {
        return v6.split(']').next().unwrap_or("");
    }
    match hostport.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => h,
        _ => hostport,
    }
}

/// Whether the engine at `url` runs on this machine: a loopback host,
/// this machine's hostname, or an IP address held here (binding a socket
/// to it succeeds only then). A local engine may announce itself by its
/// LAN address (self-registration).
pub fn is_this_machine(url: &str) -> bool {
    let host = url_host(url);
    if matches!(host, "" | "localhost") {
        return true;
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return ip.is_loopback() || std::net::UdpSocket::bind((ip, 0)).is_ok();
    }
    let me = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .or_else(|_| std::fs::read_to_string("/etc/hostname"))
        .unwrap_or_default();
    let me = me.trim();
    // Exact, or a bare name equal to our short name: never a same-named
    // host in another domain, which would be handed our minted token.
    !me.is_empty()
        && (host.eq_ignore_ascii_case(me)
            || (!host.contains('.') && host.eq_ignore_ascii_case(me.split('.').next().unwrap_or(""))))
}

/// The node's Kubernetes apiserver, read for each node volume's PV and
/// PVC (#28). The family default, as stormconsole's: the apiserver on
/// loopback, its TLS not verified there.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KubeConfig {
    pub enabled: bool,
    pub server: String,
    /// Bearer token file: `$KUBE_TOKEN` wins; otherwise the first readable
    /// of this file and [`DEFAULT_KUBE_TOKEN_FILES`]. None: anonymous.
    pub token_file: Option<String>,
    /// CA to verify the apiserver with.
    pub ca_file: Option<String>,
    /// Unset: skip verification for a loopback server only.
    pub insecure_skip_tls_verify: Option<bool>,
}

impl Default for KubeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            server: "https://127.0.0.1:6443".into(),
            token_file: None,
            ca_file: None,
            insecure_skip_tls_verify: None,
        }
    }
}

/// Where an apiserver token is looked for when nothing names one: the
/// node-admin token stormcert writes (stormcos#60), then a pod's
/// ServiceAccount token.
pub const DEFAULT_KUBE_TOKEN_FILES: &[&str] = &[
    "/data/stormcert/node-admin.token",
    "/var/run/secrets/kubernetes.io/serviceaccount/token",
];

impl KubeConfig {
    pub fn token(&self) -> Option<String> {
        self.token_source().0
    }

    /// The token and where it came from, or why there is none.
    pub fn token_source(&self) -> (Option<String>, String) {
        if let Some(t) = std::env::var("KUBE_TOKEN").ok().filter(|v| !v.trim().is_empty()) {
            return (Some(t.trim().to_string()), "$KUBE_TOKEN".into());
        }
        let mut files: Vec<String> = self.token_file.iter().map(|p| p.trim().to_string()).collect();
        files.extend(DEFAULT_KUBE_TOKEN_FILES.iter().map(|p| p.to_string()));
        files.dedup();
        token_search(None, &files)
    }

    pub fn skip_tls_verify(&self) -> bool {
        self.insecure_skip_tls_verify.unwrap_or_else(|| {
            let host = self
                .server
                .split("://")
                .nth(1)
                .unwrap_or(&self.server)
                .split('/')
                .next()
                .unwrap_or("");
            host.starts_with("127.") || host.starts_with("localhost") || host.starts_with("[::1]")
        })
    }
}

/// Where an engine token file is looked for when nothing names one.
pub const DEFAULT_TOKEN_FILES: &[&str] = &[
    "/etc/stormblock/api_token",
    "/var/lib/stormblock/api_token",
    "/run/stormblock/engine/api_token",
];

/// The token files to try, in order: config, `$STORMBLOCK_TOKEN_FILE`,
/// then the defaults.
fn token_files(config: Option<&str>, env: Option<&str>) -> Vec<String> {
    let mut v: Vec<String> = config.into_iter().chain(env).map(|p| p.trim().to_string()).collect();
    v.extend(DEFAULT_TOKEN_FILES.iter().map(|p| p.to_string()));
    v.dedup();
    v
}

pub fn token_search(env_token: Option<String>, files: &[String]) -> (Option<String>, String) {
    if let Some(t) = env_token {
        return (Some(t.trim().to_string()), "$STORMBLOCK_API_TOKEN".into());
    }
    let mut why = Vec::new();
    for path in files {
        match std::fs::read_to_string(path) {
            Ok(s) if !s.trim().is_empty() => return (Some(s.trim().to_string()), path.clone()),
            Ok(_) => why.push(format!("{path} is empty")),
            Err(e) => why.push(format!("{path}: {e}")),
        }
    }
    (None, format!("no token: {}", why.join("; ")))
}

/// Peer stormstorage instances (one per site/cluster). Durable-intent
/// state (volumes, registered nodes) replicates to every peer on change;
/// last-writer-wins by revision. See src/replicate.rs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    /// Base URLs of the other instances, e.g. ["http://siteb:9093"].
    pub peers: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9093".into(),
            data_dir: None,
            federation: FederationConfig::default(),
            poll: PollConfig::default(),
            api: ApiConfig::default(),
            replication: ReplicationConfig::default(),
            local: LocalConfig::default(),
            recovery: RecoveryConfig::default(),
            legs: LegsConfig::default(),
            kubernetes: KubeConfig::default(),
            nodes: Vec::new(),
            pools: Vec::new(),
        }
    }
}

/// How a leg is served to its head (#27, stormblock#210).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LegsConfig {
    /// The host NQN a head presents to its legs; `{node}` is the head's
    /// node name. Each leg is attached for this NQN alone (served from a
    /// subsystem of that host's own), and the head's drive URI carries it
    /// as `hostnqn=`, so the head presents exactly this name whatever its
    /// engine's default initiator NQN is.
    pub host_nqn: String,
}

impl Default for LegsConfig {
    fn default() -> Self {
        Self { host_nqn: "nqn.2026-10.lo.storm:stormstorage:{node}".into() }
    }
}

impl LegsConfig {
    /// The host NQN of head `node`.
    pub fn host_nqn_for(&self, node: &str) -> String {
        self.host_nqn.replace("{node}", node)
    }

    fn validate(&self) -> anyhow::Result<()> {
        let n = self.host_nqn_for("x");
        // What the engine accepts (nqn., at most 223 bytes); `&`, `?`,
        // `/` and whitespace would break the drive URI it rides in.
        if !n.starts_with("nqn.") || self.host_nqn.len() > 200 {
            anyhow::bail!("legs.host_nqn {:?}: must start with nqn. and be short (≤ 200)", self.host_nqn);
        }
        if !self.host_nqn.contains("{node}") {
            anyhow::bail!("legs.host_nqn {:?}: needs {{node}}, so each head has its own", self.host_nqn);
        }
        if n.chars().any(|c| matches!(c, '&' | '?' | '/' | '#') || c.is_whitespace()) {
            anyhow::bail!("legs.host_nqn {:?}: no & ? / # or spaces (it goes into a drive URI)", self.host_nqn);
        }
        Ok(())
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
    /// Bearer token. Non-empty: every inbound write (volume create,
    /// delete, move, export; replicate) needs `Authorization: Bearer
    /// <token>`, and outbound replication pushes send it — peers share one
    /// token. Reads and stormblock self-registration stay open. Empty = no
    /// auth (#6).
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
    /// A file holding that engine's token, read on every call (#12). After
    /// `api_token`, before the `[local]` tokens.
    #[serde(default)]
    pub token_file: Option<String>,
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
            Ok(s) => {
                for key in unknown_top_level_keys(&s) {
                    tracing::warn!(
                        ?path,
                        key,
                        "unknown top-level config key, ignored (engine token: [local] token_file)"
                    );
                }
                Ok(toml::from_str(&s)?)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(?path, "no config file, using defaults");
                Ok(Self::default())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Top-level keys serde would silently drop: a `token_file` there
    /// (instead of under `[local]`) left a node's engine polled bare (#42).
    pub fn unknown_top_level_keys(text: &str) -> Vec<String> {
        unknown_top_level_keys(text)
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
        self.legs.validate()?;
        if self.poll.interval_secs == 0 {
            anyhow::bail!("poll.interval_secs must be non-zero");
        }
        Ok(())
    }

    pub fn pool(&self, name: &str) -> Option<&PoolConfig> {
        self.pools.iter().find(|p| p.name == name)
    }
}

fn unknown_top_level_keys(text: &str) -> Vec<String> {
    const KNOWN: &[&str] = &[
        "listen_addr", "data_dir", "federation", "poll", "api", "replication",
        "local", "recovery", "legs", "kubernetes", "nodes", "pools",
    ];
    match text.parse::<toml::Table>() {
        Ok(t) => t.keys().filter(|k| !KNOWN.contains(&k.as_str())).cloned().collect(),
        Err(_) => Vec::new(),
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

    #[test]
    fn token_files_family_order() {
        let f = token_files(Some("/cfg/t"), Some("/env/t"));
        assert_eq!(f[0], "/cfg/t");
        assert_eq!(f[1], "/env/t");
        assert_eq!(&f[2..], DEFAULT_TOKEN_FILES);
        assert_eq!(token_files(None, None), DEFAULT_TOKEN_FILES);
        assert!(DEFAULT_TOKEN_FILES.contains(&"/run/stormblock/engine/api_token"));
    }

    #[test]
    fn token_search_takes_first_readable() {
        let dir = std::env::temp_dir().join(format!("ss-tok-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty");
        let good = dir.join("good");
        std::fs::write(&empty, "  \n").unwrap();
        std::fs::write(&good, "tok\n").unwrap();
        let files: Vec<String> = [dir.join("missing"), empty.clone(), good.clone()]
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        let (t, src) = token_search(None, &files);
        assert_eq!(t.as_deref(), Some("tok"));
        assert_eq!(src, good.display().to_string());
        let (t, src) = token_search(Some(" envtok ".into()), &files);
        assert_eq!(t.as_deref(), Some("envtok"));
        assert_eq!(src, "$STORMBLOCK_API_TOKEN");
        let (t, why) = token_search(None, &files[..2]);
        assert!(t.is_none());
        assert!(why.contains("missing") && why.contains("is empty"), "{why}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn this_machine_by_url() {
        assert_eq!(url_host("http://10.0.0.5:9090/x"), "10.0.0.5");
        assert_eq!(url_host("http://[::1]:9090"), "::1");
        assert_eq!(url_host("http://node-a"), "node-a");
        assert!(is_this_machine("http://127.0.0.1:9090"));
        assert!(is_this_machine("http://localhost:9090"));
        assert!(is_this_machine("http://[::1]:9090"));
        // TEST-NET-1: never held by a build box.
        assert!(!is_this_machine("http://192.0.2.1:9090"));
        assert!(!is_this_machine("http://peer-that-is-not-me.invalid:9090"));
        let me = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default();
        if !me.trim().is_empty() {
            assert!(is_this_machine(&format!("http://{}:9090", me.trim())));
        }
    }

    /// #12: the minted token goes to this machine's engine only; a shared
    /// one goes everywhere. (Run only without $STORMBLOCK_API_TOKEN set,
    /// which would be the shared token for every engine.)
    #[test]
    fn minted_token_stays_local_shared_goes_to_peers() {
        if std::env::var("STORMBLOCK_API_TOKEN").is_ok_and(|v| !v.trim().is_empty()) {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ss-peer-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let minted = dir.join("minted");
        let shared = dir.join("shared");
        std::fs::write(&minted, "mint\n").unwrap();
        std::fs::write(&shared, "fleet\n").unwrap();
        let mut l = LocalConfig { token_file: Some(minted.display().to_string()), ..Default::default() };
        assert_eq!(l.token_for("http://127.0.0.1:9090").0.as_deref(), Some("mint"));
        let (t, why) = l.token_for("http://192.0.2.1:9090");
        assert!(t.is_none());
        assert!(why.contains("shared_token_file"), "{why}");
        l.shared_token_file = Some(shared.display().to_string());
        assert_eq!(l.token_for("http://192.0.2.1:9090").0.as_deref(), Some("fleet"));
        assert_eq!(l.token_for("http://127.0.0.1:9090").0.as_deref(), Some("fleet"), "shared wins, as in stormblock");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn legs_host_nqn_template() {
        let l = LegsConfig::default();
        assert_eq!(l.host_nqn_for("server1"), "nqn.2026-10.lo.storm:stormstorage:server1");
        let c: Config = toml::from_str("[legs]\nhost_nqn = \"nqn.x:h-{node}\"\n").unwrap();
        assert_eq!(c.legs.host_nqn_for("a"), "nqn.x:h-a");
        c.validate().unwrap();
        for bad in ["nqn.x:fixed", "iqn.x:{node}", "nqn.x:{node}&y"] {
            let c = Config { legs: LegsConfig { host_nqn: bad.into() }, ..Config::default() };
            assert!(c.validate().is_err(), "{bad}");
        }
    }

    #[test]
    fn flags_top_level_token_file() {
        let text = "listen_addr = \"0.0.0.0:9093\"\ntoken_file = \"/x\"\n[local]\nenabled = true\n";
        assert_eq!(Config::unknown_top_level_keys(text), vec!["token_file".to_string()]);
        assert!(Config::unknown_top_level_keys("[local]\ntoken_file = \"/x\"\n").is_empty());
    }

    /// The component entry's shipped config (stormcentral, #42), verbatim.
    #[test]
    fn shipped_entry_config() {
        let text = r#"# stormstorage under stormd, in a golden.
listen_addr = "0.0.0.0:9093"
data_dir    = "/var/lib/stormstorage"

[local]
# The node's stormblock engine refuses calls without its minted token
# (stormblock 19); the unit mounts the host's /run/stormblock read-only.
token_file  = "/run/stormblock/engine/api_token""#;
        assert!(Config::unknown_top_level_keys(text).is_empty());
        let c: Config = toml::from_str(text).unwrap();
        assert_eq!(c.data_dir.as_deref(), Some("/var/lib/stormstorage"));
        assert_eq!(c.local.token_file.as_deref(), Some("/run/stormblock/engine/api_token"));
    }
}
