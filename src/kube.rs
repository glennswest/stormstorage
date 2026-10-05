//! The PV and PVC of each node volume (#28), from the node's Kubernetes
//! apiserver.
//!
//! The engine does not know them: rustkube-node's mirror (rustkube-node#59,
//! `pkg/kubelet/src/system_claims.rs`) writes a PV and its bound PVC for
//! every stormblock volume a node holds — its `-data`, `-state` and `-logs`
//! volumes and the built-in driver's claims. Each PV carries the volume:
//! `spec.csi.driver` is `stormblock.storm.io`, `spec.csi.volumeHandle` and
//! the `storm.io/volume` annotation name it, and `storm.io/node` (else the
//! `nodeAffinity` hostname) names the node. `storm.io/volume-kind` and
//! `storm.io/component` say what it holds.
//!
//! Read once per poll (two lists: PVs and PVCs), joined onto every node's
//! inventory by (node, volume name). A failed read keeps the last view; a
//! change between readable and not is logged and an event, once.

use crate::api::AppState;
use crate::config::KubeConfig;
use crate::events::Severity;
use crate::inventory::NodeInventory;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// The CSI driver of a stormblock PV.
pub const DRIVER: &str = "stormblock.storm.io";

const TIMEOUT: Duration = Duration::from_secs(5);

/// A node volume's PV, and the claim bound to it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct VolumeClaim {
    /// The PV's name.
    pub pv: String,
    /// `Available`, `Bound`, `Released`, `Failed` (`status.phase`).
    pub phase: String,
    /// `Retain` or `Delete`.
    pub reclaim: String,
    pub capacity: String,
    /// `storm.io/volume-kind`: `data`, `state`, `logs`.
    pub volume_kind: Option<String>,
    /// `storm.io/component`.
    pub component: Option<String>,
    /// The PVC the PV's `claimRef` names, if any.
    pub claim: Option<ClaimRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClaimRef {
    pub namespace: String,
    pub name: String,
    pub uid: Option<String>,
    /// The PVC's `status.phase`; `None` when the claim does not exist.
    pub phase: Option<String>,
    /// The pair names each other: the PVC exists, its `spec.volumeName`
    /// is the PV, and its uid is the one the PV's `claimRef` carries.
    pub bound: bool,
}

impl VolumeClaim {
    /// The claim it names, `kube-system/fastetcd-data-n1`, or `no claim`.
    pub fn describe(&self) -> String {
        match &self.claim {
            Some(c) => format!("{}/{}", c.namespace, c.name),
            None => "no claim".into(),
        }
    }
    /// A PV and its claim, together and naming each other.
    pub fn complete(&self) -> bool {
        self.phase == "Bound" && self.claim.as_ref().is_some_and(|c| c.bound)
    }
}

/// Every stormblock PV the apiserver holds, by (node, volume name).
#[derive(Debug, Clone, Default)]
pub struct KubeView {
    pub claims: BTreeMap<(String, String), VolumeClaim>,
    pub fetched_at: Option<SystemTime>,
    /// Why the last read failed, if it did (the previous view is kept).
    pub error: Option<String>,
}

/// Build the view from a PV list and a PVC list (`items` of each).
pub fn view_of(pvs: &[Value], pvcs: &[Value]) -> BTreeMap<(String, String), VolumeClaim> {
    let s = |v: &Value| v.as_str().filter(|x| !x.is_empty()).map(String::from);
    let claims_by_name: BTreeMap<(String, String), &Value> = pvcs
        .iter()
        .filter_map(|c| {
            let m = &c["metadata"];
            Some(((s(&m["namespace"]).unwrap_or_default(), s(&m["name"])?), c))
        })
        .collect();
    let mut out: BTreeMap<(String, String), VolumeClaim> = BTreeMap::new();
    for pv in pvs {
        let spec = &pv["spec"];
        if spec["csi"]["driver"].as_str() != Some(DRIVER) {
            continue;
        }
        let meta = &pv["metadata"];
        let Some(name) = s(&meta["name"]) else { continue };
        let ann = &meta["annotations"];
        let Some(volume) = s(&ann["storm.io/volume"]).or_else(|| s(&spec["csi"]["volumeHandle"]))
        else {
            continue;
        };
        let Some(node) = s(&ann["storm.io/node"]).or_else(|| affinity_node(spec)) else {
            continue;
        };
        let claim = spec["claimRef"]["name"].as_str().filter(|n| !n.is_empty()).map(|cname| {
            let ns = spec["claimRef"]["namespace"].as_str().unwrap_or_default().to_string();
            let uid = s(&spec["claimRef"]["uid"]);
            let pvc = claims_by_name.get(&(ns.clone(), cname.to_string()));
            let bound = pvc.is_some_and(|c| {
                c["spec"]["volumeName"].as_str() == Some(name.as_str())
                    && (uid.is_none() || s(&c["metadata"]["uid"]) == uid)
            });
            ClaimRef {
                namespace: ns,
                name: cname.to_string(),
                uid,
                phase: pvc.and_then(|c| s(&c["status"]["phase"])),
                bound,
            }
        });
        let labels = &meta["labels"];
        let vc = VolumeClaim {
            pv: name,
            phase: s(&pv["status"]["phase"]).unwrap_or_default(),
            reclaim: s(&spec["persistentVolumeReclaimPolicy"]).unwrap_or_default(),
            capacity: s(&spec["capacity"]["storage"]).unwrap_or_default(),
            volume_kind: s(&labels["storm.io/volume-kind"]),
            component: s(&labels["storm.io/component"]),
            claim,
        };
        // Two PVs for one volume: a pair under the old unqualified names
        // beside the node-qualified one (rustkube-node#107, no migration).
        // The complete pair wins, then the node-qualified name.
        let key = (node.clone(), volume);
        let better = match out.get(&key) {
            None => true,
            Some(old) => rank(&vc, &node) > rank(old, &node),
        };
        if better {
            out.insert(key, vc);
        }
    }
    out
}

fn rank(vc: &VolumeClaim, node: &str) -> (bool, bool) {
    (vc.complete(), vc.pv.ends_with(&format!("-{node}")))
}

/// The hostname a PV's `nodeAffinity` pins it to.
fn affinity_node(spec: &Value) -> Option<String> {
    spec["nodeAffinity"]["required"]["nodeSelectorTerms"]
        .as_array()?
        .iter()
        .flat_map(|t| t["matchExpressions"].as_array().into_iter().flatten())
        .find(|e| e["key"] == "kubernetes.io/hostname" && e["operator"] == "In")
        .and_then(|e| e["values"][0].as_str())
        .map(String::from)
}

/// Do a stormstorage node name and a Kubernetes node name name the same
/// machine? Equal, or equal before the first dot (`n1` and `n1.g8.lo`).
pub fn same_node(a: &str, b: &str) -> bool {
    let short = |s: &str| s.split('.').next().unwrap_or(s).to_ascii_lowercase();
    a.eq_ignore_ascii_case(b) || short(a) == short(b)
}

/// Put each volume's PV/PVC on the node's inventory.
pub fn join(node: &str, inv: &mut NodeInventory, claims: &BTreeMap<(String, String), VolumeClaim>) {
    for v in &mut inv.volumes {
        v.pv = claims
            .get(&(node.to_string(), v.volume.name.clone()))
            .or_else(|| {
                claims
                    .iter()
                    .find(|((n, vol), _)| vol == &v.volume.name && same_node(n, node))
                    .map(|(_, c)| c)
            })
            .cloned();
    }
}

async fn fetch(cfg: &KubeConfig) -> anyhow::Result<(Vec<Value>, Vec<Value>)> {
    let mut http = reqwest::Client::builder()
        .timeout(TIMEOUT)
        .danger_accept_invalid_certs(cfg.skip_tls_verify());
    if let Some(ca) = cfg.ca_file.as_deref().filter(|p| !p.trim().is_empty()) {
        let pem = std::fs::read(ca).map_err(|e| anyhow::anyhow!("ca_file {ca}: {e}"))?;
        http = http.add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
    }
    let http = http.build()?;
    let token = cfg.token();
    let base = cfg.server.trim_end_matches('/');
    let mut lists = Vec::new();
    for path in ["/api/v1/persistentvolumes", "/api/v1/persistentvolumeclaims"] {
        let mut req = http.get(format!("{base}{path}"));
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let (_, source) = cfg.token_source();
            anyhow::bail!("GET {path}: {status} (token: {source})");
        }
        let body: Value = resp.json().await?;
        lists.push(body["items"].as_array().cloned().unwrap_or_default());
    }
    let pvcs = lists.pop().unwrap_or_default();
    let pvs = lists.pop().unwrap_or_default();
    Ok((pvs, pvcs))
}

/// Read the apiserver and join the result onto every node's inventory.
/// Called by the poller after the inventories are refreshed.
pub async fn refresh(state: &Arc<AppState>) {
    let cfg = &state.config.kubernetes;
    if !cfg.enabled {
        return;
    }
    let result = fetch(cfg).await;
    let claims = {
        let mut view = state.kube.write().await;
        let was_ok = view.fetched_at.is_some() && view.error.is_none();
        let first = view.fetched_at.is_none() && view.error.is_none();
        let (msg, sev) = match result {
            Ok((pvs, pvcs)) => {
                view.claims = view_of(&pvs, &pvcs);
                view.fetched_at = Some(SystemTime::now());
                view.error = None;
                let msg = (!was_ok).then(|| {
                    format!(
                        "kubernetes {}: {} stormblock PVs read",
                        cfg.server,
                        view.claims.len()
                    )
                });
                (msg, Severity::Info)
            }
            Err(e) => {
                let e = format!("{e:#}");
                let msg = (was_ok || first).then(|| {
                    format!("kubernetes {}: PVs not readable, node volumes show no PV/PVC: {e}", cfg.server)
                });
                view.error = Some(e);
                (msg, Severity::Warning)
            }
        };
        if let Some(m) = msg {
            match sev {
                Severity::Warning => tracing::warn!("{m}"),
                _ => tracing::info!("{m}"),
            }
            state.events.write().await.push(None, sev, "kubernetes", m);
        }
        view.claims.clone()
    };
    let mut inv = state.inventory.write().await;
    for (node, ni) in inv.iter_mut() {
        join(node, ni, &claims);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A PV and PVC as rustkube-node's mirror writes them.
    fn pair(volume: &str, node: &str, pv_name: &str, claim: &str, uid: &str) -> (Value, Value) {
        let pv = json!({
            "metadata": {
                "name": pv_name,
                "labels": {"storm.io/system-volume": "true", "storm.io/volume-kind": "data",
                           "storm.io/component": "fastetcd"},
                "annotations": {"storm.io/node": node, "storm.io/volume": volume},
            },
            "spec": {
                "capacity": {"storage": "1Gi"},
                "persistentVolumeReclaimPolicy": "Retain",
                "claimRef": {"kind": "PersistentVolumeClaim", "namespace": "kube-system",
                             "name": claim, "uid": uid},
                "csi": {"driver": DRIVER, "volumeHandle": volume},
            },
            "status": {"phase": "Bound"},
        });
        let pvc = json!({
            "metadata": {"name": claim, "namespace": "kube-system", "uid": uid},
            "spec": {"volumeName": pv_name},
            "status": {"phase": "Bound"},
        });
        (pv, pvc)
    }

    #[test]
    fn joins_pv_and_claim_by_node_and_volume() {
        let (pv, pvc) = pair("fastetcd-data", "n1", "storm-fastetcd-data-n1", "fastetcd-data-n1", "u1");
        let (pv2, pvc2) = pair("fastetcd-data", "n2", "storm-fastetcd-data-n2", "fastetcd-data-n2", "u2");
        let foreign = json!({"metadata": {"name": "nfs-1"}, "spec": {"nfs": {}}});
        let v = view_of(&[pv, pv2, foreign], &[pvc, pvc2]);
        assert_eq!(v.len(), 2);
        let c = &v[&("n1".to_string(), "fastetcd-data".to_string())];
        assert_eq!(c.pv, "storm-fastetcd-data-n1");
        assert_eq!(c.phase, "Bound");
        assert_eq!(c.reclaim, "Retain");
        assert_eq!(c.volume_kind.as_deref(), Some("data"));
        assert_eq!(c.component.as_deref(), Some("fastetcd"));
        let cl = c.claim.as_ref().unwrap();
        assert_eq!((cl.namespace.as_str(), cl.name.as_str()), ("kube-system", "fastetcd-data-n1"));
        assert!(cl.bound && c.complete());
        assert_eq!(c.describe(), "kube-system/fastetcd-data-n1");
    }

    #[test]
    fn claim_gone_or_rebound_is_not_bound() {
        let (pv, _) = pair("a-logs", "n1", "storm-a-logs-n1", "a-logs-n1", "u1");
        let v = view_of(std::slice::from_ref(&pv), &[]);
        let c = &v[&("n1".to_string(), "a-logs".to_string())];
        assert!(!c.complete());
        assert_eq!(c.claim.as_ref().unwrap().phase, None);
        // A claim of the same name made again has a new uid.
        let (_, mut pvc) = pair("a-logs", "n1", "storm-a-logs-n1", "a-logs-n1", "u1");
        pvc["metadata"]["uid"] = json!("u9");
        let v = view_of(&[pv], &[pvc]);
        assert!(!v[&("n1".to_string(), "a-logs".to_string())].claim.as_ref().unwrap().bound);
    }

    #[test]
    fn node_from_affinity_and_qualified_pair_wins() {
        // Old unqualified pair, released; new node-qualified pair, bound.
        let (mut old, _) = pair("fastetcd-data", "n1", "storm-fastetcd-data", "fastetcd-data", "u0");
        old["status"]["phase"] = json!("Released");
        old["metadata"]["annotations"] = json!({});
        old["spec"]["nodeAffinity"] = json!({"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "In", "values": ["n1"]}]}]}});
        let (new, pvc) = pair("fastetcd-data", "n1", "storm-fastetcd-data-n1", "fastetcd-data-n1", "u1");
        for pvs in [[old.clone(), new.clone()], [new, old]] {
            let v = view_of(&pvs, std::slice::from_ref(&pvc));
            assert_eq!(v.len(), 1);
            assert_eq!(v[&("n1".to_string(), "fastetcd-data".to_string())].pv, "storm-fastetcd-data-n1");
        }
    }

    #[test]
    fn short_and_full_node_names_match() {
        assert!(same_node("n1", "N1.g8.lo"));
        assert!(same_node("n1.g8.lo", "n1"));
        assert!(!same_node("n1", "n10"));
    }
}
