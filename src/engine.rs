//! Client for one stormblock engine's management API. stormstorage only
//! ever *drives* engines — it is never in the data path.

use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone)]
pub struct Engine {
    url: String,
    token: Option<String>,
    http: reqwest::Client,
}

/// Where a leg's namespace answers: the attach coordinates the head node
/// turns into an `nvme-tcp://` drive URI.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttachedLeg {
    pub nqn: String,
    pub traddr: String,
    pub trsvcid: u16,
    pub nsid: u32,
}

impl AttachedLeg {
    pub fn drive_uri(&self) -> String {
        format!(
            "nvme-tcp://{}:{}/{}?nsid={}",
            self.traddr, self.trsvcid, self.nqn, self.nsid
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct Capacity {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub topology: BTreeMap<String, String>,
}

impl Engine {
    pub fn new(url: &str, token: Option<String>) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            token,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
        }
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        let r = self.http.get(format!("{}{path}", self.url));
        match &self.token {
            Some(t) if !t.is_empty() => r.bearer_auth(t),
            _ => r,
        }
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let r = self.http.request(method, format!("{}{path}", self.url));
        match &self.token {
            Some(t) if !t.is_empty() => r.bearer_auth(t),
            _ => r,
        }
    }

    /// GET /v1/nodes/capacity. The response is one NodeCapacity for an SNO
    /// node but may be a list or an object wrapper — parse tolerantly.
    pub async fn capacity(&self) -> anyhow::Result<Capacity> {
        let v: Value = self
            .get("/v1/nodes/capacity")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let obj = first_capacity_object(&v)
            .ok_or_else(|| anyhow::anyhow!("unrecognized capacity shape: {v}"))?;
        let mut topology = BTreeMap::new();
        if let Some(t) = obj.get("topology").and_then(|t| t.as_object()) {
            for (k, val) in t {
                if let Some(s) = val.as_str() {
                    topology.insert(k.clone(), s.to_string());
                }
            }
        }
        Ok(Capacity {
            total_bytes: obj.get("total_bytes").and_then(|x| x.as_u64()).unwrap_or(0),
            free_bytes: obj.get("free_bytes").and_then(|x| x.as_u64()).unwrap_or(0),
            topology,
        })
    }

    /// POST /v1/volumes — name-idempotent create per the /v1 contract.
    /// `slaves: 0`: a leg is a standalone volume — cross-node redundancy is
    /// stormstorage's job (legs across nodes), not the engine's replica
    /// machinery; an SNO node has no peers to host a slave anyway.
    pub async fn create_volume(&self, name: &str, size_bytes: u64) -> anyhow::Result<Value> {
        let resp = self
            .req(reqwest::Method::POST, "/v1/volumes")
            .json(&serde_json::json!({
                "name": name,
                "size_bytes": size_bytes,
                "replica_tier": { "slaves": 0 },
            }))
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            anyhow::bail!(
                "create {name}: {status}: {}",
                body.get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("no message")
            );
        }
        Ok(body)
    }

    /// The engine's own node name for a /v1 volume — read-write attach is
    /// gated on asking as the master node.
    pub fn master_node_of(v: &Value) -> Option<String> {
        v.get("replicas")?
            .as_array()?
            .iter()
            .find(|r| r.get("role").and_then(|x| x.as_str()) == Some("master"))?
            .get("node")?
            .as_str()
            .map(|s| s.to_string())
    }

    async fn v1_post(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        let resp = self.req(reqwest::Method::POST, path).json(&body).send().await?;
        let status = resp.status();
        let out: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            anyhow::bail!(
                "{path}: {status}: {}",
                out.get("message")
                    .or_else(|| out.get("error"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("no message")
            );
        }
        Ok(out)
    }

    /// POST /v1/volumes/{id}/attach — hot-add the volume as an NVMe-TCP
    /// namespace and return the attach coordinates. `node` must be the
    /// volume's master node (the engine's own name, captured at create).
    pub async fn attach_volume(&self, id: &str, node: &str) -> anyhow::Result<AttachedLeg> {
        let v = self
            .v1_post(
                &format!("/v1/volumes/{id}/attach"),
                serde_json::json!({ "node": node, "mode": "read_write" }),
            )
            .await?;
        if v.get("transport").and_then(|t| t.as_str()) != Some("nvme_tcp") {
            anyhow::bail!("attach {id}: unexpected transport in {v}");
        }
        let addr = v
            .get("addresses")
            .and_then(|a| a.as_array())
            .and_then(|a| a.first())
            .ok_or_else(|| anyhow::anyhow!("attach {id}: no addresses in {v}"))?;
        let mut traddr = addr
            .get("traddr")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let trsvcid = addr.get("trsvcid").and_then(|x| x.as_u64()).unwrap_or(4420) as u16;
        // A wildcard listen address is useless to a remote initiator —
        // substitute the engine's own host.
        if traddr.is_empty() || traddr == "0.0.0.0" || traddr == "::" {
            traddr = self.host();
        }
        let nsid = v
            .get("nsid")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "attach {id}: engine returned no nsid — its NVMe-oF target is not running \
                     (an engine started without an export device has no listener)"
                )
            })? as u32;
        Ok(AttachedLeg {
            nqn: v
                .get("nqn")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            traddr,
            trsvcid,
            nsid,
        })
    }

    /// POST /v1/volumes/{id}/detach.
    pub async fn detach_volume(&self, id: &str, node: &str) -> anyhow::Result<()> {
        self.v1_post(
            &format!("/v1/volumes/{id}/detach"),
            serde_json::json!({ "node": node }),
        )
        .await?;
        Ok(())
    }

    /// POST /api/v1/arrays — RAID1 across already-opened drives.
    pub async fn create_raid1(&self, drive_uuids: &[String]) -> anyhow::Result<Value> {
        self.v1_post(
            "/api/v1/arrays",
            serde_json::json!({ "level": "Raid1", "drive_uuids": drive_uuids }),
        )
        .await
    }

    pub async fn get_array(&self, id: &str) -> anyhow::Result<Value> {
        Ok(self
            .get(&format!("/api/v1/arrays/{id}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn delete_array(&self, id: &str) -> anyhow::Result<()> {
        let resp = self
            .req(reqwest::Method::DELETE, &format!("/api/v1/arrays/{id}"))
            .send()
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("delete array {id}: {}", resp.status());
        }
        Ok(())
    }

    /// POST /api/v1/arrays/{id}/members — returns the member uuid.
    pub async fn array_add_member(&self, array_id: &str, drive_uuid: &str) -> anyhow::Result<String> {
        let v = self
            .v1_post(
                &format!("/api/v1/arrays/{array_id}/members"),
                serde_json::json!({ "drive_uuid": drive_uuid }),
            )
            .await?;
        v.get("member_uuid")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("add member: no member_uuid in {v}"))
    }

    pub async fn array_remove_member(&self, array_id: &str, member_uuid: &str) -> anyhow::Result<()> {
        let resp = self
            .req(
                reqwest::Method::DELETE,
                &format!("/api/v1/arrays/{array_id}/members/{member_uuid}"),
            )
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("remove member {member_uuid}: {}", resp.status());
        }
        Ok(())
    }

    /// GET /api/v1/drives — the engine's open drives ({items, count} wrapper).
    pub async fn list_drives(&self) -> anyhow::Result<Vec<Value>> {
        let v: Value = self
            .get("/api/v1/drives")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(match v {
            Value::Array(a) => a,
            Value::Object(mut o) => o
                .remove("items")
                .or_else(|| o.remove("drives"))
                .and_then(|d| d.as_array().cloned())
                .unwrap_or_default(),
            _ => Vec::new(),
        })
    }

    /// POST /api/v1/drives — open a drive/URI, tolerating "already open":
    /// on conflict the existing drive's uuid is looked up by path.
    pub async fn add_drive_idempotent(&self, path: &str) -> anyhow::Result<String> {
        let resp = self
            .req(reqwest::Method::POST, "/api/v1/drives")
            .json(&serde_json::json!({ "path": path }))
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if status.is_success() {
            return body
                .get("uuid")
                .and_then(|u| u.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("open {path}: no uuid in {body}"));
        }
        if status == reqwest::StatusCode::CONFLICT {
            for d in self.list_drives().await? {
                if d.get("path").and_then(|p| p.as_str()) == Some(path) {
                    if let Some(u) = d.get("uuid").and_then(|u| u.as_str()) {
                        return Ok(u.to_string());
                    }
                }
            }
        }
        anyhow::bail!(
            "open {path}: {status}: {}",
            body.get("error")
                .or_else(|| body.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("no message")
        )
    }

    /// DELETE /api/v1/drives/{id_or_path} — close an opened drive. 404 is
    /// success from our side (already gone).
    pub async fn delete_drive(&self, id_or_path: &str, force: bool) -> anyhow::Result<()> {
        let enc = id_or_path.replace('%', "%25").replace('/', "%2F");
        let q = if force { "?force=true" } else { "" };
        let resp = self
            .req(reqwest::Method::DELETE, &format!("/api/v1/drives/{enc}{q}"))
            .send()
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("close drive {id_or_path}: {}", resp.status());
        }
        Ok(())
    }

    /// The host portion of the engine URL.
    pub fn host(&self) -> String {
        self.url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split([':', '/'])
            .next()
            .unwrap_or_default()
            .to_string()
    }

    pub async fn delete_volume(&self, id: &str) -> anyhow::Result<()> {
        let resp = self
            .req(reqwest::Method::DELETE, &format!("/v1/volumes/{id}"))
            .send()
            .await?;
        // 404 = already gone: deletion is idempotent from our side.
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("delete {id}: {}", resp.status());
        }
        Ok(())
    }

    pub async fn list_volumes(&self) -> anyhow::Result<Vec<Value>> {
        let v: Value = self
            .get("/v1/volumes")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(match v {
            Value::Array(a) => a,
            Value::Object(mut o) => o
                .remove("volumes")
                .and_then(|x| x.as_array().cloned())
                .unwrap_or_default(),
            _ => Vec::new(),
        })
    }
}

/// Find the first object carrying capacity fields in whatever wrapper the
/// engine used: bare object, array, or {"nodes": [...]}.
fn first_capacity_object(v: &Value) -> Option<&serde_json::Map<String, Value>> {
    match v {
        Value::Object(o) => {
            if o.contains_key("total_bytes") || o.contains_key("free_bytes") {
                Some(o)
            } else if let Some(Value::Array(a)) = o.get("nodes") {
                a.first().and_then(|x| x.as_object())
            } else {
                None
            }
        }
        Value::Array(a) => a.first().and_then(|x| x.as_object()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_node_and_attach_helpers() {
        let v: Value = serde_json::json!({
            "id": "vol-x", "replicas": [
                {"node":"nodeb","role":"master","sync":{"state":"in_sync"}}
            ]});
        assert_eq!(Engine::master_node_of(&v), Some("nodeb".into()));
        assert_eq!(Engine::master_node_of(&serde_json::json!({})), None);

        let leg = AttachedLeg {
            nqn: "nqn.2024.io.stormblock:b".into(),
            traddr: "10.0.0.2".into(),
            trsvcid: 4420,
            nsid: 7,
        };
        assert_eq!(
            leg.drive_uri(),
            "nvme-tcp://10.0.0.2:4420/nqn.2024.io.stormblock:b?nsid=7"
        );

        let e = Engine::new("http://192.168.8.150:9090", None);
        assert_eq!(e.host(), "192.168.8.150");
    }

    #[test]
    fn capacity_shapes_parse() {
        let bare: Value =
            serde_json::json!({"node":"n1","total_bytes":100,"free_bytes":40,"topology":{"rack":"r1"}});
        let arr: Value = serde_json::json!([{"total_bytes":1,"free_bytes":1}]);
        let wrapped: Value = serde_json::json!({"nodes":[{"total_bytes":2,"free_bytes":2}]});
        assert!(first_capacity_object(&bare).is_some());
        assert!(first_capacity_object(&arr).is_some());
        assert!(first_capacity_object(&wrapped).is_some());
        assert!(first_capacity_object(&serde_json::json!("nope")).is_none());
    }
}
