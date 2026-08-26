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
    pub async fn create_volume(&self, name: &str, size_bytes: u64) -> anyhow::Result<Value> {
        let resp = self
            .req(reqwest::Method::POST, "/v1/volumes")
            .json(&serde_json::json!({ "name": name, "size_bytes": size_bytes }))
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
