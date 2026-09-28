//! The node's stormstorage REST API, as a test sees it.

use serde_json::{json, Value};
use std::time::Duration;

use crate::report::Why;

pub struct Api {
    base: String,
    token: Option<String>,
    http: reqwest::Client,
}

pub struct Resp {
    pub status: u16,
    pub body: Value,
}

impl Resp {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

impl Api {
    pub fn new(base: &str, token: Option<String>) -> Api {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            // An engine write behind a create can take minutes on a loaded
            // node (stormstorage's own engine write timeout is 300 s).
            .timeout(Duration::from_secs(330))
            .build()
            .expect("http client");
        Api { base: base.to_string(), token, http }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    pub async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>, auth: bool) -> Result<Resp, Why> {
        let mut req = self.http.request(method.clone(), format!("{}{path}", self.base));
        if auth {
            if let Some(t) = &self.token {
                req = req.bearer_auth(t);
            }
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let r = req.send().await.map_err(|e| Why::Fail(format!("{method} {path}: {e}")))?;
        let status = r.status().as_u16();
        let text = r.text().await.unwrap_or_default();
        let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
        Ok(Resp { status, body })
    }

    pub async fn get(&self, path: &str) -> Result<Value, Why> {
        let r = self.call(reqwest::Method::GET, path, None, true).await?;
        if !r.ok() {
            return Err(Why::Fail(format!("GET {path}: {} {}", r.status, r.body)));
        }
        Ok(r.body)
    }

    /// A write. 401 means the node's API is closed and this run has no
    /// token: a skip, never a pass.
    pub async fn write(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Resp, Why> {
        let r = self.call(method, path, body, true).await?;
        if r.status == 401 {
            return Err(Why::Skip(if self.token.is_some() {
                format!("{path}: 401 with STORM_STORMSTORAGE_TOKEN — the token is wrong")
            } else {
                format!("{path}: the node's stormstorage requires api_token and no STORM_STORMSTORAGE_TOKEN was given")
            }));
        }
        Ok(r)
    }

    pub async fn post(&self, path: &str, body: Value) -> Result<Resp, Why> {
        self.write(reqwest::Method::POST, path, Some(body)).await
    }

    pub async fn delete(&self, path: &str) -> Result<Resp, Why> {
        self.write(reqwest::Method::DELETE, path, None).await
    }

    /// Create a volume; its record on success.
    pub async fn create(&self, name: &str, size: u64, replicas: u32) -> Result<Value, Why> {
        let r = self
            .post("/api/v1/volumes", json!({"name": name, "size_bytes": size, "replicas": replicas}))
            .await?;
        if !r.ok() {
            return Err(Why::Fail(format!("create {name}: {} {}", r.status, r.body)));
        }
        Ok(r.body)
    }

    /// Delete a volume; 404 counts as gone.
    pub async fn remove(&self, name: &str) -> Result<(), Why> {
        let r = self.delete(&format!("/api/v1/volumes/{name}")).await?;
        if r.ok() || r.status == 404 {
            Ok(())
        } else {
            Err(Why::Fail(format!("delete {name}: {} {}", r.status, r.body)))
        }
    }

    pub async fn volumes(&self) -> Result<Vec<Value>, Why> {
        Ok(self.get("/api/v1/volumes").await?["volumes"].as_array().cloned().unwrap_or_default())
    }

    pub async fn nodes(&self) -> Result<Vec<Value>, Why> {
        Ok(self.get("/api/v1/nodes").await?["nodes"].as_array().cloned().unwrap_or_default())
    }

    /// Healthy nodes' names.
    pub async fn healthy(&self) -> Result<Vec<String>, Why> {
        Ok(self
            .nodes()
            .await?
            .iter()
            .filter(|n| n["status"]["healthy"] == true)
            .filter_map(|n| n["name"].as_str().map(str::to_string))
            .collect())
    }

    /// Engine volume names on a node, as stormstorage's inventory last saw
    /// them (a leg's engine volume carries the distributed volume's name).
    pub async fn engine_volume_names(&self, node: &str) -> Result<Vec<String>, Why> {
        let inv = self.get(&format!("/api/v1/nodes/{node}/inventory")).await?;
        Ok(inv["volumes"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v["name"].as_str().map(str::to_string)).collect())
            .unwrap_or_default())
    }

    /// Wait up to `secs` for `f` over a node's engine volume names; the
    /// inventory refreshes once per poll.
    pub async fn wait_engine(&self, node: &str, secs: u64, f: impl Fn(&[String]) -> bool) -> Result<bool, Why> {
        let end = std::time::Instant::now() + Duration::from_secs(secs);
        loop {
            if f(&self.engine_volume_names(node).await?) {
                return Ok(true);
            }
            if std::time::Instant::now() >= end {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
