//! The cluster's apiserver, as the Job's ServiceAccount sees it (#113's
//! two-node suite: nodes, the fault ConfigMap, a StorageClass, claims and
//! pods). In-cluster only: `KUBERNETES_SERVICE_HOST`, the mounted token
//! and CA. Without them every Kubernetes step is a skip.

use serde_json::Value;
use std::time::Duration;

use crate::report::Why;

const SA: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

pub struct Kube {
    base: String,
    token: String,
    http: reqwest::Client,
    pub namespace: String,
}

pub struct KResp {
    pub status: u16,
    pub body: Value,
}

impl KResp {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

impl Kube {
    /// The in-cluster client, or why there is none.
    pub fn in_cluster(namespace: Option<String>) -> Result<Kube, String> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST").map_err(|_| "not in a cluster (no KUBERNETES_SERVICE_HOST)")?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        let token = std::fs::read_to_string(format!("{SA}/token"))
            .map_err(|e| format!("no ServiceAccount token ({e}): the Job must mount it"))?;
        let ca = std::fs::read(format!("{SA}/ca.crt")).map_err(|e| format!("no ServiceAccount CA: {e}"))?;
        let ca = reqwest::Certificate::from_pem(&ca).map_err(|e| format!("ServiceAccount CA: {e}"))?;
        let namespace = namespace
            .or_else(|| std::fs::read_to_string(format!("{SA}/namespace")).ok())
            .ok_or("no namespace (STORM_NAMESPACE)")?;
        let host = if host.contains(':') { format!("[{host}]") } else { host };
        let http = reqwest::Client::builder()
            .add_root_certificate(ca)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Kube { base: format!("https://{host}:{port}"), token: token.trim().to_string(), http, namespace })
    }

    pub async fn call(&self, method: reqwest::Method, path: &str, body: Option<&Value>) -> Result<KResp, Why> {
        let mut req = self.http.request(method.clone(), format!("{}{path}", self.base)).bearer_auth(&self.token);
        if let Some(b) = body {
            req = req.json(b);
        }
        let r = req.send().await.map_err(|e| Why::Fail(format!("{method} {path}: {e}")))?;
        let status = r.status().as_u16();
        let text = r.text().await.unwrap_or_default();
        let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
        Ok(KResp { status, body })
    }

    /// GET that must succeed.
    pub async fn get(&self, path: &str) -> Result<Value, Why> {
        let r = self.call(reqwest::Method::GET, path, None).await?;
        if r.status == 403 {
            return Err(Why::Skip(format!("GET {path}: 403 — the suite's RBAC (test/requires.toml) does not allow it")));
        }
        if !r.ok() {
            return Err(Why::Fail(format!("GET {path}: {} {}", r.status, r.body)));
        }
        Ok(r.body)
    }

    /// GET; None on 404.
    pub async fn find(&self, path: &str) -> Result<Option<Value>, Why> {
        let r = self.call(reqwest::Method::GET, path, None).await?;
        match r.status {
            404 => Ok(None),
            403 => Err(Why::Skip(format!("GET {path}: 403 — the suite's RBAC (test/requires.toml) does not allow it"))),
            _ if r.ok() => Ok(Some(r.body)),
            s => Err(Why::Fail(format!("GET {path}: {s} {}", r.body))),
        }
    }

    pub async fn create(&self, path: &str, obj: &Value) -> Result<Value, Why> {
        let r = self.call(reqwest::Method::POST, path, Some(obj)).await?;
        if !r.ok() {
            return Err(Why::Fail(format!("create {path}: {} {}", r.status, r.body)));
        }
        Ok(r.body)
    }

    /// Delete; 404 counts as gone. `grace0` deletes at once (a pod on a node
    /// that is powered off never confirms).
    pub async fn delete(&self, path: &str, grace0: bool) -> Result<(), Why> {
        let body = grace0.then(|| serde_json::json!({"kind": "DeleteOptions", "apiVersion": "v1", "gracePeriodSeconds": 0}));
        let r = self.call(reqwest::Method::DELETE, path, body.as_ref()).await?;
        if r.ok() || r.status == 404 {
            Ok(())
        } else {
            Err(Why::Fail(format!("delete {path}: {} {}", r.status, r.body)))
        }
    }

    pub fn ns(&self, kind: &str) -> String {
        format!("/api/v1/namespaces/{}/{kind}", self.namespace)
    }

    /// A pod's log (the container's stdout), as text.
    pub async fn log(&self, pod: &str) -> Result<String, Why> {
        let r = self.call(reqwest::Method::GET, &format!("{}/{pod}/log", self.ns("pods")), None).await?;
        if !r.ok() {
            return Err(Why::Fail(format!("log of {pod}: {} {}", r.status, r.body)));
        }
        Ok(match r.body {
            Value::String(s) => s,
            v => v.to_string(),
        })
    }
}
