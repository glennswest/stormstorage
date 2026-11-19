//! A Kubernetes bearer for the disk-migration verbs (#117).
//!
//! In a live migration the kubelet mirrors a VM's disks through stormstorage
//! (rustkube-node#40, #44). On a multi-node cluster there is one
//! stormstorage, the elected leader (#56), and its `[api] api_token` is its
//! own, on its own node: a kubelet anywhere else holds nothing it accepts.
//! Every node does hold a Kubernetes bearer the cluster issued (stormcos
//! `SECRETS.md`, `/data/stormcert/<sa>.token`), so for these verbs only, a
//! bearer that is not the `api_token` is reviewed against the cluster's
//! apiserver (`[kubernetes]`), as stormblock reviews one (stormblock#274):
//!
//! 1. **TokenReview**: is the bearer valid, and who is it?
//! 2. **SubjectAccessReview** for that user: may it `<verb>` the
//!    `storage.storm.io` resource `vmmigrations`? `create` starts a disk
//!    migration; `update` is cutover, complete, abort and retry (named by
//!    the migration id).
//!
//! stormstorage's own credential for the two reviews is its `[kubernetes]`
//! token (allowed to create both, as `system:auth-delegator` is). An answer
//! is kept for a minute per (bearer, verb, name): a migration's few steps
//! ask once each, and a revoked role takes effect within the minute. An
//! apiserver that cannot be asked is a 503 and is never cached.

use crate::config::KubeConfig;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long an answer is kept.
const CACHE_TTL: Duration = Duration::from_secs(60);

/// The `storage.storm.io` resource the migration verbs are reviewed as.
pub const RESOURCE: &str = "vmmigrations";

/// What a request is, for a review: the verb and, when the path names one,
/// the migration.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ask {
    pub verb: &'static str,
    pub name: Option<String>,
}

/// The disk-migration verbs a node's bearer may be reviewed for, by path.
/// Every other guarded write takes the `api_token` alone.
pub fn migration_ask(method: &axum::http::Method, path: &str) -> Option<Ask> {
    if method != axum::http::Method::POST {
        return None;
    }
    let parts: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    match parts.as_slice() {
        ["", "api", "v1", "nodes", n, "volumes", v, "migrate"] if !n.is_empty() && !v.is_empty() => {
            Some(Ask { verb: "create", name: None })
        }
        ["", "api", "v1", "migrations", id, step]
            if !id.is_empty() && matches!(*step, "cutover" | "complete" | "abort" | "retry") =>
        {
            Some(Ask { verb: "update", name: Some((*id).to_string()) })
        }
        _ => None,
    }
}

/// The outcome of reviewing a bearer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Review {
    /// Allowed: the user.
    Allowed(String),
    /// Valid bearer, not allowed: who, and the apiserver's reason.
    Denied(String, String),
    /// Not a valid bearer.
    Unauthenticated(String),
    /// The apiserver could not be asked.
    Unavailable(String),
}

/// Answers kept for [`CACHE_TTL`], by (bearer, ask). In memory only.
#[derive(Default)]
pub struct Reviews {
    cache: Mutex<HashMap<(String, Ask), (Instant, Review)>>,
}

impl Reviews {
    /// May `bearer` do `ask` on `vmmigrations`, by the apiserver `cfg` names?
    pub async fn review(&self, cfg: &KubeConfig, bearer: &str, ask: &Ask) -> Review {
        if !cfg.enabled {
            return Review::Unavailable("[kubernetes] is off: no apiserver to review a bearer with".into());
        }
        let key = (bearer.to_string(), ask.clone());
        if let Some((at, r)) = self.cache.lock().unwrap_or_else(|e| e.into_inner()).get(&key) {
            if at.elapsed() < CACHE_TTL {
                return r.clone();
            }
        }
        let r = match review_uncached(cfg, bearer, ask).await {
            Ok(r) => r,
            Err(e) => return Review::Unavailable(format!("{e:#}")),
        };
        let mut c = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        c.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
        c.insert(key, (Instant::now(), r.clone()));
        r
    }
}

async fn post(cfg: &KubeConfig, path: &str, body: Value) -> anyhow::Result<Value> {
    let http = crate::kube::client(cfg)?;
    let url = format!("{}{path}", cfg.server.trim_end_matches('/'));
    let mut req = http.post(&url).json(&body);
    let (token, from) = cfg.token_source();
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let resp = req.send().await.map_err(|e| anyhow::anyhow!("POST {path}: {e}"))?;
    let status = resp.status();
    let v: Value = resp.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        anyhow::bail!("POST {path}: {status} (token: {from}): {}", v["message"].as_str().unwrap_or(""));
    }
    Ok(v)
}

async fn review_uncached(cfg: &KubeConfig, bearer: &str, ask: &Ask) -> anyhow::Result<Review> {
    let tr = post(
        cfg,
        "/apis/authentication.k8s.io/v1/tokenreviews",
        json!({ "apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "spec": { "token": bearer } }),
    )
    .await?;
    if tr["status"]["authenticated"] != json!(true) {
        return Ok(Review::Unauthenticated(tr["status"]["error"].as_str().unwrap_or("not authenticated").to_string()));
    }
    let user = tr["status"]["user"]["username"].as_str().unwrap_or("").to_string();
    let mut attrs = json!({ "group": "storage.storm.io", "resource": RESOURCE, "verb": ask.verb });
    if let Some(n) = &ask.name {
        attrs["name"] = json!(n);
    }
    let sar = post(
        cfg,
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "spec": {
                "user": user,
                "groups": tr["status"]["user"]["groups"],
                "uid": tr["status"]["user"]["uid"],
                "resourceAttributes": attrs,
            },
        }),
    )
    .await?;
    if sar["status"]["allowed"] == json!(true) {
        Ok(Review::Allowed(user))
    } else {
        Ok(Review::Denied(user, sar["status"]["reason"].as_str().unwrap_or("not allowed").to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn the_migration_verbs_and_nothing_else() {
        assert_eq!(
            migration_ask(&Method::POST, "/api/v1/nodes/n1/volumes/vm-root/migrate"),
            Some(Ask { verb: "create", name: None })
        );
        for step in ["cutover", "complete", "abort", "retry"] {
            assert_eq!(
                migration_ask(&Method::POST, &format!("/api/v1/migrations/m1/{step}")),
                Some(Ask { verb: "update", name: Some("m1".into()) }),
                "{step}"
            );
        }
        assert_eq!(migration_ask(&Method::GET, "/api/v1/migrations/m1"), None);
        assert_eq!(migration_ask(&Method::POST, "/api/v1/migrations/m1/delete"), None);
        assert_eq!(migration_ask(&Method::POST, "/api/v1/volumes/v/migrate"), None, "a fleet volume's migrate is not a VM disk's");
        assert_eq!(migration_ask(&Method::POST, "/api/v1/volumes"), None);
        assert_eq!(migration_ask(&Method::POST, "/api/v1/replicate"), None);
        assert_eq!(migration_ask(&Method::DELETE, "/api/v1/nodes/n1/volumes/v/migrate"), None);
    }
}
