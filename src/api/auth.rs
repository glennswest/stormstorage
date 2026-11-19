//! Inbound API auth (#6).
//!
//! With `[api] api_token` set, every request that changes state needs
//! `Authorization: Bearer <token>`: volume create, delete, move and export,
//! and peer replication (which replaces the whole volume table). Peers
//! already send the same token on their pushes (`replicate::push_to_peers`),
//! so a replicating pair shares one `api_token`.
//!
//! Open even with a token:
//! * reads (GET, the component websocket) and the placement dry run, which
//!   changes nothing: the family posture is that anyone who can reach the
//!   port may look;
//! * `storage/register` and `storage/deregister`: stormblock's `[stormfs]`
//!   heartbeat sends no token, and a node enrolls with no engine change.
//!   They close once the heartbeat can carry one (stormblock#214).
//!
//! The disk-migration verbs a kubelet calls (`POST
//! /api/v1/nodes/{n}/volumes/{v}/migrate` and a migration's cutover,
//! complete, abort and retry) also take a Kubernetes bearer the cluster
//! issued, reviewed against `[kubernetes]` (#117, [`super::kubeauth`]): on a
//! multi-node cluster the leader's `api_token` is on the leader's node only.
//!
//! An empty token leaves the API open, as before.

use super::AppState;
use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::sync::Arc;

/// Paths that stay open to a write when a token is set.
const OPEN_WRITES: &[&str] = &[
    "/api/v1/placement/plan",
    "/api/v1/storage/register",
    "/api/v1/storage/deregister",
];

/// Does this request need the token?
pub fn guarded(method: &Method, path: &str) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) && !OPEN_WRITES.contains(&path)
}

/// Compare without leaking how long a matching prefix was.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn refuse(status: StatusCode, code: &str, error: String) -> Response {
    let mut r = (status, Json(json!({ "error": error, "code": code }))).into_response();
    if status == StatusCode::UNAUTHORIZED {
        r.headers_mut().insert(header::WWW_AUTHENTICATE, header::HeaderValue::from_static("Bearer"));
    }
    r
}

pub async fn require_token(State(s): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let token = s.config.api.api_token.as_bytes();
    if token.is_empty() || !guarded(req.method(), req.uri().path()) {
        return next.run(req).await;
    }
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string);
    if presented.as_deref().is_some_and(|t| same(t.as_bytes(), token)) {
        return next.run(req).await;
    }
    let (method, path) = (req.method().clone(), req.uri().path().to_string());
    // A disk-migration verb with a bearer that is not ours: the cluster's
    // apiserver says whether it may (#117).
    if let (Some(bearer), Some(ask)) = (presented.as_deref(), super::kubeauth::migration_ask(&method, &path)) {
        use super::kubeauth::{Review, RESOURCE};
        return match s.reviews.review(&s.config.kubernetes, bearer, &ask).await {
            Review::Allowed(user) => {
                tracing::info!(%user, verb = ask.verb, "{method} {path}: Kubernetes bearer allowed");
                next.run(req).await
            }
            Review::Denied(user, why) => {
                tracing::warn!(%user, "{method} {path}: forbidden: {why}");
                refuse(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    format!("{user} may not {} storage.storm.io {RESOURCE}: {why}", ask.verb),
                )
            }
            Review::Unauthenticated(why) => {
                tracing::warn!("{method} {path}: bearer is neither api_token nor a valid Kubernetes token: {why}");
                refuse(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    format!("this request needs Authorization: Bearer <api.api_token> or a Kubernetes bearer: {why}"),
                )
            }
            Review::Unavailable(why) => {
                tracing::warn!("{method} {path}: cannot review a Kubernetes bearer: {why}");
                refuse(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "review_unavailable",
                    format!("the bearer is not api_token, and the apiserver could not review it: {why}"),
                )
            }
        };
    }
    tracing::warn!("unauthorized {method} {path}");
    refuse(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "this request needs Authorization: Bearer <api.api_token>".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_are_guarded_reads_and_enrollment_are_not() {
        assert!(guarded(&Method::POST, "/api/v1/volumes"));
        assert!(guarded(&Method::DELETE, "/api/v1/volumes/v"));
        assert!(guarded(&Method::POST, "/api/v1/volumes/v/move"));
        assert!(guarded(&Method::POST, "/api/v1/volumes/v/export"));
        assert!(guarded(&Method::POST, "/api/v1/volumes/v/assemble"));
        assert!(guarded(&Method::POST, "/api/v1/replicate"));
        assert!(!guarded(&Method::GET, "/api/v1/volumes"));
        assert!(!guarded(&Method::GET, "/ws/components"));
        assert!(!guarded(&Method::POST, "/api/v1/placement/plan"));
        assert!(!guarded(&Method::POST, "/api/v1/storage/register"));
        assert!(!guarded(&Method::POST, "/api/v1/storage/deregister"));
    }

    #[test]
    fn compare() {
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd"));
        assert!(!same(b"abc", b"abcd"));
    }
}
