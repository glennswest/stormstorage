//! #117: on a multi-node cluster the kubelet on a node that does not lead
//! holds no `api_token` the leader accepts. The disk-migration verbs take a
//! Kubernetes bearer instead, which stormstorage reviews against
//! `[kubernetes]`: a TokenReview, then a SubjectAccessReview on
//! `storage.storm.io` `vmmigrations`. Every other write still takes the
//! `api_token` alone.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use stormstorage::api::AppState;
use stormstorage::config::Config;
use stormstorage::model::FedState;

const OWN: &str = "leader-own-api-token";
const REVIEWER: &str = "stormstorage-reviewer-token";
const GOOD: &str = "node-sa-bearer";
const DENIED: &str = "some-other-sa-bearer";
const GOOD_USER: &str = "system:serviceaccount:kube-system:stormstorage";

#[derive(Default)]
struct Seen {
    token_reviews: usize,
    /// The resourceAttributes of each SubjectAccessReview.
    sars: Vec<Value>,
    /// The bearer each review was asked with: stormstorage's own.
    reviewer: Vec<String>,
}
type Api = Arc<Mutex<Seen>>;

fn bearer_of(h: &HeaderMap) -> String {
    h.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
}

async fn token_review(State(api): State<Api>, h: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
    let mut a = api.lock().unwrap();
    a.token_reviews += 1;
    a.reviewer.push(bearer_of(&h));
    let status = match body["spec"]["token"].as_str() {
        Some(GOOD) => json!({"authenticated": true, "user": {"username": GOOD_USER, "uid": "u1",
                             "groups": ["system:serviceaccounts", "system:authenticated"]}}),
        Some(DENIED) => json!({"authenticated": true, "user": {"username": "system:serviceaccount:default:x",
                               "groups": ["system:authenticated"]}}),
        _ => json!({"authenticated": false, "error": "invalid bearer token"}),
    };
    Json(json!({"kind": "TokenReview", "status": status}))
}

async fn access_review(State(api): State<Api>, h: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
    let mut a = api.lock().unwrap();
    a.reviewer.push(bearer_of(&h));
    let attrs = body["spec"]["resourceAttributes"].clone();
    a.sars.push(attrs.clone());
    let allowed = body["spec"]["user"] == GOOD_USER
        && attrs["group"] == "storage.storm.io"
        && attrs["resource"] == "vmmigrations"
        && (attrs["verb"] == "create" || attrs["verb"] == "update");
    Json(json!({"kind": "SubjectAccessReview", "status": {"allowed": allowed, "reason": if allowed { "" } else { "no RBAC rule" }}}))
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn stormstorage(apiserver: &str, name: &str) -> String {
    let token_file = std::env::temp_dir().join(format!("stormstorage-117-{name}-{}.token", std::process::id()));
    std::fs::write(&token_file, REVIEWER).unwrap();
    let mut config = Config::default();
    config.api.api_token = OWN.into();
    config.local.enabled = false;
    config.kubernetes.server = apiserver.into();
    config.kubernetes.token_file = Some(token_file.to_string_lossy().into_owned());
    let state = Arc::new(AppState::new(config, FedState::default(), None));
    serve(stormstorage::api::router(state)).await + "/api/v1"
}

/// The request got past auth to the handler, which knows no such node or
/// migration: a 404, not a refusal of the credential.
fn reached_handler(status: u16, body: &Value) {
    assert_eq!(status, 404, "{body}");
}

#[tokio::test]
async fn a_nodes_bearer_drives_the_migration_verbs_and_nothing_else() {
    let api: Api = Default::default();
    let apiserver = serve(
        Router::new()
            .route("/apis/authentication.k8s.io/v1/tokenreviews", post(token_review))
            .route("/apis/authorization.k8s.io/v1/subjectaccessreviews", post(access_review))
            .with_state(api.clone()),
    )
    .await;
    let s = stormstorage(&apiserver, "main").await;
    let c = reqwest::Client::new();

    // Start a disk migration with the node's bearer: reviewed as `create`.
    let r = c
        .post(format!("{s}/nodes/n1/volumes/vm-root/migrate"))
        .bearer_auth(GOOD)
        .json(&json!({"to": "n2"}))
        .send()
        .await
        .unwrap();
    let status = r.status().as_u16();
    reached_handler(status, &r.json().await.unwrap_or(Value::Null));
    {
        let a = api.lock().unwrap();
        assert_eq!(a.token_reviews, 1);
        assert_eq!(a.sars.last().unwrap(), &json!({"group": "storage.storm.io", "resource": "vmmigrations", "verb": "create"}));
        assert!(a.reviewer.iter().all(|b| b == &format!("Bearer {REVIEWER}")), "asked with its own [kubernetes] token: {:?}", a.reviewer);
    }

    // A step: `update`, named by the migration.
    for step in ["cutover", "complete", "abort", "retry"] {
        let r = c.post(format!("{s}/migrations/m-117/{step}")).bearer_auth(GOOD).json(&json!({})).send().await.unwrap();
        let status = r.status().as_u16();
        reached_handler(status, &r.json().await.unwrap_or(Value::Null));
    }
    let reviews = {
        let a = api.lock().unwrap();
        assert_eq!(
            a.sars.last().unwrap(),
            &json!({"group": "storage.storm.io", "resource": "vmmigrations", "verb": "update", "name": "m-117"})
        );
        a.token_reviews
    };
    assert_eq!(reviews, 2, "one review per (bearer, verb, name), kept for a minute");

    // Kept: asking again does not ask the apiserver.
    let r = c.post(format!("{s}/migrations/m-117/abort")).bearer_auth(GOOD).json(&json!({})).send().await.unwrap();
    let status = r.status().as_u16();
    reached_handler(status, &r.json().await.unwrap_or(Value::Null));
    assert_eq!(api.lock().unwrap().token_reviews, reviews);

    // Valid, but no role: 403, naming who and what.
    let r = c.post(format!("{s}/migrations/m-117/abort")).bearer_auth(DENIED).json(&json!({})).send().await.unwrap();
    assert_eq!(r.status(), 403);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["code"], "forbidden");
    assert!(body["error"].as_str().unwrap().contains("system:serviceaccount:default:x"), "{body}");

    // Not a token at all: 401.
    let r = c.post(format!("{s}/migrations/m-117/abort")).bearer_auth("garbage").json(&json!({})).send().await.unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(r.json::<Value>().await.unwrap()["code"], "unauthorized");

    // No bearer: 401, and nothing to review.
    let before = api.lock().unwrap().token_reviews;
    let r = c.post(format!("{s}/migrations/m-117/abort")).json(&json!({})).send().await.unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(api.lock().unwrap().token_reviews, before);

    // Every other write takes the api_token alone: the node's bearer is not
    // reviewed there.
    for (m, p) in [
        (reqwest::Method::POST, "/volumes"),
        (reqwest::Method::DELETE, "/volumes/v"),
        (reqwest::Method::POST, "/volumes/v/migrate"),
        (reqwest::Method::POST, "/replicate"),
    ] {
        let r = c.request(m.clone(), format!("{s}{p}")).bearer_auth(GOOD).json(&json!({})).send().await.unwrap();
        assert_eq!(r.status(), 401, "{m} {p}");
    }
    assert_eq!(api.lock().unwrap().token_reviews, before);

    // The api_token still works on the migration verbs, unreviewed.
    let r = c.post(format!("{s}/migrations/m-117/abort")).bearer_auth(OWN).json(&json!({})).send().await.unwrap();
    let status = r.status().as_u16();
    reached_handler(status, &r.json().await.unwrap_or(Value::Null));
    assert_eq!(api.lock().unwrap().token_reviews, before);
}

#[tokio::test]
async fn no_apiserver_is_a_503_not_a_pass() {
    // A port nothing listens on.
    let gone = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", gone.local_addr().unwrap());
    drop(gone);
    let s = stormstorage(&url, "gone").await;
    let r = reqwest::Client::new()
        .post(format!("{s}/migrations/m-117/abort"))
        .bearer_auth(GOOD)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    assert_eq!(r.json::<Value>().await.unwrap()["code"], "review_unavailable");
}
