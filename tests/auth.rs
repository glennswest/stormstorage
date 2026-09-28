//! #6: with `[api] api_token` set, inbound writes need the bearer token;
//! reads and stormblock self-registration stay open; an empty token leaves
//! the API open.

use serde_json::{json, Value};
use std::sync::Arc;
use stormstorage::api::AppState;
use stormstorage::config::Config;
use stormstorage::model::FedState;

async fn serve(token: &str) -> String {
    let mut config = Config::default();
    config.api.api_token = token.to_string();
    config.local.enabled = false;
    let state = Arc::new(AppState::new(config, FedState::default(), None));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state);
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}/api/v1")
}

fn replicate_body() -> Value {
    json!({"revision": 1_000_000, "volumes": {}, "registered": []})
}

#[tokio::test]
async fn token_set_guards_writes() {
    let api = serve("s3cret").await;
    let c = reqwest::Client::new();

    // No header, and a wrong one: 401 with the family error envelope.
    let r = c.post(format!("{api}/replicate")).json(&replicate_body()).send().await.unwrap();
    assert_eq!(r.status(), 401);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["code"], "unauthorized");
    let r = c
        .post(format!("{api}/replicate"))
        .bearer_auth("wrong")
        .json(&replicate_body())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    for (m, p) in [
        (reqwest::Method::POST, "/volumes"),
        (reqwest::Method::DELETE, "/volumes/v"),
        (reqwest::Method::POST, "/volumes/v/move"),
        (reqwest::Method::POST, "/volumes/v/export"),
    ] {
        let r = c.request(m.clone(), format!("{api}{p}")).json(&json!({})).send().await.unwrap();
        assert_eq!(r.status(), 401, "{m} {p}");
    }

    // The right token gets through to the handler.
    let r = c
        .post(format!("{api}/replicate"))
        .bearer_auth("s3cret")
        .json(&replicate_body())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = c.delete(format!("{api}/volumes/nope")).bearer_auth("s3cret").send().await.unwrap();
    assert_eq!(r.status(), 404, "reached the handler: unknown volume");

    // Reads stay open.
    for p in ["/health", "/volumes", "/nodes", "/events", "/components"] {
        let r = c.get(format!("{api}{p}")).send().await.unwrap();
        assert_eq!(r.status(), 200, "GET {p}");
    }

    // Self-registration stays open: stormblock's heartbeat sends no token
    // (stormblock#214).
    let r = c
        .post(format!("{api}/storage/register"))
        .json(&json!({"node_addr": "127.0.0.1:1", "hostname": "n1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = c
        .post(format!("{api}/storage/deregister"))
        .json(&json!({"node_addr": "127.0.0.1:1", "hostname": "n1"}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "deregister: {}", r.status());
}

#[tokio::test]
async fn empty_token_is_open() {
    let api = serve("").await;
    let r = reqwest::Client::new()
        .post(format!("{api}/replicate"))
        .json(&replicate_body())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}
