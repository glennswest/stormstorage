//! #38: every engine call carries the configured engine token, also for a
//! node stormblock registered itself (no token of its own) and for the
//! adopted local engine; an engine that refuses the token is backed off
//! instead of being asked again every poll, and a re-minted token file is
//! tried at once.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::{FedState, Node, NodeSource, NodeStatus};

const TOKEN: &str = "minted-by-stormblock";

#[derive(Default)]
struct Calls {
    ok: AtomicUsize,
    refused: AtomicUsize,
}

fn authorized(h: &HeaderMap, calls: &Calls) -> bool {
    let ok = h.get("authorization").and_then(|v| v.to_str().ok())
        == Some(&format!("Bearer {TOKEN}"));
    if ok {
        calls.ok.fetch_add(1, Ordering::SeqCst);
    } else {
        calls.refused.fetch_add(1, Ordering::SeqCst);
    }
    ok
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response()
}

/// A stormblock that, like v17+, answers nothing without its token.
async fn mock_engine(calls: Arc<Calls>) -> String {
    let route = |calls: Arc<Calls>, body: serde_json::Value| {
        get(move |h: HeaderMap| {
            let calls = calls.clone();
            let body = body.clone();
            async move {
                if authorized(&h, &calls) {
                    Json(body).into_response()
                } else {
                    unauthorized()
                }
            }
        })
    };
    let app = Router::new()
        .route(
            "/api/v1/discovery",
            route(calls.clone(), json!({"local_node": "node-a", "nodes": []})),
        )
        .route(
            "/v1/nodes/capacity",
            route(
                calls.clone(),
                json!([{"node": "node-a", "total_bytes": 1u64 << 30, "free_bytes": 1u64 << 29}]),
            ),
        )
        .route("/api/v1/slabs", route(calls.clone(), json!({"items": [], "count": 0})))
        .route("/api/v1/volumes", route(calls.clone(), json!({"items": [], "count": 0})));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn token_file(test: &str, content: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("token-{test}"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("api_token");
    std::fs::write(&path, content).unwrap();
    path
}

/// A node as stormblock's heartbeat registers it: no token of its own.
fn registered(url: &str) -> FedState {
    let mut fed = FedState::default();
    fed.nodes.insert(
        "node-a".into(),
        Node {
            config: NodeConfig {
                name: "node-a".into(),
                engine_url: url.into(),
                api_token: None,
                labels: Default::default(),
                tier: None,
            },
            status: NodeStatus::new(NodeSource::Registered),
        },
    );
    fed
}

#[tokio::test]
async fn self_registered_node_is_polled_with_the_configured_token() {
    let calls = Arc::new(Calls::default());
    let url = mock_engine(calls.clone()).await;
    let mut config = Config::default();
    config.local.enabled = false;
    config.local.token_file = Some(token_file("registered", &format!("{TOKEN}\n")).display().to_string());
    let state = Arc::new(AppState::new(config, registered(&url), None));

    stormstorage::registry::poll_once(&state).await;

    let fed = state.fed.read().await;
    let a = &fed.nodes["node-a"];
    assert!(a.status.healthy);
    assert_eq!(a.status.total_bytes, 1 << 30);
    assert_eq!(calls.refused.load(Ordering::SeqCst), 0, "no call went out bare");
    assert!(calls.ok.load(Ordering::SeqCst) >= 3, "capacity, slabs, volumes");
}

#[tokio::test]
async fn adopted_local_engine_gets_the_token_and_it_is_not_persisted() {
    let calls = Arc::new(Calls::default());
    let url = mock_engine(calls.clone()).await;
    let mut config = Config::default();
    config.local.engine_url = url;
    config.local.token_file = Some(token_file("adopted", TOKEN).display().to_string());
    let state = Arc::new(AppState::new(config, FedState::default(), None));

    stormstorage::registry::poll_once(&state).await;

    let fed = state.fed.read().await;
    let a = fed.nodes.get("node-a").expect("adopted by its discovery name");
    assert!(a.status.healthy);
    assert_eq!(a.config.api_token, None, "the token stays out of the state file");
    assert_eq!(calls.refused.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_refusing_engine_is_backed_off_and_a_new_token_is_tried_at_once() {
    let calls = Arc::new(Calls::default());
    let url = mock_engine(calls.clone()).await;
    let file = token_file("refused", "stale-token");
    let mut config = Config::default();
    config.local.engine_url = url.clone();
    config.local.token_file = Some(file.display().to_string());
    config.poll.interval_secs = 15;
    let state = Arc::new(AppState::new(config, registered(&url), None));

    // Twenty polls back to back: one refused call, then back-off.
    for _ in 0..20 {
        stormstorage::registry::poll_once(&state).await;
    }
    assert_eq!(calls.refused.load(Ordering::SeqCst), 1, "one refused call, then back-off");
    {
        let fed = state.fed.read().await;
        assert!(!fed.nodes["node-a"].status.healthy, "a refusing engine is not usable");
    }
    let refusals = state
        .events
        .read()
        .await
        .since(0)
        .iter()
        .filter(|e| e.kind == "auth")
        .count();
    assert_eq!(refusals, 1, "the refusal is reported once");

    // stormblock re-mints the file: the next poll uses it without waiting.
    std::fs::write(&file, TOKEN).unwrap();
    stormstorage::registry::poll_once(&state).await;
    assert_eq!(calls.refused.load(Ordering::SeqCst), 1);
    let fed = state.fed.read().await;
    assert!(fed.nodes["node-a"].status.healthy);
    let events = state.events.read().await;
    assert!(events
        .since(0)
        .iter()
        .any(|e| e.kind == "auth" && e.message.contains("accepts")));
}
