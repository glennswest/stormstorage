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
                token_file: None,
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

/// #12: per node — its `api_token`, then its `token_file`, then the
/// `[local]` rule: the minted token only for an engine on this machine.
#[tokio::test]
async fn per_node_token_order_and_no_minted_token_for_a_peer() {
    if std::env::var("STORMBLOCK_API_TOKEN").is_ok_and(|v| !v.trim().is_empty()) {
        return;
    }
    let mut config = Config::default();
    config.local.token_file = Some(token_file("peer-minted", "minted").display().to_string());
    let state = AppState::new(config, FedState::default(), None);
    let node = |url: &str, toml_extra: &str| -> NodeConfig {
        toml::from_str(&format!("name = \"n\"\nengine_url = \"{url}\"\n{toml_extra}")).unwrap()
    };
    assert_eq!(state.engine_token(&node("http://127.0.0.1:9090", "")).as_deref(), Some("minted"));
    let (t, why) = state.engine_token_source(&node("http://192.0.2.1:9090", ""));
    assert!(t.is_none(), "a peer is never sent this machine's minted token");
    assert!(why.contains("minted token means nothing"), "{why}");
    let f = token_file("peer-own", "peer-token");
    let own = format!("token_file = \"{}\"", f.display());
    assert_eq!(state.engine_token(&node("http://192.0.2.1:9090", &own)).as_deref(), Some("peer-token"));
    assert_eq!(
        state.engine_token(&node("http://192.0.2.1:9090", &format!("api_token = \"inline\"\n{own}"))).as_deref(),
        Some("inline")
    );
}

/// What a #274 engine was sent: (method path, bearer).
type Seen = Arc<std::sync::Mutex<Vec<(String, String)>>>;

/// A stormblock after #274 (`admin_gate = enforce`): reads and ordinary
/// verbs on the node token, destructive ones (array create/delete,
/// members, drive close) on the admin credential only. `gated = false` is
/// an engine from before #274: it takes the node token for everything and
/// refuses any other bearer.
async fn gated_engine(seen: Seen, gated: bool) -> String {
    use axum::extract::Request;
    let app = Router::new().fallback(move |req: Request| {
        let seen = seen.clone();
        async move {
            let bearer = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .trim_start_matches("Bearer ")
                .to_string();
            let what = format!("{} {}", req.method(), req.uri().path());
            seen.lock().unwrap().push((what.clone(), bearer.clone()));
            let destructive = what == "POST /api/v1/arrays"
                || what.starts_with("POST /api/v1/arrays/") && what.ends_with("/members")
                || (what.starts_with("DELETE ") && !what.ends_with("/attach"));
            let ok = match (gated, destructive) {
                (true, true) => bearer == "admin",
                _ => bearer == "node",
            };
            if !ok {
                return unauthorized();
            }
            Json(json!({"id": "arr", "member_uuid": "m1", "items": []})).into_response()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// #47: the destructive verbs carry the admin credential, everything else
/// the node token; an engine from before #274 still works (retry with the
/// node token after a refused admin bearer).
#[tokio::test]
async fn destructive_verbs_carry_the_admin_credential() {
    use stormstorage::engine::Engine;
    for gated in [true, false] {
        let seen: Seen = Default::default();
        let url = gated_engine(seen.clone(), gated).await;
        let e = Engine::new(&url, Some("node".into())).with_admin(Some("admin".into()));
        e.create_raid1(&["d1".into(), "d2".into()]).await.unwrap();
        e.array_add_member("arr", "d3").await.unwrap();
        e.array_remove_member("arr", "m1").await.unwrap();
        e.delete_array("arr").await.unwrap();
        e.forget_array("arr").await.unwrap();
        e.delete_drive("nvme-tcp://x", false).await.unwrap();
        e.list_arrays().await.unwrap();
        e.detach_any("vol").await.unwrap();
        let seen = seen.lock().unwrap().clone();
        let first = |w: &str| seen.iter().find(|(x, _)| x == w).map(|(_, b)| b.clone()).unwrap();
        assert_eq!(first("POST /api/v1/arrays"), "admin", "{seen:?}");
        assert_eq!(first("DELETE /api/v1/drives/nvme-tcp:%2F%2Fx"), "admin", "{seen:?}");
        assert_eq!(first("GET /api/v1/arrays"), "node", "reads stay on the node token");
        assert_eq!(first("DELETE /api/v1/volumes/vol/attach"), "node", "a detach is ordinary");
        if gated {
            assert_eq!(seen.len(), 8, "no retries on an engine that takes the admin credential: {seen:?}");
        } else {
            assert_eq!(seen.len(), 14, "each of the 6 destructive calls retried once with the node token: {seen:?}");
        }
    }
}

/// #47: which admin credential an engine is sent.
#[tokio::test]
async fn admin_credential_order() {
    if std::env::var("STORMBLOCK_ADMIN_TOKEN").is_ok_and(|v| !v.trim().is_empty()) {
        return;
    }
    let mut config = Config::default();
    config.local.admin_token_file = Some(token_file("admin-local", "admin-minted").display().to_string());
    config.kubernetes.token_file = Some(token_file("admin-kube", "kube-bearer").display().to_string());
    let state = AppState::new(config, FedState::default(), None);
    assert_eq!(state.admin_credential("http://127.0.0.1:9090").as_deref(), Some("admin-minted"));
    assert_eq!(
        state.admin_credential("http://192.0.2.1:9090").as_deref(),
        Some("kube-bearer"),
        "a peer gets the Kubernetes bearer, never this machine's admin token"
    );
}
