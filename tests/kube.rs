//! #28 end to end: a mock stormblock on "this machine" and a mock
//! apiserver holding what rustkube-node's mirror writes (rustkube-node#59):
//! a PV + bound PVC per node volume, node-qualified, plus another node's
//! pair and a foreign PV. stormstorage reads them with its token, puts each
//! node volume's PV/PVC on the inventory and the feed, and keeps the last
//! view when the apiserver stops answering.

use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use stormstorage::api::AppState;
use stormstorage::config::Config;
use stormstorage::model::FedState;

const TOKEN: &str = "kube-test-token";

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr.to_string()
}

fn volume(id: &str, name: &str) -> Value {
    json!({
        "id": id, "name": name, "virtual_size_bytes": 1u64 << 30, "allocated_bytes": 0,
        "shared_bytes": 0, "redundancy": "none", "health": "healthy", "sealed": false,
        "role": "data", "kind": "volume"
    })
}

async fn mock_engine() -> String {
    let app = Router::new()
        .route(
            "/api/v1/discovery",
            get(|| async {
                Json(json!({"local_node": "n1", "cluster_id": "c", "cluster_name": "lab",
                            "nodes": [], "clusters": [], "cluster_peer_count": 0}))
            }),
        )
        .route(
            "/v1/nodes/capacity",
            get(|| async { Json(json!([{"node": "n1", "total_bytes": 1u64 << 34, "free_bytes": 1u64 << 33}])) }),
        )
        .route("/api/v1/slabs", get(|| async { Json(json!({"items": [], "count": 0})) }))
        .route(
            "/api/v1/volumes",
            get(|| async {
                let items = vec![
                    volume("v1", "fastetcd-data"),
                    volume("v2", "fastetcd-logs"),
                    volume("v3", "scratch"),
                ];
                Json(json!({"count": items.len(), "items": items}))
            }),
        );
    serve(app).await
}

fn pv(volume: &str, node: &str, claim: &str, uid: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {
            "name": format!("storm-{volume}-{node}"),
            "labels": {"storm.io/system-volume": "true", "storm.io/volume-kind": kind,
                       "storm.io/component": "fastetcd"},
            "annotations": {"storm.io/node": node, "storm.io/volume": volume},
        },
        "spec": {
            "capacity": {"storage": "1Gi"},
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "stormblock",
            "claimRef": {"kind": "PersistentVolumeClaim", "namespace": "kube-system",
                         "name": claim, "uid": uid},
            "csi": {"driver": "stormblock.storm.io", "volumeHandle": volume},
        },
        "status": {"phase": "Bound"},
    })
}

fn pvc(name: &str, pv: &str, uid: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": "kube-system", "uid": uid},
        "spec": {"volumeName": pv},
        "status": {"phase": "Bound"},
    })
}

/// An apiserver that wants `TOKEN`, and can be told to fail.
async fn mock_apiserver(down: Arc<AtomicBool>) -> String {
    let authed = move |h: &HeaderMap, down: &AtomicBool| -> Result<(), StatusCode> {
        if down.load(Ordering::SeqCst) {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        match h.get("authorization").and_then(|v| v.to_str().ok()) {
            Some(v) if v == format!("Bearer {TOKEN}") => Ok(()),
            _ => Err(StatusCode::UNAUTHORIZED),
        }
    };
    let d1 = down.clone();
    let d2 = down;
    let app = Router::new()
        .route(
            "/api/v1/persistentvolumes",
            get(move |h: HeaderMap| async move {
                authed(&h, &d1)?;
                Ok::<_, StatusCode>(Json(json!({"kind": "PersistentVolumeList", "items": [
                    pv("fastetcd-data", "n1", "fastetcd-data-n1", "u1", "data"),
                    // The claim was deleted: the PV stays (Retain).
                    pv("fastetcd-logs", "n1", "fastetcd-logs-n1", "u2", "logs"),
                    // Another node's volume of the same name.
                    pv("fastetcd-data", "n2", "fastetcd-data-n2", "u3", "data"),
                    {"metadata": {"name": "nfs-1"}, "spec": {"nfs": {"server": "x", "path": "/"}}},
                ]})))
            }),
        )
        .route(
            "/api/v1/persistentvolumeclaims",
            get(move |h: HeaderMap| async move {
                authed(&h, &d2)?;
                Ok::<_, StatusCode>(Json(json!({"kind": "PersistentVolumeClaimList", "items": [
                    pvc("fastetcd-data-n1", "storm-fastetcd-data-n1", "u1"),
                    pvc("fastetcd-data-n2", "storm-fastetcd-data-n2", "u3"),
                ]})))
            }),
        );
    serve(app).await
}

#[tokio::test]
async fn node_volumes_carry_their_pv_and_claim() {
    let dir = std::env::temp_dir().join(format!("stormstorage-kube-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let token_file = dir.join("token");
    std::fs::write(&token_file, format!("{TOKEN}\n")).unwrap();

    let down = Arc::new(AtomicBool::new(false));
    let api = mock_apiserver(down.clone()).await;
    let engine = mock_engine().await;
    let mut config = Config::default();
    config.local.engine_url = format!("http://{engine}");
    config.local.token_file = Some("/nonexistent/stormstorage-test-token".into());
    config.kubernetes.server = format!("http://{api}");
    config.kubernetes.token_file = Some(token_file.display().to_string());
    let state = Arc::new(AppState::new(config, FedState::default(), None));

    stormstorage::registry::poll_once(&state).await;

    let check = |inv: &Value| {
        let vols = inv["volumes"].as_array().unwrap();
        let by = |n: &str| vols.iter().find(|v| v["name"] == n).unwrap().clone();
        let data = by("fastetcd-data");
        assert_eq!(data["pv"]["pv"], "storm-fastetcd-data-n1", "this node's pair, not n2's");
        assert_eq!(data["pv"]["phase"], "Bound");
        assert_eq!(data["pv"]["reclaim"], "Retain");
        assert_eq!(data["pv"]["volume_kind"], "data");
        assert_eq!(data["pv"]["component"], "fastetcd");
        assert_eq!(data["pv"]["claim"]["name"], "fastetcd-data-n1");
        assert_eq!(data["pv"]["claim"]["namespace"], "kube-system");
        assert_eq!(data["pv"]["claim"]["phase"], "Bound");
        assert_eq!(data["pv"]["claim"]["bound"], true);
        let logs = by("fastetcd-logs");
        assert_eq!(logs["pv"]["claim"]["name"], "fastetcd-logs-n1");
        assert_eq!(logs["pv"]["claim"]["bound"], false, "its claim is gone");
        assert!(logs["pv"]["claim"]["phase"].is_null());
        assert!(by("scratch").get("pv").is_none(), "no PV: no field");
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let get_inv = || async {
        reqwest::get(format!("http://{addr}/api/v1/nodes/n1/inventory"))
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    };
    check(&get_inv().await);

    // The feed: the node volume names its PV and claim.
    let feed = stormstorage::components::collect(&state).await;
    let c = feed.iter().find(|c| c.id == "nvol:n1/v1").expect("node volume in the feed");
    let m = |l: &str| c.metrics.iter().find(|m| m.label == l).map(|m| m.value.clone());
    assert_eq!(m("pv").as_deref(), Some("storm-fastetcd-data-n1 Bound"));
    assert_eq!(m("claim").as_deref(), Some("kube-system/fastetcd-data-n1"));
    assert_eq!(m("holds").as_deref(), Some("data of fastetcd"));

    // The apiserver stops answering: the last view stays, one event says so.
    down.store(true, Ordering::SeqCst);
    stormstorage::registry::poll_once(&state).await;
    stormstorage::registry::poll_once(&state).await;
    check(&get_inv().await);
    let events = state.events.read().await;
    let warned = events
        .since(0)
        .iter()
        .filter(|e| e.kind == "kubernetes" && e.message.contains("not readable"))
        .count();
    assert_eq!(warned, 1, "one event for the change, not one per poll");
    drop(events);
    assert!(state.kube.read().await.error.is_some());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn wrong_token_reads_nothing_and_says_why() {
    let api = mock_apiserver(Arc::new(AtomicBool::new(false))).await;
    let engine = mock_engine().await;
    let mut config = Config::default();
    config.local.engine_url = format!("http://{engine}");
    config.local.token_file = Some("/nonexistent/stormstorage-test-token".into());
    config.kubernetes.server = format!("http://{api}");
    config.kubernetes.token_file = Some("/nonexistent/kube-token".into());
    let state = Arc::new(AppState::new(config, FedState::default(), None));
    stormstorage::registry::poll_once(&state).await;
    let err = state.kube.read().await.error.clone().expect("401 recorded");
    assert!(err.contains("401"), "{err}");
    let inv = state.inventory.read().await;
    assert!(inv["n1"].volumes.iter().all(|v| v.pv.is_none()));
}
