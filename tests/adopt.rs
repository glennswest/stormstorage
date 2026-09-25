//! #9 end to end against in-process mock stormblocks: stormstorage adopts
//! the engine on "its machine" and that engine's cluster peer, shows every
//! slab as a pool with its volumes, sums them per tier, and the components
//! feed is no longer empty. The mock serves the shapes stormblock's own
//! handlers produce (src/mgmt/api/{slabs,volumes,discovery,v1}.rs).

use axum::extract::Path;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;
use stormstorage::api::AppState;
use stormstorage::config::Config;
use stormstorage::model::{FedState, NodeSource};

const SYS: &str = "11111111-0000-0000-0000-000000000001";
const DATA: &str = "22222222-0000-0000-0000-000000000002";
const GOLDEN: &str = "aaaaaaaa-0000-0000-0000-000000000001";
const CLONE: &str = "aaaaaaaa-0000-0000-0000-000000000002";
const ETCD: &str = "aaaaaaaa-0000-0000-0000-000000000003";

fn slab(id: &str, role: &str, total: u64, free: u64) -> Value {
    json!({
        "id": id, "tier": "hot", "role": role, "domain": format!("drive=file+{role}"),
        "slot_size": 1u64 << 20, "total_slots": total, "free_slots": free,
        "allocated_slots": total - free,
        "total_bytes": total << 20, "total_bytes_human": "",
        "free_bytes": free << 20, "free_bytes_human": ""
    })
}

fn volume(id: &str, name: &str, role: &str, parent: Option<&str>, sealed: bool, owner: Value) -> Value {
    let mut v = json!({
        "id": id, "name": name, "virtual_size_bytes": 1u64 << 30, "virtual_size_human": "",
        "allocated_bytes": 0, "allocated_human": "", "shared_bytes": 0, "shared_human": "",
        "array_id": null, "redundancy": "none", "health": "healthy", "physical_bytes": 0,
        "sealed": sealed, "access": "rw", "writable": !sealed, "role": role
    });
    if let Some(p) = parent {
        v["parent"] = json!(p);
    }
    if !owner.is_null() {
        v["owner"] = owner;
    }
    v
}

/// A stormblock named `name`; `peer` is a cluster peer it hears.
async fn mock_engine(name: &'static str, peer: Option<(&'static str, String)>) -> String {
    let peers: Vec<Value> = peer
        .into_iter()
        .map(|(n, addr)| {
            json!({"version": 1, "node_name": n, "mgmt_addr": addr, "cluster_id": "c-1",
                   "cluster_name": "lab", "total_bytes": 0, "free_bytes": 0,
                   "engine_version": "x", "age_secs": 1, "stale": false})
        })
        .collect();
    let discovery = json!({
        "local_node": name, "cluster_id": "c-1", "cluster_name": "lab",
        "nodes": peers, "clusters": [], "cluster_peer_count": 1
    });
    let app = Router::new()
        .route("/api/v1/discovery", get(move || async move { Json(discovery.clone()) }))
        .route(
            "/v1/nodes/capacity",
            get(move || async move {
                Json(json!([{"node": name, "total_bytes": 300u64 << 20, "free_bytes": 100u64 << 20}]))
            }),
        )
        .route(
            "/api/v1/slabs",
            get(|| async {
                Json(json!({"items": [slab(SYS, "system", 100, 20), slab(DATA, "data", 200, 80)],
                            "count": 2}))
            }),
        )
        .route(
            "/api/v1/volumes",
            get(|| async {
                let items = vec![
                    volume(GOLDEN, "stormpump", "system", None, true, Value::Null),
                    volume(CLONE, "stormpump-root", "system", Some(GOLDEN), false,
                           json!({"kind": "Container", "namespace": "", "name": "stormpump"})),
                    volume(ETCD, "fastetcd-data", "data", None, false,
                           json!({"kind": "PersistentVolumeClaim", "namespace": "kube-system",
                                  "name": "fastetcd-data"})),
                ];
                Json(json!({"count": items.len(), "items": items}))
            }),
        )
        .route(
            "/api/v1/slabs/{id}/slots",
            get(|Path(id): Path<String>| async move {
                let owner = if id == SYS { GOLDEN } else { ETCD };
                Json(json!({"count": 2, "items": [
                    {"slot_idx": 0, "volume_id": owner, "virtual_extent_idx": 0, "ref_count": 1, "generation": 1},
                    {"slot_idx": 1, "volume_id": owner, "virtual_extent_idx": 1, "ref_count": 1, "generation": 1}
                ]}))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr.to_string()
}

#[tokio::test]
async fn adopts_local_engine_and_shows_slabs_as_pools() {
    let peer_addr = mock_engine("node-b", None).await;
    let local_addr = mock_engine("node-a", Some(("node-b", peer_addr.clone()))).await;

    let mut config = Config::default();
    config.local.engine_url = format!("http://{local_addr}");
    config.local.token_file = Some("/nonexistent/stormstorage-test-token".into());
    let state = Arc::new(AppState::new(config, FedState::default(), None));

    stormstorage::registry::poll_once(&state).await;

    {
        let fed = state.fed.read().await;
        let a = fed.nodes.get("node-a").expect("local engine adopted by its own name");
        assert_eq!(a.status.source, NodeSource::Local);
        assert!(a.status.healthy);
        assert_eq!(a.status.volumes, 3);
        let b = fed.nodes.get("node-b").expect("cluster peer adopted");
        assert_eq!(b.config.engine_url, format!("http://{peer_addr}"));
        assert_eq!(fed.revision, 0, "adoption is local, not replicated intent");
    }

    // Placement: golden by its slots, its clone through the parent, the
    // claim on the data slab.
    {
        let inv = state.inventory.read().await;
        let a = &inv["node-a"];
        assert_eq!(a.slabs.len(), 2);
        let on = |vid: &str| {
            a.volumes
                .iter()
                .find(|v| v.volume.id == vid)
                .unwrap()
                .slabs
                .clone()
        };
        assert_eq!(on(GOLDEN), vec![SYS.to_string()]);
        assert_eq!(on(CLONE), vec![SYS.to_string()]);
        assert_eq!(on(ETCD), vec![DATA.to_string()]);
    }

    // The REST surface.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap();
    let router = stormstorage::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let pools: Value = reqwest::get(format!("http://{api}/api/v1/pools"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let pools = pools["pools"].as_array().unwrap();
    let slabs: Vec<&Value> = pools.iter().filter(|p| p["kind"] == "slab").collect();
    assert_eq!(slabs.len(), 4, "two slabs on each of two nodes");
    let sys_a = slabs
        .iter()
        .find(|p| p["node"] == "node-a" && p["slab"] == SYS)
        .unwrap();
    assert_eq!(sys_a["volumes"], 2);
    assert_eq!(sys_a["free_bytes"], 20u64 << 20);
    let hot = pools.iter().find(|p| p["kind"] == "tier").unwrap();
    assert_eq!(hot["tier"], "hot");
    assert_eq!(hot["total_bytes"], 2 * (300u64 << 20));
    assert_eq!(hot["volumes"], 6);

    let inv: Value = reqwest::get(format!("http://{api}/api/v1/nodes/node-a/inventory"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(inv["volumes"].as_array().unwrap().len(), 3);
    assert_eq!(inv["volumes"][1]["placed_by"], "parent");

    // The components feed: real counts, pools with their volumes.
    let feed = stormstorage::components::collect(&state).await;
    let system = feed.iter().find(|c| c.id == "system").unwrap();
    assert!(
        system.detail.starts_with("2/2 nodes · 4 pools · 6 volumes"),
        "{}",
        system.detail
    );
    let pool = feed
        .iter()
        .find(|c| c.id == format!("pool:node-a/{SYS}"))
        .unwrap();
    assert_eq!(pool.kind, "pool");
    let vols = pool.relations.iter().find(|r| r.name == "volumes").unwrap();
    assert_eq!(
        vols.targets,
        vec![format!("nvol:node-a/{GOLDEN}"), format!("nvol:node-a/{CLONE}")]
    );
    let claim = feed
        .iter()
        .find(|c| c.id == format!("nvol:node-a/{ETCD}"))
        .unwrap();
    assert!(claim.detail.contains("PersistentVolumeClaim kube-system/fastetcd-data"));
    assert!(feed.iter().any(|c| c.id == "tier:hot"));
}

#[tokio::test]
async fn no_local_engine_adopts_nothing() {
    let mut config = Config::default();
    // Nothing listens on port 9 (discard) on loopback in the build sandbox.
    config.local.engine_url = "http://127.0.0.1:9".into();
    let state = Arc::new(AppState::new(config, FedState::default(), None));
    stormstorage::registry::poll_once(&state).await;
    assert!(state.fed.read().await.nodes.is_empty());
}
