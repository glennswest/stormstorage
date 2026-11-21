//! Finding ea8110684a65: members of one stormcluster share a failure domain
//! at the `cluster` rung. They are adopted from the apiserver's
//! ClusterMember list with the `cluster.storm.io/cluster` label
//! stormcluster puts on each, and placement at that rung never puts two
//! legs of one volume in the same cluster.

mod common;

use serde_json::json;
use std::sync::{Arc, Mutex};
use stormstorage::api::AppState;
use stormstorage::config::{Config, NodeConfig};
use stormstorage::model::{FedState, NodeSource};

fn member(name: &str, address: &str, cluster: Option<&str>) -> serde_json::Value {
    let mut m = common::member(name, address, true);
    if let Some(c) = cluster {
        m["metadata"]["labels"] = json!({ "cluster.storm.io/cluster": c });
    }
    m
}

#[tokio::test]
async fn members_of_one_cluster_are_one_domain_at_the_cluster_rung() {
    let api: common::ApiMock = Arc::new(Mutex::new(common::Api::default()));
    // storm: m1 and m2 labeled, m4 without the label (one apiserver's list
    // is one cluster). m3 says another cluster.
    api.lock().unwrap().members = vec![
        // This machine: the local engine, not a member node.
        member("m0", "127.0.0.1", Some("storm")),
        member("m1", "10.0.0.1", Some("storm")),
        member("m2", "10.0.0.2", Some("storm")),
        member("m3", "10.0.0.3", Some("other")),
        member("m4", "10.0.0.4", None),
    ];
    let url = common::mock_apiserver(api.clone()).await;
    let mut config = Config::default();
    config.local.enabled = true;
    config.kubernetes.server = url;
    // A node outside the cluster, by config: its own cluster (SNO default).
    config
        .nodes
        .push(toml::from_str::<NodeConfig>("name = \"sno\"\nengine_url = \"http://10.0.1.1:9090\"").unwrap());
    let mut fed = FedState::default();
    fed.apply_config_nodes(&config.nodes);
    let state = Arc::new(AppState::new(config, fed, None));
    stormstorage::registry::adopt_members(&state).await;

    let fed = state.fed.read().await;
    let cluster = |n: &str| fed.nodes[n].labels()["cluster"].clone();
    assert_eq!(fed.nodes["m1"].status.source, NodeSource::Member);
    assert_eq!((cluster("m1"), cluster("m2"), cluster("m4")), ("storm".into(), "storm".into(), "storm".into()));
    assert_eq!(cluster("m3"), "other");
    assert_eq!(cluster("sno"), "sno");
    assert_eq!(fed.nodes["m1"].labels()["node"], "m1", "each is still its own node");

    // The storm members have the most room: by name alone, both legs of a
    // 2-leg volume at the cluster rung would go there.
    let free = |n: &str| match n {
        "m1" | "m2" | "m4" => 90,
        _ => 10,
    };
    let rungs = state.config.federation.rungs.clone();
    let cands: Vec<stormstorage::placement::Candidate> = fed
        .nodes
        .values()
        .map(|n| stormstorage::placement::Candidate {
            name: n.config.name.clone(),
            labels: n.labels(),
            free_bytes: free(&n.config.name),
            total_bytes: 100,
            io_busy: None,
        })
        .collect();
    for replicas in [2, 3] {
        let picks = stormstorage::placement::plan(&cands, &rungs, "cluster", replicas, 1, 0.0).unwrap();
        let clusters: std::collections::BTreeSet<String> = picks.iter().map(|p| cluster(p)).collect();
        assert_eq!(clusters.len(), picks.len(), "{replicas} legs in distinct clusters: {picks:?}");
        assert_eq!(picks.iter().filter(|p| cluster(p) == "storm").count(), 1, "{picks:?}");
    }
    // Four legs: only three clusters.
    assert!(stormstorage::placement::plan(&cands, &rungs, "cluster", 4, 1, 0.0).is_err());
    assert!(!fed.nodes.contains_key("m0"), "this machine is the local engine");
    drop(fed);

    // This machine's member: its cluster is kept for the local node's label.
    let m = state.members.read().unwrap().clone();
    assert_eq!((m.local_name.as_deref(), m.local_cluster.as_deref()), (Some("m0"), Some("storm")));
}
