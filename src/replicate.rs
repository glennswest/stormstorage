//! Peer replication of the control plane's durable intent.
//!
//! stormstorage instances (one per site/cluster, "peers") replicate the
//! state that cannot be re-derived: the distributed-volume records and the
//! self-registered node configs. Poll status is deliberately NOT
//! replicated — every peer watches the engines itself, so observability is
//! local and peers cannot ping-pong overwrites of each other's freshness.
//!
//! Mechanism: every durable mutation bumps `FedState.revision` and pushes
//! a full payload to every configured peer; a peer applies a payload only
//! when its revision is newer than local. Last-writer-wins by revision —
//! honest async replication for a control plane whose state is also
//! rebuildable from the engines. Consensus (StormKV/fastetcd) is the
//! phase-5 ladder, same as stormblock's own GEM plan.

use crate::api::AppState;
use crate::config::NodeConfig;
use crate::events::Severity;
use crate::model::{DistVolume, Node, NodeSource, NodeStatus};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub revision: u64,
    pub volumes: BTreeMap<String, DistVolume>,
    /// Configs of self-registered nodes (static nodes travel in each
    /// peer's own config file).
    pub registered: Vec<NodeConfig>,
}

pub async fn build_payload(state: &Arc<AppState>) -> Payload {
    let fed = state.fed.read().await;
    Payload {
        revision: fed.revision,
        volumes: fed.volumes.clone(),
        registered: fed
            .nodes
            .values()
            .filter(|n| n.status.source == NodeSource::Registered)
            .map(|n| n.config.clone())
            .collect(),
    }
}

/// Apply a peer's payload if it is newer. Returns whether it was applied.
pub async fn apply(state: &Arc<AppState>, payload: Payload) -> bool {
    let mut fed = state.fed.write().await;
    if payload.revision <= fed.revision {
        return false;
    }
    fed.volumes = payload.volumes;
    for nc in payload.registered {
        match fed.nodes.get_mut(&nc.name) {
            Some(n) => {
                // Never let a peer overwrite a node this instance
                // configures statically.
                if n.status.source == NodeSource::Registered {
                    n.config = nc;
                }
            }
            None => {
                fed.nodes.insert(
                    nc.name.clone(),
                    Node {
                        config: nc,
                        status: NodeStatus::new(NodeSource::Registered),
                    },
                );
            }
        }
    }
    let rev = payload.revision;
    fed.revision = rev;
    drop(fed);
    state.events.write().await.push(
        None,
        Severity::Info,
        "replicate",
        format!("applied peer state at revision {rev}"),
    );
    state.persist().await;
    true
}

/// Bump the revision (call while holding intent to persist) and push to
/// every peer in the background.
pub fn push_to_peers(state: Arc<AppState>) {
    let peers = state.config.replication.peers.clone();
    if peers.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let payload = build_payload(&state).await;
        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(_) => return,
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client");
        for peer in peers {
            let url = format!("{}/api/v1/replicate", peer.trim_end_matches('/'));
            let mut req = client
                .post(&url)
                .header("content-type", "application/json")
                .body(body.clone());
            if !state.config.api.api_token.is_empty() {
                req = req.bearer_auth(&state.config.api.api_token);
            }
            match req.send().await {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => tracing::warn!(%url, status = %r.status(), "peer rejected replication push"),
                Err(e) => tracing::warn!(%url, "peer unreachable for replication: {e}"),
            }
        }
    });
}
