//! Registry poller: adopt the local engine and its cluster peers, enrich
//! every node from its engine, track health, and refresh each node's
//! inventory (slabs and volumes).

use crate::api::AppState;
use crate::config::NodeConfig;
use crate::engine::Engine;
use crate::events::Severity;
use crate::inventory::{self, NodeInventory};
use crate::model::{Node, NodeSource, NodeStatus};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub async fn run(state: Arc<AppState>) {
    loop {
        poll_once(&state).await;
        tokio::time::sleep(Duration::from_secs(state.config.poll.interval_secs.max(1))).await;
    }
}

pub async fn poll_once(state: &Arc<AppState>) {
    if state.config.local.enabled {
        adopt_local(state).await;
    }
    let snapshot: Vec<(String, String, Option<String>)> = {
        let fed = state.fed.read().await;
        fed.nodes
            .values()
            .map(|n| {
                (
                    n.config.name.clone(),
                    n.config.engine_url.clone(),
                    n.config.api_token.clone(),
                )
            })
            .collect()
    };
    for (name, url, token) in snapshot {
        let engine = Engine::new(&url, token);
        let result = engine.capacity().await;
        let reachable = result.is_ok();
        let mut fed = state.fed.write().await;
        let Some(node) = fed.nodes.get_mut(&name) else {
            continue;
        };
        match result {
            Ok(cap) => {
                let was_healthy = node.status.healthy;
                node.status.healthy = true;
                node.status.consecutive_failures = 0;
                node.status.last_ok = Some(SystemTime::now());
                node.status.total_bytes = cap.total_bytes;
                node.status.free_bytes = cap.free_bytes;
                node.status.engine_topology = cap.topology;
                drop(fed);
                if !was_healthy {
                    state.events.write().await.push(
                        Some(name.clone()),
                        Severity::Info,
                        "node",
                        format!("{name}: engine reachable ({url})"),
                    );
                }
            }
            Err(e) => {
                node.status.consecutive_failures += 1;
                let flipping = node.status.healthy
                    && node.status.consecutive_failures >= state.config.poll.fail_threshold;
                if flipping {
                    node.status.healthy = false;
                }
                let failures = node.status.consecutive_failures;
                let healthy = node.status.healthy;
                drop(fed);
                // An unhealthy node's inventory would say the opposite of
                // the truth — drop it.
                if !healthy {
                    state.inventory.write().await.remove(&name);
                }
                if flipping {
                    state.events.write().await.push(
                        Some(name.clone()),
                        Severity::Error,
                        "node",
                        format!("{name}: engine unreachable after {failures} polls: {e:#}"),
                    );
                }
            }
        }
        if reachable {
            refresh_inventory(state, &name, &engine).await;
        }
    }
    // Forget inventory of nodes no longer known.
    {
        let fed = state.fed.read().await;
        state
            .inventory
            .write()
            .await
            .retain(|n, _| fed.nodes.contains_key(n));
    }
    state.persist().await;
}

/// Read one node's slabs, volumes and slot owners, and place the volumes.
/// A failed read keeps the previous inventory and records why.
async fn refresh_inventory(state: &Arc<AppState>, name: &str, engine: &Engine) {
    let result: anyhow::Result<NodeInventory> = async {
        let slabs = engine.list_slabs().await?;
        let volumes = engine.list_engine_volumes().await?;
        let mut owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for slab in &slabs {
            for vid in engine.slab_volume_ids(&slab.id).await? {
                owners.entry(vid).or_default().insert(slab.id.clone());
            }
        }
        let volumes = inventory::place(&slabs, volumes, &owners);
        Ok(NodeInventory {
            slabs,
            volumes,
            fetched_at: Some(SystemTime::now()),
            error: None,
        })
    }
    .await;
    match result {
        Ok(inv) => {
            let count = inv.volumes.len() as u64;
            state.inventory.write().await.insert(name.to_string(), inv);
            if let Some(n) = state.fed.write().await.nodes.get_mut(name) {
                n.status.volumes = count;
            }
        }
        Err(e) => {
            let mut all = state.inventory.write().await;
            let inv = all.entry(name.to_string()).or_default();
            inv.error = Some(format!("{e:#}"));
        }
    }
}

/// This machine's hostname (the node's, under `share uts`).
fn hostname() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Adopt the engine on this machine once it answers, and the live peers of
/// its stormblock cluster (#9). Nodes the config or a heartbeat already
/// names are left as they are.
pub async fn adopt_local(state: &Arc<AppState>) {
    let local = &state.config.local;
    let token = local.token();
    let engine = Engine::new(&local.engine_url, token.clone());
    let discovery = engine.discovery().await.ok().flatten();
    if discovery.is_none() && engine.capacity().await.is_err() {
        return; // No engine here (yet).
    }
    let local_url = local.engine_url.trim_end_matches('/').to_string();
    let name = local
        .name
        .clone()
        .or_else(|| {
            discovery
                .as_ref()
                .map(|d| d.local_node.clone())
                .filter(|n| !n.is_empty())
        })
        .or_else(hostname)
        .unwrap_or_else(|| "local".into());

    let mut wanted: Vec<NodeConfig> = vec![NodeConfig {
        name,
        engine_url: local_url,
        api_token: token.clone(),
        labels: Default::default(),
        tier: local.tier.clone(),
    }];
    if local.cluster_peers {
        if let Some(d) = &discovery {
            for p in d.cluster_peers() {
                wanted.push(NodeConfig {
                    name: p.node_name.clone(),
                    engine_url: crate::api::engine_url_from(&p.mgmt_addr),
                    api_token: token.clone(),
                    labels: Default::default(),
                    tier: local.tier.clone(),
                });
            }
        }
    }

    let mut adopted = Vec::new();
    let mut dropped = Vec::new();
    {
        let mut fed = state.fed.write().await;
        for nc in &wanted {
            // Another source already covers this engine or this name.
            let covered = fed.nodes.values().any(|n| {
                n.status.source != NodeSource::Local
                    && (n.config.name == nc.name || n.config.engine_url == nc.engine_url)
            });
            if covered {
                continue;
            }
            match fed.nodes.get_mut(&nc.name) {
                Some(n) => n.config = nc.clone(),
                None => {
                    fed.nodes.insert(
                        nc.name.clone(),
                        Node {
                            config: nc.clone(),
                            status: NodeStatus::new(NodeSource::Local),
                        },
                    );
                    adopted.push(format!("{} ({})", nc.name, nc.engine_url));
                }
            }
        }
        // Adopted nodes no longer found: renamed, or left the cluster.
        // Without a discovery answer this poll, peers are not judged.
        let keep: BTreeSet<&str> = wanted.iter().map(|n| n.name.as_str()).collect();
        let judge_peers = discovery.is_some() || !local.cluster_peers;
        let local_url = &wanted[0].engine_url;
        fed.nodes.retain(|name, n| {
            let stays = n.status.source != NodeSource::Local
                || keep.contains(name.as_str())
                || (!judge_peers && &n.config.engine_url != local_url);
            if !stays {
                dropped.push(name.clone());
            }
            stays
        });
    }
    let mut events = state.events.write().await;
    for a in adopted {
        events.push(None, Severity::Info, "adopt", format!("adopted {a}"));
    }
    for d in dropped {
        events.push(
            Some(d.clone()),
            Severity::Warning,
            "adopt",
            format!("{d}: no longer this engine or its cluster peer — forgotten"),
        );
    }
}
