//! Registry poller: enrich every node from its engine and track health.

use crate::api::AppState;
use crate::engine::Engine;
use crate::events::Severity;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub async fn run(state: Arc<AppState>) {
    loop {
        poll_once(&state).await;
        tokio::time::sleep(Duration::from_secs(state.config.poll.interval_secs.max(1))).await;
    }
}

pub async fn poll_once(state: &Arc<AppState>) {
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
                drop(fed);
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
    }
    state.persist().await;
}
