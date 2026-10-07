//! Rebalance (#30): move legs off the fullest nodes of a pool onto its
//! emptiest, by the pool's watermarks, with the leg-move sequence that
//! failure recovery uses (new leg → rebuild → old leg retired, so a volume
//! never has fewer copies than before). Opt-in per pool: a pool without
//! watermarks is never rebalanced.

use crate::api::AppState;
use crate::config::PoolConfig;
use crate::events::Severity;
use crate::model::{AssemblyState, FedState, LegState, Node};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// One proposed leg move.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Move {
    pub pool: String,
    pub volume: String,
    pub from: String,
    pub to: String,
    /// Used fraction of `from` and `to` when proposed.
    pub from_used: f64,
    pub to_used: f64,
}

/// Why a pool proposes nothing, for the dry run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Held {
    pub pool: String,
    pub why: String,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct Plan {
    pub moves: Vec<Move>,
    pub held: Vec<Held>,
}

fn used(n: &Node) -> f64 {
    if n.status.total_bytes == 0 {
        return 0.0;
    }
    1.0 - n.status.free_bytes as f64 / n.status.total_bytes as f64
}

/// The moves each watermarked pool wants now. Pure, deterministic.
///
/// For a pool whose nodes all answer: a node used above `high_watermark` is
/// a source (fullest first). Each of its legs — of an idle, assembled,
/// unfenced volume of that pool, not the head's own leg — may go to a node
/// of the pool used below `low_watermark` that stays at or under `high`
/// with the leg added, in a domain distinct from the volume's other legs
/// (the move-target rule), best by `placement::plan`. The gap between the
/// watermarks keeps a move from making a new source. At most `max_moves`
/// moves per pool are in flight, and one per volume.
pub fn plan(
    fed: &FedState,
    pools: &[PoolConfig],
    rungs: &[String],
    io_weight: f64,
    busy: &dyn Fn(&str) -> bool,
    now: SystemTime,
) -> Plan {
    let mut out = Plan::default();
    for pool in pools {
        let (Some(high), Some(low)) = (pool.high_watermark, pool.low_watermark) else {
            continue;
        };
        let hold = |why: String| Held { pool: pool.name.clone(), why };
        if !(0.0..=1.0).contains(&low) || !(0.0..=1.0).contains(&high) || low >= high {
            out.held.push(hold(format!("watermarks low {low} / high {high}: need 0 ≤ low < high ≤ 1")));
            continue;
        }
        let members: Vec<&Node> = fed.nodes.values().filter(|n| pool.selector.matches(&n.config)).collect();
        if let Some(down) = members.iter().find(|n| !n.status.healthy) {
            out.held.push(hold(format!("node {} is unreachable — recovery comes before rebalancing", down.config.name)));
            continue;
        }
        let in_pool = |v: &&crate::model::DistVolume| v.pool.as_deref() == Some(pool.name.as_str());
        let in_flight = fed.volumes.values().filter(in_pool).filter(|v| v.replacing.is_some()).count() as u32;
        let mut room = pool.max_moves.saturating_sub(in_flight);
        if room == 0 {
            out.held.push(hold(format!("{in_flight} move(s) already in flight (max_moves {})", pool.max_moves)));
            continue;
        }
        // Sizes promised by moves proposed in this pass, per target.
        let mut promised: std::collections::BTreeMap<String, u64> = Default::default();
        let mut sources: Vec<&&Node> = members.iter().filter(|n| used(n) > high).collect();
        sources.sort_by(|a, b| used(b).partial_cmp(&used(a)).unwrap_or(std::cmp::Ordering::Equal).then(a.config.name.cmp(&b.config.name)));
        if sources.is_empty() {
            continue;
        }
        let mut moved_volumes = std::collections::BTreeSet::new();
        'sources: for src in sources {
            let mut vols: Vec<&crate::model::DistVolume> = fed
                .volumes
                .values()
                .filter(in_pool)
                .filter(|v| v.assembly == AssemblyState::Assembled)
                .filter(|v| !v.fenced && v.replacing.is_none() && v.dual_attach.is_none() && v.migration.is_none())
                .filter(|v| !busy(&v.name) && v.next_releg_after.map_or(true, |t| t <= now))
                .filter(|v| v.head.as_deref() != Some(src.config.name.as_str()))
                .filter(|v| v.legs.iter().any(|l| l.node == src.config.name && l.state == LegState::Created))
                .collect();
            // Largest first: fewest moves to bring the source down.
            vols.sort_by(|a, b| b.size_bytes.cmp(&a.size_bytes).then(a.name.cmp(&b.name)));
            for v in vols {
                if room == 0 {
                    break 'sources;
                }
                if moved_volumes.contains(&v.name) {
                    continue;
                }
                let cands: Vec<crate::placement::Candidate> =
                    crate::orchestrate::move_target_candidates(fed, rungs, v, &src.config.name)
                        .into_iter()
                        .filter(|c| members.iter().any(|m| m.config.name == c.name))
                        .filter(|c| c.total_bytes > 0)
                        .filter(|c| {
                            let extra = promised.get(&c.name).copied().unwrap_or(0);
                            let used_now = 1.0 - c.free_bytes as f64 / c.total_bytes as f64;
                            let after = (c.total_bytes - c.free_bytes.min(c.total_bytes)) as f64
                                + (extra + v.size_bytes) as f64;
                            used_now < low && after / c.total_bytes as f64 <= high
                        })
                        .collect();
                let Ok(mut pick) = crate::placement::plan(&cands, rungs, &v.rung, 1, v.size_bytes, io_weight) else {
                    continue;
                };
                let to = pick.remove(0);
                let to_used = fed.nodes.get(&to).map(used).unwrap_or(0.0);
                *promised.entry(to.clone()).or_default() += v.size_bytes;
                moved_volumes.insert(v.name.clone());
                room -= 1;
                out.moves.push(Move {
                    pool: pool.name.clone(),
                    volume: v.name.clone(),
                    from: src.config.name.clone(),
                    to,
                    from_used: used(src),
                    to_used,
                });
            }
        }
        if out.moves.iter().all(|m| m.pool != pool.name) {
            out.held.push(hold(format!(
                "nodes above high {high}, but no leg can move to a node below low {low} that stays under high"
            )));
        }
    }
    out
}

/// Run what [`plan`] proposes (after each poll), on the instance that acts
/// on recovery. A move that cannot start waits `recovery.cooldown_secs`.
pub async fn run(state: &Arc<AppState>, now: SystemTime) {
    if !state.config.recovery.active(!state.config.replication.peers.is_empty()) {
        return;
    }
    if state.config.pools.iter().all(|p| p.high_watermark.is_none()) {
        return;
    }
    let p = {
        let fed = state.fed.read().await;
        plan(
            &fed,
            &state.config.pools,
            &state.config.federation.rungs,
            state.config.placement.io_weight,
            &|n| state.replacing.lock().expect("replacing lock").contains(n),
            now,
        )
    };
    for m in p.moves {
        let msg = format!(
            "{}: rebalance in pool {}: moving its leg from {} ({:.0}% used) to {} ({:.0}% used)",
            m.volume,
            m.pool,
            m.from,
            m.from_used * 100.0,
            m.to,
            m.to_used * 100.0
        );
        state.events.write().await.push(Some(m.volume.clone()), Severity::Info, "rebalance", msg);
        if let Err(e) = crate::orchestrate::start_replacement(state, &m.volume, &m.from, Some(m.to.clone()), "rebalance").await {
            let cooldown = state.config.recovery.cooldown_secs;
            {
                let mut fed = state.fed.write().await;
                if let Some(v) = fed.volumes.get_mut(&m.volume) {
                    v.next_releg_after = Some(now + Duration::from_secs(cooldown));
                }
            }
            state.persist().await;
            state.events.write().await.push(
                Some(m.volume.clone()),
                Severity::Warning,
                "rebalance",
                format!("{}: rebalance move {} → {} did not start, retry in {cooldown}s: {e:#}", m.volume, m.from, m.to),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::model::{DistVolume, Leg, NodeSource, NodeStatus, Replacement};

    fn node(name: &str, free: u64) -> Node {
        let config: NodeConfig =
            toml::from_str(&format!("name = \"{name}\"\nengine_url = \"http://{name}:9090\"")).unwrap();
        let mut status = NodeStatus::new(NodeSource::Static);
        status.healthy = true;
        status.free_bytes = free;
        status.total_bytes = 100;
        Node { config, status }
    }

    fn vol(name: &str, legs: &[&str], size: u64) -> DistVolume {
        DistVolume {
            name: name.into(),
            size_bytes: size,
            pool: Some("p".into()),
            replicas: legs.len() as u32,
            rung: "node".into(),
            legs: legs
                .iter()
                .map(|n| Leg {
                    node: n.to_string(),
                    volume_id: Some(format!("{name}-{n}")),
                    state: LegState::Created,
                    message: None,
                    master_node: None,
                    export: None,
                    drive_uuid: None,
                    member_uuid: None,
                    epoch: None,
                })
                .collect(),
            assembly: AssemblyState::Assembled,
            head: Some(legs[0].to_string()),
            array_id: Some("arr".into()),
            created_at: SystemTime::now(),
            replacing: None,
            next_releg_after: None,
            next_assemble_after: None,
            export: Default::default(),
            epoch: 1,
            fenced: false,
            bandwidth_class: Default::default(),
            extent_size_bytes: None,
            rate_pending: false,
            dual_attach: None,
            migration: None,
        }
    }

    fn pool(high: Option<f64>, low: Option<f64>) -> PoolConfig {
        let mut p: PoolConfig = toml::from_str("name = \"p\"").unwrap();
        p.high_watermark = high;
        p.low_watermark = low;
        p
    }

    fn rungs() -> Vec<String> {
        vec!["node".into()]
    }

    /// a is full (90% used) and c just joined (10% used); b is in between
    /// (55%). v and w have legs on h (their head) and a.
    fn fed() -> FedState {
        let mut fed = FedState::default();
        for (n, free) in [("h", 50), ("a", 10), ("b", 45), ("c", 90)] {
            fed.nodes.insert(n.into(), node(n, free));
        }
        fed.volumes.insert("v".into(), vol("v", &["h", "a"], 20));
        fed.volumes.insert("w".into(), vol("w", &["h", "a"], 5));
        fed
    }

    #[test]
    fn moves_the_largest_leg_off_a_full_node_to_an_empty_one() {
        let now = SystemTime::now();
        let p = plan(&fed(), &[pool(Some(0.8), Some(0.4))], &rungs(), 0.3, &|_| false, now);
        assert_eq!(p.moves.len(), 1, "{p:?}");
        let m = &p.moves[0];
        assert_eq!((m.volume.as_str(), m.from.as_str(), m.to.as_str()), ("v", "a", "c"));
        assert!((m.from_used - 0.9).abs() < 1e-9);
        // b (55% used) is not below low; h is the head, never a source.
        let p = plan(&fed(), &[pool(Some(0.8), Some(0.4))], &rungs(), 0.0, &|_| false, now);
        assert_eq!(p.moves[0].to, "c");
    }

    #[test]
    fn off_without_watermarks_and_gated_otherwise() {
        let now = SystemTime::now();
        assert_eq!(plan(&fed(), &[pool(None, None)], &rungs(), 0.3, &|_| false, now), Plan::default());
        // Inverted watermarks: refused, said why.
        let p = plan(&fed(), &[pool(Some(0.4), Some(0.8))], &rungs(), 0.3, &|_| false, now);
        assert!(p.moves.is_empty() && p.held[0].why.contains("low"), "{p:?}");
        // A node of the pool down: recovery first.
        let mut f = fed();
        f.nodes.get_mut("b").unwrap().status.healthy = false;
        let p = plan(&f, &[pool(Some(0.8), Some(0.4))], &rungs(), 0.3, &|_| false, now);
        assert!(p.moves.is_empty() && p.held[0].why.contains("unreachable"), "{p:?}");
        // A move already in flight in the pool (max_moves 1): none more.
        let mut f = fed();
        let leg = f.volumes["w"].legs[1].clone();
        f.volumes.get_mut("w").unwrap().replacing =
            Some(Replacement { from: "a".into(), leg, reason: "rebalance".into(), started_at: now });
        let p = plan(&f, &[pool(Some(0.8), Some(0.4))], &rungs(), 0.3, &|_| false, now);
        assert!(p.moves.is_empty() && p.held[0].why.contains("in flight"), "{p:?}");
        // Busy or cooling down: skipped; w (smaller) goes instead.
        let p = plan(&fed(), &[pool(Some(0.8), Some(0.4))], &rungs(), 0.3, &|n| n == "v", now);
        assert_eq!(p.moves[0].volume, "w");
        let mut f = fed();
        f.volumes.get_mut("v").unwrap().next_releg_after = Some(now + Duration::from_secs(60));
        assert_eq!(plan(&f, &[pool(Some(0.8), Some(0.4))], &rungs(), 0.3, &|_| false, now).moves[0].volume, "w");
    }

    #[test]
    fn a_target_never_crosses_high_and_max_moves_caps() {
        let now = SystemTime::now();
        // c has 90 free of 100; v (20) + w (5) both fit under high 0.8? c
        // used 10 → 30 → 35: yes, and with max_moves 2 both go there.
        let mut p2 = pool(Some(0.8), Some(0.4));
        p2.max_moves = 2;
        let p = plan(&fed(), &[p2.clone()], &rungs(), 0.3, &|_| false, now);
        assert_eq!(p.moves.iter().map(|m| m.volume.as_str()).collect::<Vec<_>>(), vec!["v", "w"]);
        // A big volume that would push c over high stays put.
        let mut f = fed();
        f.volumes.get_mut("v").unwrap().size_bytes = 75;
        let p = plan(&f, &[p2], &rungs(), 0.3, &|_| false, now);
        assert_eq!(p.moves.iter().map(|m| m.volume.as_str()).collect::<Vec<_>>(), vec!["w"], "{p:?}");
    }
}
