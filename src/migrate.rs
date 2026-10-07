//! Tier migration (#32): move every leg of a volume into another pool, one
//! leg at a time, through the leg-move sequence (new leg in the
//! destination → rebuild → old leg retired), so the volume never has fewer
//! copies. The volume's `pool` becomes the destination once every leg is
//! there.
//!
//! The head's own leg moves by promote only (as with prestage), and a
//! handover from a head that is alive needs the engine to release an array
//! without writing to its members (stormblock#296). Until then a migration
//! ends `waiting_handover` with every other leg moved.

use crate::api::AppState;
use crate::config::PoolConfig;
use crate::events::Severity;
use crate::model::{AssemblyState, DistVolume, FedState, LegState, Migration, MigrationState};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// What a migration does next. Pure.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// Move the leg on `from` to `to`.
    Move { from: String, to: String },
    /// Every leg is in the destination.
    Done,
    /// Only the head's own leg is outside the destination (stormblock#296).
    WaitingHandover,
    /// Nothing can move now, and why.
    Blocked(String),
}

fn in_pool(fed: &FedState, pool: &PoolConfig, node: &str) -> bool {
    fed.nodes.get(node).is_some_and(|n| pool.selector.matches(&n.config))
}

/// The next step of `vol`'s migration into `pool`: the first leg outside
/// the pool that is not the head's, to the best node of the pool in a
/// domain (at the pool's rung) distinct from the legs that stay.
pub fn next_step(fed: &FedState, pool: &PoolConfig, rungs: &[String], io_weight: f64, vol: &DistVolume) -> Step {
    if let Some(l) = vol.legs.iter().find(|l| l.state != LegState::Created) {
        return Step::Blocked(format!("leg on {} is {:?} — recovery comes first", l.node, l.state));
    }
    let outside: Vec<&str> = vol
        .legs
        .iter()
        .map(|l| l.node.as_str())
        .filter(|n| !in_pool(fed, pool, n))
        .collect();
    if outside.is_empty() {
        return Step::Done;
    }
    let head = vol.head.as_deref();
    let Some(from) = outside.iter().copied().filter(|n| Some(*n) != head).min() else {
        return Step::WaitingHandover;
    };
    // Domains as the destination pool spreads them.
    let at_rung = DistVolume { rung: pool.rung.clone(), ..vol.clone() };
    let cands: Vec<crate::placement::Candidate> = crate::orchestrate::move_target_candidates(fed, rungs, &at_rung, from)
        .into_iter()
        .filter(|c| in_pool(fed, pool, &c.name))
        .collect();
    match crate::placement::plan(&cands, rungs, &pool.rung, 1, vol.size_bytes, io_weight) {
        Ok(mut p) => Step::Move { from: from.to_string(), to: p.remove(0) },
        Err(e) => Step::Blocked(format!("no target in pool {} for the leg on {from}: {e}", pool.name)),
    }
}

/// Why a migration cannot start.
#[derive(Debug)]
pub enum Refusal {
    NotFound(String),
    Conflict(String),
}

/// `POST /api/v1/volumes/{name}/migrate {pool}`: record the migration. The
/// reconciler moves the legs. The destination must have room for every leg
/// in distinct domains at its rung now, or it is refused.
pub async fn start(state: &Arc<AppState>, name: &str, to: &str) -> Result<Migration, Refusal> {
    let pool = state
        .config
        .pools
        .iter()
        .find(|p| p.name == to)
        .cloned()
        .ok_or_else(|| Refusal::NotFound(format!("pool {to:?}")))?;
    let mig = {
        let mut fed = state.fed.write().await;
        let fed_ro: &FedState = &fed;
        let vol = fed_ro
            .volumes
            .get(name)
            .cloned()
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        if let Some(m) = &vol.migration {
            if m.to_pool == to {
                return Ok(m.clone());
            }
            return Err(Refusal::Conflict(format!("{name}: already migrating to pool {}", m.to_pool)));
        }
        match vol.assembly {
            AssemblyState::Assembled => {}
            AssemblyState::SingleLeg => {
                return Err(Refusal::Conflict(format!("{name}: a single leg — a move needs a mirror to rebuild from")))
            }
            other => return Err(Refusal::Conflict(format!("{name}: {other:?} — recover or assemble it first"))),
        }
        if vol.fenced || vol.replacing.is_some() || vol.dual_attach.is_some() {
            return Err(Refusal::Conflict(format!("{name}: fenced, replacing a leg or in a dual-attach window")));
        }
        if vol.legs.iter().all(|l| in_pool(fed_ro, &pool, &l.node)) {
            return Err(Refusal::Conflict(format!("{name}: every leg is already in pool {to}")));
        }
        // Room for the whole volume in the destination, at its rung.
        let cands: Vec<crate::placement::Candidate> = fed_ro
            .nodes
            .values()
            .filter(|n| n.status.healthy && pool.selector.matches(&n.config))
            .map(|n| crate::placement::Candidate {
                name: n.config.name.clone(),
                labels: n.labels(),
                free_bytes: n.status.free_bytes,
                total_bytes: n.status.total_bytes,
                io_busy: n.status.io.map(|i| i.busy),
            })
            .collect();
        crate::placement::plan(
            &cands,
            &state.config.federation.rungs,
            &pool.rung,
            vol.legs.len() as u32,
            vol.size_bytes,
            state.config.placement.io_weight,
        )
        .map_err(|e| Refusal::Conflict(format!("{name}: pool {to} cannot hold its {} legs: {e}", vol.legs.len())))?;
        let m = Migration {
            to_pool: to.to_string(),
            from_pool: vol.pool.clone(),
            started_at: SystemTime::now(),
            state: MigrationState::Moving,
            message: None,
        };
        let v = fed.volumes.get_mut(name).expect("read above");
        v.migration = Some(m.clone());
        fed.revision += 1;
        m
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    event(
        state,
        name,
        Severity::Info,
        format!(
            "{name}: tier migration to pool {to} started (from {})",
            mig.from_pool.as_deref().unwrap_or("no pool")
        ),
    )
    .await;
    Ok(mig)
}

/// `DELETE /api/v1/volumes/{name}/migrate`: stop. Legs already moved stay
/// where they are; a move in flight finishes.
pub async fn cancel(state: &Arc<AppState>, name: &str) -> Result<bool, Refusal> {
    let had = {
        let mut fed = state.fed.write().await;
        let v = fed
            .volumes
            .get_mut(name)
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        let had = v.migration.take().is_some();
        if had {
            fed.revision += 1;
        }
        had
    };
    if had {
        crate::replicate::push_to_peers(state.clone());
        state.persist().await;
        event(state, name, Severity::Info, format!("{name}: tier migration cancelled; legs already moved stay")).await;
    }
    Ok(had)
}

async fn event(state: &Arc<AppState>, name: &str, sev: Severity, msg: String) {
    state.events.write().await.push(Some(name.to_string()), sev, "migrate", msg);
}

/// One step for every migrating, idle volume (after each poll), on the
/// instance that acts on recovery.
pub async fn run(state: &Arc<AppState>, now: SystemTime) {
    if !state.config.recovery.active(!state.config.replication.peers.is_empty()) {
        return;
    }
    let due: Vec<(String, Step, MigrationState, Option<String>, String)> = {
        let fed = state.fed.read().await;
        let busy = state.replacing.lock().expect("replacing lock").clone();
        fed.volumes
            .values()
            .filter(|v| v.replacing.is_none() && !busy.contains(&v.name))
            .filter(|v| !v.fenced && v.dual_attach.is_none())
            .filter(|v| v.next_releg_after.map_or(true, |t| t <= now))
            .filter_map(|v| {
                let m = v.migration.as_ref()?;
                let step = match state.config.pools.iter().find(|p| p.name == m.to_pool) {
                    Some(pool) => next_step(&fed, pool, &state.config.federation.rungs, state.config.placement.io_weight, v),
                    None => Step::Blocked(format!("pool {} is no longer in the config", m.to_pool)),
                };
                Some((v.name.clone(), step, m.state, m.message.clone(), m.to_pool.clone()))
            })
            .collect()
    };
    for (name, step, was, msg, to) in due {
        match step {
            Step::Move { from, to: target } => {
                let r = crate::orchestrate::start_replacement(state, &name, &from, Some(target.clone()), "tier migration").await;
                let (sev, text) = match &r {
                    Ok(_) => (Severity::Info, format!("{name}: tier migration to {to}: moving the leg on {from} to {target}")),
                    Err(e) => (
                        Severity::Warning,
                        format!("{name}: tier migration to {to}: the move {from} → {target} did not start, retry in {}s: {e:#}", state.config.recovery.cooldown_secs),
                    ),
                };
                {
                    let mut fed = state.fed.write().await;
                    if let Some(v) = fed.volumes.get_mut(&name) {
                        if let Some(m) = v.migration.as_mut() {
                            m.state = MigrationState::Moving;
                            m.message = r.as_ref().err().map(|e| format!("{e:#}"));
                        }
                        if r.is_err() {
                            v.next_releg_after = Some(now + Duration::from_secs(state.config.recovery.cooldown_secs));
                        }
                    }
                    fed.revision += 1;
                }
                crate::replicate::push_to_peers(state.clone());
                state.persist().await;
                event(state, &name, sev, text).await;
            }
            Step::Done => {
                {
                    let mut fed = state.fed.write().await;
                    if let Some(v) = fed.volumes.get_mut(&name) {
                        v.pool = Some(to.clone());
                        if let Some(p) = state.config.pools.iter().find(|p| p.name == to) {
                            v.rung = p.rung.clone();
                        }
                        v.migration = None;
                    }
                    fed.revision += 1;
                }
                crate::replicate::push_to_peers(state.clone());
                state.persist().await;
                event(state, &name, Severity::Info, format!("{name}: tier migration done — every leg is in pool {to}")).await;
            }
            Step::WaitingHandover => {
                if was != MigrationState::WaitingHandover {
                    let text = format!(
                        "{name}: tier migration to {to}: every leg but the head's is in the pool; the head's own leg moves by \
                         promote only, and a handover from a live head waits on stormblock#{}",
                        crate::head::RELEASE_ISSUE
                    );
                    set(state, &name, MigrationState::WaitingHandover, Some(text.clone())).await;
                    event(state, &name, Severity::Warning, text).await;
                }
            }
            Step::Blocked(why) => {
                if msg.as_deref() != Some(why.as_str()) {
                    set(state, &name, MigrationState::Moving, Some(why.clone())).await;
                    event(state, &name, Severity::Warning, format!("{name}: tier migration to {to} held: {why}")).await;
                }
            }
        }
    }
}

async fn set(state: &Arc<AppState>, name: &str, st: MigrationState, message: Option<String>) {
    {
        let mut fed = state.fed.write().await;
        if let Some(m) = fed.volumes.get_mut(name).and_then(|v| v.migration.as_mut()) {
            m.state = st;
            m.message = message;
        }
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::model::{Leg, Node, NodeSource, NodeStatus};

    fn node(name: &str, free: u64) -> Node {
        let config: NodeConfig =
            toml::from_str(&format!("name = \"{name}\"\nengine_url = \"http://{name}:9090\"")).unwrap();
        let mut status = NodeStatus::new(NodeSource::Static);
        status.healthy = true;
        status.free_bytes = free;
        status.total_bytes = 100;
        Node { config, status }
    }

    fn vol(legs: &[&str]) -> DistVolume {
        DistVolume {
            name: "v".into(),
            size_bytes: 10,
            pool: Some("src".into()),
            replicas: legs.len() as u32,
            rung: "node".into(),
            legs: legs
                .iter()
                .map(|n| Leg {
                    node: n.to_string(),
                    volume_id: Some(format!("v-{n}")),
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
            dual_attach: None,
            migration: None,
        }
    }

    fn dst() -> PoolConfig {
        toml::from_str("name = \"dst\"\n[selector]\nnodes = [\"c\", \"d\"]").unwrap()
    }

    fn fed(legs: &[&str]) -> FedState {
        let mut fed = FedState::default();
        for (n, free) in [("a", 50), ("b", 50), ("c", 60), ("d", 90)] {
            fed.nodes.insert(n.into(), node(n, free));
        }
        fed.volumes.insert("v".into(), vol(legs));
        fed
    }

    fn step(legs: &[&str]) -> Step {
        let f = fed(legs);
        next_step(&f, &dst(), &["node".to_string()], 0.3, &f.volumes["v"])
    }

    #[test]
    fn moves_non_head_legs_into_the_pool_then_waits_for_the_head() {
        // a (head) and b outside: b moves first, to the emptiest of c/d.
        assert_eq!(step(&["a", "b"]), Step::Move { from: "b".into(), to: "d".into() });
        // b already moved to d: only the head's leg is outside.
        assert_eq!(step(&["a", "d"]), Step::WaitingHandover);
        // Every leg in the pool.
        assert_eq!(step(&["c", "d"]), Step::Done);
        // A leg on c stays: the moved leg goes to d, a distinct domain.
        assert_eq!(step(&["c", "b"]), Step::Move { from: "b".into(), to: "d".into() });
    }

    #[test]
    fn a_lost_leg_or_a_full_pool_blocks() {
        let mut f = fed(&["a", "b"]);
        f.volumes.get_mut("v").unwrap().legs[1].state = LegState::Lost;
        assert!(matches!(next_step(&f, &dst(), &["node".to_string()], 0.3, &f.volumes["v"]), Step::Blocked(w) if w.contains("recovery")));
        let mut f = fed(&["a", "b"]);
        f.nodes.get_mut("c").unwrap().status.free_bytes = 1;
        f.nodes.get_mut("d").unwrap().status.free_bytes = 1;
        assert!(matches!(next_step(&f, &dst(), &["node".to_string()], 0.3, &f.volumes["v"]), Step::Blocked(w) if w.contains("no target")));
    }
}
