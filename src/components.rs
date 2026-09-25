//! The stormview components feed: nodes, pools, and distributed volumes
//! as `ComponentSummary` entries with relations (pool has_many volumes and
//! nodes; volume belongs_to pool, legs target node components) and real
//! actions — so stormd, stormsh, and stormconsole render and *drive* the
//! federation with no per-UI code.

use crate::api::AppState;
use crate::inventory::{NodeInventory, PlacedBy, PlacedVolume, Slab, TierRollup};
use crate::model::{AssemblyState, DistVolume, LegState, Node};
use std::collections::BTreeMap;
use std::sync::Arc;
use stormview::{Action, ComponentSummary, Health, Metric, Relation};

fn slab_pool_id(node: &str, slab: &str) -> String {
    format!("pool:{node}/{slab}")
}

fn node_volume_id(node: &str, vol: &str) -> String {
    format!("nvol:{node}/{vol}")
}

fn short(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

/// A node's slab as a pool: its tier, role and failure domain, what is
/// free, and the volumes on it (#9).
fn slab_component(node: &Node, inv: &NodeInventory, slab: &Slab) -> ComponentSummary {
    let name = &node.config.name;
    let vols: Vec<String> = crate::inventory::volumes_on(inv, &slab.id)
        .iter()
        .map(|v| node_volume_id(name, &v.volume.id))
        .collect();
    let free_ratio = if slab.total_bytes == 0 {
        0.0
    } else {
        slab.free_bytes as f64 / slab.total_bytes as f64
    };
    let health = if !node.status.healthy {
        Health::Error
    } else if free_ratio < 0.10 {
        Health::Warn
    } else {
        Health::Ok
    };
    ComponentSummary {
        id: slab_pool_id(name, &slab.id),
        kind: "pool".into(),
        label: format!("{name} {} {} ({})", slab.role, slab.tier, short(&slab.id)),
        health,
        detail: format!(
            "{} of {} free · {} used · {} volumes · tier {} · role {} · {}",
            stormview::format_bytes(slab.free_bytes),
            stormview::format_bytes(slab.total_bytes),
            stormview::format_bytes(slab.allocated_bytes()),
            vols.len(),
            slab.tier,
            slab.role,
            slab.domain
        ),
        metrics: vec![
            Metric::new("free", stormview::format_bytes(slab.free_bytes)),
            Metric::new("total", stormview::format_bytes(slab.total_bytes)).tone("muted"),
            Metric::new("used", stormview::format_bytes(slab.allocated_bytes())),
            Metric::new("vols", vols.len().to_string()).tone("accent"),
            Metric::new("tier", slab.tier.clone()),
            Metric::new("role", slab.role.clone()),
        ],
        actions: Vec::new(),
        relations: vec![
            Relation::belongs_to("node", format!("node:{name}")),
            Relation::belongs_to("tier", format!("tier:{}", slab.tier)),
            Relation::has_many("volumes", vols),
        ],
        link: None,
    }
}

fn tier_component(t: &TierRollup) -> ComponentSummary {
    ComponentSummary {
        id: format!("tier:{}", t.tier),
        kind: "tier".into(),
        label: format!("tier {}", t.tier),
        health: Health::Ok,
        detail: format!(
            "{} of {} free · {} slab(s) on {} node(s) · {} volumes",
            stormview::format_bytes(t.free_bytes),
            stormview::format_bytes(t.total_bytes),
            t.slabs.len(),
            t.nodes.len(),
            t.volumes
        ),
        metrics: vec![
            Metric::new("free", stormview::format_bytes(t.free_bytes)),
            Metric::new("total", stormview::format_bytes(t.total_bytes)).tone("muted"),
            Metric::new("used", stormview::format_bytes(t.allocated_bytes)),
            Metric::new("vols", t.volumes.to_string()).tone("accent"),
        ],
        actions: Vec::new(),
        relations: vec![Relation::has_many(
            "pools",
            t.slabs
                .iter()
                .map(|s| format!("pool:{s}"))
                .collect(),
        )],
        link: None,
    }
}

/// One volume on a node's engine, in its pool(s), with what owns it.
fn node_volume_component(node: &str, v: &PlacedVolume) -> ComponentSummary {
    let ev = &v.volume;
    let health = match ev.health.as_str() {
        "healthy" => Health::Ok,
        "degraded" => Health::Warn,
        "failed" => Health::Error,
        _ => Health::Unknown,
    };
    let consumer = ev
        .owner
        .as_ref()
        .map(|o| o.describe())
        .unwrap_or_else(|| if ev.sealed { "sealed".into() } else { "unowned".into() });
    let placed = match v.placed_by {
        PlacedBy::Unknown => "slab unknown".to_string(),
        _ => format!(
            "on {}",
            v.slabs.iter().map(|s| short(s)).collect::<Vec<_>>().join("+")
        ),
    };
    let mut metrics = vec![
        Metric::new("size", stormview::format_bytes(ev.virtual_size_bytes)),
        Metric::new("used", stormview::format_bytes(ev.allocated_bytes)),
        Metric::new("role", ev.role.clone()),
    ];
    if ev.sealed {
        metrics.push(Metric::new("sealed", "yes").tone("muted"));
    }
    let mut relations = vec![Relation::belongs_to("node", format!("node:{node}"))];
    if !v.slabs.is_empty() {
        relations.push(Relation::has_many(
            "pools",
            v.slabs.iter().map(|s| slab_pool_id(node, s)).collect(),
        ));
    }
    ComponentSummary {
        id: node_volume_id(node, &ev.id),
        kind: "volume".into(),
        label: ev.name.clone(),
        health,
        detail: format!(
            "{} · {} · {} · {placed} · {consumer}",
            stormview::format_bytes(ev.virtual_size_bytes),
            node,
            if ev.redundancy.is_empty() { "none" } else { ev.redundancy.as_str() },
        ),
        metrics,
        actions: Vec::new(),
        relations,
        link: None,
    }
}

fn node_component(n: &Node, inv: Option<&NodeInventory>) -> ComponentSummary {
    let health = if n.status.healthy { Health::Ok } else { Health::Error };
    let mut detail = vec![n.config.engine_url.clone()];
    if let Some(t) = &n.config.tier {
        detail.push(format!("tier {t}"));
    }
    detail.push(if n.status.healthy {
        "reachable".into()
    } else {
        "unreachable".into()
    });
    ComponentSummary {
        id: format!("node:{}", n.config.name),
        kind: "node".into(),
        label: n.config.name.clone(),
        health,
        detail: detail.join(" · "),
        metrics: vec![
            Metric::new("free", stormview::format_bytes(n.status.free_bytes)),
            Metric::new("total", stormview::format_bytes(n.status.total_bytes)).tone("muted"),
            Metric::new("vols", n.status.volumes.to_string()),
        ],
        actions: Vec::new(),
        relations: vec![
            Relation::belongs_to("system", "system"),
            Relation::has_many(
                "pools",
                inv.map(|i| {
                    i.slabs
                        .iter()
                        .map(|s| slab_pool_id(&n.config.name, &s.id))
                        .collect()
                })
                .unwrap_or_default(),
            ),
        ],
        link: None,
    }
}

fn volume_component(v: &DistVolume) -> ComponentSummary {
    let failed_legs = v.legs.iter().filter(|l| l.state == LegState::Failed).count();
    let health = if failed_legs > 0 { Health::Error } else { Health::Ok };
    let mut metrics = vec![
        Metric::new("size", stormview::format_bytes(v.size_bytes)),
        Metric::new("legs", v.legs.len().to_string()).tone("accent"),
    ];
    match v.assembly {
        AssemblyState::Assembled => {
            metrics.push(Metric::new("assembly", "raid1").tone("ok"));
        }
        AssemblyState::PendingEngineSupport => {
            metrics.push(Metric::new("assembly", "pending").tone("warn"));
        }
        AssemblyState::SingleLeg => {}
    }
    let mut relations = vec![Relation::has_many(
        "legs",
        v.legs.iter().map(|l| format!("node:{}", l.node)).collect(),
    )];
    if let Some(p) = &v.pool {
        relations.push(Relation::belongs_to("pool", format!("pool:{p}")));
    }
    ComponentSummary {
        id: format!("volume:{}", v.name),
        kind: "volume".into(),
        label: v.name.clone(),
        health,
        detail: format!(
            "{} · {} leg(s) at rung {:?}{}",
            stormview::format_bytes(v.size_bytes),
            v.legs.len(),
            v.rung,
            v.pool
                .as_ref()
                .map(|p| format!(" · pool {p}"))
                .unwrap_or_default()
        ),
        metrics,
        actions: vec![Action {
            id: "delete".into(),
            label: "Delete".into(),
            method: "DELETE".into(),
            path: format!("/api/v1/volumes/{}", v.name),
            enabled: true,
            danger: true,
        }],
        relations,
        link: None,
    }
}

pub async fn collect(state: &Arc<AppState>) -> Vec<ComponentSummary> {
    let fed = state.fed.read().await;
    let inventory = state.inventory.read().await;
    build(state, &fed, &inventory)
}

fn build(
    state: &AppState,
    fed: &crate::model::FedState,
    inventory: &BTreeMap<String, NodeInventory>,
) -> Vec<ComponentSummary> {
    let mut out = Vec::new();
    let tiers = crate::inventory::tiers(inventory);
    let slab_pools: Vec<String> = inventory
        .iter()
        .flat_map(|(n, i)| i.slabs.iter().map(move |s| slab_pool_id(n, &s.id)))
        .collect();
    let node_volumes: usize = inventory.values().map(|i| i.volumes.len()).sum();

    let total_nodes = fed.nodes.len();
    let healthy = fed.nodes.values().filter(|n| n.status.healthy).count();
    let volumes = fed.volumes.len() + node_volumes;
    let pools = state.config.pools.len() + slab_pools.len();
    let free: u64 = fed
        .nodes
        .values()
        .filter(|n| n.status.healthy)
        .map(|n| n.status.free_bytes)
        .sum();
    let system_health = if total_nodes == 0 {
        Health::Idle
    } else if healthy == 0 {
        Health::Error
    } else if healthy < total_nodes {
        Health::Warn
    } else {
        Health::Ok
    };
    out.push(ComponentSummary {
        id: "system".into(),
        kind: "storage".into(),
        label: "stormstorage".into(),
        health: system_health,
        detail: format!(
            "{healthy}/{total_nodes} nodes · {pools} pools · {volumes} volumes · {} free · rev {}",
            stormview::format_bytes(free),
            fed.revision
        ),
        metrics: vec![
            Metric::new("nodes", format!("{healthy}/{total_nodes}")),
            Metric::new("pools", pools.to_string()),
            Metric::new("volumes", volumes.to_string()).tone("accent"),
            Metric::new("free", stormview::format_bytes(free)),
        ],
        actions: Vec::new(),
        relations: vec![
            Relation::has_many(
                "nodes",
                fed.nodes.keys().map(|n| format!("node:{n}")).collect(),
            ),
            Relation::has_many(
                "pools",
                state
                    .config
                    .pools
                    .iter()
                    .map(|p| format!("pool:{}", p.name))
                    .chain(slab_pools.iter().cloned())
                    .collect(),
            ),
            Relation::has_many(
                "tiers",
                tiers.iter().map(|t| format!("tier:{}", t.tier)).collect(),
            ),
        ],
        link: None,
    });

    for pool in &state.config.pools {
        let members: Vec<&Node> = fed
            .nodes
            .values()
            .filter(|n| pool.selector.matches(&n.config))
            .collect();
        let healthy_members = members.iter().filter(|n| n.status.healthy).count();
        let free: u64 = members
            .iter()
            .filter(|n| n.status.healthy)
            .map(|n| n.status.free_bytes)
            .sum();
        let pool_volumes: Vec<String> = fed
            .volumes
            .values()
            .filter(|v| v.pool.as_deref() == Some(pool.name.as_str()))
            .map(|v| format!("volume:{}", v.name))
            .collect();
        let health = if members.is_empty() || healthy_members == 0 {
            Health::Error
        } else if healthy_members < members.len() {
            Health::Warn
        } else {
            Health::Ok
        };
        out.push(ComponentSummary {
            id: format!("pool:{}", pool.name),
            kind: "pool".into(),
            label: pool.name.clone(),
            health,
            detail: format!(
                "{healthy_members}/{} nodes · replicas {} at rung {:?} · {} free",
                members.len(),
                pool.replicas,
                pool.rung,
                stormview::format_bytes(free)
            ),
            metrics: vec![
                Metric::new("nodes", format!("{healthy_members}/{}", members.len())),
                Metric::new("volumes", pool_volumes.len().to_string()).tone("accent"),
                Metric::new("free", stormview::format_bytes(free)),
            ],
            actions: Vec::new(),
            relations: vec![
                Relation::has_many(
                    "nodes",
                    members
                        .iter()
                        .map(|n| format!("node:{}", n.config.name))
                        .collect(),
                ),
                Relation::has_many("volumes", pool_volumes),
            ],
            link: None,
        });
    }

    for t in &tiers {
        out.push(tier_component(t));
    }
    for n in fed.nodes.values() {
        let inv = inventory.get(&n.config.name);
        out.push(node_component(n, inv));
        if let Some(inv) = inv {
            for slab in &inv.slabs {
                out.push(slab_component(n, inv, slab));
            }
        }
    }
    for v in fed.volumes.values() {
        out.push(volume_component(v));
    }
    for (node, inv) in inventory {
        for v in &inv.volumes {
            out.push(node_volume_component(node, v));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Leg;
    use std::time::SystemTime;

    #[test]
    fn volume_component_relations_and_delete_action() {
        let v = DistVolume {
            name: "v1".into(),
            size_bytes: 1 << 30,
            pool: Some("fast".into()),
            replicas: 2,
            rung: "cluster".into(),
            legs: ["a", "b"]
                .iter()
                .map(|n| Leg {
                    node: n.to_string(),
                    volume_id: Some("x".into()),
                    state: LegState::Created,
                    message: None,
                    master_node: None,
                    export: None,
                    drive_uuid: None,
                    member_uuid: None,
                })
                .collect(),
            assembly: AssemblyState::PendingEngineSupport,
            head: None,
            array_id: None,
            created_at: SystemTime::now(),
        };
        let c = volume_component(&v);
        assert_eq!(c.health, Health::Ok);
        assert!(c.relations.iter().any(|r| r.name == "pool" && r.targets == vec!["pool:fast".to_string()]));
        let legs = c.relations.iter().find(|r| r.name == "legs").unwrap();
        assert_eq!(legs.targets, vec!["node:a".to_string(), "node:b".to_string()]);
        let del = &c.actions[0];
        assert_eq!(del.method, "DELETE");
        assert!(del.danger);
        assert!(c.metrics.iter().any(|m| m.label == "assembly"));
    }
}
