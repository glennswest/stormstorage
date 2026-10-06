//! The stormview components feed: nodes, pools, and distributed volumes
//! as `ComponentSummary` entries with relations (pool has_many volumes and
//! nodes; volume belongs_to pool, legs target node components) and real
//! actions — so stormd, stormsh, and stormconsole render and *drive* the
//! federation with no per-UI code.

use crate::api::AppState;
use crate::inventory::{NodeInventory, PlacedBy, PlacedVolume, Slab, TierRollup};
use crate::model::{AssemblyState, DistVolume, ExportState, LegState, Node};
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
            slab.drive
                .as_ref()
                .map(|d| format!("drive {}", d.describe()))
                .unwrap_or_else(|| slab.domain.clone())
        ),
        metrics: vec![
            Metric::new("free", stormview::format_bytes(slab.free_bytes)),
            Metric::new("total", stormview::format_bytes(slab.total_bytes)).tone("muted"),
            Metric::new("used", stormview::format_bytes(slab.allocated_bytes())),
            Metric::new("vols", vols.len().to_string()).tone("accent"),
            Metric::new("tier", slab.tier.clone()),
            Metric::new("role", slab.role.clone()),
            Metric::new(
                "drive",
                slab.drive.as_ref().map(|d| d.describe()).unwrap_or_else(|| "?".into()),
            )
            .tone("muted"),
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
    let pl = ev.placement.as_ref();
    let mut health = match ev.health.as_str() {
        "healthy" => Health::Ok,
        "degraded" => Health::Warn,
        "failed" => Health::Error,
        _ => Health::Unknown,
    };
    // A failed slab or a partner that is not active is at least a warning,
    // whatever the volume's own health says.
    if matches!(health, Health::Ok | Health::Unknown)
        && pl.is_some_and(|p| p.bad_slabs() > 0 || p.partners_down() > 0 || (p.rebuild != "none" && !p.rebuild.is_empty()))
    {
        health = Health::Warn;
    }
    let consumer = ev
        .consumer()
        .map(|o| o.describe())
        .unwrap_or_else(|| if ev.sealed { "sealed".into() } else { "unowned".into() });
    let drives: Vec<String> = pl
        .map(|p| p.drives.iter().map(|d| format!("{}@{}", d.drive.describe(), d.node)).collect())
        .unwrap_or_default();
    let partners: Vec<String> = pl
        .map(|p| {
            p.arrays
                .iter()
                .flat_map(|a| &a.members)
                .map(|m| format!("{}@{} {}", m.drive.describe(), m.node, m.state))
                .collect()
        })
        .unwrap_or_default();
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
    if let Some(k) = &ev.kind {
        metrics.push(Metric::new("kind", k.clone()).tone("muted"));
    }
    if let Some(u) = ev.in_use {
        let transports: Vec<&str> = ev.attachments.iter().map(|a| a.transport.as_str()).collect();
        metrics.push(if u {
            Metric::new("in use", transports.join("+")).tone("accent")
        } else {
            Metric::new("in use", "no").tone("muted")
        });
    }
    if !drives.is_empty() {
        metrics.push(Metric::new("drives", drives.join(" ")));
    }
    if !partners.is_empty() {
        let down = pl.map(|p| p.partners_down()).unwrap_or(0);
        metrics.push(Metric::new("partners", partners.join(" ")).tone(if down > 0 { "warn" } else { "ok" }));
    }
    if let Some(p) = pl.filter(|p| !p.rebuild.is_empty() && p.rebuild != "none") {
        metrics.push(Metric::new("rebuild", p.rebuild.clone()).tone("warn"));
    }
    if let Some(pv) = &v.pv {
        let tone = if pv.complete() { "ok" } else { "warn" };
        metrics.push(Metric::new("pv", format!("{} {}", pv.pv, pv.phase)).tone(tone));
        let claim = match &pv.claim {
            Some(c) if c.bound => pv.describe(),
            Some(c) if c.phase.is_none() => format!("{} (missing)", pv.describe()),
            Some(_) => format!("{} (not bound)", pv.describe()),
            None => pv.describe(),
        };
        metrics.push(Metric::new("claim", claim).tone(tone));
        if let Some(k) = &pv.volume_kind {
            let what = match &pv.component {
                Some(c) => format!("{k} of {c}"),
                None => k.clone(),
            };
            metrics.push(Metric::new("holds", what).tone("muted"));
        }
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

fn volume_component(v: &DistVolume, reading: Option<&crate::head::ArrayReading>) -> ComponentSummary {
    let failed_legs = v.legs.iter().filter(|l| l.state == LegState::Failed).count();
    let lost_legs: Vec<&str> = v
        .legs
        .iter()
        .filter(|l| l.state == LegState::Lost)
        .map(|l| l.node.as_str())
        .collect();
    let head_lost = v.head.as_deref().map(|h| lost_legs.contains(&h)).unwrap_or(false);
    let health = if failed_legs > 0 || head_lost {
        Health::Error
    } else if !lost_legs.is_empty() || v.replacing.is_some() {
        Health::Warn
    } else {
        Health::Ok
    };
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
        AssemblyState::Degraded => {
            metrics.push(Metric::new("assembly", "degraded").tone("warn"));
        }
        AssemblyState::SingleLeg => {}
    }
    match v.export.state {
        ExportState::Published => {
            let tone = if v.export.coordinates_changed { "warn" } else { "ok" };
            metrics.push(Metric::new("export", "published").tone(tone));
        }
        ExportState::Failed => metrics.push(Metric::new("export", "failed").tone("error")),
        ExportState::None => {}
    }
    // Consumer hosts it is served to (#51).
    if v.export.per_host {
        let failed = v.export.hosts.iter().filter(|h| h.message.is_some()).count();
        let tone = if failed > 0 { "error" } else { "ok" };
        metrics.push(Metric::new("hosts", format!("{}", v.export.hosts.len())).tone(tone));
    }
    if let Some(r) = &v.replacing {
        metrics.push(Metric::new("rebuilding", format!("{} → {}", r.from, r.leg.node)).tone("warn"));
    }
    // Copies in sync, as read from the head's array (#33).
    if v.legs.len() > 1 {
        let reps = crate::head::replicas(v, reading);
        let in_sync = reps.iter().filter(|r| r.sync == crate::head::SyncState::InSync).count();
        let resync = reps.iter().find_map(|r| match r.sync {
            crate::head::SyncState::Resyncing { progress_pct, .. } => Some((r.node.clone(), progress_pct)),
            _ => None,
        });
        let tone = if in_sync >= v.replicas as usize { "ok" } else { "warn" };
        metrics.push(Metric::new("in sync", format!("{in_sync}/{}", reps.len())).tone(tone));
        // The head did not answer: sync from the legs' superblocks (#48).
        if reading.is_some_and(|r| r.source == crate::head::SyncSource::Superblock) {
            metrics.push(Metric::new("sync from", "leg superblocks").tone("warn"));
        }
        if let Some((node, pct)) = resync {
            metrics.push(Metric::new("resync", format!("{node} {pct:.1}%")).tone("warn"));
        }
    }
    if v.epoch > 1 || v.fenced {
        metrics.push(
            Metric::new("epoch", format!("{}{}", v.epoch, if v.fenced { " fenced" } else { "" }))
                .tone(if v.fenced { "warn" } else { "accent" }),
        );
    }
    let mut relations = vec![Relation::has_many(
        "legs",
        v.legs
            .iter()
            .chain(v.replacing.as_ref().map(|r| &r.leg))
            .map(|l| format!("node:{}", l.node))
            .collect(),
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
            "{} · {} leg(s) at rung {:?}{}{}{}{}",
            stormview::format_bytes(v.size_bytes),
            v.legs.len(),
            v.rung,
            v.pool
                .as_ref()
                .map(|p| format!(" · pool {p}"))
                .unwrap_or_default(),
            if lost_legs.is_empty() {
                String::new()
            } else if head_lost {
                format!(" · head {} lost (promote a surviving leg's node)", lost_legs.join(", "))
            } else {
                format!(" · lost: {}", lost_legs.join(", "))
            },
            v.replacing
                .as_ref()
                .map(|r| format!(" · re-leg {} → {} ({})", r.from, r.leg.node, r.reason))
                .unwrap_or_default(),
            match (&v.export.state, &v.export.coordinates) {
                (ExportState::Published, Some(c)) => format!(" · served at {}", c.drive_uri()),
                (ExportState::Failed, _) => format!(
                    " · export failed: {}",
                    v.export.message.as_deref().unwrap_or("")
                ),
                _ => String::new(),
            }
        ),
        metrics,
        actions: vec![
            Action {
                id: "export".into(),
                label: if v.export.state == ExportState::Published { "Republish" } else { "Publish" }.into(),
                method: "POST".into(),
                path: format!("/api/v1/volumes/{}/export", v.name),
                enabled: v.assembly != AssemblyState::PendingEngineSupport,
                danger: false,
            },
            Action {
                id: "assemble".into(),
                label: "Assemble".into(),
                method: "POST".into(),
                path: format!("/api/v1/volumes/{}/assemble", v.name),
                enabled: v.assembly == AssemblyState::PendingEngineSupport && v.legs.len() >= 2,
                danger: false,
            },
            Action {
            id: "delete".into(),
            label: "Delete".into(),
            method: "DELETE".into(),
            path: format!("/api/v1/volumes/{}", v.name),
            enabled: true,
            danger: true,
        },
        ],
        relations,
        link: None,
    }
}

pub async fn collect(state: &Arc<AppState>) -> Vec<ComponentSummary> {
    let fed = state.fed.read().await;
    let inventory = state.inventory.read().await;
    let heads = state.heads.read().await;
    build(state, &fed, &inventory, &heads)
}

fn build(
    state: &AppState,
    fed: &crate::model::FedState,
    inventory: &BTreeMap<String, NodeInventory>,
    heads: &BTreeMap<String, crate::head::ArrayReading>,
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
            "{healthy}/{total_nodes} nodes · {pools} pools · {volumes} volumes · {} free{} · rev {}",
            stormview::format_bytes(free),
            if fed.orphans.is_empty() {
                String::new()
            } else {
                format!(" · {} orphaned leg(s) to reap", fed.orphans.len())
            },
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
        out.push(volume_component(v, heads.get(&v.name)));
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
                    epoch: None,
                })
                .collect(),
            assembly: AssemblyState::PendingEngineSupport,
            head: None,
            array_id: None,
            created_at: SystemTime::now(),
            replacing: None,
            next_releg_after: None,
            next_assemble_after: None,
            export: Default::default(),
            epoch: 1,
            fenced: false,
            bandwidth_class: Default::default(),
            dual_attach: None,
        };
        let c = volume_component(&v, None);
        assert_eq!(c.health, Health::Ok);
        assert!(c.relations.iter().any(|r| r.name == "pool" && r.targets == vec!["pool:fast".to_string()]));
        let legs = c.relations.iter().find(|r| r.name == "legs").unwrap();
        assert_eq!(legs.targets, vec!["node:a".to_string(), "node:b".to_string()]);
        let del = c.actions.iter().find(|a| a.id == "delete").unwrap();
        assert_eq!(del.method, "DELETE");
        assert!(del.danger);
        assert!(c.metrics.iter().any(|m| m.label == "assembly"));
    }

    #[test]
    fn node_volume_shows_consumer_drives_and_partners() {
        let ev: crate::inventory::EngineVolume = serde_json::from_value(serde_json::json!({
            "id": "v1", "name": "db", "health": "healthy", "kind": "volume", "in_use": true,
            "attachments": [{"transport": "nvme-tcp", "nsid": 3}],
            "consumer": {"kind": "PersistentVolumeClaim", "namespace": "shop", "name": "db"},
            "placement": {
                "slabs": [{"id": "d1", "drive": {"serial": "WD1"}, "node": "n1", "state": "ok"}],
                "drives": [{"drive": {"serial": "WD1"}, "node": "n1"}],
                "rebuild": "none",
                "arrays": [{"id": "a", "level": "raid1", "members": [
                    {"index": 0, "state": "active", "drive": {"serial": "WD1"}, "node": "n1"},
                    {"index": 1, "state": "rebuilding", "drive": {"serial": "S3"}, "node": "n2"}]}]
            }
        }))
        .unwrap();
        let p = crate::inventory::place(&[], vec![ev], &Default::default());
        let c = node_volume_component("n1", &p[0]);
        let m = |l: &str| c.metrics.iter().find(|m| m.label == l).map(|m| m.value.clone());
        assert_eq!(m("kind").as_deref(), Some("volume"));
        assert_eq!(m("in use").as_deref(), Some("nvme-tcp"));
        assert_eq!(m("drives").as_deref(), Some("WD1@n1"));
        assert_eq!(m("partners").as_deref(), Some("WD1@n1 active S3@n2 rebuilding"));
        assert!(c.detail.contains("PersistentVolumeClaim shop/db"), "{}", c.detail);
        assert!(c.detail.contains("on d1"), "{}", c.detail);
        assert!(matches!(c.health, Health::Warn), "a partner rebuilding is a warning");
        assert!(c.relations.iter().any(|r| r.name == "pools" && r.targets == vec!["pool:n1/d1".to_string()]));
    }
    #[test]
    fn node_volume_shows_its_pv_and_claim() {
        let ev: crate::inventory::EngineVolume = serde_json::from_value(serde_json::json!({
            "id": "v1", "name": "fastetcd-data", "health": "healthy"
        }))
        .unwrap();
        let mut p = crate::inventory::place(&[], vec![ev], &Default::default());
        let m = |c: &ComponentSummary, l: &str| c.metrics.iter().find(|m| m.label == l).map(|m| (m.value.clone(), m.tone.clone()));
        assert!(m(&node_volume_component("n1", &p[0]), "pv").is_none(), "no PV known: no metric");
        let mut pv = crate::kube::VolumeClaim {
            pv: "storm-fastetcd-data-n1".into(),
            phase: "Bound".into(),
            reclaim: "Retain".into(),
            capacity: "1Gi".into(),
            volume_kind: Some("data".into()),
            component: Some("fastetcd".into()),
            claim: Some(crate::kube::ClaimRef {
                namespace: "kube-system".into(),
                name: "fastetcd-data-n1".into(),
                uid: Some("u1".into()),
                phase: Some("Bound".into()),
                bound: true,
            }),
        };
        p[0].pv = Some(pv.clone());
        let c = node_volume_component("n1", &p[0]);
        let (v, tone) = m(&c, "pv").unwrap();
        assert_eq!(v, "storm-fastetcd-data-n1 Bound");
        assert_eq!(tone.as_deref(), Some("ok"));
        assert_eq!(m(&c, "claim").unwrap().0, "kube-system/fastetcd-data-n1");
        assert_eq!(m(&c, "holds").unwrap().0, "data of fastetcd");
        // The claim deleted: the PV is still there, the pair is not complete.
        pv.claim.as_mut().unwrap().phase = None;
        pv.claim.as_mut().unwrap().bound = false;
        p[0].pv = Some(pv);
        let c = node_volume_component("n1", &p[0]);
        let (v, tone) = m(&c, "claim").unwrap();
        assert_eq!(v, "kube-system/fastetcd-data-n1 (missing)");
        assert_eq!(tone.as_deref(), Some("warn"));
    }
}
