//! Checks shared by the suites. Each returns an `Outcome`: `Ok(detail)`
//! passes, `Why::Skip` when the machine or the run lacks what it needs.

use serde_json::{json, Value};

use crate::api::Api;
use crate::env::Env;
use crate::report::{ensure, Outcome, Why};

pub const MIB: u64 = 1 << 20;
/// Small enough for any machine, big enough to be a real volume.
pub const SIZE: u64 = 64 * MIB;

/// The service answers and says its version.
pub async fn api_up(api: &Api) -> Outcome {
    let h = api.get("/api/v1/health").await?;
    ensure(h["status"] == "ok", format!("health: {h}"))?;
    Ok(format!("stormstorage {} at {}", h["version"].as_str().unwrap_or("?"), api.base()))
}

/// At least one storage node is registered and healthy — on a stormcos node,
/// the local stormblock adopted by `[local]` (#9).
pub async fn engine_adopted(api: &Api) -> Outcome {
    let nodes = api.nodes().await?;
    let healthy: Vec<String> = nodes
        .iter()
        .filter(|n| n["status"]["healthy"] == true)
        .map(|n| {
            format!(
                "{} ({}, {} free)",
                n["name"].as_str().unwrap_or("?"),
                n["status"]["source"].as_str().or(n["source"].as_str()).unwrap_or("?"),
                n["status"]["free_bytes"].as_u64().unwrap_or(0) / MIB
            )
        })
        .collect();
    ensure(!healthy.is_empty(), format!("no healthy storage node among {} registered", nodes.len()))?;
    Ok(format!("healthy: {}", healthy.join(", ")))
}

/// The node's slabs show as pools, and the stormview feed renders.
pub async fn pools_and_feed(api: &Api) -> Outcome {
    let pools = api.get("/api/v1/pools").await?;
    let pools = pools["pools"].as_array().cloned().unwrap_or_default();
    let slabs = pools.iter().filter(|p| p["kind"] == "slab").count();
    ensure(slabs > 0, format!("no slab pools among {} pools", pools.len()))?;
    let feed = api.get("/api/v1/components").await?;
    let feed = feed.as_array().cloned().unwrap_or_default();
    ensure(feed.iter().any(|c| c["kind"] == "storage"), "feed has no storage summary")?;
    Ok(format!("{slabs} slab pool(s), {} components in the feed", feed.len()))
}

/// Free bytes across healthy nodes (what placement has to work with).
pub async fn free_bytes(api: &Api) -> Result<u64, Why> {
    Ok(api
        .nodes()
        .await?
        .iter()
        .filter(|n| n["status"]["healthy"] == true)
        .map(|n| n["status"]["free_bytes"].as_u64().unwrap_or(0))
        .sum())
}

/// Create a single-leg volume, check it is served as its leg, delete it.
pub async fn single_leg_lifecycle(api: &Api, env: &Env, tag: &str) -> Outcome {
    if free_bytes(api).await? < 4 * SIZE {
        return Err(Why::Skip(format!("less than {} MiB free on healthy nodes", 4 * SIZE / MIB)));
    }
    let name = env.vol(tag);
    let v = api.create(&name, SIZE, 1).await?;
    let result = async {
        ensure(v["assembly"] == "single_leg", format!("assembly {}", v["assembly"]))?;
        ensure(v["legs"][0]["state"] == "created", format!("leg: {}", v["legs"][0]))?;
        let ex = &v["export"];
        ensure(ex["state"] == "published", format!("export: {ex}"))?;
        ensure(ex["volume_id"] == v["legs"][0]["volume_id"], "served volume is not the leg")?;
        let c = &crate::api::served_host(ex)["coordinates"];
        ensure(c["nqn"].is_string() && c["nsid"].is_u64(), format!("coordinates for the consumer host: {ex}"))?;
        Ok::<_, Why>(format!(
            "{name} on {} served at {}:{} nsid {}",
            v["legs"][0]["node"].as_str().unwrap_or("?"),
            c["traddr"].as_str().unwrap_or("?"),
            c["trsvcid"],
            c["nsid"]
        ))
    }
    .await;
    // Delete whatever happened above.
    api.remove(&name).await?;
    let detail = result?;
    let left = api.volumes().await?.iter().any(|x| x["name"] == name.as_str());
    ensure(!left, format!("{name} still listed after delete"))?;
    Ok(detail)
}

/// #109: a create with an owner records it (`GET` and the listing) and the
/// leader writes it to the engine volume (stormblock#115), seen through the
/// node's inventory — what stormblock-csi sends for a claim.
pub async fn owner_recorded(api: &Api, env: &Env, tag: &str) -> Outcome {
    if free_bytes(api).await? < 4 * SIZE {
        return Err(Why::Skip(format!("less than {} MiB free on healthy nodes", 4 * SIZE / MIB)));
    }
    let name = env.vol(tag);
    let uid = format!("stormstorage-test-{}", env.run_id);
    let owner = json!({"kind": "PersistentVolumeClaim", "namespace": "stormstorage-test", "name": name, "uid": uid});
    let r = api
        .post(
            "/api/v1/volumes",
            json!({"name": name, "size_bytes": SIZE, "replicas": 1,
                   "hosts": [{"host_nqn": crate::api::CONSUMER_NQN}], "owner": owner}),
        )
        .await?;
    if !r.ok() {
        return Err(Why::Fail(format!("create {name}: {} {}", r.status, r.body)));
    }
    let result = async {
        let v = api.get(&format!("/api/v1/volumes/{name}")).await?;
        ensure(v["owner"] == owner, format!("GET owner: {}", v["owner"]))?;
        let listed = api.volumes().await?;
        let l = listed.iter().find(|x| x["name"] == name.as_str());
        ensure(l.is_some_and(|l| l["owner"] == owner), "the listing does not carry the owner")?;
        let node = v["legs"][0]["node"].as_str().unwrap_or_default().to_string();
        ensure(!node.is_empty(), format!("no leg node: {v}"))?;
        // The inventory refreshes once per poll; the create already sent it
        // and the poll re-applies it, so a few polls are plenty.
        let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let inv = api.get(&format!("/api/v1/nodes/{node}/inventory")).await?;
            let ev = inv["volumes"]
                .as_array()
                .and_then(|a| a.iter().find(|x| x["name"] == name.as_str()).cloned());
            if let Some(ev) = &ev {
                let o = &ev["owner"];
                if o["kind"] == "PersistentVolumeClaim" && o["uid"] == uid.as_str() && o["name"] == name.as_str() {
                    return Ok::<_, Why>(format!("{name}: owner on the record, the listing and {node}'s engine volume ({o})"));
                }
            }
            if std::time::Instant::now() >= end {
                return Err(Why::Fail(format!(
                    "{node}'s engine volume {name} after 60 s: {}",
                    ev.map_or("not in the inventory".to_string(), |e| format!("owner {}", e["owner"]))
                )));
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
    .await;
    api.remove(&name).await?;
    result
}

/// Delete every volume this run created (by name prefix). Best-effort; the
/// count of what could not be removed.
pub async fn cleanup(api: &Api, env: &Env) -> usize {
    let prefix = env.prefix();
    let Ok(vols) = api.volumes().await else { return 0 };
    let mut left = 0;
    for v in vols {
        let Some(name) = v["name"].as_str() else { continue };
        if name.starts_with(&prefix) && api.remove(name).await.is_err() {
            left += 1;
        }
    }
    left
}

/// Body for a create that must be refused.
pub fn create_body(name: &str, size: u64, replicas: u32) -> Value {
    json!({"name": name, "size_bytes": size, "replicas": replicas})
}
