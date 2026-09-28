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
        let c = &ex["coordinates"];
        ensure(c["nqn"].is_string() && c["nsid"].is_u64(), format!("coordinates: {c}"))?;
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
