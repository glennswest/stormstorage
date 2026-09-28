//! `medium` (< 30 min): features and failure paths, end to end, against the
//! node's stormstorage and the engine(s) under it.

use serde_json::json;

use crate::api::Api;
use crate::checks::{self, SIZE};
use crate::env::Env;
use crate::report::{ensure, Outcome, Report, Why};

/// Seconds to wait for stormstorage's inventory (one poll is 15 s).
const INVENTORY_WAIT: u64 = 90;

pub async fn run(env: &Env, api: &Api, r: &mut Report) -> Result<(), String> {
    if !r.run("api-up", checks::api_up(api)).await {
        return Err(format!("stormstorage does not answer at {}", api.base()));
    }
    r.run("engine-adopted", checks::engine_adopted(api)).await;
    r.run("pools-and-feed", checks::pools_and_feed(api)).await;
    r.run("single-leg-lifecycle", checks::single_leg_lifecycle(api, env, "m1")).await;
    r.run("leg-on-engine", leg_on_engine(api, env)).await;
    r.run("placement-dry-run", placement_dry_run(api)).await;
    r.run("create-refusals", create_refusals(api, env)).await;
    r.run("republish-unchanged", republish(api, env)).await;
    r.run("one-node-refusals", one_node_refusals(api, env)).await;
    r.run("not-found", not_found(api, env)).await;
    r.run("auth-guard", auth_guard(api)).await;
    r.run("raid1-across-nodes", raid1(api, env)).await;
    r.run("events-and-orphans", events_and_orphans(api, env)).await;
    let left = checks::cleanup(api, env).await;
    r.run("cleanup", async move {
        ensure(left == 0, format!("{left} volume(s) of this run could not be deleted"))?;
        Ok::<_, Why>("nothing of this run left".into())
    })
    .await;
    Ok(())
}

/// A leg is a real volume on its node's engine, and delete removes it there.
async fn leg_on_engine(api: &Api, env: &Env) -> Outcome {
    let name = env.vol("leg");
    let v = api.create(&name, SIZE, 1).await?;
    let node = v["legs"][0]["node"].as_str().unwrap_or_default().to_string();
    let seen = api.wait_engine(&node, INVENTORY_WAIT, |n| n.iter().any(|x| *x == name)).await;
    api.remove(&name).await?;
    ensure(seen?, format!("{name} never appeared on {node}'s engine within {INVENTORY_WAIT}s"))?;
    let gone = api.wait_engine(&node, INVENTORY_WAIT, |n| !n.iter().any(|x| *x == name)).await?;
    ensure(gone, format!("{name} still on {node}'s engine {INVENTORY_WAIT}s after delete"))?;
    Ok(format!("{name} created on {node}'s engine and gone after delete"))
}

/// The dry run places what fits and explains what does not.
async fn placement_dry_run(api: &Api) -> Outcome {
    let healthy = api.healthy().await?.len() as u32;
    let r = api
        .call(reqwest::Method::POST, "/api/v1/placement/plan", Some(json!({"size_bytes": SIZE, "replicas": 1})), true)
        .await?;
    ensure(r.ok(), format!("plan replicas=1: {} {}", r.status, r.body))?;
    ensure(r.body["legs"].as_array().map(|a| a.len()) == Some(1), format!("plan: {}", r.body))?;
    let too_many = healthy + 1;
    let r = api
        .call(
            reqwest::Method::POST,
            "/api/v1/placement/plan",
            Some(json!({"size_bytes": SIZE, "replicas": too_many})),
            true,
        )
        .await?;
    ensure((400..500).contains(&r.status), format!("replicas={too_many} on {healthy} node(s): {} {}", r.status, r.body))?;
    ensure(r.body["error"].is_string(), format!("no explanation: {}", r.body))?;
    Ok(format!("1 leg placed; {too_many} refused: {}", r.body["error"].as_str().unwrap_or_default()))
}

/// Duplicates, reserved names and unplaceable volumes are refused, and a
/// refused create leaves nothing behind.
async fn create_refusals(api: &Api, env: &Env) -> Outcome {
    let healthy = api.healthy().await?.len() as u32;
    let dup = env.vol("dup");
    api.create(&dup, SIZE, 1).await?;
    let result = async {
        let r = api.post("/api/v1/volumes", checks::create_body(&dup, SIZE, 1)).await?;
        ensure(r.status == 409, format!("duplicate: {} {}", r.status, r.body))?;
        let mirror = format!("{}-mirror", env.vol("rsv"));
        let rm = api.post("/api/v1/volumes", checks::create_body(&mirror, SIZE, 1)).await?;
        ensure((400..500).contains(&rm.status), format!("reserved -mirror name: {} {}", rm.status, rm.body))?;
        let big = env.vol("big");
        let r = api.post("/api/v1/volumes", checks::create_body(&big, SIZE, healthy + 1)).await?;
        ensure(!r.ok(), format!("{} replicas on {healthy} node(s) accepted", healthy + 1))?;
        let names: Vec<String> =
            api.volumes().await?.iter().filter_map(|v| v["name"].as_str().map(str::to_string)).collect();
        ensure(!names.contains(&big) && !names.contains(&mirror), "a refused create was recorded")?;
        Ok::<_, Why>(format!("duplicate 409, -mirror {}, unplaceable {}", rm.status, r.status))
    }
    .await;
    api.remove(&dup).await?;
    result
}

/// Republishing an unchanged volume reports the same coordinates.
async fn republish(api: &Api, env: &Env) -> Outcome {
    let name = env.vol("rep");
    let v = api.create(&name, SIZE, 1).await?;
    let result = async {
        let r = api.post(&format!("/api/v1/volumes/{name}/export"), json!({})).await?;
        ensure(r.ok(), format!("republish: {} {}", r.status, r.body))?;
        ensure(r.body["state"] == "published", format!("state: {}", r.body))?;
        ensure(r.body["coordinates_changed"] == false, format!("coordinates changed: {}", r.body))?;
        ensure(r.body["coordinates"] == v["export"]["coordinates"], "coordinates differ from create's")?;
        Ok::<_, Why>("published, coordinates unchanged".to_string())
    }
    .await;
    api.remove(&name).await?;
    result
}

/// With one leg there is nothing to assemble; with one node nowhere to move.
async fn one_node_refusals(api: &Api, env: &Env) -> Outcome {
    let healthy = api.healthy().await?;
    let name = env.vol("one");
    let v = api.create(&name, SIZE, 1).await?;
    let node = v["legs"][0]["node"].as_str().unwrap_or_default().to_string();
    let result = async {
        let r = api.post(&format!("/api/v1/volumes/{name}/assemble"), json!({})).await?;
        ensure(r.status == 409, format!("assemble a single leg: {} {}", r.status, r.body))?;
        let r = api.post(&format!("/api/v1/volumes/{name}/move"), json!({"from": node})).await?;
        ensure(!r.ok(), format!("move of a single-leg volume accepted: {}", r.body))?;
        Ok::<_, Why>(format!("assemble 409, move {} ({} healthy node(s))", r.status, healthy.len()))
    }
    .await;
    api.remove(&name).await?;
    result
}

async fn not_found(api: &Api, env: &Env) -> Outcome {
    let name = env.vol("nope");
    let r = api.call(reqwest::Method::GET, &format!("/api/v1/volumes/{name}"), None, true).await?;
    ensure(r.status == 404, format!("GET unknown: {}", r.status))?;
    let r = api.delete(&format!("/api/v1/volumes/{name}")).await?;
    ensure(r.status == 404, format!("DELETE unknown: {}", r.status))?;
    let r = api.call(reqwest::Method::GET, &format!("/api/v1/nodes/{name}/inventory"), None, true).await?;
    ensure(r.status == 404, format!("inventory of unknown node: {}", r.status))?;
    Ok("GET, DELETE and inventory answer 404".into())
}

/// A write without the token is refused when the node's API is closed.
/// The body is invalid on purpose: an open API answers 4xx, never creates.
async fn auth_guard(api: &Api) -> Outcome {
    let r = api.call(reqwest::Method::POST, "/api/v1/volumes", Some(json!({})), false).await?;
    ensure(!r.ok(), format!("an empty create was accepted: {}", r.body))?;
    let reads = api.call(reqwest::Method::GET, "/api/v1/volumes", None, false).await?;
    ensure(reads.ok(), format!("reads need a token: {}", reads.status))?;
    if r.status == 401 {
        Ok(format!("closed: writes need the token (this run has one: {}), reads are open", api.has_token()))
    } else {
        Ok(format!("open (no api_token on the node): unauthenticated write reached validation ({})", r.status))
    }
}

/// RAID1 across two nodes: assembled on the head, served from its array.
async fn raid1(api: &Api, env: &Env) -> Outcome {
    let healthy = api.healthy().await?;
    if healthy.len() < 2 {
        return Err(Why::Skip(format!("requires 2 healthy storage nodes, {} here", healthy.len())));
    }
    let name = env.vol("r1");
    let v = api.create(&name, SIZE, 2).await?;
    let result = async {
        ensure(v["assembly"] == "assembled", format!("assembly {}: {}", v["assembly"], v))?;
        let nodes: Vec<&str> = v["legs"].as_array().unwrap().iter().filter_map(|l| l["node"].as_str()).collect();
        ensure(nodes.len() == 2 && nodes[0] != nodes[1], format!("legs on {nodes:?}"))?;
        let head = v["head"].as_str().unwrap_or_default().to_string();
        ensure(v["export"]["state"] == "published", format!("export: {}", v["export"]))?;
        ensure(v["export"]["node"] == head.as_str(), "not served from the head")?;
        let served = format!("{name}-mirror");
        let seen = api.wait_engine(&head, INVENTORY_WAIT, |n| n.iter().any(|x| *x == served)).await?;
        ensure(seen, format!("{served} not on {head}'s engine"))?;
        Ok::<_, Why>(format!("RAID1 on {head} across {nodes:?}, served as {served}"))
    }
    .await;
    api.remove(&name).await?;
    result
}

async fn events_and_orphans(api: &Api, env: &Env) -> Outcome {
    let ev = api.get("/api/v1/events").await?;
    let msgs: Vec<&str> =
        ev["events"].as_array().map(|a| a.iter().filter_map(|e| e["message"].as_str()).collect()).unwrap_or_default();
    let prefix = env.prefix();
    let created = msgs.iter().filter(|m| m.starts_with(&prefix) && m.contains(": created")).count();
    if created == 0 {
        return Err(Why::Skip("no create of this run succeeded (writes skipped?)".into()));
    }
    let orphans = api.get("/api/v1/orphans").await?;
    let mine = orphans["orphans"]
        .as_array()
        .map(|a| a.iter().filter(|o| o["of_volume"].as_str().is_some_and(|v| v.starts_with(&prefix))).count())
        .unwrap_or(0);
    ensure(mine == 0, format!("{mine} orphaned leg(s) of this run"))?;
    Ok(format!("{created} create event(s) of this run; no orphans"))
}
