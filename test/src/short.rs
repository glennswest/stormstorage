//! `short` (< 2 min): stormstorage is up on the node and does its main job —
//! it sees the node's storage and places, serves and deletes a volume.

use crate::api::Api;
use crate::checks;
use crate::env::Env;
use crate::report::Report;

pub async fn run(env: &Env, api: &Api, r: &mut Report) -> Result<(), String> {
    if !r.run("api-up", checks::api_up(api)).await {
        return Err(format!("stormstorage does not answer at {}", api.base()));
    }
    r.run("engine-adopted", checks::engine_adopted(api)).await;
    r.run("pools-and-feed", checks::pools_and_feed(api)).await;
    r.run("single-leg-lifecycle", checks::single_leg_lifecycle(api, env, "s")).await;
    Ok(())
}
