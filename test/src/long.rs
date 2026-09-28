//! `long` (the night window): waves of volumes sized from the node's own free
//! capacity. Each wave creates volumes concurrently, deletes them, waits
//! for the engines to settle, and measures create latency and what was
//! left behind. A wave with errors, a residue, or a create p50 more than
//! twice the first wave's is a failure, even when every call answered.

use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::Api;
use crate::checks::{self, SIZE};
use crate::env::Env;
use crate::report::{Report, Why};

/// Volumes created at once within a wave.
const PARALLEL: usize = 8;
/// Keep this much of the window for cleanup and the summary.
const MARGIN: Duration = Duration::from_secs(300);

pub async fn run(env: &Env, api: Arc<Api>, r: &mut Report) -> Result<(), String> {
    if !r.run("api-up", checks::api_up(&api)).await {
        return Err(format!("stormstorage does not answer at {}", api.base()));
    }
    r.run("engine-adopted", checks::engine_adopted(&api)).await;
    let free = checks::free_bytes(&api).await.map_err(|_| "cannot read node capacity".to_string())?;
    // One volume per 20 × its size of free space, 4 to 64, capped by
    // STORM_WAVE_MAX: a larger machine gets larger waves.
    let mut full = ((free / (SIZE * 20)) as usize).clamp(4, 64);
    if let Some(m) = env.wave_max {
        full = full.min(m.max(1));
    }
    r.metric(json!({"free_bytes": free, "wave_full": full, "parallel": PARALLEL}));

    let start = Instant::now();
    let mut first_p50: Option<u64> = None;
    let mut regressed: Option<u32> = None;
    let mut last = Duration::from_secs(60);
    let mut wave = 0u32;
    while start.elapsed() + last + MARGIN < env.timeout {
        wave += 1;
        // Vary the size: half, three quarters, full, and around again.
        let n = (full * (2 + (wave as usize - 1) % 3) / 4).max(1);
        let t = Instant::now();
        let w = one_wave(&api, env, wave, n).await;
        last = t.elapsed();
        let name = format!("wave-{wave}");
        match w {
            Ok(m) => {
                let p50 = m.p50;
                let base = *first_p50.get_or_insert(p50);
                r.metric(json!({
                    "wave": wave, "volumes": n, "create_p50_ms": p50, "create_max_ms": m.max,
                    "errors": m.errors, "delete_ms": m.delete_ms, "settle_ms": m.settle_ms,
                    "residue": m.residue, "wave_ms": last.as_millis() as u64,
                }));
                let slow = wave > 1 && p50 > 2 * base && p50 > base + 1000;
                if slow && regressed.is_none() {
                    regressed = Some(wave);
                }
                let outcome = if m.errors > 0 {
                    Err(Why::Fail(format!("{} of {n} creates failed: {}", m.errors, m.first_error)))
                } else if m.residue > 0 {
                    Err(Why::Fail(format!("{} left behind after the wave settled", m.residue)))
                } else if slow {
                    Err(Why::Fail(format!("create p50 {p50} ms vs {base} ms on wave 1")))
                } else {
                    Ok(format!("{n} volumes, create p50 {p50} ms, max {} ms, no residue", m.max))
                };
                r.record(&name, &outcome, last.as_millis());
            }
            Err(e) => {
                r.record(&name, &Err(e), last.as_millis());
                break;
            }
        }
    }
    let left = checks::cleanup(&api, env).await;
    let outcome = match (wave, regressed, left) {
        (0, _, _) => Err(Why::Skip("the window left no time for a wave".into())),
        (_, _, l) if l > 0 => Err(Why::Fail(format!("{l} volume(s) of this run could not be deleted"))),
        (_, Some(w), _) => Err(Why::Fail(format!("first regressed wave: {w} of {wave}"))),
        _ => Ok(format!("{wave} waves, no regression")),
    };
    r.record("trend", &outcome, 0);
    Ok(())
}

struct WaveMetrics {
    p50: u64,
    max: u64,
    errors: usize,
    first_error: String,
    delete_ms: u64,
    settle_ms: u64,
    residue: usize,
}

async fn one_wave(api: &Arc<Api>, env: &Env, wave: u32, n: usize) -> Result<WaveMetrics, Why> {
    let names: Vec<String> = (0..n).map(|i| env.vol(&format!("w{wave}-{i}"))).collect();
    let sem = Arc::new(tokio::sync::Semaphore::new(PARALLEL));
    let mut set = tokio::task::JoinSet::new();
    for name in names.clone() {
        let (api, sem) = (api.clone(), sem.clone());
        set.spawn(async move {
            let _permit = sem.acquire_owned().await;
            let t = Instant::now();
            let r = api.create(&name, SIZE, 1).await;
            (t.elapsed().as_millis() as u64, r)
        });
    }
    let mut lat = Vec::new();
    let mut errors = 0;
    let mut first_error = String::new();
    while let Some(j) = set.join_next().await {
        let (ms, r) = j.map_err(|e| Why::Fail(format!("task: {e}")))?;
        match r {
            Ok(_) => lat.push(ms),
            Err(Why::Skip(s)) => return Err(Why::Skip(s)),
            Err(Why::Fail(e)) => {
                errors += 1;
                if first_error.is_empty() {
                    first_error = e;
                }
            }
        }
    }
    lat.sort_unstable();
    let p50 = lat.get(lat.len() / 2).copied().unwrap_or(0);
    let max = lat.last().copied().unwrap_or(0);

    let t = Instant::now();
    for name in &names {
        api.remove(name).await?;
    }
    let delete_ms = t.elapsed().as_millis() as u64;

    // Settle: the engines' inventory (one poll is 15 s) shows none of it.
    let t = Instant::now();
    let prefix = env.prefix();
    let nodes = api.healthy().await?;
    let mut residue = 0;
    for node in &nodes {
        let clean = api
            .wait_engine(node, 90, |v| !v.iter().any(|x| x.starts_with(&prefix)))
            .await?;
        if !clean {
            residue += api.engine_volume_names(node).await?.iter().filter(|x| x.starts_with(&prefix)).count();
        }
    }
    residue += api.volumes().await?.iter().filter(|v| v["name"].as_str().is_some_and(|x| x.starts_with(&prefix))).count();
    let orphans = api.get("/api/v1/orphans").await?;
    residue += orphans["orphans"]
        .as_array()
        .map(|a| a.iter().filter(|o| o["of_volume"].as_str().is_some_and(|v| v.starts_with(&prefix))).count())
        .unwrap_or(0);
    Ok(WaveMetrics { p50, max, errors, first_error, delete_ms, settle_ms: t.elapsed().as_millis() as u64, residue })
}
