//! `raid1-two-nodes` (#113): RAID1 across the two nodes of the runner's
//! `two-node` cluster (a master, the seed the Job runs on, and a worker),
//! then node loss, the leg's return and head loss.
//!
//! Every power-off is the worker's: the seed runs this Job and the only
//! apiserver. So two volumes are made at once, `M` headed on the master
//! and `W` headed on the worker (`prefer_node`, #50), and one power cycle
//! of the worker is node loss for `M` and head loss for `W`:
//!
//! 1. `pair` — a master and a worker, both Ready, both healthy storage
//!    nodes of this stormstorage.
//! 2. `volumes` — `M` and `W`, two legs each (one per node), assembled on
//!    their heads and every copy `in_sync`.
//! 3. `claim` — a StorageClass of stormblock-csi with one slave per node
//!    (`mirror2` when the pool exists), a claim, a pod on the master writing
//!    a pattern and a tick a second; its PV is a volume with two legs on
//!    two nodes. A skip where stormblock-csi is not deployed (it is
//!    optional on stormcos).
//! 4. `vm` — a skip: a VM root disk replicated across nodes has no way to
//!    be made yet (the owner's decision on stormstorage#113).
//! 5. `power-off-worker` — the runner's `stop-worker` fault
//!    (stormcentral#1454): a hard power-off, no drain.
//! 6. `node-loss` — `M` degraded with the worker's leg lost and an event;
//!    the claim's pod still running and still writing.
//! 7. `head-loss` — `W`'s head lost; re-served from the master's leg, by
//!    the automatic re-head (#14) when it is on, else fence + promote
//!    (#33); seconds from power-off to served.
//! 8. `power-on-worker` — the fault's restore: Node Ready again.
//! 9. `resync` — both volumes `assembled` again: the leg came back
//!    (#113's rejoin), with `last_resync` (mode, bytes, seconds).
//! 10. `claim-intact` — a reader pod on the master finds the pattern intact.
//! 11. `vm-move` — a skip, as `vm`.
//!
//! Then the line `raid1-two-nodes`: pass only when every step passed (a
//! skip anywhere is no proof). Each step's objects — the stormstorage
//! volumes, the PV and the claim — are written to
//! `/results/raid1-two-nodes/<step>.json`.

use serde_json::{json, Value};
use std::time::{Duration, Instant};

use crate::api::{Api, CONSUMER_NQN};
use crate::env::Env;
use crate::kube::Kube;
use crate::report::{ensure, Outcome, Report, Why};

const MIB: u64 = 1 << 20;
const SIZE: u64 = 256 * MIB;
const CSI_DRIVER: &str = "csi.stormblock.io";
const POWER_FAULT: &str = "stop-worker";
const NO_POWER_FAULT: &str = "the runner has no hard power-off of a worker in two-node (stormcentral#1454)";
const NO_VM: &str = "a VM root disk mirrored across nodes cannot be made yet — VM roots are node-local golden clones; which component seeds a replicated root is the owner's decision on stormstorage#113";
const PREFIX: &str = "raid1-two-nodes";

#[derive(Default)]
struct Pair {
    kube: Option<Kube>,
    /// Kubernetes node names.
    master: String,
    worker: String,
    /// The same nodes as stormstorage names them.
    ss_master: String,
    ss_worker: String,
    m: String,
    w: String,
    claim: Option<Claim>,
    off_at: Option<Instant>,
    passed: Vec<&'static str>,
}

struct Claim {
    class: String,
    pvc: String,
    pod: String,
    volume: String,
}

pub async fn run(env: &Env, api: &Api, r: &mut Report) {
    let t = Instant::now();
    let mut p = Pair { m: env.vol("r1m"), w: env.vol("r1w"), ..Default::default() };
    let steps: [&'static str; 11] = [
        "pair",
        "volumes",
        "claim",
        "vm",
        "power-off-worker",
        "node-loss",
        "head-loss",
        "power-on-worker",
        "resync",
        "claim-intact",
        "vm-move",
    ];
    for step in steps {
        let name = format!("{PREFIX}/{step}");
        let started = Instant::now();
        let outcome = match needs(&p, step) {
            Some(missing) => Err(Why::Skip(format!("needs {PREFIX}/{missing}"))),
            None => match step {
                "pair" => pair(&mut p, api).await,
                "volumes" => volumes(&mut p, api).await,
                "claim" => claim(&mut p, env, api).await,
                "vm" | "vm-move" => Err(Why::Skip(NO_VM.into())),
                "power-off-worker" => power_off(&mut p).await,
                "node-loss" => node_loss(&p, api).await,
                "head-loss" => head_loss(&p, api).await,
                "power-on-worker" => power_on(&p).await,
                "resync" => resync(&p, api).await,
                _ => claim_intact(&p, env).await,
            },
        };
        if outcome.is_ok() {
            p.passed.push(step);
        }
        let ms = started.elapsed().as_millis();
        snapshot(&p, env, api, step).await;
        r.record(&name, &outcome, ms);
    }
    cleanup(&p).await;
    let missing: Vec<&str> = steps.iter().copied().filter(|s| !p.passed.contains(s)).collect();
    let outcome: Outcome = if missing.is_empty() {
        Ok(format!(
            "{} and {} on {} + {}: node loss, the leg back, head loss re-served",
            p.m, p.w, p.ss_master, p.ss_worker
        ))
    } else {
        Err(Why::Skip(format!("not proven: {} did not pass", missing.join(", "))))
    };
    // The proof line (`log:raid1-two-nodes .* pass`): plain text, ending in
    // the status, so only a full pass matches.
    let (status, detail) = match &outcome {
        Ok(d) => ("pass", d.clone()),
        Err(Why::Skip(d)) | Err(Why::Fail(d)) => ("not-passed", d.clone()),
    };
    println!("{PREFIX} {detail} {status}");
    r.record(PREFIX, &outcome, t.elapsed().as_millis());
}

/// The step a step needs, if that one did not pass.
fn needs(p: &Pair, step: &str) -> Option<&'static str> {
    let need: &[&'static str] = match step {
        "pair" | "vm" | "vm-move" => &[],
        "volumes" | "claim" => &["pair"],
        "power-off-worker" => &["volumes"],
        "node-loss" | "head-loss" => &["power-off-worker"],
        "power-on-worker" => &["power-off-worker"],
        "resync" => &["power-on-worker"],
        _ => &["claim", "power-on-worker"],
    };
    need.iter().copied().find(|n| !p.passed.contains(n))
}

fn kube(p: &Pair) -> Result<&Kube, Why> {
    p.kube.as_ref().ok_or_else(|| Why::Skip("no Kubernetes client".into()))
}

fn ready(node: &Value) -> bool {
    node["status"]["conditions"]
        .as_array()
        .is_some_and(|c| c.iter().any(|c| c["type"] == "Ready" && c["status"] == "True"))
}

fn is_master(node: &Value) -> bool {
    let l = &node["metadata"]["labels"];
    l.get("node-role.kubernetes.io/control-plane").is_some() || l.get("node-role.kubernetes.io/master").is_some()
}

fn internal_ip(node: &Value) -> Option<String> {
    node["status"]["addresses"]
        .as_array()?
        .iter()
        .find(|a| a["type"] == "InternalIP")
        .and_then(|a| a["address"].as_str())
        .map(str::to_string)
}

/// stormstorage's name for a Kubernetes node: the same name, else the
/// node whose engine URL is on the node's InternalIP.
fn ss_name(node: &Value, ss: &[Value]) -> Option<String> {
    let name = node["metadata"]["name"].as_str()?;
    if ss.iter().any(|n| n["name"] == name) {
        return Some(name.to_string());
    }
    let ip = internal_ip(node)?;
    ss.iter()
        .find(|n| {
            n["engine_url"].as_str().is_some_and(|u| {
                let host = u.split("://").nth(1).unwrap_or(u).split('/').next().unwrap_or_default();
                host == ip || host.starts_with(&format!("{ip}:")) || host.starts_with(&format!("[{ip}]"))
            })
        })
        .and_then(|n| n["name"].as_str().map(str::to_string))
}

async fn pair(p: &mut Pair, api: &Api) -> Outcome {
    let kube = Kube::in_cluster(std::env::var("STORM_NAMESPACE").ok()).map_err(Why::Skip)?;
    let nodes = kube.get("/api/v1/nodes").await?;
    let nodes: Vec<Value> = nodes["items"].as_array().cloned().unwrap_or_default().into_iter().filter(ready).collect();
    let topology = std::env::var("STORM_TOPOLOGY").unwrap_or_default();
    let (masters, workers): (Vec<&Value>, Vec<&Value>) = nodes.iter().partition(|n| is_master(n));
    let (Some(m), Some(w)) = (masters.first(), workers.first()) else {
        return Err(Why::Skip(format!(
            "needs a Ready master and a Ready worker (the runner's two-node pair, stormcentral#1271); topology {topology:?}, {} master(s), {} worker(s) Ready",
            masters.len(),
            workers.len()
        )));
    };
    let ss = api.nodes().await?;
    let healthy: Vec<Value> = ss.into_iter().filter(|n| n["status"]["healthy"] == true).collect();
    let name = |n: &Value| n["metadata"]["name"].as_str().unwrap_or_default().to_string();
    p.master = name(m);
    p.worker = name(w);
    p.ss_master = ss_name(m, &healthy)
        .ok_or_else(|| Why::Fail(format!("master {} is not a healthy storage node of this stormstorage", p.master)))?;
    p.ss_worker = ss_name(w, &healthy).ok_or_else(|| {
        Why::Fail(format!(
            "worker {} is not a healthy storage node of this stormstorage (one cluster stormstorage, #56)",
            p.worker
        ))
    })?;
    p.kube = Some(kube);
    Ok(format!(
        "master {} (storage {}), worker {} (storage {})",
        p.master, p.ss_master, p.worker, p.ss_worker
    ))
}

async fn create(api: &Api, name: &str, head: &str) -> Result<Value, Why> {
    let r = api
        .post(
            "/api/v1/volumes",
            json!({"name": name, "size_bytes": SIZE, "replicas": 2, "prefer_node": head,
                   "hosts": [{"host_nqn": CONSUMER_NQN}]}),
        )
        .await?;
    ensure(r.ok(), format!("create {name}: {} {}", r.status, r.body))?;
    Ok(r.body)
}

fn leg_nodes(v: &Value) -> Vec<String> {
    v["legs"].as_array().map(|a| a.iter().filter_map(|l| l["node"].as_str().map(str::to_string)).collect()).unwrap_or_default()
}

fn leg_state<'a>(v: &'a Value, node: &str) -> &'a str {
    v["legs"]
        .as_array()
        .and_then(|a| a.iter().find(|l| l["node"] == node))
        .and_then(|l| l["state"].as_str())
        .unwrap_or("none")
}

/// Wait up to `secs` for `f` over the volume's record; the last record.
async fn wait_vol(api: &Api, name: &str, secs: u64, f: impl Fn(&Value) -> bool) -> Result<(bool, Value), Why> {
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        let v = api.get(&format!("/api/v1/volumes/{name}")).await?;
        if f(&v) {
            return Ok((true, v));
        }
        if Instant::now() >= end {
            return Ok((false, v));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Every copy `in_sync` within `secs`; the seconds it took.
async fn wait_in_sync(api: &Api, name: &str, secs: u64) -> Result<f64, Why> {
    let t = Instant::now();
    loop {
        let r = api.get(&format!("/api/v1/volumes/{name}/replicas")).await?;
        let reps = r["replicas"].as_array().cloned().unwrap_or_default();
        if reps.len() == 2 && reps.iter().all(|x| x["sync"]["state"] == "in_sync") {
            return Ok(t.elapsed().as_secs_f64());
        }
        if t.elapsed() > Duration::from_secs(secs) {
            return Err(Why::Fail(format!("{name}: copies not in sync after {secs}s: {}", r["replicas"])));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn volumes(p: &mut Pair, api: &Api) -> Outcome {
    let mut said = Vec::new();
    for (name, head, other) in [(&p.m, &p.ss_master, &p.ss_worker), (&p.w, &p.ss_worker, &p.ss_master)] {
        let v = create(api, name, head).await?;
        ensure(v["assembly"] == "assembled", format!("{name}: assembly {}", v["assembly"]))?;
        ensure(v["head"] == head.as_str(), format!("{name}: head {} (asked {head}; prefer_node_honored {})", v["head"], v["prefer_node_honored"]))?;
        let mut legs = leg_nodes(&v);
        legs.sort();
        let mut want = vec![head.clone(), other.clone()];
        want.sort();
        ensure(legs == want, format!("{name}: legs on {legs:?}, not one on each of {want:?}"))?;
        let secs = wait_in_sync(api, name, 600).await?;
        said.push(format!("{name}: head {head}, legs {legs:?}, in sync after {secs:.0}s"));
    }
    Ok(said.join("; "))
}

async fn claim(p: &mut Pair, env: &Env, api: &Api) -> Outcome {
    let kube = kube(p)?;
    if kube.find(&format!("/apis/storage.k8s.io/v1/csidrivers/{CSI_DRIVER}")).await?.is_none() {
        return Err(Why::Skip(format!(
            "stormblock-csi is not deployed on this cluster (no CSIDriver {CSI_DRIVER}; optional on stormcos, rustkube-node#68 puts replicated claims there)"
        )));
    }
    let image = std::env::var("STORM_IMAGE").map_err(|_| Why::Skip("no STORM_IMAGE for the claim's pods".into()))?;
    let pools = api.get("/api/v1/pools").await?;
    let mirror2 = pools["pools"].as_array().is_some_and(|a| a.iter().any(|x| x["name"] == "mirror2"));
    let mut params = json!({"stormblock.io/replicaSlaves": "1", "stormblock.io/spread": "node"});
    if mirror2 {
        params["stormblock.io/pool"] = json!("mirror2");
    }
    let run = env.vol("");
    let c = Claim {
        class: env.vol("mirror2"),
        pvc: env.vol("claim"),
        pod: env.vol("writer"),
        volume: String::new(),
    };
    let labels = json!({"storm.io/test-run": std::env::var("STORM_RUN_ID").unwrap_or_else(|_| run.clone())});
    kube.create(
        "/apis/storage.k8s.io/v1/storageclasses",
        &json!({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": {"name": c.class, "labels": labels},
                "provisioner": CSI_DRIVER, "parameters": params,
                "reclaimPolicy": "Delete", "volumeBindingMode": "WaitForFirstConsumer"}),
    )
    .await?;
    kube.create(
        &kube.ns("persistentvolumeclaims"),
        &json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": c.pvc, "labels": labels},
                "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": c.class,
                         "resources": {"requests": {"storage": "1Gi"}}}}),
    )
    .await?;
    kube.create(&kube.ns("pods"), &pattern_pod(&c.pod, &c.pvc, &p.master, &image, "write", &run, &labels)).await?;
    let mut c = c;
    // Running and writing.
    let end = Instant::now() + Duration::from_secs(300);
    loop {
        let log = kube.log(&c.pod).await.unwrap_or_default();
        if log.contains("\"written\"") && log.contains("tick ") {
            break;
        }
        if Instant::now() >= end {
            let pod = kube.find(&format!("{}/{}", kube.ns("pods"), c.pod)).await?.unwrap_or_default();
            p.claim = Some(c);
            return Err(Why::Fail(format!("writer not writing after 300s: phase {}, {}", pod["status"]["phase"], pod["status"]["conditions"])));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    let pvc = kube.get(&format!("{}/{}", kube.ns("persistentvolumeclaims"), c.pvc)).await?;
    let pv_name = pvc["spec"]["volumeName"].as_str().unwrap_or_default().to_string();
    let pv = kube.get(&format!("/api/v1/persistentvolumes/{pv_name}")).await?;
    c.volume = pv["spec"]["csi"]["volumeHandle"].as_str().unwrap_or_default().to_string();
    let volume = c.volume.clone();
    p.claim = Some(c);
    let v = api.get(&format!("/api/v1/volumes/{volume}")).await?;
    let mut legs = leg_nodes(&v);
    legs.sort();
    let mut want = vec![p.ss_master.clone(), p.ss_worker.clone()];
    want.sort();
    ensure(legs == want, format!("PV {pv_name} → {volume}: legs on {legs:?}, not one on each node"))?;
    let secs = wait_in_sync(api, &volume, 600).await?;
    Ok(format!(
        "PV {pv_name} = {volume} (pool {}), head {}, legs {legs:?}, in sync after {secs:.0}s; writer on {}",
        if mirror2 { "mirror2" } else { "none, spread node" },
        v["head"],
        p.master
    ))
}

fn pattern_pod(name: &str, pvc: &str, node: &str, image: &str, mode: &str, seed: &str, labels: &Value) -> Value {
    json!({"apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/hostname": node},
            "automountServiceAccountToken": false,
            "securityContext": {"runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532},
            "containers": [{"name": "pattern", "image": image,
                "command": ["/test", "pattern", mode, "/data", seed],
                "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                "securityContext": {"allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]}}}],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": pvc}}]}})
}

/// Ask the runner for a fault and wait for `state` in the fault ConfigMap.
async fn fault(p: &Pair, restore: bool, want: &str, secs: u64) -> Result<Value, Why> {
    let faults = std::env::var("STORM_FAULTS").unwrap_or_default();
    if !faults.split([',', ' ']).any(|f| f.trim() == POWER_FAULT) {
        return Err(Why::Skip(format!("{NO_POWER_FAULT}; STORM_FAULTS {faults:?}")));
    }
    let kube = kube(p)?;
    let cm = std::env::var("STORM_FAULT_CONFIGMAP").unwrap_or_else(|_| "storm-fault".into());
    let ask = if restore { json!({"fault": POWER_FAULT, "restore": true}) } else { json!({"fault": POWER_FAULT}) };
    println!("{ask}");
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(c) = kube.find(&format!("{}/{cm}", kube.ns("configmaps"))).await? {
            if let Some(s) = c["data"][POWER_FAULT].as_str() {
                let a: Value = serde_json::from_str(s).unwrap_or(Value::Null);
                if a["state"] == want {
                    return Ok(a);
                }
                if a["state"] == "failed" {
                    return Err(Why::Fail(format!("{POWER_FAULT}: {a}")));
                }
            }
        }
        if Instant::now() >= end {
            return Err(Why::Fail(format!("{POWER_FAULT}: no {want:?} in {cm} after {secs}s")));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn power_off(p: &mut Pair) -> Outcome {
    let a = fault(p, false, "done", 600).await?;
    p.off_at = Some(Instant::now());
    Ok(format!("{} ({}) powered off: {a}", p.worker, a["machine"].as_str().unwrap_or("?")))
}

async fn node_loss(p: &Pair, api: &Api) -> Outcome {
    let off = p.off_at.expect("needs power-off");
    let (ok, v) = wait_vol(api, &p.m, 240, |v| v["assembly"] == "degraded" && leg_state(v, &p.ss_worker) == "lost").await?;
    ensure(ok, format!("{}: {} with the worker's leg {} 240s after the power-off", p.m, v["assembly"], leg_state(&v, &p.ss_worker)))?;
    let seen = off.elapsed().as_secs_f64();
    let ev = api.get("/api/v1/events").await?;
    let lost = format!("{}: leg on {} lost", p.m, p.ss_worker);
    let evented = ev["events"].as_array().is_some_and(|a| a.iter().any(|e| e["message"].as_str().is_some_and(|m| m.starts_with(&lost))));
    ensure(evented, format!("no event {lost:?}"))?;
    ensure(v["head"] == p.ss_master.as_str(), format!("{}: head moved to {}", p.m, v["head"]))?;
    let mut said = format!("{}: degraded {seen:.0}s after the power-off, event said, head {} serving", p.m, p.ss_master);
    if let Some(c) = &p.claim {
        let kube = kube(p)?;
        let tick = |log: &str| log.lines().rev().find_map(|l| l.strip_prefix("tick ").and_then(|n| n.trim().parse::<u64>().ok()));
        let a = tick(&kube.log(&c.pod).await?);
        tokio::time::sleep(Duration::from_secs(10)).await;
        let b = tick(&kube.log(&c.pod).await?);
        ensure(b > a, format!("the claim's writer stopped: tick {a:?} then {b:?} 10s later"))?;
        let pv = api.get(&format!("/api/v1/volumes/{}", c.volume)).await?;
        said.push_str(&format!("; claim {}: {}, writer still writing (tick {a:?} → {b:?})", c.volume, pv["assembly"]));
    }
    Ok(said)
}

async fn head_loss(p: &Pair, api: &Api) -> Outcome {
    let off = p.off_at.expect("needs power-off");
    let (ok, v) = wait_vol(api, &p.w, 240, |v| leg_state(v, &p.ss_worker) == "lost").await?;
    ensure(ok, format!("{}: the head's leg is {} 240s after the power-off", p.w, leg_state(&v, &p.ss_worker)))?;
    // The automatic re-head (#14), when this stormstorage has it on.
    let (auto, v) = wait_vol(api, &p.w, 90, |v| v["head"] == p.ss_master.as_str()).await?;
    let how = if auto {
        "automatic re-head ([recovery] rehead, #14)".to_string()
    } else {
        let epoch = v["epoch"].as_u64().unwrap_or(1);
        let f = api.post(&format!("/api/v1/volumes/{}/fence", p.w), json!({"expected_epoch": epoch})).await?;
        ensure(f.ok(), format!("fence {} at {epoch}: {} {}", p.w, f.status, f.body))?;
        let fenced = f.body["epoch"].as_u64().unwrap_or(epoch + 1);
        let pr = api
            .post(
                &format!("/api/v1/volumes/{}/promote", p.w),
                json!({"target_node": p.ss_master, "fenced_epoch": fenced}),
            )
            .await?;
        ensure(pr.ok(), format!("promote {} onto {}: {} {}", p.w, p.ss_master, pr.status, pr.body))?;
        format!("fence (epoch {epoch} → {fenced}) + promote (#33); [recovery] rehead is off")
    };
    let (ok, v) = wait_vol(api, &p.w, 120, |v| {
        v["head"] == p.ss_master.as_str() && v["export"]["node"] == p.ss_master.as_str() && v["export"]["state"] == "published"
    })
    .await?;
    ensure(ok, format!("{}: head {}, export {} after the promote", p.w, v["head"], v["export"]))?;
    Ok(format!(
        "{}: head {} lost, re-served from {} {:.0}s after the power-off by {how}; the claim's head loss waits on the head at the consumer (stormblock-csi#56)",
        p.w,
        p.ss_worker,
        p.ss_master,
        off.elapsed().as_secs_f64()
    ))
}

async fn power_on(p: &Pair) -> Outcome {
    let a = fault(p, true, "restored", 900).await?;
    Ok(format!("{} back, Node Ready: {a}", p.worker))
}

async fn resync(p: &Pair, api: &Api) -> Outcome {
    let mut said = Vec::new();
    let mut vols = vec![p.m.clone(), p.w.clone()];
    if let Some(c) = &p.claim {
        vols.push(c.volume.clone());
    }
    for name in vols {
        let (ok, v) = wait_vol(api, &name, 900, |v| v["assembly"] == "assembled" && leg_state(v, &p.ss_worker) == "created").await?;
        ensure(ok, format!("{name}: {} with the worker's leg {} 900s after the worker came back: {}", v["assembly"], leg_state(&v, &p.ss_worker), v["legs"]))?;
        let secs = wait_in_sync(api, &name, 300).await?;
        let r = &v["last_resync"];
        said.push(format!(
            "{name}: assembled, {} ({} bytes in {:.1}s; in sync {secs:.0}s later)",
            r["mode"].as_str().unwrap_or("no last_resync"),
            r["bytes"],
            r["secs"].as_f64().unwrap_or(0.0)
        ));
    }
    Ok(said.join("; "))
}

async fn claim_intact(p: &Pair, env: &Env) -> Outcome {
    let c = p.claim.as_ref().ok_or_else(|| Why::Skip("no claim".into()))?;
    let kube = kube(p)?;
    let image = std::env::var("STORM_IMAGE").unwrap_or_default();
    let reader = env.vol("reader");
    let labels = json!({"storm.io/test-run": std::env::var("STORM_RUN_ID").unwrap_or_default()});
    kube.create(&kube.ns("pods"), &pattern_pod(&reader, &c.pvc, &p.master, &image, "verify", &env.vol(""), &labels)).await?;
    let end = Instant::now() + Duration::from_secs(300);
    loop {
        let pod = kube.get(&format!("{}/{reader}", kube.ns("pods"))).await?;
        let phase = pod["status"]["phase"].as_str().unwrap_or_default().to_string();
        if phase == "Succeeded" || phase == "Failed" {
            let log = kube.log(&reader).await?;
            ensure(phase == "Succeeded" && log.contains("\"ok\""), format!("reader {phase}: {}", log.trim()))?;
            return Ok(format!("pattern intact after node loss and the leg's return: {}", log.trim()));
        }
        if Instant::now() >= end {
            return Err(Why::Fail(format!("reader not done after 300s: {phase}")));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// The stormstorage volumes and the claim's objects after a step, for the
/// report (`/results/raid1-two-nodes/<step>.json`).
async fn snapshot(p: &Pair, env: &Env, api: &Api, step: &str) {
    let mut out = json!({"step": step, "volumes": {}});
    let mut names = vec![p.m.clone(), p.w.clone()];
    if let Some(c) = p.claim.as_ref().filter(|c| !c.volume.is_empty()) {
        names.push(c.volume.clone());
    }
    for n in names {
        if let Ok(v) = api.get(&format!("/api/v1/volumes/{n}")).await {
            out["volumes"][&n] = v;
        }
        if let Ok(v) = api.get(&format!("/api/v1/volumes/{n}/replicas")).await {
            out["replicas"][&n] = v;
        }
    }
    if let (Some(k), Some(c)) = (p.kube.as_ref(), p.claim.as_ref()) {
        if let Ok(Some(pvc)) = k.find(&format!("{}/{}", k.ns("persistentvolumeclaims"), c.pvc)).await {
            if let Some(pv) = pvc["spec"]["volumeName"].as_str() {
                if let Ok(Some(pv)) = k.find(&format!("/api/v1/persistentvolumes/{pv}")).await {
                    out["pv"] = pv;
                }
            }
            out["pvc"] = pvc;
        }
    }
    let dir = env.results.join(PREFIX);
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join(format!("{step}.json")), serde_json::to_vec_pretty(&out).unwrap_or_default());
}

/// The claim's pods, claim and class (the namespace goes with the run
/// anyway; the class is cluster-scoped). The volumes go with the run's
/// prefix in the suite's cleanup.
async fn cleanup(p: &Pair) {
    let (Some(k), Some(c)) = (p.kube.as_ref(), p.claim.as_ref()) else { return };
    for pod in [c.pod.clone(), c.pod.replace("writer", "reader")] {
        let _ = k.delete(&format!("{}/{pod}", k.ns("pods")), true).await;
    }
    let _ = k.delete(&format!("{}/{}", k.ns("persistentvolumeclaims"), c.pvc), false).await;
    let _ = k.delete(&format!("/apis/storage.k8s.io/v1/storageclasses/{}", c.class), false).await;
}
