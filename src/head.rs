//! Replication on the RAID head (#33): what stormblock-csi's `/v1` replica
//! surface asks of an engine, done by stormstorage over its distributed
//! volumes instead (owner, stormblock#179 option b).
//!
//! - **Sync state**: each poll reads the head's array; every leg becomes a
//!   replica `{node, role, sync}` in /v1's exact shape (`in_sync`,
//!   `resyncing {progress_pct, lag_bytes}`, `detached`).
//! - **Epoch + fence**: a per-volume epoch; a fence is a CAS on it and also
//!   fences every reachable leg on its engine (`/v1/volumes/{leg}/fence`),
//!   so under stormblock#6 a fenced head's writes to the legs are refused.
//! - **Promote**: move the head onto a node holding a leg — open the
//!   surviving legs there and put the array back together from their
//!   superblocks (`/api/v1/arrays/assemble`), then serve the volume that
//!   came across with it.
//! - **Prestage**: replace a slave leg (the move machinery), with the
//!   resync capped by the volume's `bandwidth_class`.
//! - **Dual-attach**: a bounded window for a live migration's target;
//!   commit = fence + promote the target, abort or expiry = close.
//!
//! docs/replication.md has the model and the contracts.

use crate::api::AppState;
use crate::events::Severity;
use crate::model::{AssemblyState, BandwidthClass, DistVolume, DualAttach, LegState, StaleHead};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A replica's sync state — the same JSON as stormblock's /v1
/// `SyncState` (and stormblock-csi's client type), so a /v1 client parses
/// it unchanged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum SyncState {
    InSync,
    Resyncing { progress_pct: f32, lag_bytes: u64 },
    /// No usable copy, or nothing read that says otherwise.
    Detached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// The leg on the head node — where the volume is served.
    Master,
    Slave,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Replica {
    pub node: String,
    pub role: Role,
    pub sync: SyncState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    Healthy,
    /// Serving, with less redundancy than asked for (a leg lost or
    /// resyncing).
    Degraded,
    /// No copy in sync.
    Faulted,
}

/// One array member as last read from the head.
#[derive(Debug, Clone, Default, Serialize)]
pub struct MemberReading {
    pub uuid: String,
    pub device_path: String,
    pub state: String,
    pub rebuilt_bytes: Option<u64>,
}

/// Where a reading's member states came from (#48).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncSource {
    /// The head's array, read live.
    #[default]
    Head,
    /// The head could not be read: the RAID superblocks the head wrote
    /// into the surviving legs, read on the legs' own engines.
    Superblock,
}

/// The head's array as last read. In memory only: every instance reads
/// the engines itself, like node status.
#[derive(Debug, Clone, Serialize)]
pub struct ArrayReading {
    pub read_at: SystemTime,
    pub head: String,
    pub array_id: String,
    /// The engine's array state: clean, degraded, rebuilding, failed
    /// ("unknown" for a reading from superblocks).
    pub state: String,
    pub member_data_bytes: u64,
    pub members: Vec<MemberReading>,
    /// Rate of the running rebuild, if one is running.
    pub rebuild_bytes_per_sec: Option<u64>,
    /// The array's event count: the head's, or the newest superblock's.
    pub events: Option<u64>,
    pub source: SyncSource,
    /// For a reading from superblocks: when the head was last read live.
    pub head_read_at: Option<SystemTime>,
}

/// Parse `GET /api/v1/arrays/{id}`.
pub fn parse_array(head: &str, array_id: &str, v: &serde_json::Value) -> ArrayReading {
    let members = v
        .get("members")
        .and_then(|m| m.as_array())
        .map(|ms| {
            ms.iter()
                .map(|m| MemberReading {
                    uuid: m.get("uuid").and_then(|x| x.as_str()).unwrap_or_default().to_string(),
                    device_path: m
                        .get("device_path")
                        .and_then(|x| x.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    state: m
                        .get("state")
                        .and_then(|x| x.as_str())
                        .unwrap_or_default()
                        .to_lowercase(),
                    rebuilt_bytes: m.get("rebuilt_bytes").and_then(|x| x.as_u64()),
                })
                .collect()
        })
        .unwrap_or_default();
    let rebuild = v.get("status").and_then(|s| s.get("rebuild"));
    ArrayReading {
        read_at: SystemTime::now(),
        head: head.to_string(),
        array_id: array_id.to_string(),
        state: v
            .get("status")
            .and_then(|s| s.get("state"))
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string(),
        member_data_bytes: v.get("member_data_bytes").and_then(|x| x.as_u64()).unwrap_or(0),
        members,
        rebuild_bytes_per_sec: rebuild
            .filter(|r| r.get("running").and_then(|x| x.as_bool()).unwrap_or(false))
            .and_then(|r| r.get("rate_bytes_per_sec"))
            .and_then(|x| x.as_u64()),
        events: v.get("events").and_then(|x| x.as_u64()),
        source: SyncSource::Head,
        head_read_at: None,
    }
}

/// One slot of a leg's RAID superblock.
#[derive(Debug, Clone, PartialEq)]
pub struct SlotRecord {
    pub member_uuid: String,
    pub state: String,
    pub rebuilt_to: u64,
}

/// The RAID superblock a head wrote into a leg (stormblock#309), as read
/// on the leg's own engine.
#[derive(Debug, Clone, PartialEq)]
pub struct LegSuperblock {
    pub array_uuid: String,
    /// The member this leg is.
    pub member_uuid: String,
    pub events: u64,
    pub data_size: u64,
    pub slots: Vec<SlotRecord>,
}

/// Parse `GET /v1/volumes/{id}/raid-superblock`. `None` for a body that
/// is not one (no array uuid, member uuid or event count).
pub fn parse_superblock(v: &serde_json::Value) -> Option<LegSuperblock> {
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(|x| x.to_lowercase());
    Some(LegSuperblock {
        array_uuid: s("array_uuid")?,
        member_uuid: s("member_uuid")?,
        events: v.get("events").and_then(|x| x.as_u64())?,
        data_size: v.get("data_size").and_then(|x| x.as_u64()).unwrap_or(0),
        slots: v
            .get("slots")
            .and_then(|x| x.as_array())
            .map(|ss| {
                ss.iter()
                    .filter_map(|x| {
                        Some(SlotRecord {
                            member_uuid: x.get("member_uuid")?.as_str()?.to_lowercase(),
                            state: x.get("state")?.as_str()?.to_lowercase(),
                            rebuilt_to: x.get("rebuilt_to").and_then(|r| r.as_u64()).unwrap_or(0),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
    })
}

/// The state a set of superblocks gives one member: worst wins
/// (failed/spare/other < rebuilding < active); a newest superblock that
/// does not list the member at all means it was taken out ("removed").
fn agreed_state(member: &str, newest: &[&LegSuperblock]) -> (String, u64) {
    let rank = |st: &str| match st {
        "active" => 2,
        "rebuilding" => 1,
        _ => 0,
    };
    let mut out: Option<(String, u64)> = None;
    for sb in newest {
        let Some(rec) = sb.slots.iter().find(|r| r.member_uuid == member) else {
            return ("removed".into(), 0);
        };
        out = Some(match out {
            None => (rec.state.clone(), rec.rebuilt_to),
            Some((st, _)) if rank(&rec.state) < rank(&st) => (rec.state.clone(), rec.rebuilt_to),
            Some((st, rb)) if rec.state == st => (st, rb.min(rec.rebuilt_to)),
            Some(keep) => keep,
        });
    }
    out.unwrap_or_else(|| ("removed".into(), 0))
}

/// A reading of a volume's array built from its surviving legs'
/// superblocks, for when the head cannot be read (#48). Pure.
///
/// A member reads `active` (→ `in_sync`) only when all of these hold:
/// its own superblock is of this array and carries the newest event count
/// among the legs read; every superblock at that count records it active;
/// its count is not below the last live reading of the head; and, unless
/// its superblock is newer than that reading, the reading had it active
/// too. Anything older or contradicted is "stale"/the reading's state,
/// which is `detached`. `last` is the last live reading of this head.
pub fn from_superblocks(
    vol: &DistVolume,
    last: Option<&ArrayReading>,
    sbs: &[LegSuperblock],
    now: SystemTime,
) -> Option<ArrayReading> {
    let head = vol.head.clone()?;
    let array_id = vol.array_id.clone()?;
    let last = last.filter(|r| r.source == SyncSource::Head && r.head == head && r.array_id == array_id);
    let ours: Vec<&LegSuperblock> =
        sbs.iter().filter(|s| s.array_uuid.eq_ignore_ascii_case(&array_id)).collect();
    let max = ours.iter().map(|s| s.events).max()?;
    let newest: Vec<&LegSuperblock> = ours.iter().copied().filter(|s| s.events == max).collect();
    let floor = last.and_then(|l| l.events);
    let members = ours
        .iter()
        .map(|sb| {
            let (mut state, mut rebuilt) = if sb.events < max || floor.is_some_and(|f| sb.events < f) {
                ("stale".to_string(), 0)
            } else {
                agreed_state(&sb.member_uuid, &newest)
            };
            // No newer evidence than the last live reading: it must agree.
            let newer = floor.is_some_and(|f| sb.events > f);
            if let Some(l) = last.filter(|_| !newer) {
                match l.members.iter().find(|m| m.uuid.eq_ignore_ascii_case(&sb.member_uuid)) {
                    Some(m) if m.state == "active" => {}
                    Some(m) if state == "active" || (state == "rebuilding" && m.state != "rebuilding") => {
                        state = m.state.clone();
                        rebuilt = m.rebuilt_bytes.unwrap_or(0);
                    }
                    Some(_) => {}
                    None => state = "removed".into(),
                }
            }
            MemberReading {
                uuid: sb.member_uuid.clone(),
                device_path: String::new(),
                state,
                rebuilt_bytes: Some(rebuilt),
            }
        })
        .collect();
    Some(ArrayReading {
        read_at: now,
        head,
        array_id,
        state: "unknown".into(),
        member_data_bytes: last
            .map(|l| l.member_data_bytes)
            .filter(|b| *b > 0)
            .unwrap_or_else(|| newest.iter().map(|s| s.data_size).max().unwrap_or(0)),
        members,
        rebuild_bytes_per_sec: None,
        events: Some(max),
        source: SyncSource::Superblock,
        head_read_at: last.map(|l| l.read_at),
    })
}

fn member_sync(m: &MemberReading, data_bytes: u64) -> SyncState {
    match m.state.as_str() {
        "active" => SyncState::InSync,
        "rebuilding" => {
            let done = m.rebuilt_bytes.unwrap_or(0).min(data_bytes);
            let pct = if data_bytes == 0 {
                0.0
            } else {
                ((done as f64 * 1000.0 / data_bytes as f64).round() / 10.0) as f32
            };
            SyncState::Resyncing { progress_pct: pct, lag_bytes: data_bytes - done }
        }
        _ => SyncState::Detached,
    }
}

/// The head array's member for a leg: by member uuid, else by the leg's
/// drive URI.
pub fn member_of<'a>(leg: &crate::model::Leg, r: &'a ArrayReading) -> Option<&'a MemberReading> {
    let uri = leg.export.as_ref().map(|x| x.drive_uri());
    r.members.iter().find(|m| {
        leg.member_uuid.as_deref().is_some_and(|u| u.eq_ignore_ascii_case(&m.uuid))
            || uri.as_deref() == Some(m.device_path.as_str())
    })
}

/// Every copy of a volume and how in sync it is. Pure. Without a reading
/// of the head (none yet, head unreachable, or a different array) every
/// mirrored leg is `detached`: a consumer that waits for `in_sync` before
/// a failover must never be told a copy is in sync on no evidence.
pub fn replicas(vol: &DistVolume, reading: Option<&ArrayReading>) -> Vec<Replica> {
    let reading = reading.filter(|r| {
        Some(r.head.as_str()) == vol.head.as_deref() && Some(r.array_id.as_str()) == vol.array_id.as_deref()
    });
    let master = match vol.assembly {
        AssemblyState::Assembled | AssemblyState::Degraded => vol.head.clone(),
        _ => vol.legs.first().map(|l| l.node.clone()),
    };
    let legs = vol.legs.iter().chain(vol.replacing.as_ref().map(|r| &r.leg));
    legs.map(|leg| {
        let role = if Some(&leg.node) == master.as_ref() { Role::Master } else { Role::Slave };
        let sync = if leg.state != LegState::Created {
            SyncState::Detached
        } else {
            match vol.assembly {
                AssemblyState::SingleLeg => SyncState::InSync,
                AssemblyState::PendingEngineSupport => SyncState::Detached,
                AssemblyState::Assembled | AssemblyState::Degraded => reading
                    .and_then(|r| member_of(leg, r).map(|m| member_sync(m, r.member_data_bytes)))
                    .unwrap_or(SyncState::Detached),
            }
        };
        Replica { node: leg.node.clone(), role, sync }
    })
    .collect()
}

/// Healthy when as many copies as asked for are in sync, faulted when
/// none is, degraded otherwise.
pub fn health(vol: &DistVolume, replicas: &[Replica]) -> Health {
    let in_sync = replicas.iter().filter(|r| r.sync == SyncState::InSync).count();
    if in_sync == 0 {
        Health::Faulted
    } else if in_sync as u32 >= vol.replicas.max(1) {
        Health::Healthy
    } else {
        Health::Degraded
    }
}

/// A volume as the API shows it: the stored record plus `replicas`,
/// `health` and when the head was last read.
pub fn view(vol: &DistVolume, reading: Option<&ArrayReading>) -> serde_json::Value {
    let reps = replicas(vol, reading);
    let mut v = serde_json::to_value(vol).unwrap_or_default();
    if let Some(o) = v.as_object_mut() {
        o.insert("health".into(), serde_json::to_value(health(vol, &reps)).unwrap_or_default());
        o.insert("replica_sync".into(), serde_json::to_value(&reps).unwrap_or_default());
        o.insert(
            "sync_read_at".into(),
            serde_json::to_value(reading.map(|r| r.read_at)).unwrap_or_default(),
        );
        o.insert(
            "sync_source".into(),
            serde_json::to_value(reading.map(|r| r.source)).unwrap_or_default(),
        );
    }
    v
}

/// Read every assembled volume's array on its head (run after each poll).
/// A head that cannot be read loses its live reading — no stale
/// `in_sync`; the surviving legs' superblocks stand in for it (#48).
pub async fn refresh(state: &Arc<AppState>) {
    type Due = (DistVolume, Option<crate::engine::Engine>, Vec<(crate::engine::Engine, String)>);
    let due: Vec<Due> = {
        let fed = state.fed.read().await;
        let healthy = |node: &str| fed.nodes.get(node).filter(|n| n.status.healthy);
        fed.volumes
            .values()
            .filter(|v| matches!(v.assembly, AssemblyState::Assembled | AssemblyState::Degraded))
            .filter(|v| v.head.is_some() && v.array_id.is_some())
            .map(|v| {
                let head = v.head.as_deref().and_then(healthy).map(|n| state.engine_for(n));
                let legs = v
                    .legs
                    .iter()
                    .chain(v.replacing.as_ref().map(|r| &r.leg))
                    .filter(|l| l.state == LegState::Created)
                    .filter_map(|l| Some((state.engine_for(healthy(&l.node)?), l.volume_id.clone()?)))
                    .collect();
                (v.clone(), head, legs)
            })
            .collect()
    };
    let mut readings = std::collections::BTreeMap::new();
    let mut live = state.head_live.read().await.clone();
    live.retain(|name, _| due.iter().any(|(v, _, _)| &v.name == name));
    for (vol, head_engine, legs) in due {
        let (Some(head), Some(array)) = (vol.head.clone(), vol.array_id.clone()) else { continue };
        let read = match &head_engine {
            Some(e) => e.get_array(&array).await.ok(),
            None => None,
        };
        if let Some(a) = read {
            let r = parse_array(&head, &array, &a);
            live.insert(vol.name.clone(), r.clone());
            readings.insert(vol.name.clone(), r);
            continue;
        }
        let mut sbs = Vec::new();
        for (engine, id) in legs {
            if let Ok(Some(v)) = engine.leg_superblock(&id).await {
                sbs.extend(parse_superblock(&v));
            }
        }
        if let Some(r) = from_superblocks(&vol, live.get(&vol.name), &sbs, SystemTime::now()) {
            readings.insert(vol.name.clone(), r);
        }
    }
    *state.head_live.write().await = live;
    *state.heads.write().await = readings;
}

async fn event(state: &Arc<AppState>, subject: &str, sev: Severity, msg: String) {
    state
        .events
        .write()
        .await
        .push(Some(subject.to_string()), sev, "replication", msg);
}

/// Why a replication call was refused. Maps onto /v1's error envelope.
#[derive(Debug)]
pub enum Refusal {
    NotFound(String),
    /// 412 `stale_epoch`, with the current epoch.
    StaleEpoch(u64),
    Conflict(String),
    /// An engine call failed.
    Upstream(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::NotFound(m) | Refusal::Conflict(m) | Refusal::Upstream(m) => f.write_str(m),
            Refusal::StaleEpoch(c) => write!(f, "stale epoch; current is {c}"),
        }
    }
}

/// The CAS step of a fence, on the record (pure): a mirrored volume at
/// `expected` goes to `expected + 1`, fenced.
pub fn apply_fence(vol: &mut DistVolume, expected: u64) -> Result<u64, Refusal> {
    if vol.epoch != expected {
        return Err(Refusal::StaleEpoch(vol.epoch));
    }
    if !matches!(vol.assembly, AssemblyState::Assembled | AssemblyState::Degraded) {
        return Err(Refusal::Conflict(format!(
            "{}: not a mirrored volume (assembly {:?}) — no other copy to promote",
            vol.name, vol.assembly
        )));
    }
    vol.epoch += 1;
    vol.fenced = true;
    Ok(vol.epoch)
}

/// What a fence did.
#[derive(Debug, Serialize)]
pub struct Fenced {
    pub epoch: u64,
    /// Legs fenced on their engines, with the leg's new epoch.
    pub legs: Vec<(String, u64)>,
    /// Legs that could not be fenced (unreachable, engine error).
    pub not_fenced: Vec<(String, String)>,
}

/// `POST /api/v1/volumes/{name}/fence {expected_epoch}`.
pub async fn fence(state: &Arc<AppState>, name: &str, expected: u64) -> Result<Fenced, Refusal> {
    let (epoch, legs) = {
        let mut fed = state.fed.write().await;
        let vol = fed
            .volumes
            .get_mut(name)
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        let epoch = apply_fence(vol, expected)?;
        let legs: Vec<(String, String)> = vol
            .legs
            .iter()
            .filter(|l| l.state == LegState::Created)
            .filter_map(|l| Some((l.node.clone(), l.volume_id.clone()?)))
            .collect();
        fed.revision += 1;
        let engines: Vec<(String, String, Option<crate::engine::Engine>)> = legs
            .into_iter()
            .map(|(node, vid)| {
                let e = fed.nodes.get(&node).filter(|n| n.status.healthy).map(|n| state.engine_for(n));
                (node, vid, e)
            })
            .collect();
        (epoch, engines)
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    let mut out = Fenced { epoch, legs: Vec::new(), not_fenced: Vec::new() };
    for (node, vid, engine) in legs {
        match engine {
            Some(e) => match e.fence_leg(&vid).await {
                Ok(le) => out.legs.push((node, le)),
                Err(err) => out.not_fenced.push((node, format!("{err:#}"))),
            },
            None => out.not_fenced.push((node, "unreachable".into())),
        }
    }
    {
        let mut fed = state.fed.write().await;
        if let Some(v) = fed.volumes.get_mut(name) {
            for (node, le) in &out.legs {
                if let Some(l) = v.legs.iter_mut().find(|l| &l.node == node) {
                    l.epoch = Some(*le);
                }
            }
        }
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    event(
        state,
        name,
        if out.not_fenced.is_empty() { Severity::Info } else { Severity::Warning },
        format!(
            "{name}: fenced at epoch {epoch}; legs fenced [{}]{}",
            out.legs.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(", "),
            if out.not_fenced.is_empty() {
                String::new()
            } else {
                format!(
                    "; not fenced: {}",
                    out.not_fenced
                        .iter()
                        .map(|(n, e)| format!("{n} ({e})"))
                        .collect::<Vec<_>>()
                        .join("; ")
                )
            }
        ),
    )
    .await;
    Ok(out)
}

/// The checks a promote makes on the record before touching an engine
/// (pure). Returns the current head.
pub fn check_promote(
    vol: &DistVolume,
    target: &str,
    fenced_epoch: u64,
    target_healthy: bool,
) -> Result<Option<String>, Refusal> {
    if !vol.fenced || vol.epoch != fenced_epoch {
        // /v1: anything but the fenced epoch is a 412, and so is a promote
        // with no fence before it.
        return Err(Refusal::StaleEpoch(vol.epoch));
    }
    if vol.dual_attach.is_some() {
        return Err(Refusal::Conflict(format!(
            "{}: a dual-attach window is open — close it (commit promotes)",
            vol.name
        )));
    }
    if !matches!(vol.assembly, AssemblyState::Assembled | AssemblyState::Degraded) {
        return Err(Refusal::Conflict(format!("{}: not assembled", vol.name)));
    }
    if vol.replacing.is_some() {
        return Err(Refusal::Conflict(format!("{}: a leg replacement is in flight", vol.name)));
    }
    let leg = vol
        .legs
        .iter()
        .find(|l| l.node == target)
        .ok_or_else(|| Refusal::Conflict(format!("{}: {target:?} holds no leg", vol.name)))?;
    if leg.state != LegState::Created || leg.volume_id.is_none() {
        return Err(Refusal::Conflict(format!("{}: the leg on {target} is {:?}", vol.name, leg.state)));
    }
    if !target_healthy {
        return Err(Refusal::Conflict(format!("{}: {target} is unreachable", vol.name)));
    }
    Ok(vol.head.clone())
}

/// `POST /api/v1/volumes/{name}/promote {target_node, fenced_epoch}`.
/// Takes the volume's claim for the whole move.
pub async fn promote(
    state: &Arc<AppState>,
    name: &str,
    target: &str,
    fenced_epoch: u64,
) -> Result<serde_json::Value, Refusal> {
    if !crate::orchestrate::claim(state, name) {
        return Err(Refusal::Conflict(format!("{name}: busy — an assembly or leg move is in flight")));
    }
    let r = promote_claimed(state, name, target, fenced_epoch).await;
    crate::orchestrate::release(state, name);
    r
}

async fn promote_claimed(
    state: &Arc<AppState>,
    name: &str,
    target: &str,
    fenced_epoch: u64,
) -> Result<serde_json::Value, Refusal> {
    let (vol, old_head, old_head_alive, engines) = {
        let fed = state.fed.read().await;
        let vol = fed
            .volumes
            .get(name)
            .cloned()
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        let healthy = |n: &str| fed.nodes.get(n).map(|n| n.status.healthy).unwrap_or(false);
        let old_head = check_promote(&vol, target, fenced_epoch, healthy(target))?;
        let alive = old_head.as_deref().map(healthy).unwrap_or(false);
        let mut engines = std::collections::BTreeMap::new();
        for l in &vol.legs {
            if let Some(n) = fed.nodes.get(&l.node).filter(|n| n.status.healthy) {
                engines.insert(l.node.clone(), state.engine_for(n));
            }
        }
        (vol, old_head, alive, engines)
    };
    let array_id = vol.array_id.clone().ok_or_else(|| Refusal::Conflict(format!("{name}: no array id")))?;
    let target_engine = engines.get(target).cloned().expect("target healthy");

    if old_head.as_deref() == Some(target) {
        // Promoting the head it already has: it keeps the array, now at the
        // new epoch. Its legs are re-attached at their fenced epochs.
        let mut vol = vol;
        reattach_legs(state, &mut vol, &engines, target).await;
        vol.fenced = false;
        store(state, vol).await;
        event(state, name, Severity::Info, format!("{name}: {target} stays head at epoch {fenced_epoch}")).await;
        return Ok(view_of(state, name).await);
    }
    if old_head_alive {
        // The old head still runs the array over the same legs. Taking them
        // over needs it to let go without writing to them, which the engine
        // cannot do yet: deleting the array is refused while the served
        // volume is pinned, and pulling its drives writes failure marks
        // into the survivors' superblocks.
        return Err(Refusal::Conflict(format!(
            "{name}: head {} is still running the array; a handover from a live head needs the engine \
             to release an array without writing to its members (stormblock#{RELEASE_ISSUE}). \
             Promote proceeds when the old head is unreachable (failover).",
            old_head.unwrap_or_default()
        )));
    }

    let mut vol = vol;
    // Every surviving leg, at its fenced epoch, opened on the new head.
    let surviving: Vec<usize> = (0..vol.legs.len())
        .filter(|&i| vol.legs[i].state == LegState::Created && engines.contains_key(&vol.legs[i].node))
        .collect();
    let served = assemble_on(state, &mut vol, &surviving, target, &target_engine, &engines).await?;
    let mirror = format!("{name}{}", crate::orchestrate::MIRROR_SUFFIX);

    let old = old_head.clone().unwrap_or_default();
    let old_uris: Vec<String> = vol
        .legs
        .iter()
        .filter_map(|l| l.export.as_ref().map(|x| x.drive_uri()))
        .collect();
    for l in vol.legs.iter_mut() {
        if l.state == LegState::Created && !engines.contains_key(&l.node) {
            l.state = LegState::Lost;
            l.message = Some(format!("node {} unreachable at promote", l.node));
        }
    }
    vol.head = Some(target.to_string());
    vol.fenced = false;
    vol.assembly = if vol.legs.iter().all(|l| l.state == LegState::Created) {
        AssemblyState::Assembled
    } else {
        AssemblyState::Degraded
    };
    vol.export = crate::model::Export {
        state: vol.export.state,
        volume_id: served.clone(),
        node: Some(target.to_string()),
        master_node: None,
        coordinates: vol.export.coordinates.clone(),
        published_at: vol.export.published_at,
        coordinates_changed: false,
        message: None,
        adopted: true,
        // Re-served to on the new head by the publish that follows.
        hosts: vol.export.hosts.clone(),
        per_host: vol.export.per_host,
        local_id: served.clone(),
        withdrawing: vol.export.withdrawing.clone(),
        gone: false,
    };
    {
        let mut fed = state.fed.write().await;
        if !old.is_empty() {
            fed.stale_heads.push(StaleHead {
                node: old.clone(),
                array_id: array_id.clone(),
                drive_uris: old_uris,
                of_volume: name.to_string(),
                epoch: vol.epoch,
                since: SystemTime::now(),
            });
        }
        fed.volumes.insert(name.to_string(), vol);
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    event(
        state,
        name,
        Severity::Info,
        format!("{name}: promoted {target} to head at epoch {fenced_epoch} (was {old}); array {array_id} assembled from {} legs", surviving.len()),
    )
    .await;
    match served {
        Some(_) => {
            let _ = crate::orchestrate::publish(state, name).await;
        }
        None => {
            let msg = format!(
                "{name}: the served volume {mirror} did not come across with the array on {target} — not serving \
                 (an empty volume in its place would hide the data)"
            );
            {
                let mut fed = state.fed.write().await;
                if let Some(v) = fed.volumes.get_mut(name) {
                    v.export.state = crate::model::ExportState::Failed;
                    v.export.message = Some(msg.clone());
                }
            }
            state.persist().await;
            event(state, name, Severity::Error, msg).await;
        }
    }
    Ok(view_of(state, name).await)
}

/// Open the legs `idxs` on `target` for it as head and put the volume's
/// array back together from their superblocks
/// (`POST /api/v1/arrays/assemble`, stormblock#252) — the same array, not
/// a new one. Records each leg's export, drive and member uuid, sets the
/// rebuild rate, and returns the served volume that came across with the
/// array's slab (`<name>-mirror`), if it did. Shared by promote (a new
/// head) and reassemble (#15: the same head after its engine restarted).
async fn assemble_on(
    state: &Arc<AppState>,
    vol: &mut DistVolume,
    idxs: &[usize],
    target: &str,
    target_engine: &crate::engine::Engine,
    engines: &std::collections::BTreeMap<String, crate::engine::Engine>,
) -> Result<Option<String>, Refusal> {
    let array_id = vol.array_id.clone().ok_or_else(|| Refusal::Conflict(format!("{}: no array id", vol.name)))?;
    let mut drive_uuids = Vec::new();
    let host = state.config.legs.host_nqn_for(target);
    for &i in idxs {
        let leg = &mut vol.legs[i];
        let engine = engines
            .get(&leg.node)
            .ok_or_else(|| Refusal::Upstream(format!("{}: unreachable", leg.node)))?;
        let vid = leg
            .volume_id
            .clone()
            .ok_or_else(|| Refusal::Conflict(format!("{}: leg has no volume", leg.node)))?;
        let master = leg.master_node.clone().unwrap_or_else(|| "localhost".into());
        let att = engine
            .attach_leg(&vid, &master, leg.epoch, Some(&host))
            .await
            .map_err(|e| Refusal::Upstream(format!("{}: attach leg: {e:#}", leg.node)))?;
        let uri = att.drive_uri();
        leg.export = Some(att);
        let uuid = target_engine
            .add_drive_idempotent(&uri)
            .await
            .map_err(|e| Refusal::Upstream(format!("{target}: open {uri}: {e:#}")))?;
        leg.drive_uuid = Some(uuid.clone());
        drive_uuids.push(uuid);
    }
    let report = target_engine
        .assemble_arrays(&drive_uuids)
        .await
        .map_err(|e| Refusal::Upstream(format!("{target}: assemble: {e:#}")))?;
    let found = report
        .get("arrays")
        .and_then(|a| a.as_array())
        .is_some_and(|a| {
            a.iter().any(|x| {
                x.get("id").and_then(|i| i.as_str()).map(|i| i.eq_ignore_ascii_case(&array_id)) == Some(true)
            })
        });
    if !found {
        return Err(Refusal::Upstream(format!(
            "{target}: the legs did not assemble into array {array_id}: {}",
            report.get("refused").cloned().unwrap_or_default()
        )));
    }
    // Members as this head numbers them.
    if let Ok(arr) = target_engine.get_array(&array_id).await {
        let r = parse_array(target, &array_id, &arr);
        for &i in idxs {
            let uri = vol.legs[i].export.as_ref().map(|x| x.drive_uri());
            if let Some(m) = r.members.iter().find(|m| Some(&m.device_path) == uri.as_ref()) {
                vol.legs[i].member_uuid = Some(m.uuid.clone());
            }
        }
    }
    let _ = target_engine.set_rebuild_rate(&array_id, state.config.recovery.rate(vol.bandwidth_class)).await;
    // The served volume came across with the array's slab. Never make a
    // new one in its place: an empty volume under the old name would read
    // as the consumer's data gone.
    let mirror = format!("{}{}", vol.name, crate::orchestrate::MIRROR_SUFFIX);
    Ok(target_engine
        .list_engine_volumes()
        .await
        .ok()
        .and_then(|vs| vs.into_iter().find(|v| v.name == mirror))
        .map(|v| v.id))
}

/// What a reassembly did (#15).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reassembled {
    /// The head still holds the array: nothing to do.
    Present,
    /// The head's arrays could not be read: try at the next poll.
    Unread,
    /// Put back together from `legs` legs; `served` = the served volume
    /// came across with it.
    Done { legs: usize, served: bool },
}

/// Put a volume's array back together on its **own** head after the
/// head's engine restarted and forgot it (#15): stormblock reassembles
/// arrays on runtime `nvme-tcp://` drives only when asked
/// (`POST /api/v1/arrays/assemble`, stormblock#252). The legs are opened
/// again for the head, the same array (same id) comes back with its slab,
/// and the served volume on it is served again. A head leg marked lost
/// while the head was away (#26) is taken back in. Never creates an array:
/// that would format over the legs.
pub async fn reassemble(state: &Arc<AppState>, name: &str) -> Result<Reassembled, Refusal> {
    let (mut vol, head, head_engine, engines) = {
        let fed = state.fed.read().await;
        let vol = fed
            .volumes
            .get(name)
            .cloned()
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        if !matches!(vol.assembly, AssemblyState::Assembled | AssemblyState::Degraded) {
            return Err(Refusal::Conflict(format!("{name}: not assembled")));
        }
        if vol.fenced || vol.replacing.is_some() || vol.dual_attach.is_some() {
            return Err(Refusal::Conflict(format!("{name}: fenced, replacing a leg or in a dual-attach window")));
        }
        let head = vol.head.clone().ok_or_else(|| Refusal::Conflict(format!("{name}: no head")))?;
        let head_node = fed
            .nodes
            .get(&head)
            .filter(|n| n.status.healthy)
            .ok_or_else(|| Refusal::Upstream(format!("{name}: head {head} is unreachable")))?;
        let head_engine = state.engine_for(head_node);
        let mut engines = std::collections::BTreeMap::new();
        for l in &vol.legs {
            if let Some(n) = fed.nodes.get(&l.node).filter(|n| n.status.healthy) {
                engines.insert(l.node.clone(), state.engine_for(n));
            }
        }
        (vol, head, head_engine, engines)
    };
    let array_id = vol.array_id.clone().ok_or_else(|| Refusal::Conflict(format!("{name}: no array id")))?;
    match head_engine.find_array(&array_id).await {
        Ok(Some(_)) => return Ok(Reassembled::Present),
        Ok(None) => {}
        Err(_) => return Ok(Reassembled::Unread),
    }
    // Every leg whose node answers: created ones, and the head's own leg
    // if it was marked lost while the head was away.
    let legs: Vec<usize> = (0..vol.legs.len())
        .filter(|&i| {
            let l = &vol.legs[i];
            engines.contains_key(&l.node)
                && (l.state == LegState::Created || (l.state == LegState::Lost && l.node == head))
        })
        .collect();
    if legs.is_empty() {
        return Err(Refusal::Conflict(format!("{name}: no leg answers")));
    }
    let served = assemble_on(state, &mut vol, &legs, &head, &head_engine, &engines).await?;
    for &i in &legs {
        vol.legs[i].state = LegState::Created;
        vol.legs[i].message = None;
    }
    vol.assembly = if vol.legs.iter().all(|l| l.state == LegState::Created) {
        AssemblyState::Assembled
    } else {
        AssemblyState::Degraded
    };
    vol.next_assemble_after = None;
    let mirror = format!("{name}{}", crate::orchestrate::MIRROR_SUFFIX);
    let was_served = vol.export.state != crate::model::ExportState::None;
    match &served {
        Some(id) => {
            // Came back with the slab: the engine-local id, attached
            // through /api/v1 like a promoted head's.
            vol.export.volume_id = Some(id.clone());
            vol.export.local_id = Some(id.clone());
            vol.export.node = Some(head.clone());
            vol.export.master_node = None;
            vol.export.adopted = true;
        }
        None if was_served => {
            vol.export.state = crate::model::ExportState::Failed;
            vol.export.gone = true;
            vol.export.message = Some(format!(
                "{name}: the served volume {mirror} did not come back with array {array_id} on {head}; its data is \
                 not recreated — POST /api/v1/volumes/{name}/export {{\"recreate\": true}} serves a new, EMPTY one"
            ));
        }
        None => {}
    }
    let gone_msg = vol.export.message.clone().filter(|_| vol.export.gone);
    store(state, vol).await;
    event(
        state,
        name,
        Severity::Info,
        format!("{name}: array {array_id} reassembled on {head} from {} legs after its engine restarted", legs.len()),
    )
    .await;
    if served.is_some() && was_served {
        let _ = crate::orchestrate::publish(state, name).await;
    }
    if let Some(m) = gone_msg {
        event(state, name, Severity::Error, m).await;
    }
    Ok(Reassembled::Done { legs: legs.len(), served: served.is_some() })
}

/// The stormblock issue for releasing an array without writing to its
/// members, which a handover from a live head needs.
pub const RELEASE_ISSUE: u32 = 296;

async fn reattach_legs(
    state: &Arc<AppState>,
    vol: &mut DistVolume,
    engines: &std::collections::BTreeMap<String, crate::engine::Engine>,
    head: &str,
) {
    let host = state.config.legs.host_nqn_for(head);
    for leg in vol.legs.iter_mut().filter(|l| l.state == LegState::Created) {
        let (Some(engine), Some(vid)) = (engines.get(&leg.node), leg.volume_id.clone()) else {
            continue;
        };
        let master = leg.master_node.clone().unwrap_or_else(|| "localhost".into());
        if let Ok(att) = engine.attach_leg(&vid, &master, leg.epoch, Some(&host)).await {
            leg.export = Some(att);
        }
    }
}

async fn store(state: &Arc<AppState>, vol: DistVolume) {
    {
        let mut fed = state.fed.write().await;
        fed.volumes.insert(vol.name.clone(), vol);
        fed.revision += 1;
    }
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
}

pub async fn view_of(state: &Arc<AppState>, name: &str) -> serde_json::Value {
    let fed = state.fed.read().await;
    let heads = state.heads.read().await;
    fed.volumes.get(name).map(|v| view(v, heads.get(name))).unwrap_or_default()
}

/// The slave a prestage replaces when the caller names none: the one lost
/// leg, else the only slave (pure).
pub fn prestage_from(vol: &DistVolume) -> Result<String, Refusal> {
    let slaves: Vec<&crate::model::Leg> =
        vol.legs.iter().filter(|l| Some(&l.node) != vol.head.as_ref()).collect();
    if let Some(l) = slaves.iter().find(|l| l.state == LegState::Lost) {
        return Ok(l.node.clone());
    }
    match slaves.as_slice() {
        [one] => Ok(one.node.clone()),
        [] => Err(Refusal::Conflict(format!("{}: no slave leg", vol.name))),
        _ => Err(Refusal::Conflict(format!("{}: several slave legs — name the one to replace (`from`)", vol.name))),
    }
}

/// `POST /api/v1/volumes/{name}/prestage {node?, from?, bandwidth_class?}`:
/// replace a slave leg with a new one on `node` (placement picks when
/// absent), its resync capped by the volume's bandwidth class.
pub async fn prestage(
    state: &Arc<AppState>,
    name: &str,
    node: Option<String>,
    from: Option<String>,
    class: Option<BandwidthClass>,
) -> Result<serde_json::Value, Refusal> {
    let (from, head_engine, array_id, class) = {
        let mut fed = state.fed.write().await;
        let vol = fed
            .volumes
            .get_mut(name)
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        if node.is_some() && node == vol.head {
            return Err(Refusal::Conflict(format!(
                "{name}: {} is the head (anti-affinity: the slave goes elsewhere)",
                node.unwrap_or_default()
            )));
        }
        let from = match from {
            Some(f) if Some(&f) == vol.head.as_ref() => {
                return Err(Refusal::Conflict(format!("{name}: {f} is the head — promote moves the master")))
            }
            Some(f) => f,
            None => prestage_from(vol)?,
        };
        if let Some(c) = class {
            vol.bandwidth_class = c;
        }
        let class = vol.bandwidth_class;
        let head_engine = vol
            .head
            .clone()
            .and_then(|h| fed.nodes.get(&h).cloned())
            .map(|n| state.engine_for(&n));
        let array_id = fed.volumes.get(name).and_then(|v| v.array_id.clone());
        fed.revision += 1;
        (from, head_engine, array_id, class)
    };
    state.persist().await;
    if let (Some(e), Some(a)) = (&head_engine, &array_id) {
        let _ = e.set_rebuild_rate(a, state.config.recovery.rate(class)).await;
    }
    let to = crate::orchestrate::start_replacement(state, name, &from, node, "prestage")
        .await
        .map_err(|e| Refusal::Conflict(format!("{e:#}")))?;
    Ok(serde_json::json!({ "replacing": from, "to": to, "bandwidth_class": class }))
}

fn ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Open a dual-attach window on the record (pure). Idempotent for the
/// same target (the expiry moves); 409 for another target or one holding
/// no leg.
pub fn apply_open_window(
    vol: &mut DistVolume,
    target: &str,
    ttl: Duration,
    now: SystemTime,
) -> Result<DualAttach, Refusal> {
    if !matches!(vol.assembly, AssemblyState::Assembled | AssemblyState::Degraded) {
        return Err(Refusal::Conflict(format!("{}: not assembled", vol.name)));
    }
    if let Some(w) = &vol.dual_attach {
        if w.target_node != target {
            return Err(Refusal::Conflict(format!(
                "{}: a window is open for {}",
                vol.name, w.target_node
            )));
        }
    }
    if Some(target) == vol.head.as_deref() {
        return Err(Refusal::Conflict(format!("{}: {target} is already the head", vol.name)));
    }
    if !vol.legs.iter().any(|l| l.node == target && l.state == LegState::Created) {
        return Err(Refusal::Conflict(format!("{}: {target} holds no slave", vol.name)));
    }
    let w = DualAttach {
        target_node: target.to_string(),
        epoch: vol.epoch,
        opened_at: vol.dual_attach.as_ref().map(|w| w.opened_at).unwrap_or(now),
        expires_at: now + ttl,
    };
    vol.dual_attach = Some(w.clone());
    Ok(w)
}

/// `POST /api/v1/volumes/{name}/dual-attach {target_node, ttl_secs}`.
pub async fn open_window(
    state: &Arc<AppState>,
    name: &str,
    target: &str,
    ttl_secs: u64,
) -> Result<serde_json::Value, Refusal> {
    let max = state.config.recovery.max_dual_attach_secs.max(1);
    let ttl = Duration::from_secs(ttl_secs.clamp(1, max));
    let w = {
        let mut fed = state.fed.write().await;
        let vol = fed
            .volumes
            .get_mut(name)
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        let w = apply_open_window(vol, target, ttl, SystemTime::now())?;
        fed.revision += 1;
        w
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    event(
        state,
        name,
        Severity::Info,
        format!("{name}: dual-attach window open for {target} at epoch {}, {}s", w.epoch, ttl.as_secs()),
    )
    .await;
    Ok(serde_json::json!({
        "volume_id": name,
        "epoch": w.epoch,
        "target_node": w.target_node,
        "expires_at_ms": ms(w.expires_at),
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Commit,
    Abort,
}

/// `POST /api/v1/volumes/{name}/dual-attach/close {epoch, outcome}`:
/// commit = fence + promote the window's target; abort = close.
pub async fn close_window(
    state: &Arc<AppState>,
    name: &str,
    epoch: u64,
    outcome: Outcome,
) -> Result<serde_json::Value, Refusal> {
    let target = {
        let mut fed = state.fed.write().await;
        let vol = fed
            .volumes
            .get_mut(name)
            .ok_or_else(|| Refusal::NotFound(format!("volume {name:?}")))?;
        let w = vol
            .dual_attach
            .clone()
            .ok_or_else(|| Refusal::Conflict(format!("{name}: no dual-attach window is open")))?;
        if w.epoch != epoch {
            return Err(Refusal::StaleEpoch(w.epoch));
        }
        vol.dual_attach = None;
        fed.revision += 1;
        w.target_node
    };
    crate::replicate::push_to_peers(state.clone());
    state.persist().await;
    match outcome {
        Outcome::Abort => {
            event(state, name, Severity::Info, format!("{name}: dual-attach window for {target} aborted")).await;
            Ok(view_of(state, name).await)
        }
        Outcome::Commit => {
            let fenced = fence(state, name, epoch).await?;
            event(state, name, Severity::Info, format!("{name}: dual-attach commit — promoting {target}")).await;
            promote(state, name, &target, fenced.epoch).await
        }
    }
}

/// Close every window past its expiry (reconciler). Returns the volumes
/// whose window it closed.
pub fn expire_windows(fed: &mut crate::model::FedState, now: SystemTime) -> Vec<(String, String)> {
    let mut closed = Vec::new();
    for v in fed.volumes.values_mut() {
        if v.dual_attach.as_ref().is_some_and(|w| w.expires_at <= now) {
            let w = v.dual_attach.take().expect("checked");
            closed.push((v.name.clone(), w.target_node));
        }
    }
    if !closed.is_empty() {
        fed.revision += 1;
    }
    closed
}

/// Reaped former heads: once its engine answers, a former head that no
/// longer holds the array (it restarted) has its leftover leg drives
/// closed. One that still holds it is left alone and reported — closing
/// its drives would write failure marks into the legs (see promote).
pub async fn reap_stale_heads(state: &Arc<AppState>) {
    let due: Vec<(StaleHead, crate::engine::Engine)> = {
        let fed = state.fed.read().await;
        fed.stale_heads
            .iter()
            .filter_map(|s| {
                fed.nodes
                    .get(&s.node)
                    .filter(|n| n.status.healthy)
                    .map(|n| (s.clone(), state.engine_for(n)))
            })
            .collect()
    };
    for (s, engine) in due {
        let Ok(arrays) = engine.list_arrays().await else { continue };
        let holds = arrays.iter().any(|a| {
            a.get("id").and_then(|i| i.as_str()).map(|i| i.eq_ignore_ascii_case(&s.array_id)) == Some(true)
        });
        if holds {
            let key = format!("stale-head:{}:{}", s.node, s.of_volume);
            if state.noted.lock().expect("noted lock").insert(key) {
                event(
                    state,
                    &s.of_volume,
                    Severity::Error,
                    format!(
                        "{}: former head {} answers and still holds array {} — left alone; its writes to the legs \
                         are refused only once stormblock#6 lands, and it can let go only once the engine can \
                         release an array without writing (stormblock#{RELEASE_ISSUE})",
                        s.of_volume, s.node, s.array_id
                    ),
                )
                .await;
            }
            continue;
        }
        for uri in &s.drive_uris {
            let _ = engine.delete_drive(uri, true).await;
        }
        {
            let mut fed = state.fed.write().await;
            fed.stale_heads.retain(|x| !(x.node == s.node && x.of_volume == s.of_volume));
            fed.revision += 1;
        }
        crate::replicate::push_to_peers(state.clone());
        state.persist().await;
        event(
            state,
            &s.of_volume,
            Severity::Info,
            format!("{}: former head {} cleaned up (no array held; leg drives closed)", s.of_volume, s.node),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Export, Leg};

    fn leg(node: &str, member: &str) -> Leg {
        Leg {
            node: node.into(),
            volume_id: Some(format!("{node}-v")),
            state: LegState::Created,
            message: None,
            master_node: Some(node.into()),
            export: None,
            drive_uuid: None,
            member_uuid: Some(member.into()),
            epoch: None,
        }
    }

    fn vol() -> DistVolume {
        DistVolume {
            name: "v".into(),
            size_bytes: 100,
            pool: None,
            replicas: 2,
            rung: "node".into(),
            legs: vec![leg("a", "ma"), leg("b", "mb")],
            assembly: AssemblyState::Assembled,
            head: Some("a".into()),
            array_id: Some("arr".into()),
            created_at: SystemTime::now(),
            replacing: None,
            next_releg_after: None,
            next_assemble_after: None,
            export: Export::default(),
            epoch: 1,
            fenced: false,
            bandwidth_class: BandwidthClass::Normal,
            dual_attach: None,
            migration: None,
        }
    }

    fn reading(b_state: &str, rebuilt: Option<u64>) -> ArrayReading {
        parse_array(
            "a",
            "arr",
            &serde_json::json!({
                "member_data_bytes": 1000,
                "status": {"state": "rebuilding", "rebuild": {"running": true, "rate_bytes_per_sec": 77}},
                "members": [
                    {"uuid": "ma", "state": "Active", "device_path": "x"},
                    {"uuid": "mb", "state": b_state, "device_path": "y", "rebuilt_bytes": rebuilt}
                ]
            }),
        )
    }

    #[test]
    fn sync_from_the_head_array() {
        let v = vol();
        let r = reading("Rebuilding", Some(250));
        assert_eq!(r.rebuild_bytes_per_sec, Some(77));
        let reps = replicas(&v, Some(&r));
        assert_eq!(reps[0], Replica { node: "a".into(), role: Role::Master, sync: SyncState::InSync });
        assert_eq!(
            reps[1].sync,
            SyncState::Resyncing { progress_pct: 25.0, lag_bytes: 750 }
        );
        assert_eq!(reps[1].role, Role::Slave);
        assert_eq!(health(&v, &reps), Health::Degraded);
        let reps = replicas(&v, Some(&reading("active", None)));
        assert_eq!(health(&v, &reps), Health::Healthy);
        // The /v1 JSON shape, tag "state".
        assert_eq!(
            serde_json::to_value(&reps[1]).unwrap(),
            serde_json::json!({"node": "b", "role": "slave", "sync": {"state": "in_sync"}})
        );
    }

    #[test]
    fn no_reading_is_never_in_sync() {
        let v = vol();
        let reps = replicas(&v, None);
        assert!(reps.iter().all(|r| r.sync == SyncState::Detached));
        assert_eq!(health(&v, &reps), Health::Faulted);
        // A reading of another head (before a promote) does not count.
        let mut r = reading("active", None);
        r.head = "b".into();
        assert!(replicas(&v, Some(&r)).iter().all(|r| r.sync == SyncState::Detached));
        // A lost leg is detached whatever the array says.
        let mut v = vol();
        v.legs[1].state = LegState::Lost;
        assert_eq!(replicas(&v, Some(&reading("active", None)))[1].sync, SyncState::Detached);
    }

    #[test]
    fn single_leg_and_pending() {
        let mut v = vol();
        v.legs.truncate(1);
        v.replicas = 1;
        v.assembly = AssemblyState::SingleLeg;
        let reps = replicas(&v, None);
        assert_eq!(reps, vec![Replica { node: "a".into(), role: Role::Master, sync: SyncState::InSync }]);
        let mut v = vol();
        v.assembly = AssemblyState::PendingEngineSupport;
        assert!(replicas(&v, None).iter().all(|r| r.sync == SyncState::Detached));
    }

    #[test]
    fn fence_is_a_cas() {
        let mut v = vol();
        assert_eq!(apply_fence(&mut v, 1).unwrap(), 2);
        assert!(v.fenced);
        // Two racing fencers: the second presents the epoch it read.
        assert!(matches!(apply_fence(&mut v, 1), Err(Refusal::StaleEpoch(2))));
        assert_eq!(apply_fence(&mut v, 2).unwrap(), 3);
        let mut s = vol();
        s.assembly = AssemblyState::SingleLeg;
        assert!(matches!(apply_fence(&mut s, 1), Err(Refusal::Conflict(_))));
    }

    #[test]
    fn promote_needs_the_fenced_epoch_and_a_leg() {
        let mut v = vol();
        // No fence before it: 412.
        assert!(matches!(check_promote(&v, "b", 1, true), Err(Refusal::StaleEpoch(1))));
        apply_fence(&mut v, 1).unwrap();
        assert!(matches!(check_promote(&v, "b", 1, true), Err(Refusal::StaleEpoch(2))));
        assert!(matches!(check_promote(&v, "c", 2, true), Err(Refusal::Conflict(_))));
        assert!(matches!(check_promote(&v, "b", 2, false), Err(Refusal::Conflict(_))));
        assert_eq!(check_promote(&v, "b", 2, true).unwrap(), Some("a".into()));
        // An open window blocks a promote: close/commit is the only cutover.
        apply_open_window(&mut v, "b", Duration::from_secs(5), SystemTime::now()).unwrap();
        assert!(matches!(check_promote(&v, "b", 2, true), Err(Refusal::Conflict(_))));
    }

    #[test]
    fn window_rules_and_expiry() {
        let mut v = vol();
        let now = SystemTime::now();
        assert!(matches!(apply_open_window(&mut v, "a", Duration::from_secs(5), now), Err(Refusal::Conflict(_))));
        assert!(matches!(apply_open_window(&mut v, "c", Duration::from_secs(5), now), Err(Refusal::Conflict(_))));
        let w = apply_open_window(&mut v, "b", Duration::from_secs(5), now).unwrap();
        assert_eq!(w.epoch, 1);
        // Idempotent for the same target; the expiry moves.
        let w2 = apply_open_window(&mut v, "b", Duration::from_secs(50), now).unwrap();
        assert_eq!(w2.opened_at, w.opened_at);
        assert!(w2.expires_at > w.expires_at);
        let mut fed = crate::model::FedState::default();
        fed.volumes.insert("v".into(), v);
        assert!(expire_windows(&mut fed, now).is_empty());
        let closed = expire_windows(&mut fed, now + Duration::from_secs(60));
        assert_eq!(closed, vec![("v".to_string(), "b".to_string())]);
        assert!(fed.volumes["v"].dual_attach.is_none());
    }

    #[test]
    fn prestage_picks_the_slave() {
        let mut v = vol();
        assert_eq!(prestage_from(&v).unwrap(), "b");
        v.legs.push(leg("c", "mc"));
        assert!(prestage_from(&v).is_err());
        v.legs[2].state = LegState::Lost;
        assert_eq!(prestage_from(&v).unwrap(), "c");
    }

    #[test]
    fn rates_by_class() {
        let r = crate::config::RecoveryConfig::default();
        assert_eq!(r.rate(BandwidthClass::Unthrottled), 0);
        assert!(r.rate(BandwidthClass::Low) < r.rate(BandwidthClass::Normal));
        assert!(r.rate(BandwidthClass::Normal) < r.rate(BandwidthClass::High));
    }

    fn sb(member: &str, events: u64, slots: &[(&str, &str, u64)]) -> LegSuperblock {
        LegSuperblock {
            array_uuid: "arr".into(),
            member_uuid: member.into(),
            events,
            data_size: 1000,
            slots: slots
                .iter()
                .map(|(m, st, rb)| SlotRecord { member_uuid: m.to_string(), state: st.to_string(), rebuilt_to: *rb })
                .collect(),
        }
    }

    fn live(events: u64, b_state: &str, rebuilt: Option<u64>) -> ArrayReading {
        let mut r = reading(b_state, rebuilt);
        r.events = Some(events);
        r
    }

    fn sync_of(v: &DistVolume, r: &ArrayReading, node: &str) -> SyncState {
        replicas(v, Some(r)).into_iter().find(|x| x.node == node).unwrap().sync
    }

    #[test]
    fn parse_a_leg_superblock() {
        let s = parse_superblock(&serde_json::json!({
            "array_uuid": "ARR", "member_uuid": "mb", "slot": 1, "level": "raid1",
            "events": 42, "update_time": 1, "data_size": 1000,
            "slots": [{"slot": 0, "member_uuid": "ma", "state": "active", "rebuilt_to": 0},
                      {"slot": 1, "member_uuid": "mb", "state": "rebuilding", "rebuilt_to": 250}]
        }))
        .unwrap();
        assert_eq!(s, sb("mb", 42, &[("ma", "active", 0), ("mb", "rebuilding", 250)]));
        assert!(parse_superblock(&serde_json::json!({"code": "no_superblock"})).is_none());
    }

    /// The head is gone: the slave's superblock, as new as the last live
    /// reading and agreeing with it, says it is in sync (#48).
    #[test]
    fn head_lost_slave_in_sync_from_its_superblock() {
        let mut v = vol();
        v.legs[0].state = LegState::Lost;
        v.assembly = AssemblyState::Degraded;
        let last = live(10, "active", None);
        let b = sb("mb", 10, &[("ma", "active", 0), ("mb", "active", 0)]);
        let r = from_superblocks(&v, Some(&last), &[b], SystemTime::now()).unwrap();
        assert_eq!(r.source, SyncSource::Superblock);
        assert_eq!(r.head_read_at, Some(last.read_at));
        assert_eq!(sync_of(&v, &r, "b"), SyncState::InSync);
        assert_eq!(sync_of(&v, &r, "a"), SyncState::Detached);
        assert_eq!(health(&v, &replicas(&v, Some(&r))), Health::Degraded);
        let view = view(&v, Some(&r));
        assert_eq!(view["sync_source"], "superblock");
        // No live reading since a restart: the superblock alone.
        let r = from_superblocks(&v, None, &[sb("mb", 10, &[("mb", "active", 0)])], SystemTime::now()).unwrap();
        assert_eq!(sync_of(&v, &r, "b"), SyncState::InSync);
    }

    #[test]
    fn older_or_contradicted_superblocks_are_not_in_sync() {
        let v = vol();
        let both = [("ma", "active", 0), ("mb", "active", 0)];
        // Older than the last live reading.
        let r = from_superblocks(&v, Some(&live(10, "active", None)), &[sb("mb", 9, &both)], SystemTime::now());
        assert_eq!(sync_of(&v, &r.unwrap(), "b"), SyncState::Detached);
        // The last reading had it rebuilding at the same count: resyncing.
        let r = from_superblocks(&v, Some(&live(10, "rebuilding", Some(250))), &[sb("mb", 10, &both)], SystemTime::now());
        assert_eq!(
            sync_of(&v, &r.unwrap(), "b"),
            SyncState::Resyncing { progress_pct: 25.0, lag_bytes: 750 }
        );
        // …but a newer superblock (the rebuild finished after it) wins.
        let r = from_superblocks(&v, Some(&live(10, "rebuilding", Some(250))), &[sb("mb", 11, &both)], SystemTime::now());
        assert_eq!(sync_of(&v, &r.unwrap(), "b"), SyncState::InSync);
        // Another array's superblock is no evidence at all.
        let mut other = sb("mb", 10, &both);
        other.array_uuid = "zzz".into();
        assert!(from_superblocks(&v, None, &[other], SystemTime::now()).is_none());
    }

    #[test]
    fn newest_superblocks_must_all_agree() {
        let mut v = vol();
        v.legs.push(leg("c", "mc"));
        v.legs[0].state = LegState::Lost;
        // c (newest) recorded b failed; b's own superblock still says active.
        let b = sb("mb", 12, &[("ma", "active", 0), ("mb", "active", 0), ("mc", "active", 0)]);
        let c = sb("mc", 12, &[("ma", "active", 0), ("mb", "failed", 0), ("mc", "active", 0)]);
        let r = from_superblocks(&v, None, &[b.clone(), c.clone()], SystemTime::now()).unwrap();
        assert_eq!(sync_of(&v, &r, "b"), SyncState::Detached);
        assert_eq!(sync_of(&v, &r, "c"), SyncState::InSync);
        // b behind c: stale, whatever it says of itself.
        let b_old = sb("mb", 11, &b.slots.iter().map(|s| (s.member_uuid.as_str(), "active", 0u64)).collect::<Vec<_>>());
        let r = from_superblocks(&v, None, &[b_old, sb("mc", 12, &[("mc", "active", 0)])], SystemTime::now()).unwrap();
        assert_eq!(sync_of(&v, &r, "b"), SyncState::Detached);
        // A newest superblock that no longer lists c: removed.
        let r = from_superblocks(&v, None, &[sb("mb", 12, &[("mb", "active", 0)]), sb("mc", 12, &[("mc", "active", 0)])], SystemTime::now()).unwrap();
        assert_eq!(sync_of(&v, &r, "b"), SyncState::Detached);
        assert_eq!(sync_of(&v, &r, "c"), SyncState::Detached);
    }
}
