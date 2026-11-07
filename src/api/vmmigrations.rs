//! Live migration of a node volume — a VM's disk — to another node (#44).
//! The steps are in `crate::vmmigrate`.

use super::{ApiError, AppState};
use crate::model::VmMigration;
use crate::vmmigrate::{self, Refused};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

static SEQ: AtomicU32 = AtomicU32::new(0);

fn new_id() -> String {
    let ms = crate::backup::now_ms(std::time::SystemTime::now());
    format!("vm-{ms}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// A step's error as the API answers it: a refusal or an engine's 409 is a
/// 409, a missing migration a 404, anything else the engine's (502).
fn answer(e: anyhow::Error) -> ApiError {
    if let Some(r) = e.downcast_ref::<Refused>() {
        return ApiError::conflict(r.0.clone());
    }
    match crate::engine::status_of(&e) {
        Some(StatusCode::CONFLICT) => ApiError::conflict(format!("{e:#}")),
        Some(StatusCode::NOT_FOUND) if e.to_string().starts_with("migration ") => ApiError::not_found(format!("{e:#}")),
        _ => ApiError::upstream(format!("{e:#}")),
    }
}

fn view(m: &VmMigration) -> Value {
    let mut v = serde_json::to_value(m).unwrap_or_default();
    // What the kubelet attaches on T (over ublk) once cut over.
    v["target"] = json!({
        "node": m.target_node,
        "name": m.target_name,
        "id": m.target_local_id,
    });
    v
}

/// Run `f` holding the migration's claim (409 while another step runs).
async fn claimed<F, Fut>(s: &Arc<AppState>, id: &str, f: F) -> Result<Json<Value>, ApiError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<VmMigration>>,
{
    if !s.fed.read().await.vm_migrations.contains_key(id) {
        return Err(ApiError::not_found(format!("migration {id:?}")));
    }
    let key = vmmigrate::claim_key(id);
    if !crate::orchestrate::claim(s, &key) {
        return Err(ApiError::conflict(format!("{id}: another step is running")));
    }
    let r = f().await;
    crate::orchestrate::release(s, &key);
    r.map(|m| Json(view(&m))).map_err(answer)
}

#[derive(Deserialize)]
pub(super) struct MigrateBody {
    to: String,
    /// The volume's name on T; default S's name.
    #[serde(default)]
    name: Option<String>,
    /// Where on T (#106): exactly one of a RAID set, a slab or a drive.
    #[serde(default)]
    placement: Option<PlacementBody>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct PlacementBody {
    #[serde(default)]
    array: Option<String>,
    #[serde(default)]
    slab: Option<String>,
    #[serde(default)]
    drive: Option<String>,
}

impl PlacementBody {
    /// The one place named, as (kind, name).
    fn one(&self) -> Result<(&'static str, String), ApiError> {
        let named: Vec<(&'static str, String)> = [("array", &self.array), ("slab", &self.slab), ("drive", &self.drive)]
            .into_iter()
            .filter_map(|(k, v)| v.as_ref().filter(|v| !v.is_empty()).map(|v| (k, v.clone())))
            .collect();
        match named.as_slice() {
            [one] => Ok(one.clone()),
            _ => Err(ApiError::bad_request("placement: exactly one of \"array\", \"slab\" or \"drive\"")),
        }
    }
}

/// Find the named place on T and check it has room (#106).
async fn resolve_placement(
    t: &crate::engine::Engine,
    target: &str,
    body: &PlacementBody,
    size: u64,
) -> Result<crate::model::TargetPlacement, ApiError> {
    let (kind, name) = body.one()?;
    let up = |e: anyhow::Error| ApiError::upstream(format!("{target}: {e:#}"));
    let lookup = match kind {
        "array" => vmmigrate::find_array(&t.list_arrays().await.map_err(up)?, &name),
        "slab" => vmmigrate::find_slab(&t.list_slabs().await.map_err(up)?, &name),
        _ => vmmigrate::find_drive(&t.list_slabs().await.map_err(up)?, &name),
    };
    vmmigrate::check_placement(target, kind, &name, size, lookup).map_err(|r| match r {
        vmmigrate::PlaceRefusal::Missing(m) => ApiError::not_found(m),
        vmmigrate::PlaceRefusal::NoRoom(m) => ApiError::conflict(m),
        vmmigrate::PlaceRefusal::NotYet(m) => ApiError::not_implemented(m),
    })
}

/// POST /api/v1/nodes/{node}/volumes/{id}/migrate {to, name?, placement?}
/// — start mirroring S's volume (engine id or name) to a new volume on
/// `to`, carved on `placement`'s RAID set when named (#106). Attached or
/// not: the mirror works on S's engine volume, not on its consumer. All or
/// nothing: a step that fails undoes T's volume and keeps no record.
pub(super) async fn start(
    State(s): State<Arc<AppState>>,
    Path((node, vol)): Path<(String, String)>,
    Json(body): Json<MigrateBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if body.to == node {
        return Err(ApiError::bad_request("to: another node than the volume's"));
    }
    let (src, dst) = {
        let fed = s.fed.read().await;
        for n in [&node, &body.to] {
            if !fed.nodes.contains_key(n) {
                return Err(ApiError::not_found(format!("node {n:?}")));
            }
        }
        (
            crate::backup::engine_on(&s, &fed, &node).map_err(|e| ApiError::conflict(format!("{e:#}")))?,
            crate::backup::engine_on(&s, &fed, &body.to).map_err(|e| ApiError::conflict(format!("{e:#}")))?,
        )
    };
    let v = src.get_any_volume(&vol).await.map_err(|e| match crate::engine::status_of(&e) {
        Some(StatusCode::NOT_FOUND) => ApiError::not_found(format!("{node} holds no volume {vol:?}")),
        _ => ApiError::upstream(format!("{node}: {e:#}")),
    })?;
    let (Some(sid), Some(sname)) = (
        v.get("id").and_then(|x| x.as_str()).map(str::to_string),
        v.get("name").and_then(|x| x.as_str()).map(str::to_string),
    ) else {
        return Err(ApiError::upstream(format!("{node}: volume {vol:?} answered no id or name: {v}")));
    };
    let size = v
        .get("virtual_size_bytes")
        .and_then(|x| x.as_u64())
        .ok_or_else(|| ApiError::upstream(format!("{node}: volume {vol:?} answered no size")))?;
    if let Some(m) = vmmigrate::MirrorReading::of(&v) {
        return Err(ApiError::conflict(format!("{node}: {sname} is already mirrored to {} ({})", m.target, m.state)));
    }
    let target_name = body.name.clone().unwrap_or_else(|| sname.clone());
    match dst.local_volume_id(&target_name).await {
        Ok(_) => {
            return Err(ApiError::conflict(format!(
                "{} already holds a volume named {target_name:?}; name another with \"name\"",
                body.to
            )))
        }
        Err(e) if crate::engine::is_not_found(&e) => {}
        Err(e) => return Err(ApiError::upstream(format!("{}: {e:#}", body.to))),
    }
    let placement = match &body.placement {
        None => None,
        Some(p) => Some(resolve_placement(&dst, &body.to, p, size).await?),
    };
    let id = new_id();
    {
        let mut fed = s.fed.write().await;
        if let Some(open) = fed
            .vm_migrations
            .values()
            .find(|m| m.source_node == node && m.source_volume_id == sid && !m.state.terminal())
        {
            return Err(ApiError::conflict(format!("{sname} on {node} is in migration {} ({:?})", open.id, open.state)));
        }
        let now = crate::backup::now_ms(std::time::SystemTime::now());
        fed.vm_migrations.insert(
            id.clone(),
            VmMigration {
                id: id.clone(),
                source_node: node.clone(),
                source_volume_id: sid,
                source_name: sname,
                size_bytes: size,
                target_node: body.to.clone(),
                target_name,
                placement,
                started_at_ms: now,
                updated_at_ms: now,
                ..Default::default()
            },
        );
        fed.revision += 1;
    }
    let key = vmmigrate::claim_key(&id);
    crate::orchestrate::claim(&s, &key);
    s.persist().await;
    let r = vmmigrate::advance(&s, &id).await;
    if let Err(e) = r {
        let m = s.fed.read().await.vm_migrations.get(&id).cloned();
        if let Some(m) = m {
            if let Err(u) = vmmigrate::undo_target(&s, &m).await {
                tracing::warn!("{id}: undo {} on {}: {u:#}", m.target_name, m.target_node);
            }
        }
        {
            let mut fed = s.fed.write().await;
            fed.vm_migrations.remove(&id);
            fed.revision += 1;
        }
        crate::orchestrate::release(&s, &key);
        crate::replicate::push_to_peers(s.clone());
        s.persist().await;
        return Err(answer(e));
    }
    crate::orchestrate::release(&s, &key);
    let m = s.fed.read().await.vm_migrations.get(&id).cloned().unwrap_or_default();
    Ok((StatusCode::ACCEPTED, Json(view(&m))))
}

/// GET /api/v1/migrations — every live migration of a node volume.
pub(super) async fn list(State(s): State<Arc<AppState>>) -> Json<Value> {
    let all: Vec<Value> = s.fed.read().await.vm_migrations.values().map(view).collect();
    Json(json!({ "migrations": all }))
}

/// GET /api/v1/migrations/{id} — with a live reading of S's mirror.
pub(super) async fn get(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let m = s
        .fed
        .read()
        .await
        .vm_migrations
        .get(&id)
        .cloned()
        .ok_or_else(|| ApiError::not_found(format!("migration {id:?}")))?;
    Ok(Json(view(&vmmigrate::live(&s, m).await)))
}

pub(super) async fn cutover(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    claimed(&s, &id, || vmmigrate::cutover(&s, &id)).await
}

pub(super) async fn complete(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    claimed(&s, &id, || vmmigrate::complete(&s, &id)).await
}

pub(super) async fn abort(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    claimed(&s, &id, || vmmigrate::abort(&s, &id)).await
}

pub(super) async fn retry(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    claimed(&s, &id, || vmmigrate::retry(&s, &id)).await
}
