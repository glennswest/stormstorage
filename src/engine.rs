//! Client for one stormblock engine's management API. stormstorage only
//! ever *drives* engines — it is never in the data path.

use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone)]
pub struct Engine {
    url: String,
    token: Option<String>,
    /// The credential for the engine's destructive verbs (#47,
    /// stormblock#274): the admin token or a Kubernetes bearer allowed
    /// `storage.storm.io`. `None`: the node token, which an engine with
    /// `admin_gate = enforce` refuses for them.
    admin: Option<String>,
    http: reqwest::Client,
}

/// Where a leg's namespace answers: the attach coordinates the head node
/// turns into an `nvme-tcp://` drive URI.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttachedLeg {
    pub nqn: String,
    pub traddr: String,
    pub trsvcid: u16,
    pub nsid: u32,
    /// The host the namespace is served to (#27, stormblock#210): the head
    /// presents it on connect (`hostnqn=` in the drive URI). `None` for a
    /// leg attached before #27 or a consumer export — the shared subsystem.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_nqn: Option<String>,
}

impl AttachedLeg {
    pub fn drive_uri(&self) -> String {
        let mut u = format!(
            "nvme-tcp://{}:{}/{}?nsid={}",
            self.traddr, self.trsvcid, self.nqn, self.nsid
        );
        if let Some(h) = &self.host_nqn {
            u.push_str("&hostnqn=");
            u.push_str(h);
        }
        u
    }
}

/// An engine answered a call with an error status. Typed so a caller can
/// tell a 404 (the thing is gone, #40) from any other failure.
#[derive(Debug)]
pub struct HttpStatus {
    pub path: String,
    pub status: reqwest::StatusCode,
    pub message: String,
}

impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}: {}", self.path, self.status, self.message)
    }
}

impl std::error::Error for HttpStatus {}

/// What to set when an engine refuses a destructive verb (#47).
fn admin_hint(status: reqwest::StatusCode) -> &'static str {
    match status {
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
            " — a destructive verb (stormblock#274) needs the engine's admin token \
             ($STORMBLOCK_ADMIN_TOKEN or [local] admin_token_file) or a [kubernetes] bearer bound to storage-admin"
        }
        _ => "",
    }
}

/// The engine said 404 — check before the error is wrapped in context.
pub fn is_not_found(e: &anyhow::Error) -> bool {
    status_of(e) == Some(reqwest::StatusCode::NOT_FOUND)
}

/// The engine's error status, if `e` is one.
pub fn status_of(e: &anyhow::Error) -> Option<reqwest::StatusCode> {
    e.chain().find_map(|c| c.downcast_ref::<HttpStatus>().map(|h| h.status))
}

#[derive(Debug, Clone, Default)]
pub struct Capacity {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub topology: BTreeMap<String, String>,
}

/// What a /v1 fence answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceOutcome {
    /// Fenced; the volume's new epoch.
    Fenced(u64),
    /// 412: the epoch was not the expected one; the current one.
    Stale(u64),
}

/// The engine refused the call: 401 or 403. The token is wrong or missing,
/// so the same call will be refused again (#38, see `crate::refusal`).
#[derive(Debug)]
pub struct Refused {
    pub status: u16,
    pub path: String,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "engine refused {}: {} (engine token missing or wrong)", self.path, self.status)
    }
}

impl std::error::Error for Refused {}

/// Is this error the engine refusing our token?
pub fn is_refused(e: &anyhow::Error) -> bool {
    e.downcast_ref::<Refused>().is_some()
}

/// Turn a 401/403 into [`Refused`].
fn check_refused(resp: &reqwest::Response, path: &str) -> anyhow::Result<()> {
    let status = resp.status().as_u16();
    if status == 401 || status == 403 {
        return Err(Refused { status, path: path.to_string() }.into());
    }
    Ok(())
}

/// Reads and polls: short, so a dead node is marked unhealthy quickly.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Writes: see [`Engine::req`]. Array create formats a slab through the
/// RAID; on a loaded box that took 47 s for 512 MiB.
const MUTATE_TIMEOUT: Duration = Duration::from_secs(300);

impl Engine {
    pub fn new(url: &str, token: Option<String>) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            token,
            admin: None,
            http: reqwest::Client::builder()
                .timeout(READ_TIMEOUT)
                .build()
                .expect("reqwest client"),
        }
    }

    /// Present `admin` on the destructive verbs (#47).
    pub fn with_admin(mut self, admin: Option<String>) -> Self {
        self.admin = admin.filter(|t| !t.trim().is_empty());
        self
    }

    /// The management API's base URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        let r = self.http.get(format!("{}{path}", self.url));
        match &self.token {
            Some(t) if !t.is_empty() => r.bearer_auth(t),
            _ => r,
        }
    }

    /// Mutations (create, attach, arrays, deletes). A loaded engine can take
    /// well over the 5 s read timeout to answer one, and giving up early
    /// leaves the engine finishing a create nobody records.
    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let r = self
            .http
            .request(method, format!("{}{path}", self.url))
            .timeout(MUTATE_TIMEOUT);
        match &self.token {
            Some(t) if !t.is_empty() => r.bearer_auth(t),
            _ => r,
        }
    }

    /// GET /v1/nodes/capacity. The response is one NodeCapacity for an SNO
    /// node but may be a list or an object wrapper — parse tolerantly.
    pub async fn capacity(&self) -> anyhow::Result<Capacity> {
        let resp = self.get("/v1/nodes/capacity").send().await?;
        check_refused(&resp, "/v1/nodes/capacity")?;
        let v: Value = resp
            .error_for_status()?
            .json()
            .await?;
        let obj = first_capacity_object(&v)
            .ok_or_else(|| anyhow::anyhow!("unrecognized capacity shape: {v}"))?;
        let mut topology = BTreeMap::new();
        if let Some(t) = obj.get("topology").and_then(|t| t.as_object()) {
            for (k, val) in t {
                if let Some(s) = val.as_str() {
                    topology.insert(k.clone(), s.to_string());
                }
            }
        }
        Ok(Capacity {
            total_bytes: obj.get("total_bytes").and_then(|x| x.as_u64()).unwrap_or(0),
            free_bytes: obj.get("free_bytes").and_then(|x| x.as_u64()).unwrap_or(0),
            topology,
        })
    }

    /// POST /v1/volumes — name-idempotent create per the /v1 contract.
    /// `slaves: 0`: a leg is a standalone volume — cross-node redundancy is
    /// stormstorage's job (legs across nodes), not the engine's replica
    /// machinery; an SNO node has no peers to host a slave anyway.
    pub async fn create_volume(&self, name: &str, size_bytes: u64) -> anyhow::Result<Value> {
        let resp = self
            .req(reqwest::Method::POST, "/v1/volumes")
            .json(&serde_json::json!({
                "name": name,
                "size_bytes": size_bytes,
                "replica_tier": { "slaves": 0 },
            }))
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            anyhow::bail!(
                "create {name}: {status}: {}",
                body.get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("no message")
            );
        }
        Ok(body)
    }

    /// POST /v1/volumes pinned to an array on this engine (stormblock#150,
    /// v19.0.0): every extent on the array's dedicated slab, so the array's
    /// redundancy is the volume's. Name-idempotent like any /v1 create.
    pub async fn create_pinned_volume(
        &self,
        name: &str,
        size_bytes: u64,
        array_id: &str,
    ) -> anyhow::Result<Value> {
        self.v1_post(
            "/v1/volumes",
            serde_json::json!({
                "name": name,
                "size_bytes": size_bytes,
                "replica_tier": { "slaves": 0 },
                "placement": { "array_id": array_id },
            }),
        )
        .await
        .map_err(|e| anyhow::anyhow!("create {name} on array {array_id}: {e:#}"))
    }

    /// The engine's own node name for a /v1 volume — read-write attach is
    /// gated on asking as the master node.
    pub fn master_node_of(v: &Value) -> Option<String> {
        v.get("replicas")?
            .as_array()?
            .iter()
            .find(|r| r.get("role").and_then(|x| x.as_str()) == Some("master"))?
            .get("node")?
            .as_str()
            .map(|s| s.to_string())
    }

    /// A destructive verb (stormblock#274: array create/delete, members,
    /// drive close): with the admin credential when there is one (#47).
    fn admin_req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let r = self
            .http
            .request(method, format!("{}{path}", self.url))
            .timeout(MUTATE_TIMEOUT);
        match self.admin.as_ref().or(self.token.as_ref()) {
            Some(t) if !t.is_empty() => r.bearer_auth(t),
            _ => r,
        }
    }

    async fn v1_post(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        self.post_json(self.req(reqwest::Method::POST, path), path, body).await
    }

    /// Send a destructive verb with the admin credential; if the engine
    /// refuses it (401/403) and it is not the node token, once more with
    /// the node token — an engine from before stormblock#274 gates nothing
    /// and does not review Kubernetes bearers.
    async fn admin_send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> anyhow::Result<reqwest::Response> {
        let with = |rb: reqwest::RequestBuilder| match body {
            Some(b) => rb.json(b),
            None => rb,
        };
        let resp = with(self.admin_req(method.clone(), path)).send().await?;
        let refused = matches!(resp.status(), reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN);
        if refused && self.admin.is_some() && self.admin != self.token {
            return Ok(with(self.req(method, path)).send().await?);
        }
        Ok(resp)
    }

    async fn admin_post(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        let resp = self.admin_send(reqwest::Method::POST, path, Some(&body)).await?;
        Self::read_post(path, resp).await
    }

    async fn post_json(&self, rb: reqwest::RequestBuilder, path: &str, body: Value) -> anyhow::Result<Value> {
        let resp = rb.json(&body).send().await?;
        Self::read_post(path, resp).await
    }

    async fn read_post(path: &str, resp: reqwest::Response) -> anyhow::Result<Value> {
        let status = resp.status();
        let out: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(HttpStatus {
                path: path.to_string(),
                status,
                message: out
                    .get("message")
                    .or_else(|| out.get("error"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("no message")
                    .to_string(),
            }
            .into());
        }
        Ok(out)
    }

    /// POST /v1/volumes/{id}/attach — hot-add the volume as an NVMe-TCP
    /// namespace and return the attach coordinates. `node` must be the
    /// volume's master node (the engine's own name, captured at create).
    pub async fn attach_volume(&self, id: &str, node: &str) -> anyhow::Result<AttachedLeg> {
        self.attach_leg(id, node, None, None).await
    }

    /// [`Self::attach_volume`] for a RAID leg: served to `host_nqn` alone,
    /// the head that opens it (#27, stormblock#210: a closed engine refuses
    /// an attach that names no host; it serves the volume from that host's
    /// own subsystem, whose NQN the reply carries). With the leg's fencing
    /// epoch (#33): the attach contract of stormblock#6, under which an
    /// engine refuses an attach at an epoch other than the volume's (412
    /// `stale_epoch`). Engines before #6 / #210 ignore the fields.
    pub async fn attach_leg(
        &self,
        id: &str,
        node: &str,
        epoch: Option<u64>,
        host_nqn: Option<&str>,
    ) -> anyhow::Result<AttachedLeg> {
        // Network coordinates even though `node` is the engine itself — the
        // head or client is elsewhere (stormblock#149, v19.1.1; older
        // engines ignore the field).
        let mut body = serde_json::json!({ "node": node, "mode": "read_write", "transport": "nvme_tcp" });
        if let Some(e) = epoch {
            body["epoch"] = serde_json::json!(e);
        }
        if let Some(h) = host_nqn {
            body["host_nqn"] = serde_json::json!(h);
        }
        let v = self.v1_post(&format!("/v1/volumes/{id}/attach"), body).await?;
        let mut att = self.parse_attach(id, &v)?;
        att.host_nqn = host_nqn.map(str::to_string);
        Ok(att)
    }

    /// The coordinates in an attach answer (`/v1` and `/api/v1` return the
    /// same `AttachInfo`).
    fn parse_attach(&self, id: &str, v: &Value) -> anyhow::Result<AttachedLeg> {
        match v.get("transport").and_then(|t| t.as_str()) {
            Some("nvme_tcp") => {}
            // Since stormblock 2337c8a an attach by the master node gets the
            // local ublk fast path, and every attach made here names the
            // master. Nothing remote can use a ublk device (stormblock#149).
            Some("ublk") => anyhow::bail!(
                "attach {id}: engine offered a local ublk device, not NVMe-TCP — \
                 a remote head or client cannot use it (stormblock#149); \
                 until that lands set [management] ublk_transport = false on this engine"
            ),
            _ => anyhow::bail!("attach {id}: unexpected transport in {v}"),
        }
        let addr = v
            .get("addresses")
            .and_then(|a| a.as_array())
            .and_then(|a| a.first())
            .ok_or_else(|| anyhow::anyhow!("attach {id}: no addresses in {v}"))?;
        let mut traddr = addr
            .get("traddr")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let trsvcid = addr.get("trsvcid").and_then(|x| x.as_u64()).unwrap_or(4420) as u16;
        // A wildcard listen address is useless to a remote initiator —
        // substitute the engine's own host.
        if traddr.is_empty() || traddr == "0.0.0.0" || traddr == "::" {
            traddr = self.host();
        }
        let nsid = v
            .get("nsid")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "attach {id}: engine returned no nsid — its NVMe-oF target is not running \
                     (an engine started without an export device has no listener)"
                )
            })? as u32;
        Ok(AttachedLeg {
            nqn: v
                .get("nqn")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            traddr,
            trsvcid,
            nsid,
            host_nqn: None,
        })
    }

    /// POST /api/v1/volumes/{id}/attach — NVMe-TCP for any engine volume,
    /// /v1 or not (stormblock#78). For a served volume that came across with
    /// its array on a promote (#33), which the new head's /v1 never made.
    pub async fn attach_any(&self, id: &str) -> anyhow::Result<AttachedLeg> {
        let v = self
            .v1_post(
                &format!("/api/v1/volumes/{id}/attach"),
                serde_json::json!({ "transport": "nvme_tcp" }),
            )
            .await?;
        self.parse_attach(id, &v)
    }

    /// POST /api/v1/volumes/{id}/attach for one consumer host (#51,
    /// stormblock#210): served from that host's own subsystem, which admits
    /// it alone; with `dhchap` the host must prove a DH-HMAC-CHAP secret.
    /// `id` is the engine-local volume id. Returns the coordinates (the
    /// per-host subsystem NQN) and the secret, if the engine gave one — the
    /// engine keeps a host's secret, so a repeat returns the same one.
    pub async fn attach_for_host(
        &self,
        id: &str,
        host_nqn: &str,
        dhchap: bool,
    ) -> anyhow::Result<(AttachedLeg, Option<String>)> {
        let v = self
            .v1_post(
                &format!("/api/v1/volumes/{id}/attach"),
                serde_json::json!({ "transport": "nvme_tcp", "host_nqn": host_nqn, "dhchap": dhchap }),
            )
            .await?;
        let mut att = self.parse_attach(id, &v)?;
        att.host_nqn = Some(host_nqn.to_string());
        let secret = v.get("dhchap_secret").and_then(|x| x.as_str()).map(str::to_string);
        Ok((att, secret))
    }

    /// DELETE /api/v1/volumes/{id}/attach?host_nqn= — stop serving it to
    /// that host only (#51). Idempotent; a 404 (volume gone) is done.
    pub async fn withdraw_host(&self, id: &str, host_nqn: &str) -> anyhow::Result<()> {
        let resp = self
            .req(reqwest::Method::DELETE, &format!("/api/v1/volumes/{id}/attach"))
            .query(&[("host_nqn", host_nqn)])
            .send()
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("withdraw {id} from {host_nqn}: {}", resp.status());
        }
        Ok(())
    }

    /// The engine-local id of the volume named `name` (GET /api/v1/volumes):
    /// a /v1 volume's id is not its engine id, and the per-host attach and
    /// withdraw take the engine id. Exactly one match, else an error.
    pub async fn local_volume_id(&self, name: &str) -> anyhow::Result<String> {
        let ids: Vec<String> = self
            .get_items("/api/v1/volumes")
            .await?
            .iter()
            .filter(|v| v.get("name").and_then(|n| n.as_str()) == Some(name))
            .filter_map(|v| v.get("id").and_then(|i| i.as_str()).map(str::to_string))
            .collect();
        match ids.as_slice() {
            [id] => Ok(id.clone()),
            [] => Err(HttpStatus {
                path: "/api/v1/volumes".into(),
                status: reqwest::StatusCode::NOT_FOUND,
                message: format!("no engine volume named {name:?}"),
            }
            .into()),
            _ => anyhow::bail!("{} engine volumes named {name:?}", ids.len()),
        }
    }

    /// DELETE /api/v1/volumes/{id}/attach — stop serving it. Idempotent.
    pub async fn detach_any(&self, id: &str) -> anyhow::Result<()> {
        let resp = self
            .req(reqwest::Method::DELETE, &format!("/api/v1/volumes/{id}/attach"))
            .send()
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("detach {id}: {}", resp.status());
        }
        Ok(())
    }

    /// DELETE /api/v1/volumes/{id} — any engine volume.
    pub async fn delete_any_volume(&self, id: &str) -> anyhow::Result<()> {
        let resp = self
            .req(reqwest::Method::DELETE, &format!("/api/v1/volumes/{id}"))
            .send()
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("delete {id}: {}", resp.status());
        }
        Ok(())
    }

    /// The /v1 epoch of a volume (GET /v1/volumes/{id}).
    pub async fn v1_epoch(&self, id: &str) -> anyhow::Result<u64> {
        let v: Value = self
            .get(&format!("/v1/volumes/{id}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        v.get("epoch")
            .and_then(|e| e.as_u64())
            .ok_or_else(|| anyhow::anyhow!("volume {id}: no epoch in {v}"))
    }

    /// GET /v1/volumes/{id}/raid-superblock (stormblock#309): the RAID
    /// superblock a head wrote into this leg, read on the leg's own engine.
    /// `None` when the volume carries none or the engine has no such route
    /// (404) — no evidence, never an error a caller must act on.
    pub async fn leg_superblock(&self, id: &str) -> anyhow::Result<Option<Value>> {
        let resp = self.get(&format!("/v1/volumes/{id}/raid-superblock")).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(resp.error_for_status()?.json().await?))
    }

    /// POST /v1/volumes/{id}/fence {expected_epoch} — the engine's CAS.
    pub async fn v1_fence(&self, id: &str, expected_epoch: u64) -> anyhow::Result<FenceOutcome> {
        let path = format!("/v1/volumes/{id}/fence");
        let resp = self
            .req(reqwest::Method::POST, &path)
            .json(&serde_json::json!({ "expected_epoch": expected_epoch }))
            .send()
            .await?;
        check_refused(&resp, &path)?;
        let status = resp.status();
        let out: Value = resp.json().await.unwrap_or(Value::Null);
        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            if let Some(c) = out.get("current_epoch").and_then(|c| c.as_u64()) {
                return Ok(FenceOutcome::Stale(c));
            }
        }
        if !status.is_success() {
            anyhow::bail!("{path}: {status}: {out}");
        }
        out.get("epoch")
            .and_then(|e| e.as_u64())
            .map(FenceOutcome::Fenced)
            .ok_or_else(|| anyhow::anyhow!("{path}: no epoch in {out}"))
    }

    /// Fence a leg volume on its engine: read its epoch, CAS it one up, and
    /// once more from the current epoch if another fence won the race. A
    /// leg has one fencer (this volume's stormstorage), so a race is a
    /// retried call of our own. Returns the leg's new epoch.
    pub async fn fence_leg(&self, id: &str) -> anyhow::Result<u64> {
        let mut expected = self.v1_epoch(id).await?;
        for _ in 0..2 {
            match self.v1_fence(id, expected).await? {
                FenceOutcome::Fenced(e) => return Ok(e),
                FenceOutcome::Stale(c) => expected = c,
            }
        }
        anyhow::bail!("fence {id}: epoch kept moving")
    }

    /// POST /api/v1/arrays/assemble {drive_uuids} — put an array back
    /// together from its members' superblocks (stormblock#252). Returns the
    /// report: `arrays[] {id, state, already}`, `refused[]`.
    pub async fn assemble_arrays(&self, drive_uuids: &[String]) -> anyhow::Result<Value> {
        self.v1_post(
            "/api/v1/arrays/assemble",
            serde_json::json!({ "drive_uuids": drive_uuids }),
        )
        .await
    }

    /// PUT /api/v1/arrays/{id}/rebuild — cap its rebuilds (0 = unlimited).
    pub async fn set_rebuild_rate(&self, array_id: &str, bytes_per_sec: u64) -> anyhow::Result<()> {
        let path = format!("/api/v1/arrays/{array_id}/rebuild");
        let resp = self
            .req(reqwest::Method::PUT, &path)
            .json(&serde_json::json!({ "max_bytes_per_sec": bytes_per_sec }))
            .send()
            .await?;
        check_refused(&resp, &path)?;
        if !resp.status().is_success() {
            anyhow::bail!("{path}: {}", resp.status());
        }
        Ok(())
    }

    /// DELETE /api/v1/arrays/{id}?keep_superblocks=true — forget an array
    /// without writing to its members: for a former head, whose members
    /// are now another head's (#33).
    pub async fn forget_array(&self, id: &str) -> anyhow::Result<()> {
        let resp = self
            .admin_send(
                reqwest::Method::DELETE,
                &format!("/api/v1/arrays/{id}?keep_superblocks=true"),
                None,
            )
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("forget array {id}: {}{}", resp.status(), admin_hint(resp.status()));
        }
        Ok(())
    }

    /// POST /v1/volumes/{id}/detach.
    pub async fn detach_volume(&self, id: &str, node: &str) -> anyhow::Result<()> {
        self.v1_post(
            &format!("/v1/volumes/{id}/detach"),
            serde_json::json!({ "node": node }),
        )
        .await?;
        Ok(())
    }

    /// POST /api/v1/arrays — RAID1 across already-opened drives.
    pub async fn create_raid1(&self, drive_uuids: &[String]) -> anyhow::Result<Value> {
        self.admin_post(
            "/api/v1/arrays",
            serde_json::json!({ "level": "Raid1", "drive_uuids": drive_uuids }),
        )
        .await
    }

    /// GET /api/v1/arrays — every array on this engine.
    pub async fn list_arrays(&self) -> anyhow::Result<Vec<Value>> {
        self.get_items("/api/v1/arrays").await
    }

    pub async fn get_array(&self, id: &str) -> anyhow::Result<Value> {
        Ok(self
            .get(&format!("/api/v1/arrays/{id}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// GET /api/v1/arrays/{id}, with a 404 as `None`: the engine no
    /// longer holds the array (it restarted, #15).
    pub async fn find_array(&self, id: &str) -> anyhow::Result<Option<Value>> {
        let resp = self.get(&format!("/api/v1/arrays/{id}")).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(resp.error_for_status()?.json().await?))
    }

    pub async fn delete_array(&self, id: &str) -> anyhow::Result<()> {
        let resp = self
            .admin_send(reqwest::Method::DELETE, &format!("/api/v1/arrays/{id}"), None)
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("delete array {id}: {}{}", resp.status(), admin_hint(resp.status()));
        }
        Ok(())
    }

    /// POST /api/v1/arrays/{id}/members — returns the member uuid.
    pub async fn array_add_member(&self, array_id: &str, drive_uuid: &str) -> anyhow::Result<String> {
        let v = self
            .admin_post(
                &format!("/api/v1/arrays/{array_id}/members"),
                serde_json::json!({ "drive_uuid": drive_uuid }),
            )
            .await?;
        v.get("member_uuid")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("add member: no member_uuid in {v}"))
    }

    pub async fn array_remove_member(&self, array_id: &str, member_uuid: &str) -> anyhow::Result<()> {
        let resp = self
            .admin_send(
                reqwest::Method::DELETE,
                &format!("/api/v1/arrays/{array_id}/members/{member_uuid}"),
                None,
            )
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("remove member {member_uuid}: {}{}", resp.status(), admin_hint(resp.status()));
        }
        Ok(())
    }

    /// GET /api/v1/drives — the engine's open drives ({items, count} wrapper).
    pub async fn list_drives(&self) -> anyhow::Result<Vec<Value>> {
        let v: Value = self
            .get("/api/v1/drives")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(match v {
            Value::Array(a) => a,
            Value::Object(mut o) => o
                .remove("items")
                .or_else(|| o.remove("drives"))
                .and_then(|d| d.as_array().cloned())
                .unwrap_or_default(),
            _ => Vec::new(),
        })
    }

    /// POST /api/v1/drives — open a drive/URI, tolerating "already open":
    /// on conflict the existing drive's uuid is looked up by path.
    pub async fn add_drive_idempotent(&self, path: &str) -> anyhow::Result<String> {
        let resp = self
            .req(reqwest::Method::POST, "/api/v1/drives")
            .json(&serde_json::json!({ "path": path }))
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if status.is_success() {
            return body
                .get("uuid")
                .and_then(|u| u.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("open {path}: no uuid in {body}"));
        }
        if status == reqwest::StatusCode::CONFLICT {
            for d in self.list_drives().await? {
                if d.get("path").and_then(|p| p.as_str()) == Some(path) {
                    if let Some(u) = d.get("uuid").and_then(|u| u.as_str()) {
                        return Ok(u.to_string());
                    }
                }
            }
        }
        anyhow::bail!(
            "open {path}: {status}: {}",
            body.get("error")
                .or_else(|| body.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("no message")
        )
    }

    /// DELETE /api/v1/drives/{id_or_path} — close an opened drive. 404 is
    /// success from our side (already gone).
    pub async fn delete_drive(&self, id_or_path: &str, force: bool) -> anyhow::Result<()> {
        let enc = id_or_path.replace('%', "%25").replace('/', "%2F");
        let q = if force { "?force=true" } else { "" };
        let resp = self
            .admin_send(reqwest::Method::DELETE, &format!("/api/v1/drives/{enc}{q}"), None)
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("close drive {id_or_path}: {}{}", resp.status(), admin_hint(resp.status()));
        }
        Ok(())
    }

    /// The host portion of the engine URL.
    pub fn host(&self) -> String {
        self.url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split([':', '/'])
            .next()
            .unwrap_or_default()
            .to_string()
    }

    pub async fn delete_volume(&self, id: &str) -> anyhow::Result<()> {
        let resp = self
            .req(reqwest::Method::DELETE, &format!("/v1/volumes/{id}"))
            .send()
            .await?;
        // 404 = already gone: deletion is idempotent from our side.
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("delete {id}: {}", resp.status());
        }
        Ok(())
    }

    pub async fn list_volumes(&self) -> anyhow::Result<Vec<Value>> {
        let v: Value = self
            .get("/v1/volumes")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(match v {
            Value::Array(a) => a,
            Value::Object(mut o) => o
                .remove("volumes")
                .and_then(|x| x.as_array().cloned())
                .unwrap_or_default(),
            _ => Vec::new(),
        })
    }
}

impl Engine {
    /// GET a management listing (`{items, count}`, or a bare array).
    async fn get_items(&self, path: &str) -> anyhow::Result<Vec<Value>> {
        let v: Value = self
            .get(path)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(match v {
            Value::Array(a) => a,
            Value::Object(mut o) => o
                .remove("items")
                .and_then(|x| x.as_array().cloned())
                .unwrap_or_default(),
            _ => Vec::new(),
        })
    }

    /// GET /api/v1/slabs — the node's slabs.
    pub async fn list_slabs(&self) -> anyhow::Result<Vec<crate::inventory::Slab>> {
        Ok(self
            .get_items("/api/v1/slabs")
            .await?
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect())
    }

    /// GET /api/v1/volumes?placement=true — every volume the engine holds
    /// (the /v1 list is only what was created through /v1), each with where
    /// it lives (stormblock#136; an older engine ignores the query).
    pub async fn list_engine_volumes(&self) -> anyhow::Result<Vec<crate::inventory::EngineVolume>> {
        Ok(self
            .get_items("/api/v1/volumes?placement=true")
            .await?
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect())
    }

    /// GET /api/v1/slabs/{id}/slots — the distinct volumes owning slots.
    pub async fn slab_volume_ids(&self, slab_id: &str) -> anyhow::Result<Vec<String>> {
        let mut ids: Vec<String> = self
            .get_items(&format!("/api/v1/slabs/{slab_id}/slots"))
            .await?
            .iter()
            .filter_map(|s| s.get("volume_id").and_then(|x| x.as_str()).map(|x| x.to_string()))
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    /// GET /api/v1/discovery — the engine's own name and the peers it
    /// hears. Engines without discovery answer an error; that is `Ok(None)`.
    /// A 401/403 is Err([`Refused`]).
    pub async fn discovery(&self) -> anyhow::Result<Option<Discovery>> {
        let resp = self.get("/api/v1/discovery").send().await?;
        check_refused(&resp, "/api/v1/discovery")?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        Ok(resp.json::<Discovery>().await.ok())
    }
}

/// The part of stormblock's `GET /api/v1/discovery` view stormstorage uses.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct Discovery {
    pub local_node: String,
    pub cluster_id: Option<String>,
    pub nodes: Vec<DiscoveredNode>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct DiscoveredNode {
    pub node_name: String,
    /// `host:port` of the peer's management API.
    pub mgmt_addr: String,
    pub cluster_id: Option<String>,
    pub stale: bool,
}

impl Discovery {
    /// Live peers in the local engine's own cluster (none when unclustered).
    pub fn cluster_peers(&self) -> Vec<&DiscoveredNode> {
        let Some(cid) = &self.cluster_id else {
            return Vec::new();
        };
        self.nodes
            .iter()
            .filter(|n| !n.stale && n.cluster_id.as_ref() == Some(cid))
            .filter(|n| !n.node_name.is_empty() && !n.mgmt_addr.is_empty())
            .filter(|n| n.node_name != self.local_node)
            .collect()
    }
}

/// Find the first object carrying capacity fields in whatever wrapper the
/// engine used: bare object, array, or {"nodes": [...]}.
fn first_capacity_object(v: &Value) -> Option<&serde_json::Map<String, Value>> {
    match v {
        Value::Object(o) => {
            if o.contains_key("total_bytes") || o.contains_key("free_bytes") {
                Some(o)
            } else if let Some(Value::Array(a)) = o.get("nodes") {
                a.first().and_then(|x| x.as_object())
            } else {
                None
            }
        }
        Value::Array(a) => a.first().and_then(|x| x.as_object()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_node_and_attach_helpers() {
        let v: Value = serde_json::json!({
            "id": "vol-x", "replicas": [
                {"node":"nodeb","role":"master","sync":{"state":"in_sync"}}
            ]});
        assert_eq!(Engine::master_node_of(&v), Some("nodeb".into()));
        assert_eq!(Engine::master_node_of(&serde_json::json!({})), None);

        let leg = AttachedLeg {
            nqn: "nqn.2024.io.stormblock:b".into(),
            traddr: "10.0.0.2".into(),
            trsvcid: 4420,
            nsid: 7,
            host_nqn: None,
        };
        assert_eq!(
            leg.drive_uri(),
            "nvme-tcp://10.0.0.2:4420/nqn.2024.io.stormblock:b?nsid=7"
        );
        // Served to one head (#27): the URI names it, as stormblock parses it.
        let hosted = AttachedLeg {
            nqn: "nqn.2024.io.stormblock:b:host:0123456789abcdef".into(),
            host_nqn: Some("nqn.2026-10.lo.storm:stormstorage:a".into()),
            ..leg.clone()
        };
        assert_eq!(
            hosted.drive_uri(),
            "nvme-tcp://10.0.0.2:4420/nqn.2024.io.stormblock:b:host:0123456789abcdef?nsid=7\
             &hostnqn=nqn.2026-10.lo.storm:stormstorage:a"
        );
        // A record from before #27 has no host and keeps its URI.
        let old: AttachedLeg =
            serde_json::from_str(r#"{"nqn":"n","traddr":"h","trsvcid":4420,"nsid":1}"#).unwrap();
        assert_eq!(old.host_nqn, None);
        assert!(!serde_json::to_string(&old).unwrap().contains("host_nqn"));

        let e = Engine::new("http://192.168.8.150:9090", None);
        assert_eq!(e.host(), "192.168.8.150");
    }

    #[test]
    fn discovery_cluster_peers() {
        let d: Discovery = serde_json::from_value(serde_json::json!({
            "local_node": "n1", "cluster_id": "c1", "cluster_name": "x",
            "cluster_peer_count": 1, "clusters": [],
            "nodes": [
                {"node_name":"n2","mgmt_addr":"10.0.0.2:9090","cluster_id":"c1","stale":false,
                 "version":1,"total_bytes":0,"free_bytes":0,"engine_version":"x","age_secs":1},
                {"node_name":"n3","mgmt_addr":"10.0.0.3:9090","cluster_id":"c1","stale":true},
                {"node_name":"n4","mgmt_addr":"10.0.0.4:9090","cluster_id":"c2","stale":false},
                {"node_name":"n5","mgmt_addr":"10.0.0.5:9090","cluster_id":null,"stale":false}
            ]
        }))
        .unwrap();
        let peers: Vec<&str> = d.cluster_peers().iter().map(|n| n.node_name.as_str()).collect();
        assert_eq!(peers, vec!["n2"]);
        let unclustered = Discovery { cluster_id: None, ..d.clone() };
        assert!(unclustered.cluster_peers().is_empty());
    }

    #[test]
    fn capacity_shapes_parse() {
        let bare: Value =
            serde_json::json!({"node":"n1","total_bytes":100,"free_bytes":40,"topology":{"rack":"r1"}});
        let arr: Value = serde_json::json!([{"total_bytes":1,"free_bytes":1}]);
        let wrapped: Value = serde_json::json!({"nodes":[{"total_bytes":2,"free_bytes":2}]});
        assert!(first_capacity_object(&bare).is_some());
        assert!(first_capacity_object(&arr).is_some());
        assert!(first_capacity_object(&wrapped).is_some());
        assert!(first_capacity_object(&serde_json::json!("nope")).is_none());
    }
}
