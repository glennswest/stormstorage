# StormStorage Development Guide

## Project Overview

The **storage control plane across nodes and clusters** in the Storm
ecosystem. stormblock executes on one node; stormdrive knows one node's
hardware; **stormstorage decides across all of them**: registry of SNO
storage clusters, pools, placement across failure domains, distributed
volumes as RAID across individual per-node volumes (legs over NVMe-TCP,
movable), tiering across clusters, and the fleet surface stormfs walks
across. Founding spec: [docs/architecture.md](docs/architecture.md).

**Version: 0.3.0** — version locations: `Cargo.toml`, `Cargo.lock`, this file.

Never in the data path. Everything node↔node and client↔node is NVMe-TCP.

## Build and ship

Build and test with **`sc-build`** from this checkout, after `git push`.
It builds the pushed commit in a scratch directory on `dev.g8.lo` as an
unprivileged user and deletes it afterwards. There is no checkout on dev,
and nothing runs as root.

```
git push && sc-build                 # cargo build && cargo test
sc-build 'cargo clippy --all-targets'
```

It ships as a stormcos service component in goldens: `stormstorage` and
`-logs` on system1, `-data` on data1. The authority on goldens is
stormcos `docs/goldens.md`. The component entry (port, health, argv,
shipped config) is in stormcentral's database: `stormcentral component
export` / `component edit stormstorage --set key=value` (#37). When an issue's work is complete, request the
golden once with
`stormcentral component build stormstorage --url http://stormcentral.g8.lo`.
`Cargo.lock` pins stormview; bump it with `cargo update -p stormview`.

## Layering (do not blur it)

| Layer | Scope | Owns |
|---|---|---|
| stormdrive :9092 | per node, below the node | hardware truth: drives, shelves, bays, health, tests |
| stormblock :9090 | per node | execution: slabs, volumes, RAID, targets, /v1 fencing |
| **stormstorage :9093** | fleet | policy: registry, pools, placement, dist-volumes, tiering |

The testbed: three SNO clusters as three levels — 2.5" shelf (high),
3.5" shelf (medium), PVE (backup). node ≅ cluster for now; the model
keeps the rungs distinct.

## Key design points (from Glenn, 2026-08-26)

- Storage nodes are **SNO stormblock clusters from the start**.
- **RAIDs are across individual volumes** — one thin volume per leg per
  node, legs placed across domains at a rung, assembled RAID1 on a head
  node over NVMe-TCP, and **legs move** (add member → rebuild → remove).
- **Multiple pools**; a pool = node selection + policy (replicas, rung,
  tier). **Load balancing** in placement (free ratio weighted against live
  NVMe-oF I/O load, #31).
- stormblock nodes self-register using their existing `[stormfs]`
  heartbeat pointed at stormstorage — zero engine changes to enroll.
- stormfs v2 consumes `GET /api/v1/nodes` + placed volumes for its
  massive shared FS; data path stays direct.

## Work Plan

### Phase 1: Registry, pools, placement, volumes — DONE (v0.1.0)
- [x] Founding spec (docs/architecture.md)
- [x] Scaffold: config (nodes/pools/rungs), model, events, persistence
- [x] Engine client (/v1: capacity, volume create/delete/list; bearer;
      legs request replica_tier slaves=0)
- [x] Poller: enrich + health-mark every node
- [x] stormblock-compatible register/deregister endpoints
- [x] Placement engine: domain grouping at rung, load-balanced, pure+tested
- [x] DistVolume create/delete (legs created per node; assembly pending #73;
      rollback on partial failure)
- [x] API + embedded UI + stormd summary card
- [x] Build/test on dev (15/15, clippy clean); e2e against a LIVE stormblock
      on dev: poll→capacity, pool create→leg in engine, delete→clean.
      Operational note: an engine started with plain --device does NOT
      reopen an existing slab — capacity reads 0 until a slab is
      formatted/adopted; matters for node provisioning.
- [x] Issues: stormblock#73 (NVMe-TCP export as drive/RAID member via API),
      stormfs#64 (consume stormstorage registry/placement)

### Phase 1.5: stormview feed + peer replication — DONE (v0.2.0)
- [x] `GET /api/v1/components` + `/ws/components` (stormview crate,
      public): system/pool/node/volume with relations for grids and a
      delete action on volumes — renders in stormd/stormsh/stormconsole
- [x] `[replication] peers`: revision-guarded async replication of
      durable intent (volumes + registered nodes); poll status stays
      local per peer. Live-verified: create on peer A → visible on B <2s

### CSI relationship (Glenn, 2026-08-26) — #36 settled 2026-10-06
stormblock is the default-everywhere storage. **PVCs on stormcos are the
built-in `stormblock` driver**: the kubelet clones the sealed blank of the
claim's size class on the pod's node and attaches it over ublk, with no CSI
(stormblock CLAUDE.md, rustkube `docs/storage.md`). CSI (stormblock-csi) is
the compatibility path for third-party drivers and *foreign* Kubernetes:
still wanted, not primary. **Settled** (owner decisions on stormblock-csi#29
and #32, 2026-10-06; shipped in stormblock-csi v0.4.0): its controller and
operator talk to **stormstorage only** (create, replicas/sync state,
fence/promote/prestage/dual-attach, attach from `export`); the CSI volume id
is the stormstorage name; the node plugin uses its own engine for local
replicas = 1. Follow-ups here: #48 (done), #51 (done), #49 (snapshots,
expand, clones; needs-owner), #50 (WaitForFirstConsumer head preference),
#56 (one elected cluster stormstorage for multi-node, with stormcos#354).
Replicated claims across servers (rustkube-node#68) go through re-leg (#1).

### Phase 2: Leg wiring — DONE (v0.3.0, 2026-08-28)
stormblock now attaches `nvme-tcp://host:port/<nqn>?nsid=N` as a drive via
POST /api/v1/drives and takes it as a RAID member; proven cross-engine on
dev (RAID-1 across a local drive + a remote NVMe-TCP leg, members active).
Plan: legs export via /v1 attach (hot-add namespace, returns
nqn/addresses/nsid); head = first placed node; head attaches every leg as
an nvme-tcp:// drive (its own leg over loopback too — uniform; local
fast-path is a later optimization) and assembles RAID1 via
/api/v1/arrays. Move = new leg → attach → add_member → poll member
active → remove_member → drop old drive/volume.
- [x] Engine client: /v1 attach/detach, arrays create/get/members, typed
      create (captures master node for attach gating)
- [x] Assembly in create flow; teardown in delete; AssemblyState::Assembled
      (needs stormblock ≥ 2026-08-28: array members expose uuid+path)
- [x] POST /api/v1/volumes/{name}/move — background leg move with events
- [x] e2e on dev: 3 engines — create → assembled RAID1 on head (member
      uuids captured); move node-c → node-d converged, both members
      active, old volume gone
- [ ] Node-loss handling: re-leg from surviving copies (#1, P1) — code
      done (999556f, 5deffda, c7d71f9); live run waits on
      stormcentral#131 (#27 host_nqn done 2026-10-06).
      Plan: `LegState::Lost` (was created, node now unhealthy) and
      `AssemblyState::Degraded`; a reconciler on every poll marks legs
      lost and starts one re-leg per volume through the move machinery.
      The replacement is recorded on the volume (`replacing`) so a restart
      resumes the wait instead of adding a second member, and there is a
      per-volume cooldown after a failed attempt (so a flapping node gives
      one re-leg). Cleanup of the dead side is best-effort: an undeletable
      old leg goes to `orphans` and is reaped when its node answers again.
      A lost **head** is reported (degraded, event) but not recovered: that
      is re-head, a later phase. /v1 attach sends `transport: nvme_tcp`
      (stormblock#149, v19.1.1). Verify with unit tests of the decisions
      and `test/e2e-releg.sh` (3 engines on dev via sc-build: create 2-leg,
      stop non-head, re-leg converges, restart reaps the orphan).
      **Status 2026-09-28:** code, unit tests and docs done (999556f,
      5deffda, c7d71f9). The re-leg itself converged on real engines inside
      the #2 e2e (b64c3ee run: `b → c (node lost)`, cleanup pending,
      republished unchanged), but `scripts/e2e-releg.sh` has no recorded
      pass. It now carries e2e-export's loaded-box fixes. Blocked like #2 on
      stormcentral#131 (no stormblock binary for an sc-build job, #25).
      When it passes: close #1 with the run, then the golden.
- [ ] Consumer serving (#2) — code done; live e2e waits on
      stormcentral#131 (#25) and, on closed engines, #53. History (2026-09-27). Unblocked:
      stormblock v19.0.0 (#150: dedicated arrays, `placement.array_id`
      pins a /v1 volume) and v19.1.1 (#149: `transport: nvme_tcp` attach).
      Plan: `DistVolume.export` {state none|published|failed, volume_id,
      node, master_node, coordinates, published_at, coordinates_changed,
      message}. Assembled/degraded: `<name>-mirror` /v1 volume pinned to the
      array on the head, attached nvme_tcp. Single leg: the leg's own
      attach, same field. Published at the end of create; delete revokes
      first (detach + delete the pinned volume, else the array delete is a
      409); `POST /api/v1/volumes/{name}/export` publishes or republishes,
      and a head node coming back healthy republishes. API, feed, UI,
      docs; tests/export.rs against a mock engine; e2e on dev
      (scripts/e2e-export.sh via sc-build).
      **Status 2026-09-27 (session restart):** code, docs, tests/export.rs
      done and pushed; `sc-build` passes (28 unit + 2 adopt + 3 export).
      Open: live e2e `sc-build scripts/e2e-export.sh` — run 1 failed on a
      harness race (fixed: 180 s engine wait), run 2 exited silently right
      after create (URI one-liner rewritten, ERR trap added), run 3 was in
      flight at restart. Run 3 never started (dev.g8.lo rebooted); run 4
      failed: engine a took >5 s to answer a leg create (#19) → engine
      writes now time out after 60 s (95d4a20), then 300 s (a172e5c: array
      create formats a slab, 47 s loaded). Runs 5–9 fixed harness bugs
      (array lists engine uuids; victim "b" matched "assembled") and one
      real one: a publish on an unreachable node was not recorded — now
      `failed` + event, retried on recovery (b64c3ee, test in export.rs).
      Run 9 passed everything up to delete: the head's engine did not answer
      the served-volume DELETE in 300 s (it finished later). Run 10
      instruments that (latency probe head vs other engine, loadavg) to
      tell an engine stall (→ stormblock issue) from box load.
      **2026-09-28: blocked on stormcentral#131.** Run 10 left no result
      here; the e2e spent over an hour of its build slot compiling
      stormblock, which the owner ruled out (#25: test at runtime, no
      stormblock compiles here). The scripts now require `STORMBLOCK_BIN`
      and stop without it; no sc-build job has a stormblock ≥ v19.1.1 to
      point at until stormblock's golden bin is reachable from a job (#131;
      GitHub releases stop at v8.2.1). #2 proposed --after
      stormcentral#131. Expect #26 (a stalled head's leg stays lost) on a
      loaded box. When it passes: close #17, #18, #19, #21 (harness
      build-failures) and #2 with what was verified, then request the
      golden once. #13 and #16 closed.
- [x] Retry a failed assembly (#7) — done 2026-09-28:
      `POST /api/v1/volumes/{name}/assemble` (409 when assembled, single
      leg or busy) → `orchestrate::assemble`, then publish. The reconciler
      retries a pending volume whose leg nodes are all healthy, gated like
      re-leg by `recovery.active`, with `next_assemble_after` = now +
      `recovery.cooldown_secs` after a failed attempt. `assemble` takes the
      per-volume claim (no concurrent re-leg/assemble). Before creating the
      RAID it lists the head's arrays: one whose members are exactly the
      legs' drive URIs is adopted (a create whose response was lost); one
      holding any of them otherwise → refuse. (Since stormblock#215 create
      itself 409s on held drives; a 409 for legs carrying a superblock of
      an array the head lost now reassembles that array, #43.) Event text
      names the retry path. Tests: mock engine in tests/assemble.rs.

### Docs from code — DONE (#4, 2026-09-24)
- [x] README rewritten from source: flags, every config key + default,
      ports, endpoints, build (sc-build), shipping (golden)
- [x] docs/architecture.md: design-only sections marked, stale bits fixed
- [x] CLAUDE.md: build rules → sc-build; status current
- [x] Cross-refs checked against the code of stormblock (`src/stormfs.rs`
      heartbeat, :9090), stormd (`[process.ui]` proxy/summary), stormcos
      (goldens, routes) and stormconsole (the stormstorage plugin)
- [x] Gaps the docs promised but the code lacks, filed as issues:
      #6 inbound API auth, #7 assembly retry

### Presentation — DONE (#5, 2026-09-24)
- [x] docs/presentation.md: an 11-slide Marp deck built from the README
      and code; the relationships slide follows stormcentral's graph
      (graph gaps filed as stormcentral#20)
- [x] Linked from the README; changelog; sc-build passing

### Node adoption: the local stormblock, slabs as pools — DONE (#9, 2026-09-25)
On a node stormstorage saw 0 nodes/pools/volumes while its stormblock had
3 slabs and 141 volumes.
- [x] `[local]` config (default on): adopt `http://127.0.0.1:9090` once it
      answers, named by the engine's discovery `local_node` (else the
      hostname), plus live peers of its stormblock cluster. Source
      `local`: never replicated, no revision bump.
- [x] Poller inventory (memory only): slabs, `/api/v1/volumes`, slab slot
      tables → each volume placed by slots / parent / only-slab-of-role /
      unknown (`src/inventory.rs`).
- [x] `/api/v1/pools` kinds policy/slab/tier;
      `/api/v1/nodes/{name}/inventory`; feed: `tier:*`, `pool:<node>/<slab>`
      with volumes, `nvol:<node>/<id>` with owner; summary + UI.
- [x] tests/adopt.rs: two mock stormblocks (local + cluster peer) → adopt,
      place, pools, feed. sc-build passes.
- [x] #11 follow-ups (except PV/PVC) — done 2026-09-28. stormblock#136 (v17.1.0)
      and #138 (v18.1.0) landed; rustkube-node#59 (PV/PVC) is still open.
      Plan: poll `GET /api/v1/volumes?placement=true` (full read each
      poll; `?since` is not used because attach/detach and slab state do
      not bump `generation`). Parse slab `drive` {serial,wwn,model,path};
      volume `placement` {slabs[{id,drive,node,state,legs,…}], drives,
      legs{policy,health,expected,missing,…}, rebuild, arrays[{members}]},
      `kind`, `in_use`, `attachments`, `consumer`. `placed_by: engine` from
      placement; the slot scan runs only for an engine that sends no
      placement (older than v17.1.0). Feed: pool detail names the drive;
      node volume: kind, in use, consumer, drives, partners, rebuild.
      UI Node volumes: kind, consumer, drive, partners. Tests: parse real
      shapes, placement precedence, feed. PV/PVC is #28, after
      rustkube-node#59.
- [x] PV/PVC per node volume (#28) — done 2026-10-05 (356227b).
      The engine does not carry the PV: the kubelet's mirror writes a PV +
      bound PVC per volume into the apiserver (`storm.io/node`,
      `storm.io/volume` annotations; `spec.csi.volumeHandle` = volume
      name, driver `stormblock.storm.io`; labels `storm.io/volume-kind`,
      `storm.io/component`). Plan: `[kubernetes]` config (enabled, server
      default `https://127.0.0.1:6443` unverified on loopback like
      stormconsole, `token_file` else `$KUBE_TOKEN`, else
      `/data/stormcert/node-admin.token`, else the pod SA token; anonymous
      without one); `src/kube.rs` reads PVs + PVCs once per poll, joins by
      (node, volume) onto `PlacedVolume.pv` {name, phase, reclaim, kind,
      component, claim {namespace, name, uid, phase, bound}}; failures keep
      the last view, event on transitions only. Feed `nvol:` metrics
      pv/claim; UI Node volumes column. Tests: unit join + tests/kube.rs
      (mock apiserver + mock engine). sc-build passes (47 unit,
      tests/kube.rs 2/2). Filed stormcos#290: mount /data/stormcert ro in
      the stormstorage unit (today anonymous works only while sno grants
      anonymous admin, stormcos#76). Not seen on a node yet.
- Build box has no clippy (stormcentral#31); `sc-build 'cargo clippy'`
  fails until that is fixed.

### Test suites (#8) — code done 2026-09-28; waiting on a test-machine run
Verified on dev: `sc-build scripts/check-test-container.sh` (47a65c9)
builds --locked, stages a 3.2 MiB static /test, and runs it against a real
stormstorage of the commit (no nodes: api-up pass, node checks fail, exit
1; results.jsonl = stdout; unreachable/no node → 2). Not yet run on a test
machine: C2NR0Q2 (the only one) last install failed and its :9093 refused.
Queued `stormcentral test run stormstorage short` = run d3d347cb97. When a
run there passes short (and medium), close #8 with it.
2026-09-28 later: blocked outside this repo. C2NR0Q2 is down (stormcos#165,
stormcentral#63), and on 11.50 stormstorage did not answer on the node at
all (stormcos#139: neither :9093 nor its stormd :9193). #8 proposed after
stormcos#139. No code change pending.
2026-10-05: stormcos#139 fixed (11.80); C2NR0Q2 up. Run ed43948592
(short, 2a4c3ac on C2NR0Q2): harness end to end OK (image built, pushed,
Job ran, cleaned up) — api-up pass, engine-adopted/pools fail, lifecycle
skip. Real bug: the node's stormstorage got 401 from its stormblock, token
looked for only at /etc/stormblock/api_token; the unit mounts it at
/run/stormblock/engine/api_token and the entry's `token_file` is top-level
(ignored, stormcentral#72). Fixed in ef3af98 (#42 step 2: family-order
search incl. that path; warn on unknown top-level keys). A pass needs a
release carrying the new golden on C2NR0Q2; then rerun short + medium.
Medium run 61a319ebe8 (2a4c3ac): 4 pass / 7 fail / 3 skip, all failures
"0 candidates" (same cause). sc-build passes on ef3af98; golden
golden-stormstorage-6a3478464394, release request stormcos#156; #8
proposed after stormcos#156. #42 step 2 done (entry fix: stormcentral#72).
2026-10-05 later: owner on #8: "go ahead and use the new image". Newest
releases 11.80/11.81 still carry golden-stormstorage-0acf7f3181ff (519f58f,
pre-fix); golden-stormstorage-2ff1f1875288 (853ba26: token fix + #28) is the
one to ship. Next: compose a release picking it, install on C2NR0Q2
(`testhost install`), then `test run stormstorage short` and `medium`.
The compose was refused in this session by the permission classifier
(production deploy), so it waits on the owner running it or allowing it.
Per stormcentral docs/test-standard.md, like every sibling: `test/` is a
workspace member crate `stormstorage-test` (static musl `/test`),
`test/build.sh` (STAGE_ONLY=1 stages test/.stage/), `test/Containerfile`
FROM scratch, `test/stormstorage-test.yaml` (Job + metadata). The suites
drive the **node's own stormstorage** at `STORM_NODE:9093` through its API
(no stormblock binary in the image: #25). Writes use
`STORM_STORMSTORAGE_TOKEN` when the node's `api_token` is set, else they
are skips. Volumes are named `t-<run id>-…`, 64 MiB, deleted on success
and failure. short: up, local engine adopted, pools + feed, single-leg
create→published→delete. medium: + dry-run placement, refusals (dup,
`-mirror`, too many replicas leaves no leg), leg on the engine and gone
after delete (inventory), republish unchanged, assemble/move refused on
one node, 404s, auth guard, RAID1 when ≥2 healthy storage nodes
(`requires: storage-nodes>=2`, else skip). long: waves sized from free
capacity (STORM_WAVE_MAX caps), create latency per wave, residue after
each (volumes, engine legs, orphans); a slower wave or growing residue
fails. When stormcentral#121 lands (golden bins, no podman) the
Containerfile goes; build.sh's static binary stays.

### Open: security
- [x] Inbound API auth (#6), 2026-09-28. Register/deregister stay open
      until stormblock#214; then close them too. Plan: a middleware on the router. With
      `api.api_token` set, every mutation (create/delete/move/export
      volume, replicate) needs `Authorization: Bearer <token>` → else 401
      `{error, code: "unauthorized"}`, constant-time compare. Reads (GET,
      ws, the dry-run `placement/plan`) stay open: family posture.
      `storage/register|deregister` stay open: stormblock's heartbeat sends
      no token (stormblock `src/stormfs.rs`), and enrolling needs no engine
      change; a stormblock issue asks for a token on it. Empty token = open,
      as today. The embedded UI asks for the token on a 401 (sessionStorage).
      tests/auth.rs: token + no header → 401, right header → 2xx, wrong →
      401, reads open, register open, empty token → open.

### Engine token on every poll, back off on 401 (#38, P0) — DONE 2026-10-02
server1's console: a stormblock WARN for `GET /api/v1/discovery` and
`/v1/nodes/capacity` every 15 s. A node with no token of its own (one
stormblock self-registered, or adopted) was polled bare.
- [x] `AppState::engine_token`: the node's own token, else the configured
      engine token (`STORMBLOCK_API_TOKEN`, `[local] token_file`), read at
      call time so a re-minted file is picked up. Adopted nodes no longer
      copy the token into the state file.
- [x] A 401/403 is a configuration error: `src/refusal.rs` backs off per
      engine URL (2×interval doubling to 5 min; a changed token retries at
      once), one WARN + event at the first refusal, then a summary at most
      every 5 min; an event when it is accepted again.
- [x] Tests: unit (backoff, token change) and tests/token.rs (a mock engine
      that demands the token; a self-registered node polled with it; a
      refusing engine called a bounded number of times).
- sc-build passes (39 unit, tests/token.rs 3/3). Not seen on a node yet:
      the golden carries it; server1's console should go quiet.

### Phase 3: Rebalance + tier migration
- [x] Pool watermarks; policy-driven leg moves to new nodes/shelves/clusters (#30) — done 2026-10-06
- [x] Placement by live IO load, not only free ratio (#31) — done 2026-10-06
- [x] Cross-cluster tier migration (pool → pool) (#32) — done 2026-10-06 (head leg waits on stormblock#296)

### Phase 4: Replication on the RAID head (#33) — code done 2026-10-05; live run waits
Re-scoped by the owner on stormblock#179 (option b, 2026-10-05): cross-node
RAID1 is the head's job here, not the engine's. stormblock #5/#7 moved
here; the engine's `/v1` prestage/promote/dual-attach are never called.
Design and contracts: docs/replication.md. Code: `src/head.rs`.
- [x] Sync state: head array read each poll (`AppState.heads`, memory
      only) → `replicas[] {node, role, sync}` in /v1's JSON; no reading =
      detached. `GET …/replicas`; view `replica_sync`/`health`; feed + UI.
- [x] `epoch` + `fenced` per volume; `POST …/fence` CAS (412 +
      current_epoch), fences each leg's /v1 epoch (`legs[].epoch`); leg
      attaches send it (`Engine::attach_leg`).
- [x] `POST …/promote`: surviving legs opened on the target,
      `/api/v1/arrays/assemble` (same array uuid), the adopted
      `<name>-mirror` served via `/api/v1/volumes/{id}/attach`
      (`export.adopted`; never an empty replacement). Live old head → 409
      until stormblock#296 (filed). `stale_heads` reaped only once the old
      head no longer holds the array.
- [x] `POST …/prestage` (slave replacement), `bandwidth_class` →
      `PUT arrays/{id}/rebuild` (`[recovery] rate_*`).
- [x] Dual-attach open/close (commit = fence + promote), expiry in the
      reconciler.
- [x] Self-demotion: not built — a fenced head fails at the legs (doc).
- [x] Tests: 8 unit in src/head.rs; tests/replication.rs 4/4 (mock engines
      sharing superblocks/slabs). sc-build passes (e64ad72: 55 unit).
- [x] Contract for stormblock#6 and the stormblock-csi#29 answer (read
      from stormstorage) posted on #33 / stormblock#6 / stormblock-csi#29.
- [ ] Live run on real engines: waits on stormcentral#131 like #1/#2;
      enforcement needs stormblock#6 (#27 done); live handover needs
      stormblock#296. Async backup legs split to #46: design pass in docs/async-legs.md
      (2026-10-06), waits on the owner's choice (point-in-time B / async
      replica A or C / both).

### Phase 5: HA
- [ ] State to StormKV/fastetcd; multiple stormstorage instances (#34) — waits on the owner (2026-10-06): fleet fastetcd / StormKV / apiserver?

### Live migration of a VM's disks (#44, P3) — waits on stormblock#295
Owner (rustkube-node#159): RAID to the destination, let it catch up, then
move the memory. A VM disk is a plain local volume on S, served over ublk
to a running guest. Only S's engine sees its writes, so the mirror is
stormblock's job (#295: `POST /api/v1/volumes/{id}/mirror {target
nvme-tcp URI}`, state copying|synced|failed + bytes_remaining, DELETE =
release/abort). Legs and RAID heads can't do it: they can't sit under an
in-use volume, and a head on S isn't local to T. Plan here once #295 lands:
`POST /api/v1/nodes/{S}/volumes/{id}/migrate {to: T}` (same-size thin
volume on T, attached for S over nvme_tcp, start the mirror) →
`GET …/migrations/{mid}` (starting|copying|synced|cut_over|completed|
aborted|failed, bytes_remaining, T's volume for the kubelet) →
`POST …/cutover` (stop the mirror, detach; S kept) → `…/complete` (retire
S) or `…/abort` (delete T). Persisted, events, feed, auth, tests against a
mock engine. Consumer: rustkube-node#40.

### Head leg back from lost (#26, P1) — DONE 2026-10-06
A head that misses `fail_threshold` polls marks its own leg lost and the
volume degraded; nothing brought it back. Plan: in the reconciler, for a
degraded, unfenced, idle volume whose head leg is lost and whose head is
healthy again, read the head's array (this poll's reading, else
`GET /api/v1/arrays/{id}`, 404 = gone). Head member active → leg
`created`, assembly `assembled` when no leg is lost, Info event. Array
gone (#15) or member not active → stays degraded, one Warning event per
change (dedup on the leg's message). Pure `apply_head_reading` + unit
tests; tests/rejoin.rs against a mock engine.
Done: sc-build passes (59 unit incl. 4 new, tests/rejoin.rs 2/2). Not
seen on a live engine (live runs wait on stormcentral#131).

### Legs served to their head alone (#27, stormblock#210) — DONE 2026-10-06
A closed engine admits no host on its shared subsystem; an attach must
name `host_nqn` and the volume is served from that host's own subsystem
(`<nqn>:host:<hex>`). The engine has no API that says what NQN its
initiator presents, but a drive URI's `&hostnqn=` sets it. So stormstorage
names the head: `[legs] host_nqn` template (default
`nqn.2026-10.lo.storm:stormstorage:{node}`, `{node}` = head). Every leg
attach (assemble, move/re-leg, promote, re-attach on promote-in-place)
sends `host_nqn` of the head it is for; `AttachedLeg.host_nqn` is kept and
`drive_uri()` adds `&hostnqn=` (old records without it keep their URI, so
existing arrays still match). The NQN in the URI is the reply's (the
per-host subsystem). Consumer publish (`<name>-mirror`) names no host
still — the consumer is unknown here; filed separately. Detach needs no
change (/v1 detach releases every host). DH-HMAC-CHAP waits on
stormblock#213. Tests: unit (URI, template), mock engines check the
attach body's `host_nqn` and the opened drive's `hostnqn=`.
Done: sc-build passes (61 unit, export 7/7, replication 4/4). Consumer
export host: #53. Not seen on a live closed engine (stormcentral#131).

### Sync evidence that survives the head (#48, P1) — DONE 2026-10-06
`/replicas` read every leg `detached` once the head was lost, so
stormblock-csi (waiting for `in_sync`) never failed over. Plan: when the
head's array cannot be read, read each surviving leg's RAID superblock on
its own engine (`GET /v1/volumes/{id}/raid-superblock`, filed as
stormblock#309; 404/err = no evidence) and build the reading from those
(`sync_source: superblock`; live = `head`). A leg is `in_sync` only if its
superblock is of this array, has the newest `events` among the reachable
legs, every newest superblock records it active, its events are not below
the last live head reading's, and that reading (kept in memory per
volume) did not show it other than active. Rebuilding → resyncing from
`rebuilt_to`. Residual window (documented): a slave dropped after the
last live reading, when only the dead head recorded the drop (2 legs).
Pure `from_superblocks` + unit tests; tests/replication.rs mock engine
serves the endpoint. Live run waits on stormblock#309 + stormcentral#131.
Done: dcda0a2; sc-build passes (65 unit incl. 4 new, replication 5/5).
Until stormblock#309 lands the route 404s, so a lost head still reads
`detached` on real engines (no evidence, never a false `in_sync`).

### Serve a volume to named consumer hosts (#51 + #53, P1) — DONE 2026-10-06
A closed engine (stormblock#210) refuses the export's shared-subsystem
attach, and a consumer node had no way in. stormblock already takes
`host_nqn` + `dhchap` on `POST /api/v1/volumes/{local}/attach` and
withdraws one host with `DELETE …/attach?host_nqn=`; both need the
engine-local uuid. Plan: `Export.hosts[] {host_nqn, dhchap, coordinates
(per-host subsystem nqn, addresses, nsid, host_nqn), coordinates_changed,
served_at, message}` + `Export.local_id` (found by exact name on the serving
engine: `<name>-mirror`, or the leg's name for a single leg; the adopted
mirror's id is already local). `POST /api/v1/volumes/{name}/export/hosts
{host_nqn, dhchap?}` → that host's coordinates + `dhchap_secret` (returned
only, never stored/replicated/logged; the engine keeps a host's secret, so
a repeat returns it). `DELETE …/export/hosts/{host_nqn}` withdraws.
`hosts` also on create and `POST …/export` (#53). Publish/republish (move,
promote, recovery) re-serves every recorded host; a volume with named hosts
gets no shared attach. Auth: mutations. Tests: mock engine checks body and
withdraw query.
Done: 83666b7 (+ 4d tests, 6343709 docs). `export.per_host` keeps a volume
per host once any host was named (no fallback to shared with zero hosts);
pending withdrawals retried each poll and on recovery. sc-build passes
(65 unit, hosts 2/2, replication 6/6). Not seen on a live closed engine
(stormcentral#131).

### A delete whose revoke failed is not republished (#24; #40 open) — DONE 2026-10-06
#24's e2e: the served volume's DELETE timed out on a loaded head (the
engine finished it later), the record kept `published` + that id, and every
recovery re-attached it → 404 for ever. Plan: `ExportState::Revoking`, set
before revoke touches the engine and kept when it fails; `republish_on`
skips it, `publish` refuses it (409: finish the DELETE), a retried DELETE
treats 404 as gone. Feed/UI show it. Test in tests/export.rs (engine
deletes but answers 500). #40's general case (a served volume gone without
a delete from here: recreate an empty `<name>-mirror`, or not?) is a data
question → asked on #40, `needs-owner`.
Done: the fix commit + b1d39bc docs; sc-build passes (export 8/8). #40 waits
on the owner's answer (recreate empty, or refuse).

### A served volume gone from its engine (#40, P2) — DONE 2026-10-06
Owner (2026-10-06, master's recommendation accepted): never hand out an
empty replacement. Plan: when publish's attach of a *recorded* served
volume (v1 attach, adopted attach_any, per-host attach, or the local-id
lookup) gets 404, the export is `failed` with `gone: true` ("served volume
gone; its data is not recreated") and an Error event; recovery and plain
`POST …/export` no longer attach it (409 says what to do). Only `POST
…/export {"recreate": true}` (assembled volumes: a new pinned
`<name>-mirror`, hosts re-served, `coordinates_changed`) or a delete moves
it on; a single-leg volume's leg is the data, so recreate is refused there.
Engine 404 is typed (`engine::HttpStatus`) so it survives wrapping.
Tests in tests/export.rs (+ a per-host case in tests/hosts.rs).
Done: sc-build passes (export 9/9, hosts 3/3); build failure #55 fixed.

### Engine token for peers (#12, P2) — DONE 2026-10-06
Default-path half done by #42 (ef3af98: family order incl.
`/run/stormblock/engine/api_token`). Peer half, by stormblock's own rule
(`mgmt::auth::token_for`, #107): a *shared* token — `$STORMBLOCK_API_TOKEN`
or the new `[local] shared_token_file` — goes to every engine; a *minted*
one (the file search) only to an engine on this machine (loopback, this
hostname, or an IP held here: a UDP bind to it succeeds). New per-node
`[[nodes]] token_file`. Order per node: `api_token`, `token_file`, shared,
minted-if-local. The refusal log says which applied. Unit tests of the
choice; tests/token.rs: a remote peer is not sent the minted token.
Done: c944a27; sc-build passes (67 unit, token 4/4).

### Reassemble the array after the head's engine restarts (#15 + #43, P2) — DONE 2026-10-06
stormblock#252: an engine reassembles arrays from v2 superblocks itself
only for configured drives; runtime `nvme-tcp://` legs need re-opening and
`POST /api/v1/arrays/assemble`; create 409s on held legs. Plan: factor
promote's "open legs on X → arrays/assemble → member uuids → rebuild rate →
find `<name>-mirror`" into `head::assemble_on`; new `head::reassemble`
(same head): legs created on healthy nodes + the head's own leg if it was
marked lost (#26's "array gone"); only when `find_array` says 404. After:
legs created, assembled/degraded, served volume adopted (`export.adopted`,
`local_id`) and republished; none → `gone` (#40). Reconciler: unfenced,
idle, no replacement/window, head healthy, no live reading this poll,
gated by `recovery.active`, cooldown `next_assemble_after` on failure.
`orchestrate::assemble` (#7): a 409 from create means legs carry a foreign
superblock → assemble instead of retrying. Test: tests/replication.rs mock
head drops its arrays + drives (restart) → reassembled, same array id,
served again. Stale #7 note about stormblock#215 fixed.
Done: aeb7bef; sc-build passes (replication 7/7). Live: an engine restart
in the e2e waits on stormcentral#131.

### Automatic re-head, opt-in (#14, P2) — DONE 2026-10-06
Owner (2026-10-06, master's recommendation): (b) automatic but opt-in,
off by default; turn it on only once the head is fenced through cluster
membership/quorum (stormcluster), not merely unreachable from here —
a partitioned head that keeps writing is split-brain. Until then failover
stays with the consumer's tiebreaker or an operator (promote).
Plan: `[recovery] rehead = false`, `rehead_after_secs = 120`. Reconciler
(only when `recovery.active` and `rehead`): a volume assembled/degraded,
not fenced (a consumer's failover in flight is left alone), idle, no
replacement/window, head unhealthy for ≥ rehead_after_secs
(consecutive_failures × interval), and a surviving leg on a healthy node
that reads `in_sync` (#48 evidence; none → no re-head, one event) →
fence at the current epoch, promote onto that leg. Failure: cooldown
(`next_assemble_after`), error event naming the manual promote. WARN at
start when on. Pure `rehead_plan` + unit tests; tests/replication.rs:
off by default does nothing; on + slave in sync (superblock) → re-headed.
Done: c4d4a88 (+ dc897ef docs); sc-build passes (68 unit, replication 8/8).
Turning it on waits on fencing through cluster membership + stormblock#6.

### Admin credential for the engine's destructive verbs (#47, P2) — DONE 2026-10-06
stormblock#274: array create/delete, member add/fail/replace/remove, and
every non-detach DELETE (drive close) need the admin token or a Kubernetes
bearer allowed `storage.storm.io` (`storage-admin`); the node token gets 401
under `admin_gate = enforce`. Calls of ours that are destructive:
create_raid1, delete_array, forget_array, array_add_member,
array_remove_member, delete_drive. Plan: `Engine.admin`, used only by those
(`admin_req`); ordinary calls keep the node token. Order:
`$STORMBLOCK_ADMIN_TOKEN` (any engine), `[local] admin_token_file` (this
machine only, like #12's minted token; no default — stormblock keeps it
out of services), else the `[kubernetes]` bearer. Tests: a mock engine
records the bearer per call.
Done: 39e43b2 (+ c6b5858 docs); sc-build passes (token 6/6). A refused admin
credential is retried with the node token (pre-#274 engines). On a node the
bearer needs stormcos#290 (mount /data/stormcert) and storage-admin.

### Placement by live IO load (#31, P3) — DONE 2026-10-06
Signal: stormblock's `/metrics` histogram `stormblock_nvmeof_io_seconds`
(every NVMe-oF I/O: legs to heads, served mirrors; ublk-local I/O is not
in it). Each poll reads `_count`/`_sum` summed over labels; the rate
between two polls gives `io.iops` and `io.busy` (I/O-seconds per second =
mean I/Os in flight) on NodeStatus. Missing metric, error or counter reset
→ no rate this poll (never fails the poll). Score:
(1−w)·free_ratio + w·(1 − busy/max_busy) over the fitting candidates,
w = `[placement] io_weight` (0.3); unknown busy = mean of known; no data
at all = today's free-ratio order. Ties by name. Tests: placement units
(hot node sheds, unknown neutral, w=0 = old), metrics parse, rate.
Done: 2d46f95 (+ e8cedca docs); sc-build passes (72 unit). Not seen on a
node yet (needs a release; ublk-local I/O is not in the signal).

### Rebalance by pool watermarks (#30, P3) — DONE 2026-10-06
Opt-in per pool: `[[pools]] high_watermark`, `low_watermark` (used
fraction), `max_moves` (in flight per pool, 1). No watermarks = no
rebalance (as today). Pure planner `src/rebalance.rs::plan`: a pool whose
nodes are all healthy; sources = pool nodes used > high, fullest first;
volumes of that pool with a created leg there, assembled (not degraded),
unfenced, idle, no replacement/window, past `next_releg_after`; target =
`move_target_candidates` ∩ pool nodes used < low and ≤ high after the
leg's size, picked by `placement::plan` (io_weight); one move per volume,
in-flight + new ≤ max_moves. Reconciler (`recovery.active`) →
`start_replacement(…, "rebalance")` (redundancy never drops), event per
move, cooldown on failure. `GET /api/v1/pools/{name}/rebalance` = the
proposal (dry run). Tests: planner units; tests via mock engines optional.
Done: 6013bf7 (+ 3ae0d9a docs); sc-build passes (75 unit). Never moves a
head's own leg. Not run against live engines (stormcentral#131).

### Tier migration pool → pool (#32, P3) — DONE 2026-10-06
`POST /api/v1/volumes/{name}/migrate {pool}` records `migration {to_pool,
from_pool, started_at, state, message}` (assembled volumes only; not
fenced/replacing/windowed; destination must fit: `placement::plan` over
its healthy nodes at its rung for every leg). The reconciler
(`recovery.active`) takes one step per idle volume: the next non-head leg
outside the destination → `start_replacement(…, to, "tier migration")`
with the target from `move_target_candidates` ∩ destination nodes. The
head's own leg moves by promote only (prestage rule), and a live head's
handover is stormblock#296: once only it is left, `state:
waiting_handover`. All legs in the destination → `pool` = to, migration
cleared, event. `DELETE …/migrate` cancels (moves done stay). Rebalance
skips migrating volumes. Pure `next_step`; unit tests + tests/replication.rs
(4 nodes, two pools).
Done: sc-build passes on 2bd672c (77 unit, replication 9/9) after four
no-slot tries. The head's own leg waits on stormblock#296.

### Routes stormblock-csi needs (#49, P2) — waits on the owner (2026-10-06)
Engine today: `/v1` leg expand yes, but no RAID1/slab grow → filed
stormblock#318 (expand waits on it). Snapshots: `/v1` + engine-local on the
head's array (one crash-consistent point) — buildable. Clone: COW on the
same slab only; no engine-to-engine copy. Group snapshots: one engine only.
`encrypted`/`qos_class`: `/v1` only records them → filed stormblock#319.
Asked on #49 (needs-owner): clone COW-on-source-array vs full copy; groups
same-head only; keep refusing encrypted/qos. Nothing built yet.

### Other open
- [x] #12 engine token default path and peer calls (done 2026-10-06).
- [x] #15 reassemble after head engine restart (done 2026-10-06).
- [x] #14 re-head — opt-in, off by default (done 2026-10-06).
- [ ] #35 forward announcements to stormfs — waits on the owner (2026-10-06): does stormfs keep its own registry (stormfs#64, unanswered; stormfs has no session)?
- [x] #42 done 2026-10-06: step 2 in ef3af98; the entry fix
      (stormcentral#72) via `component edit stormstorage --set config=…`:
      `token_file` is now under `[local]` (checked with `component export`).
- Docs refreshed from the code 2026-09-28, and again 2026-10-02 (#38's
  engine token and back-off; the component entry lives in stormcentral's
  database, #37; auth covers assemble; replicate carries orphans); the
  promises without code are #30–#36.

## Rules recap
- Conventional commits; changelog every change; docs ship with code.
- No claude attribution. Check `gh issue list --state open` at session start.
- Bugs in stormblock/stormdrive/stormfs → file issues there.
