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
stormcos `docs/goldens.md`. When an issue's work is complete, request the
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
  tier). **Load balancing** in placement (free-ratio now, IO load later).
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

### Open: CSI relationship (Glenn, 2026-08-26)
stormblock is the default-everywhere storage; rustkube and the rest of
the Storm stack integrate it natively, which makes CSI the compatibility
path for *foreign* Kubernetes, not the primary path. Still wanted, but
stormblock-first. To look at: stormblock-csi targets a single engine's
/v1 today — with stormstorage above the engines it should target
stormstorage (fleet placement) instead. Needs an analysis pass over
stormblock-csi before changing anything.

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
- [ ] Node-loss handling: re-leg from surviving copies (#1, P1) — IN PROGRESS
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
- [ ] Consumer serving (#2) — IN PROGRESS (2026-09-27). Unblocked:
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
- [ ] Retry a failed assembly (#7). Today the volume stays pending.

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
      shapes, placement precedence, feed. PV/PVC stays open on
      rustkube-node#59.
- Build box has no clippy (stormcentral#31); `sc-build 'cargo clippy'`
  fails until that is fixed.

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

### Phase 3: Rebalance + tier migration
- [ ] Pool watermarks; policy-driven leg moves to new nodes/shelves/clusters
- [ ] Cross-cluster tier migration (pool → pool)

### Phase 4: Native replication
- [ ] Orchestrate /v1 prestage/fence/promote when stormblock #5/#6/#7 data
      path lands; async catchup legs for the backup tier

### Phase 5: HA
- [ ] State to StormKV/fastetcd; multiple stormstorage instances

## Rules recap
- Conventional commits; changelog every change; docs ship with code.
- No claude attribution. Check `gh issue list --state open` at session start.
- Bugs in stormblock/stormdrive/stormfs → file issues there.
