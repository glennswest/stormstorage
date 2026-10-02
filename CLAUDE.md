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

### Open: CSI relationship (Glenn, 2026-08-26) — #36
stormblock is the default-everywhere storage. **PVCs on stormcos are the
built-in `stormblock` driver**: the kubelet clones the sealed blank of the
claim's size class on the pod's node and attaches it over ublk, with no CSI
(stormblock CLAUDE.md, rustkube `docs/storage.md`). CSI (stormblock-csi) is
the compatibility path for third-party drivers and *foreign* Kubernetes:
still wanted, not primary. To look at (#36): stormblock-csi targets a
single engine's /v1 today; for a foreign multi-node cluster it could
target stormstorage (fleet placement, mirrored volumes, #2's export)
instead. Needs an analysis pass over stormblock-csi before changing
anything. Replicated claims across servers (rustkube-node#68) go through
re-leg (#1).

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
      stormcentral#131, and current engines need #27 (host_nqn).
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
      stormcentral#131 (#25) and #27. History (2026-09-27). Unblocked:
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
      holding any of them otherwise → refuse. stormblock's array create
      does not refuse drives already in an array, so a blind retry would
      format a second array over the legs → stormblock issue. Event text
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
- [ ] Pool watermarks; policy-driven leg moves to new nodes/shelves/clusters (#30)
- [ ] Placement by live IO load, not only free ratio (#31)
- [ ] Cross-cluster tier migration (pool → pool) (#32)

### Phase 4: Native replication
- [ ] Orchestrate /v1 prestage/fence/promote when stormblock #5/#6/#7 data
      path lands; async catchup legs for the backup tier (#33)

### Phase 5: HA
- [ ] State to StormKV/fastetcd; multiple stormstorage instances (#34)

### Other open
- [ ] #12 engine token default path and peer calls; #14 re-head; #15
      reassemble after head engine restart; #26 stalled head leg stays
      lost; #27 host_nqn on attach; #28 PV/PVC; #35 forward announcements
      to stormfs; #42 the registry entry's config has `token_file` at the
      top level (ignored; needs `[local]`) and `$STORMBLOCK_TOKEN_FILE` is
      not read — the entry edit was refused from this session, owner/
      stormcentral to fix.
- Docs refreshed from the code 2026-09-28, and again 2026-10-02 (#38's
  engine token and back-off; the component entry lives in stormcentral's
  database, #37; auth covers assemble; replicate carries orphans); the
  promises without code are #30–#36, and #42.

## Rules recap
- Conventional commits; changelog every change; docs ship with code.
- No claude attribution. Check `gh issue list --state open` at session start.
- Bugs in stormblock/stormdrive/stormfs → file issues there.
