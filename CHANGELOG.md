# Changelog

## [Unreleased]
<!-- New unreleased changes go here -->

### 2026-09-27
- **fix(test):** e2e-export checks the served volume on the head's array by name: the array lists the engine's volume uuid, not the /v1 id (#2).
- **fix:** Engine writes (volume create, attach, arrays, deletes) time out after 60 s instead of 5 s. On a loaded engine a leg create took longer than 5 s, failed the create, and could leave the engine finishing a volume nobody recorded. Reads and polls keep 5 s (#2, found by the e2e, #19).
- **fix(test):** e2e scripts wait up to 180 s for an engine (slab adoption took 38 s on a busy dev) and fail if it never answers, instead of reading an unminted token.
- **feat:** Consumer serving (#2). A distributed volume is served to
  consumers as `export` {state, volume_id, node, master_node, coordinates,
  published_at, coordinates_changed, message}. An assembled volume is served
  as a `<name>-mirror` /v1 volume pinned to the head's array
  (`placement.array_id`, stormblock#150 v19.0.0), attached over NVMe-TCP. A
  single-leg volume is served as its leg, with the same field. It is
  published at the end of create. `POST /api/v1/volumes/{name}/export`
  publishes or republishes and reports whether the coordinates changed. A
  head whose engine answers again is republished. Delete revokes the export
  first (detach, then delete the pinned volume) and keeps the record if that
  fails, because a dedicated array refuses deletion while a volume is
  pinned. Feed: `export` metric, Publish/Republish action. UI: Export column.
- **BREAKING:** volume names ending in `-mirror` are refused (reserved for
  served mirrors; a /v1 create is name-idempotent).
- **test:** `tests/export.rs`, mock engines keeping stormblock's rules:
  assembled → served from the array, republish unchanged, revoke before
  array delete; single-leg served as its leg; refusals.
- **feat:** Re-leg on node loss (#1). A reconciler runs after every poll: a
  leg whose node is unhealthy becomes `lost` and its volume `degraded`, and
  one replacement per volume is started through the leg-move sequence
  (`reason: node lost`). Dead-side cleanup never blocks it: undeletable old
  legs become orphans (`GET /api/v1/orphans`), reaped when the node answers.
  There is a cooldown after a failed attempt, and a lost leg stays lost, so
  a flapping node gives one re-leg. A lost head is reported, not recovered
  (#14). New `[recovery]` config. With replication peers, only an instance
  with `enabled = true` acts.
- **feat:** A leg replacement (operator move or re-leg) is recorded on the
  volume (`replacing`). A restart resumes the rebuild wait instead of losing
  the new leg, and a second concurrent move is refused. A replacement that
  fails part-way, or never rebuilds, undoes its new leg.
- **feat:** Deleting a volume with a leg on an unreachable node succeeds and
  leaves that leg as an orphan instead of returning 502 forever. The delete
  also removes an in-flight replacement leg.
- **feat:** Feed, summary and UI show `degraded`, lost legs, a re-leg in
  progress and orphans. The move button also works on degraded volumes.
- **fix:** `/v1` attach sends `transport: nvme_tcp` (stormblock#149, v19.1.1),
  so leg exports get NVMe-TCP coordinates on engines that offer ublk.
- **test:** `scripts/e2e-releg.sh`: three real engines, kill the non-head,
  re-leg converges, orphan reaped on restart.

### 2026-09-25
- **fix:** A leg attach answered with ublk now fails with an error that
  names the cause (stormblock#149) and the workaround
  (`[management] ublk_transport = false`), instead of "unexpected
  transport" (#2).
- **docs:** Consumer serving (#2) decided: a volume carved on the array, not
  the array as a namespace. Rationale and shape are in docs/architecture.md.
  Blocked on stormblock#150 (pin a volume to an array's slab) and
  stormblock#149 (/v1 attach returns ublk to the master node since
  stormblock 2337c8a, which also breaks leg assembly where ublk is
  available).
- **feat:** Adopt the local stormblock (#9). With `[local]` (on by
  default) the engine at `http://127.0.0.1:9090` is registered once it
  answers, under its own name from `GET /api/v1/discovery`, with the live
  peers of its stormblock cluster. Adopted nodes (source `local`) are not
  replicated. Token from `$STORMBLOCK_API_TOKEN` or
  `/etc/stormblock/api_token` when readable.
- **feat:** Node inventory (#9). Each poll reads every reachable engine's
  slabs, volumes and slab slot tables and places each volume on its slab(s)
  (by slots, else its parent's, else the only slab of its role, else
  `unknown`). New `GET /api/v1/nodes/{name}/inventory`.
- **feat:** Slabs are pools (#9). `GET /api/v1/pools` now carries `kind`:
  `policy` (the configured pools, as before), `slab` (one per slab per
  node: tier, role, domain, total/free/allocated, volume count) and `tier`
  (slabs summed per tier across nodes).
- **feat:** Components feed shows real counts (#9): `tier:<tier>`, slab pools
  `pool:<node>/<slab>` with their volumes, node volumes `nvol:<node>/<id>`
  with their pool(s) and owner; the system card counts pools and volumes.
  `/api/v1/summary` and the embedded UI show the same (Pools table by kind,
  a Node volumes table; the create form offers only policy pools).
- **docs:** README (local adoption, `[local]` keys, inventory, pool kinds,
  new route), architecture.md, example config.

### 2026-09-24
- **docs:** `docs/presentation.md`: an 11-slide Marp deck on purpose, place in
  stormcos (from stormcentral's graph), how it works, current features,
  planned work, interfaces, shipping and status. Built from the code (#5).
- **docs:** README rewritten from the code (#4). It covers what runs today, every
  CLI flag and config key with its default, the full API route table, the
  engine calls made, self-registration (stormblock `[stormfs]` needs
  `enabled` + `advertise_addr`), building with `sc-build`, and how it ships
  in goldens (stormcos `docs/goldens.md`, closes #3). Gaps found in the code
  are filed as #6 (inbound API auth) and #7 (no assembly retry).
- **docs:** docs/architecture.md checked against the code. Design-only
  sections are marked (head failover, native replication, IO-load placement,
  rebalance, tiering, stormfs forwarding). The API table is corrected (move
  takes `{from, to?}`, plan takes `size_bytes`/`tier`, and the components,
  replicate and replication-status routes are added). The DistVolume shape
  now matches `src/model.rs`.
- **docs:** Stale comments fixed in `src/config.rs` (`api_token` is
  outbound-only), `src/events.rs` (the actual event kinds) and `src/lib.rs`,
  and in the example config (#73 has landed; self-registration keys).
- **docs:** CLAUDE.md now builds with `sc-build` instead of ssh to root@dev,
  records the golden/shipping facts, and has a current work plan.

## [v0.3.0] — 2026-08-28

### Added
- Phase 2 — leg wiring. Multi-leg volumes now assemble into a real
  **RAID1 on the head node**: every leg is exported via `/v1 attach`
  (hot-added NVMe-TCP namespace), the head opens each as an
  `nvme-tcp://` drive (stormblock#73; its own leg over loopback —
  uniform, local fast-path later) and mirrors across them via
  `/api/v1/arrays`. Legs record master node, export coordinates, head
  drive uuid, and RAID member uuid; volumes record head + array id;
  `AssemblyState::Assembled`.
- **Leg move** — `POST /api/v1/volumes/{name}/move {from, to?}`: new leg
  placed (distinct staying domains at the volume's rung, emptiest first,
  or an explicit target), created, attached, added as a member; a
  background task waits for the rebuild to reach active, then retires
  the old member, closes the old head drive, and deletes the old volume.
  The same sequence is failure recovery and evacuation.
- Delete tears the assembly down first (array, head drives, exports),
  best-effort, before removing leg volumes.
- Engine client: attach/detach, arrays create/get/add/remove member,
  idempotent drive open, drive close, drive listing.
- UI: assembly chip (raid1/pending), head shown, per-leg move button.

### Verified
- e2e on dev.g8.lo with three live engines: create → assembled RAID1
  (legs node-b/node-c, member uuids captured); move off node-c →
  converged to node-b/node-d, both members active on the head, old
  volume gone from node-c. Requires stormblock ≥ the 2026-08-28 build
  (array members expose uuid + device_path).

## [v0.2.0] — 2026-08-26

### Added
- stormview integration — `GET /api/v1/components` +
  `/ws/components` serving system/pools/nodes/volumes with relations
  (pool has_many nodes+volumes, volume legs target nodes) and actions
  (volume delete), so stormd/stormsh/stormconsole render and drive the
  federation generically
- Peer replication — `[replication] peers`, revision-guarded
  last-writer-wins push of durable intent (volumes + registered nodes) to
  every peer on change (`POST /api/v1/replicate`,
  `GET /api/v1/replication/status`); poll status deliberately local so
  peers cannot ping-pong. Verified live on dev: volume created on peer A
  visible on peer B at the pushed revision within 2 s

## [v0.1.0] — 2026-08-26

### Added
- Phase 1 scaffold: node registry (static config + stormblock-compatible
  self-registration endpoints), capacity/health poller against /v1,
  pools (selector + replicas + rung), placement engine (label-chain
  domains at any rung, load-balanced by free ratio, deterministic),
  distributed volumes with per-node legs created via /v1 (slaves=0 —
  redundancy is stormstorage's, via legs; rollback on partial failure),
  events, persisted federation state, axum API on :9093, embedded UI,
  stormd summary card, deploy files (testbed-shaped example config,
  systemd unit, stormd [process.ui] snippet)
- End-to-end verified on dev.g8.lo against a live stormblock engine:
  poll → healthy with real capacity, pool create → leg volume visible in
  the engine, delete → both sides clean

### Documentation (bootstrap)
- **docs:** Founding specification (docs/architecture.md): control plane
  across SNO storage clusters — registry (static + stormblock-compatible
  self-registration), federation tree, pools, load-balanced placement at
  rungs, DistVolume = RAID across individual per-node volumes with
  movable NVMe-TCP legs, cross-cluster tiering, stormfs consumption,
  phased plan
- **chore:** Project bootstrap — CLAUDE.md work plan, README, changelog,
  .gitignore
