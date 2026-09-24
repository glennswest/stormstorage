# Changelog

## [Unreleased]
<!-- New unreleased changes go here -->

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
