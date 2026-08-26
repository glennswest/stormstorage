# Changelog

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

## [Unreleased]
<!-- New unreleased changes go here -->
