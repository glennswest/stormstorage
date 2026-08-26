# StormStorage Development Guide

## Project Overview

The **storage control plane across nodes and clusters** in the Storm
ecosystem. stormblock executes on one node; stormdrive knows one node's
hardware; **stormstorage decides across all of them**: registry of SNO
storage clusters, pools, placement across failure domains, distributed
volumes as RAID across individual per-node volumes (legs over NVMe-TCP,
movable), tiering across clusters, and the fleet surface stormfs walks
across. Founding spec: [docs/architecture.md](docs/architecture.md).

**Version: 0.1.0** — version locations: `Cargo.toml`, `Cargo.lock`, this file.

Never in the data path. Everything node↔node and client↔node is NVMe-TCP.

## Build on dev, never on this Mac

Same rule as stormblock/stormdrive: **every cargo command runs on
`root@dev.g8.lo`** (`/root/stormstorage`).

```
commit → push → ssh root@dev.g8.lo 'cd /root/stormstorage && git pull && cargo test'
```

Clean `target/debug` on dev when done; check `df -h /`.

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

### Phase 1: Registry, pools, placement, volumes — IN PROGRESS
- [x] Founding spec (docs/architecture.md)
- [ ] Scaffold: config (nodes/pools/rungs), model, events, persistence
- [ ] Engine client (/v1: capacity, volume create/delete/list; bearer)
- [ ] Poller: enrich + health-mark every node
- [ ] stormblock-compatible register/deregister endpoints
- [ ] Placement engine: domain grouping at rung, load-balanced, pure+tested
- [ ] DistVolume create/delete (legs created per node; assembly pending #73)
- [ ] API + embedded UI + stormd summary card
- [ ] Build/test on dev; v0.1.0
- [ ] Issues: stormblock#73 (NVMe-TCP export as drive/RAID member via API),
      stormfs (consume stormstorage registry/placement)

### Phase 2: Leg wiring
- [ ] Exports per leg; head assembly via stormblock#73
- [ ] Leg move: create → export → add_member → rebuild-verified → remove →
      delete (also the failure-recovery and evacuation path)
- [ ] Node-loss handling: re-leg from surviving copies

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
