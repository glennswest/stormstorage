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

## Build on dev, never on this Mac

Same rule as stormblock/stormdrive: **every cargo command runs on
`root@dev.g8.lo`** (`/root/stormstorage`).

```
commit → push → ssh root@dev.g8.lo 'cd /root/stormstorage && git pull && \
    CARGO_TARGET_DIR=/build/cargo/stormstorage cargo test'
```

Target dirs live on dev's 2 TB spinning drive (`/build/cargo/<project>`),
never on the SSD root.

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
- [ ] Node-loss handling: re-leg from surviving copies (next)
- [ ] Consumer serving: export the head array itself (a volume on the
      array, or the array as a namespace) so clients attach the mirror
      (next)

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
