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
It builds the pushed commit on a fresh build VM as an unprivileged user and
deletes it afterwards. Nothing persists and nothing runs as root.
dev.g8.lo was retired on 2026-10-07 (owner); plain `sc-build` runs on a
VM by default (stormcentral#521, closed). Older notes that say "on dev" are
history.

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
stormview is pinned by `rev` in Cargo.toml (#64; golden builds refuse
unpinned git deps); bump the `rev`, then `cargo update -p stormview`.

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

## Work plan and history

Moved to [docs/work-plan.md](docs/work-plan.md) (CLAUDE.md is loaded into every session; its size made the session's context thrash). Nothing was dropped. Not loaded by default: `grep -n '^##' docs/work-plan.md`, then read only the section you need. Write new intentions and progress there, not here.

## Rules recap
- Conventional commits; changelog every change; docs ship with code.
- No claude attribution. Check `gh issue list --state open` at session start.
- Bugs in stormblock/stormdrive/stormfs → file issues there.
