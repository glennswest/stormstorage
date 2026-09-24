---
marp: true
paginate: true
title: StormStorage
description: The storage control plane across Storm nodes and clusters
style: |
  section { font-size: 22px; }
  pre, code { font-size: 0.78em; }
  table { font-size: 0.8em; }
---

<!--
Render: npx @marp-team/marp-cli docs/presentation.md          (HTML)
        npx @marp-team/marp-cli docs/presentation.md --pdf    (PDF)
Written from the code at v0.3.0 (2026-09-24). Every claim points at a file;
README.md has the full reference. Diagrams are ASCII because Marp does not
render Mermaid.
-->

# StormStorage

**The storage control plane across Storm nodes and clusters**

v0.3.0 · `glennswest/stormstorage` · Rust, one binary, port **9093**

---

## What it is, and the problem it solves

- **stormblock** runs storage on *one* node: slabs, volumes, RAID and
  NVMe-TCP targets.
- **stormdrive** knows *one* node's hardware.
- No component decides **across** nodes: which node gets a volume, how to
  keep a copy in another failure domain, or how to move data off a node.

**stormstorage is that decision layer.** It keeps a registry of storage
nodes and pools, places volumes across failure domains, and builds each
one as a **RAID1 of per-node volumes joined over NVMe-TCP**, whose legs
can move.

It is **never in the data path**. It only calls engine management APIs,
and the data flows engine to engine.

---

## Where it sits in stormcos

From stormcentral's relationship graph (`config/stormcentral.toml`):

```
                    stormcos  (ships it in goldens)
                        │ depends_on
                        ▼
                  ┌──────────────┐
                  │ stormstorage │  :9093
                  └──────┬───────┘
      ┌─────────────┬────┴────────┬──────────────┐
      ▼             ▼             ▼              ▼
  stormblock    stormdrive     stormview       stormd
  engines :9090 (hardware,     (components     (supervises it,
  /v1 + /api/v1  via engine    feed crate)     UI proxy + card)
                 topology)
```

- **Runtime consumer:** stormconsole's `stormstorage` plugin reads the
  feed on :9093.
- **Planned consumer:** stormfs v2 (stormfs#64).
- stormstorage has no code that calls stormdrive. The stormdrive
  dependency is indirect: its labels arrive through the engine's
  topology.

---

## How it works

```
   config [[nodes]]   stormblock [stormfs] heartbeat
          │                 │ POST /api/v1/storage/register
          ▼                 ▼
   ┌──────────── registry (FedState) ─────────────┐   state.json
   │  nodes · volumes · revision                   │──(data_dir)
   └──┬───────────────┬──────────────┬─────────────┘
      │ poller        │ placement    │ orchestrate
      │ every 15 s    │ (pure fn)    │ assemble / teardown / move
      ▼               ▼              ▼
  GET /v1/nodes/   domains at     leg: POST /v1/volumes → /attach
  capacity         rung, emptiest head: POST /api/v1/drives nvme-tcp://…
  → health, free   first          head: POST /api/v1/arrays (RAID1)
                                          │
   peers ◀── POST /api/v1/replicate ──────┘ on every durable change
```

The source is `src/registry.rs`, `src/placement.rs`,
`src/orchestrate.rs` and `src/replicate.rs`.

---

## What it does today (1/2)

- **Registry.** Nodes come from static `[[nodes]]` or from stormblock's
  existing heartbeat, with no engine changes. The poller marks a node
  unhealthy after `fail_threshold` (default 3) failed polls.
- **Pools.** A pool is a selector (tier, labels, names) plus default
  `replicas` and `rung`. Pools may overlap.
- **Placement.** Takes one node per distinct failure domain at a rung.
  Only healthy nodes with enough free space count, and the node with the
  highest free ratio wins. The result is deterministic. If there are too
  few domains, the request fails with an explanation.
- **Volume create.** Creates one thin volume per leg through `/v1`. If a
  leg fails, the legs already made are rolled back.

---

## What it does today (2/2)

- **Assembly.** Each leg is exported over NVMe-TCP. The head (the first
  leg's node) opens every leg as an `nvme-tcp://` drive and builds a
  **RAID1**. This was verified live on dev with three engines.
- **Leg move.** `POST …/move {from, to?}` runs these steps:
  1. create a new leg, attach it and add it as a member;
  2. wait for the member to report active (up to 1 h);
  3. retire the old leg.
- **Delete.** Tears down the array, head drives and exports first, then
  the legs.
- **Peer replication.** Volumes and registered nodes are pushed to peers.
  The newest revision wins. Each peer polls the engines itself.
- **Surfaces.** An embedded UI, a stormd card, a stormview feed (REST and
  WebSocket) and an event ring.

---

## Planned: not in the code yet

| | Issue |
|---|---|
| Re-leg automatically when a node is lost | #1 (P1) |
| Inbound API auth (`api_token` is outbound-only today) | #6 (P1) |
| Export the assembled mirror so consumers can attach it | #2 (P2) |
| Retry a failed assembly | #7 (P2) |
| Rebalance on pool watermarks; tier migration between pools | phase 3 |
| Native `/v1` replication (prestage/fence/promote) | phase 4 |
| HA state in StormKV/fastetcd | phase 5 |
| Head failover; placement by IO load | design |

---

## Interfaces: CLI and config

**CLI:** `stormstorage --config <path> [--listen addr] [--data-dir dir]`.
The config defaults to `/etc/stormstorage/stormstorage.toml`; a missing
file means all defaults. Logging is set with `RUST_LOG`.

**Config:** `listen_addr` (default `0.0.0.0:9093`), `data_dir`,
`[federation] rungs`, `[poll] interval_secs=15 fail_threshold=3`,
`[api] api_token`, `[replication] peers`, `[[nodes]]`, `[[pools]]`.
README.md lists every key with its default.

**Self-registration:** in the engine's config, set `[stormfs] enabled = true`,
`metadata_url = "http://<host>:9093"` and `advertise_addr = "<engine>:9090"`.

---

## Interfaces: API on :9093

Errors return `{error, code}` with HTTP 404/400/409/502.

| Endpoints | Purpose |
|---|---|
| `/api/v1/health` | health |
| `nodes`, `topology`, `pools` | registry |
| `placement/plan` | dry-run placement |
| `volumes[/{name}[/move]]` | volumes |
| `events`, `summary` | events, stormd card |
| `components`, `/ws/components` | stormview feed |
| `replicate`, `replication/status` | peers |
| `storage/register`, `storage/deregister` | self-registration |

**Metrics:** there is no metrics endpoint. The card and feed carry counts.

---

## How it ships and runs

- A stormcos **service component**, built by stormcos
  `deploy/build-goldens.sh` as a static musl binary.

  | Golden | Slab |
  |---|---|
  | `stormstorage` | system1 |
  | `stormstorage-logs` | system1 |
  | `stormstorage-data` | data1, so its state survives installs |

- **Start.** stormd supervises it with
  `--config /etc/stormstorage/stormstorage.toml`. The shipped config sets
  only `listen_addr` and `data_dir`, so a node starts with an empty
  registry until engines register.
- **Health** is `GET /api/v1/health` on 9093. The gateway route is
  `storage.storm1.g8.lo`.
- **Update.**
  1. Push, then `sc-build` passes.
  2. Run `stormcentral component build stormstorage`, which produces a
     new golden and files a stormcos release request.
  3. A release is composed and nodes clone it copy-on-write.

---

## Status

- **Done:**
  - Phase 1 (v0.1.0): registry, pools, placement, volumes.
  - Phase 1.5 (v0.2.0): stormview feed, peer replication.
  - Phase 2 (v0.3.0): RAID1 assembly and leg move, proven on dev.
- **18 unit tests** (placement, config, move targets, engine response
  shapes, state persistence, components feed, events), run by
  `sc-build`.
- **Biggest risks today:**
  - a lost node leaves its volumes degraded but still reported as
    `assembled` (#1);
  - the API is unauthenticated, including `/api/v1/replicate` (#6).
- **Next:** #1, then #6, then #2, which lets stormfs and CSI consume
  mirrored volumes.

Docs: `README.md` (reference), `docs/architecture.md` (design),
`CLAUDE.md` (work plan).
