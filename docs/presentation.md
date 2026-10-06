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
Written from the code at v0.3.0 (2026-09-24), refreshed from main on
2026-09-28 (unreleased work since v0.3.0 is marked). Every claim points at a file;
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

- **Registry.** Nodes come from static `[[nodes]]`, from stormblock's
  existing heartbeat (no engine changes), or are **adopted**: the engine
  on this machine and its cluster peers (`[local]`, on by default). The
  poller marks a node unhealthy after `fail_threshold` (default 3)
  failed polls.
- **Node inventory.** Every poll reads each engine's slabs (with their
  drive) and volumes with where they live: slabs, drives, RAID partners,
  rebuild state, kind, in use and consumer. Slabs show as pools.
- **Pools.** A pool is a selector (tier, labels, names) plus default
  `replicas` and `rung`. Pools may overlap.
- **Placement.** Takes one node per distinct failure domain at a rung.
  Only healthy nodes with enough free space count. The best score wins:
  free ratio, weighted against live NVMe-oF load (#31). The result is
  deterministic. If there are too
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
- **Consumer serving (#2).** Consumers attach the mirror: a volume pinned
  to the head's array, served over NVMe-TCP, published at create and
  revoked first on delete.
- **Re-leg on node loss (#1)** and **assembly retry (#7)**: automatic,
  through the leg-move sequence; `POST …/assemble` retries at once.
- **Auth (#6).** With `[api] api_token` set, every write needs the token.
- Kubernetes PVCs on stormcos are the **built-in stormblock driver** on
  the pod's node, not this; CSI is for third-party drivers.
- **PV/PVC per node volume (#28).** Each node volume shows the PV and
  bound PVC rustkube-node writes for it, read from the apiserver.
- **Surfaces.** An embedded UI, a stormd card, a stormview feed (REST and
  WebSocket) and an event ring.

---

## Planned: not in the code yet

| | Issue |
|---|---|
| Live runs of re-leg and serving (need a built stormblock in the job) | #1, #2 → stormcentral#131 |
| Rebalance on pool watermarks | #30 (phase 3) |
| Tier migration between pools | #32 (phase 3) |
| Native `/v1` replication (prestage/fence/promote) | #33 (phase 4) |
| HA state in StormKV/fastetcd | #34 (phase 5) |

---

## Interfaces: CLI and config

**CLI:** `stormstorage --config <path> [--listen addr] [--data-dir dir]`.
The config defaults to `/etc/stormstorage/stormstorage.toml`; a missing
file means all defaults. Logging is set with `RUST_LOG`.

**Config:** `listen_addr` (default `0.0.0.0:9093`), `data_dir`,
`[federation] rungs`, `[poll] interval_secs=15 fail_threshold=3`,
`[api] api_token`, `[replication] peers`, `[local]` (adoption, engine
token), `[recovery] enabled cooldown_secs=300 rebuild_timeout_secs=3600`,
`[[nodes]]`, `[[pools]]`. README.md lists every key with its default.

**Self-registration:** in the engine's config, set `[stormfs] enabled = true`,
`metadata_url = "http://<host>:9093"` and `advertise_addr = "<engine>:9090"`.

---

## Interfaces: API on :9093

Errors return `{error, code}` with HTTP 404/400/409/502, and 401 on a
write without the token when `api_token` is set.

| Endpoints | Purpose |
|---|---|
| `/api/v1/health` | health |
| `nodes`, `nodes/{name}/inventory`, `topology`, `pools` | registry, inventory |
| `placement/plan` | dry-run placement |
| `volumes[/{name}[/move\|/export\|/assemble]]`, `orphans` | volumes |
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
  `listen_addr`, `data_dir` and `[local] token_file` (the engine's minted
  token), and the node adopts its own engine once it answers. The config
  text is part of the component entry in stormcentral's database
  (`stormcentral component edit`). Its `token_file` sits under
  `[local]` (fixed 2026-10-06, #42).
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
- **Since v0.3.0 (unreleased):** local adoption and inventory (#9, #11),
  auth (#6), re-leg (#1), serving (#2), assembly retry (#7), test
  container (#8), the engine token on every call with a back-off on 401
  (#38).
- **Tests:** 39 unit, plus integration tests against mock engines
  (`tests/adopt.rs`, `auth.rs`, `export.rs`, `token.rs`), run by `sc-build`. The node
  suites `/test short|medium|long` (`test/`) have not passed on a test
  machine yet.
- **Biggest risks today:**
  - re-leg and serving are not yet verified on live engines
    (stormcentral#131);
  - serving to named consumer hosts (#51) is verified only against mock
    engines so far.
- **Next:** the live runs; turning on automatic re-head once a lost head
  is fenced by cluster membership (#14 is in, off by default).

Docs: `README.md` (reference), `docs/architecture.md` (design),
`CLAUDE.md` (work plan).
