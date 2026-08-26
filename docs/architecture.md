# StormStorage — Founding Specification

**Status:** v1, 2026-08-26. Written with stormblock v9.13.0, stormdrive
v0.3.0, and stormfs v2 (in design) as the surrounding stack.

## Mission

StormStorage is the **storage control plane across nodes and clusters** —
the brain that stormblock deliberately is not. stormblock is a per-node
*execution engine* (slabs, volumes, RAID, NVMe-oF/iSCSI targets, epoch
fencing); stormdrive is per-node *hardware truth* (drives, shelves, bays,
health). StormStorage sits above both and owns everything that spans
machines:

- the **registry** of storage nodes and the federation tree above them
- **pools** — named capacity groupings with policy, spanning nodes
- **placement** — which node gets which leg, spread across failure
  domains at a chosen rung, load-balanced
- **redundancy** — distributed volumes as RAID across individual per-node
  volumes, legs movable between nodes
- **tiering across clusters** and the migrations it implies
- the volume fabric that **stormfs** walks across for its massive shared
  filesystem

Direction from Glenn (2026-08-26): storage nodes are **SNO (single-node)
stormblock clusters from the start**; other clusters consume them; manage
across all of them; redundancy and replication are required now;
**everything speaks NVMe over TCP**; multiple storage pools; load
balancing across drives; RAIDs are across *individual volumes*, placed
across domains, movable across nodes.

## The stack

```
            consumers: k8s (CSI/wander), VMs, micro-VMs, containers, apps
                                   │
      ┌────────────────────────────┼──────────────────────────────┐
      │                            ▼                              │
      │   stormfs fleet  ──── metadata (sharded KV) ────┐         │
      │   (shared FS; data path goes DIRECT to nodes)   │         │
      ▼                                                 ▼         │
┌───────────────────────────────────────────────────────────────┐ │
│  stormstorage  :9093  — control plane (this spec)             │ │
│  registry · pools · placement · dist-volumes · tiering        │ │
└──────┬──────────────────┬──────────────────┬──────────────────┘ │
       │ drives /v1       │                  │                    │
       ▼                  ▼                  ▼                    │
┌────────────┐     ┌────────────┐     ┌────────────┐              │
│ SNO node A │     │ SNO node B │     │ SNO node C │ ◀── data ────┘
│ stormblock │◀───▶│ stormblock │◀───▶│ stormblock │   (NVMe-TCP,
│ stormdrive │ NVMe│ stormdrive │ -TCP│ stormdrive │    no control
└────────────┘ legs└────────────┘     └────────────┘    plane in path)
```

**StormStorage is never in the data path.** Data moves node↔node over
NVMe-TCP (RAID legs, migrations) and client↔node over NVMe-TCP (stormfs,
consumers). StormStorage only decides and orchestrates.

## Core concepts

### StorageNode
An SNO stormblock cluster: one engine endpoint, its labels, its role.
`node ≅ cluster` at the start — the model keeps them distinct rungs so
multi-node clusters slot in later without a remodel.

```rust
NodeConfig  { name, engine_url, api_token?, labels: {rung→value}, tier? }
NodeStatus  { healthy, last_ok, total_bytes, free_bytes,
              engine_topology, volumes, source: static|registered }
```

Nodes enter the registry two ways:
1. **Static** — `[[nodes]]` in stormstorage.toml.
2. **Self-registration** — stormstorage implements stormblock's *existing*
   outbound heartbeat verbatim: `POST /api/v1/storage/register` with
   `{node_addr, hostname, volumes[]}` every 30 s and `/deregister` on
   shutdown (stormblock `src/stormfs.rs`). Point a node's
   `[stormfs] metadata_url` at stormstorage and it announces itself with
   **zero engine changes**.

Either way, the poller enriches each node from its engine
(`GET /v1/nodes/capacity`: totals + topology labels) and marks nodes
unhealthy after consecutive failures.

### The federation tree

Physical chain (what fails together) and logical overlay (who groups with
whom), as pinned in the stormdrive docs and stormblock#72:

```
PHYSICAL  site ⊃ building ⊃ floor/room ⊃ row ⊃ rack ⊃ node ⊃ hba ⊃ shelf ⊃ bay
LOGICAL   multicluster ⊃ … ⊃ multicluster ⊃ cluster ⊃ node
```

A node's position is its label chain. Below the node, labels come from
stormdrive (`hba`, `shelf`, `bay`); stormstorage owns node-and-above. A
**domain at rung R** is the label-chain prefix down to R — spreading at R
means distinct prefixes.

### Pool
A named capacity grouping spanning nodes, with policy. Pools are how
"three clusters as three levels" is expressed:

```toml
[[pools]]
name     = "fast"          # 2.5" shelf cluster(s)
selector = { tier = "high" }        # nodes whose labels/tier match
replicas = 2
rung     = "cluster"                # spread legs across clusters
[[pools]]
name     = "capacity"
selector = { tier = "medium" }
[[pools]]
name     = "backup"
selector = { tier = "backup" }
replicas = 1
```

A volume is created *in a pool*; the pool supplies defaults (replicas,
spread rung, tier) that the request may override. Multiple pools may
select overlapping nodes — a pool is policy + selection, not exclusive
ownership. (Within a node, stormblock's slab/tier machinery is the
node-local pool; stormblock#71 adds sub-node spreading.)

### DistVolume — RAID across individual volumes

The redundancy object. **A distributed RAID is built from individual
per-node volumes**: each leg is an ordinary stormblock thin volume on one
node; legs are placed across failure domains at the pool's rung; the
volume is assembled as RAID1 on a **head node** whose stormblock attaches
the remote legs over NVMe-TCP and mirrors across them.

```
DistVolume { name, size, pool, replicas, rung,
             head: node, legs: [ {node, volume_id, export, state} ] }
```

- **Everything is NVMe-TCP** — remote legs are NVMe-TCP namespaces
  exported by their node and attached by the head as drives/RAID members.
- **Legs move.** Replace/relocate a leg = create volume on the new node →
  export → head `add_member` → rebuild → `remove_member` old → delete old
  volume. stormblock RAID1's dynamic add/rebuild/remove *is* the leg-move
  primitive; the same sequence serves failure recovery, rebalancing, tier
  migration, and shelf/node evacuation. (Rebuild-error hardening is
  stormblock#69.)
- **Head failover**: the legs are plain volumes — a new head can attach
  the surviving legs and reassemble (RAID superblocks identify members).
  Orchestrated re-head is a later phase; the data is never trapped.
- Phase-1 fallback until the engine gap closes (stormblock#73): legs are
  placed and created, exports wired, assembly recorded as `pending` —
  placement, moves, and bookkeeping all real, mirroring waiting on the
  engine.

The engine gap this needs — **attach an NVMe-TCP export as a drive /
RAID member via the management API** — is stormblock#73. The pieces
(NVMe-oF initiator would mirror `iscsi_dev.rs`; RAID1 add/remove exists;
`open_one_drive` currently dispatches only block-device-or-file) are all
in the engine's lane.

Native `/v1` replication (prestage, fence/promote, epoch-carrying writes
— stormblock #5/#6/#7) is the *second* redundancy mechanism when its data
path lands; stormstorage orchestrates either through one DistVolume
model.

### Placement

Pure function; inputs: candidate nodes (pool selection ∩ healthy ∩
capacity ≥ size ∩ tier match), rung, replica count.

1. Group candidates by domain (label-chain prefix at rung).
2. Require ≥ replicas distinct domains — else a hard, explained error.
3. Within each domain, **load-balance**: score by free-capacity ratio
   (v1), later by live IO load (per-drive utilization from stormdrive /
   per-node from engine metrics) so hot nodes shed new legs.
4. Deterministic given equal inputs (testable); ties broken by name.

**Rebalance** (phase 3) reuses leg moves: when a new node/shelf/cluster
joins a pool, stormstorage proposes leg moves from the fullest domains to
the emptiest until spread converges — same operation as failure
recovery, driven by policy instead of alarm.

### Tiering across clusters

A tier can be an entire cluster (testbed: 2.5" = high, 3.5" = medium,
PVE = backup). Tier migration = leg moves between pools: create legs in
the destination pool, mirror over, drop source legs. The backup tier is
asymmetric by design — an async catchup leg (engine #5/#6/#7 machinery
when it lands), not a synchronous mirror member.

### StormFS

stormfs v2 puts its namespace in an embedded sharded KV across fleet
nodes and writes file data **directly** to stormblock volumes over
NVMe-TCP — no server in the data path. What it needs from stormstorage
is exactly what consumers get:

- `GET /api/v1/nodes` — the live fleet (which engines exist, health,
  capacity, labels) — this is the "walk across" surface
- `POST /api/v1/volumes` — chunk-carrier volumes placed by pool/rung
  policy, so stormfs chunks land spread across the federation without
  stormfs re-implementing placement
- the registration endpoint means stormfs and stormstorage can share one
  fleet view: nodes announce once, both read it (stormstorage can also
  *forward* announcements to a stormfs metadata endpoint if stormfs
  keeps its own).

### Redundancy of stormstorage itself

Control plane only — if stormstorage is down, data keeps flowing
(NVMe-TCP sessions, RAID rebuilds in progress, exports: all engine-side).

**Peer replication (implemented, v0.2.0):** run one instance per
site/cluster and list the others under `[replication] peers`. Every
durable-intent mutation (volumes, registered nodes) bumps a revision and
pushes the full payload to every peer (`POST /api/v1/replicate`); a peer
applies only newer revisions. Poll status deliberately does not replicate
— each peer watches the engines itself, so freshness is local and peers
cannot ping-pong overwrites. This is honest async last-writer-wins for
state that is also rebuildable from the engines; consensus
(StormKV/fastetcd) is the phase-5 ladder, same as stormblock's GEM (#44).
State persists per-instance in `<data_dir>/state.json` (atomic writes).

## API (:9093)

```
GET  /                                embedded UI
GET  /api/v1/health
GET  /api/v1/nodes                    registry + status
GET  /api/v1/topology                 federation tree (nodes grouped by chain)
GET  /api/v1/pools                    pools + per-pool capacity/health rollup
POST /api/v1/placement/plan           dry-run: {pool|size,replicas?,rung?} → legs
GET|POST /api/v1/volumes              distributed volumes; create places+creates legs
GET|DELETE /api/v1/volumes/{name}
POST /api/v1/volumes/{name}/move      {leg: node, to?: node} — leg move (phase 2)
GET  /api/v1/events?since=
GET  /api/v1/summary                  stormd RemoteSummary card
POST /api/v1/storage/register         stormblock-compatible self-registration
POST /api/v1/storage/deregister
```

Error envelope `{error, code}` (family convention). `api_token` in config
from day one, off by default.

## UI

stormd newer-UI extension, same contract as stormdrive: `[process.ui]`
with `proxy` (embedded page at `/`, proxy-prefix aware) + `summary`
(dashboard card). Page: nodes table (health, capacity, labels), pools
rollup, volumes with leg states, create-volume form, event feed.

## Phases

1. **Registry + placement + volumes** (this scaffold): static + announced
   nodes, poller, pools, placement engine, DistVolume create/delete with
   legs created per node via `/v1`, assembly `pending` on #73, UI, card.
2. **Leg wiring**: exports per leg, head assembly via #73, leg move
   (add/rebuild/remove sequence), failure-driven re-leg on node loss.
3. **Rebalance + tier migration**: policy-driven leg moves; pool
   capacity watermarks.
4. **Native replication**: orchestrate /v1 prestage/fence/promote when
   the engine data path (#5/#6/#7) lands; async backup legs.
5. **HA**: state to StormKV/fastetcd; multiple instances.

## What this asked of the neighbours

- **stormblock#73** (new): attach an NVMe-TCP export as a drive / RAID1
  member via the management API — the one engine gap between "placed
  legs" and "mirrored legs".
- stormblock#70/#71/#72: label chains, sub-node spreading, remotely
  drivable /v1 — unchanged, this spec is their consumer.
- **stormfs**: consume `GET /api/v1/nodes` + volume placement instead of
  a private registry (issue filed on stormfs).
- **stormdrive**: none — its labels/health flow through stormblock and
  (later) directly to stormstorage's load model.
