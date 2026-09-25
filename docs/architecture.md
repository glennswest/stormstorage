# StormStorage — Founding Specification

**Status:** v1, 2026-08-26. Written with stormblock v9.13.0, stormdrive
v0.3.0, and stormfs v2 (in design) as the surrounding stack. Checked
against the code on 2026-09-24 (v0.3.0). Sections that describe what
does not run yet are marked **Design, not implemented**. The
[README](../README.md) documents what runs, including every config key
and route.

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
NodeStatus  { healthy, last_ok, consecutive_failures, total_bytes,
              free_bytes, engine_topology, volumes,
              source: static|registered|local }
```

Nodes enter the registry three ways:
1. **Static** — `[[nodes]]` in stormstorage.toml.
2. **Self-registration** — stormstorage implements stormblock's *existing*
   outbound heartbeat verbatim: `POST /api/v1/storage/register` with
   `{node_addr, hostname, volumes[]}` every `heartbeat_secs` (default
   30 s) and `/deregister` on shutdown (stormblock `src/stormfs.rs`). Set
   a node's `[stormfs] enabled = true`, `metadata_url` to stormstorage
   and `advertise_addr` to its own `host:9090`, and it announces itself
   with **zero engine changes**.
3. **Local adoption** (*implemented, #9*) — with `[local] enabled` (the
   default), the engine on stormstorage's own machine
   (`http://127.0.0.1:9090`) is adopted once it answers, under the
   engine's own name (`GET /api/v1/discovery` `local_node`), and so are
   the live peers of its stormblock cluster (same `cluster_id`, at their
   `mgmt_addr`). A node already named statically or by heartbeat wins.
   Adopted nodes are local to the instance: never replicated.

Every way, the poller enriches each node from its engine
(`GET /v1/nodes/capacity`: totals + topology labels) and marks nodes
unhealthy after `poll.fail_threshold` consecutive failures. Marking a
node unhealthy removes it from placement. Nothing yet acts on the
volumes that have legs there (#1).

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

**Slab pools and tiers** (*implemented, #9*). Those node-local pools are
shown as pools in their own right: each slab of each node is a `slab`
pool (tier, role, failure domain, total/free/allocated), and slabs of one
tier are summed across nodes as a `tier` pool. The poller reads each
node's slabs, volumes and slab slot tables and places every engine volume
on its slab(s): where it owns slots, else where its parent does (a fresh
clone), else the only slab of its role — otherwise `unknown`, never
guessed. This inventory is observed state (memory only, not replicated).
Still to come from the engine: the drive under each slab and each
volume's RAID/replica partners (stormblock#136), and each volume's
consumer beyond its `owner` (stormblock#138).

### DistVolume — RAID across individual volumes

The redundancy object. **A distributed RAID is built from individual
per-node volumes**: each leg is an ordinary stormblock thin volume on one
node; legs are placed across failure domains at the pool's rung; the
volume is assembled as RAID1 on a **head node** whose stormblock attaches
the remote legs over NVMe-TCP and mirrors across them.

```
DistVolume { name, size_bytes, pool, replicas, rung, created_at,
             assembly: single_leg | pending_engine_support | assembled,
             head: node?, array_id?,
             legs: [ {node, volume_id, state: created|failed, message,
                      master_node, export: {nqn, traddr, trsvcid, nsid},
                      drive_uuid, member_uuid} ] }
```

The head is the first placed node (`legs[0]`). Every leg, including the
head's own, is attached by the head as an `nvme-tcp://` drive over the
network (loopback for its own leg). A local fast-path is a later
optimization.

- **Everything is NVMe-TCP** — remote legs are NVMe-TCP namespaces
  exported by their node and attached by the head as drives/RAID members.
- **Legs move.** Replace/relocate a leg = create volume on the new node →
  export → head `add_member` → rebuild → `remove_member` old → delete old
  volume. stormblock RAID1's dynamic add/rebuild/remove *is* the leg-move
  primitive; the same sequence serves failure recovery, rebalancing, tier
  migration, and shelf/node evacuation. (Rebuild-error hardening is
  stormblock#69.)
- **Head failover** (*design, not implemented*): the legs are plain volumes — a new head can attach
  the surviving legs and reassemble (RAID superblocks identify members).
  Orchestrated re-head is a later phase; the data is never trapped.
- **Implemented (v0.3.0)**: stormblock#73 landed (2026-08-28) — the
  engine attaches `nvme-tcp://` URIs as drives and RAID members. Assembly
  runs inline at create; teardown at delete; leg moves via
  `POST /api/v1/volumes/{name}/move` with a background rebuild-wait.
  Proven live: create → assembled RAID1; move → converged with both
  members active and the old leg's volume deleted.
- **Not implemented yet:**
  - failure-driven re-leg when a node is lost (#1);
  - an export of the assembled array that consumers can attach (#2).
    Today only the legs are exported, for the head. Decided, blocked on
    the engine — see *Consumer serving* below.
  - retrying a failed assembly (#7). The volume stays
    `pending_engine_support`.

#### Consumer serving (#2) — *decided 2026-09-25, blocked on stormblock#149/#150*

A client must attach **the mirror**, never a leg: a leg's own export is
one unreplicated side, written behind the head's back.

**Chosen: a volume carved on the array** (option 1), over exporting the
array itself as a namespace (option 2). Why:

- **It is an ordinary volume.** The consumer gets the /v1 surface it
  already speaks — attach/detach, fencing, expand, reset, clones, the
  ublk fast path when the consumer is on the head — and stormblock-csi
  and stormfs need no new kind of thing. Option 2 is a new export kind
  with none of that.
- **It survives a re-head.** `POST /api/v1/arrays` already formats a slab
  on the array, and a slab carries its own volume records. A new head
  that re-assembles the same members can adopt the slab and the consumer
  volume comes with it, so republishing is "attach it again on the new
  head". A raw array namespace has no identity of its own to carry.
- **Option 2 conflicts with what the engine does today.** The array
  already holds a slab, so serving the raw array would expose slab
  metadata to a client writing over it.

Shape, once the engine can do it:

```
DistVolume.export  { state: none | pending_engine_support | published | failed,
                     volume_id,              // the consumer volume (head, or the leg's node for single_leg)
                     node,                   // where it is served
                     coordinates: {nqn, traddr, trsvcid, nsid},   // the AttachedLeg shape
                     published_at, republished: bool, message }
```

- `single_leg`: the coordinates are the only leg's own export — the same
  field, so consumers need no special case.
- `assembled`: a volume pinned to the array on the head, exported.
- **Delete** detaches the consumer export first, then the array teardown.
- **Republish** (`POST /api/v1/volumes/{name}/export`, and on noticing
  the head engine restarted) re-attaches and records whether the
  coordinates changed. NSID reuse (stormblock#96) is why the record keeps
  the volume id beside the coordinates.

What it needs from stormblock:
- **stormblock#150** — create a volume pinned to an array's slab, and keep
  array slabs out of general allocation. Today `array_id` on create is
  checked and then ignored, and the head's own volumes can land on a
  mirror's slab.
- **stormblock#149** — `/v1` attach cannot return NVMe-TCP coordinates
  for a locally backed volume. Since stormblock 2337c8a (2026-08-30) an
  attach by the master node (which is every attach stormstorage makes)
  gets ublk wherever ublk is available. That also breaks **leg assembly**
  on such engines until #149 lands or the engine sets
  `[management] ublk_transport = false`.

*Design, not implemented:* native `/v1` replication (prestage, fence/promote, epoch-carrying writes
— stormblock #5/#6/#7) is the *second* redundancy mechanism when its data
path lands; stormstorage orchestrates either through one DistVolume
model.

### Placement

Pure function; inputs: candidate nodes (pool selection ∩ healthy ∩
capacity ≥ size ∩ tier match), rung, replica count.

1. Group candidates by domain (label-chain prefix at rung).
2. Require ≥ replicas distinct domains — else a hard, explained error.
3. Within each domain, **load-balance**: the highest free-capacity ratio
   wins. Domains are then taken emptiest-first. A node that reports no
   capacity scores 0 but stays eligible. *(Design: score by live IO load
   as well, so hot nodes shed new legs. Not implemented.)*
4. Deterministic given equal inputs (testable); ties broken by name.

A leg move's automatic target uses the same function over healthy nodes
that carry no leg and sit in a domain distinct from every staying leg.

**Rebalance** (phase 3, *design, not implemented*) reuses leg moves: when a new node/shelf/cluster
joins a pool, stormstorage proposes leg moves from the fullest domains to
the emptiest until spread converges — same operation as failure
recovery, driven by policy instead of alarm.

### Tiering across clusters

*Design, not implemented.* Today a tier is only a node attribute that
pool selectors and create requests filter on.

A tier can be an entire cluster (testbed: 2.5" = high, 3.5" = medium,
PVE = backup). Tier migration = leg moves between pools: create legs in
the destination pool, mirror over, drop source legs. The backup tier is
asymmetric by design — an async catchup leg (engine #5/#6/#7 machinery
when it lands), not a synchronous mirror member.

### StormFS

*Design.* The stormstorage side exists (`GET /api/v1/nodes`,
`POST /api/v1/volumes`). Consuming it is stormfs#64. Forwarding
announcements to a stormfs endpoint is not implemented.

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
GET  / , /ui , /ui/                   embedded UI
GET  /api/v1/health                   {status, version}
GET  /api/v1/nodes                    registry + status
GET  /api/v1/nodes/{name}/inventory   slabs + engine volumes placed on them
GET  /api/v1/topology                 rungs + each node's label chain
GET  /api/v1/pools                    policy, slab and tier pools with rollups
POST /api/v1/placement/plan           dry-run: {size_bytes, pool?, replicas?, rung?, tier?} → legs
GET|POST /api/v1/volumes              distributed volumes; create places, creates legs, assembles
GET|DELETE /api/v1/volumes/{name}
POST /api/v1/volumes/{name}/move      {from: node, to?: node} — leg move
GET  /api/v1/events?since=
GET  /api/v1/summary                  stormd RemoteSummary card
GET  /api/v1/components               stormview feed (also WS /ws/components)
POST /api/v1/replicate                peer push {revision, volumes, registered}
GET  /api/v1/replication/status       {revision, peers}
POST /api/v1/storage/register         stormblock-compatible self-registration
POST /api/v1/storage/deregister
```

Error envelope `{error, code}` (family convention). `[api] api_token` is
in the config, but today it is only sent on outbound peer pushes. No
inbound request is checked (#6).

## UI

stormd newer-UI extension, same contract as stormdrive: `[process.ui]`
with `proxy` (embedded page at `/`, proxy-prefix aware) + `summary`
(dashboard card); see `deploy/stormd-ui.toml`. The stormview feed also
drives stormconsole's `stormstorage` plugin. Page: nodes table (health, capacity, labels), pools
(policy, slab, tier), node volumes with their slab and owner, distributed
volumes with leg states, create-volume form, event feed.

## Phases

1. **Registry + placement + volumes** (*done, v0.1.0*): static + announced
   nodes, poller, pools, placement engine, DistVolume create/delete with
   legs created per node via `/v1`, assembly `pending` on #73, UI, card.
2. **Leg wiring** (*done, v0.3.0*, except the last item): exports per
   leg, head assembly via #73, leg move (add/rebuild/remove sequence).
   Failure-driven re-leg on node loss is still open (#1).
3. **Rebalance + tier migration**: policy-driven leg moves; pool
   capacity watermarks.
4. **Native replication**: orchestrate /v1 prestage/fence/promote when
   the engine data path (#5/#6/#7) lands; async backup legs.
5. **HA**: state to StormKV/fastetcd; multiple instances.

## What this asked of the neighbours

- **stormblock#73** (landed 2026-08-28): attach an NVMe-TCP export as a drive / RAID1
  member via the management API — the one engine gap between "placed
  legs" and "mirrored legs".
- stormblock#70/#71/#72: label chains, sub-node spreading, remotely
  drivable /v1 — unchanged, this spec is their consumer.
- **stormfs**: consume `GET /api/v1/nodes` + volume placement instead of
  a private registry (issue filed on stormfs).
- **stormdrive**: none — its labels/health flow through stormblock and
  (later) directly to stormstorage's load model.
