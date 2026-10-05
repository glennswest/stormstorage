# Replication on the RAID head (#33)

How stormstorage provides what stormblock-csi's `/v1` replica surface asks
for (sync state, prestage, fence, promote, dual-attach) over its
distributed volumes, and what it asks of the engine in return.

**Why here and not in the engine.** The owner chose option **(b)** on
stormblock#179 (2026-10-05): cross-node RAID1 is built on stormstorage's
RAID heads over NVMe/TCP legs, not inside the engine. stormblock #5
(prestage, sync state, resync throttle) and #7 (bounded dual-attach) moved
to this issue. The engine's own `/v1/prestage`, `/v1/fence`,
`/v1/promote` and `/v1/dual-attach` stay control-plane only: they record
state and copy nothing. **stormstorage never calls `/v1/prestage`,
`/v1/promote` or `/v1/dual-attach`.** It does call `/v1/fence` on each
leg (see *Fence*), because the engine's per-volume epoch is the hook
stormblock#6 builds on.

## The model

A distributed volume (`DistVolume`) is N legs. Each leg is a thin `/v1`
volume on its own node. The **head** opens every leg as an `nvme-tcp://`
drive and runs a RAID1 across them (`/api/v1/arrays`). Consumers attach
`<name>-mirror`, a volume pinned to that array and served by the head.

| /v1 concept (stormblock-csi) | Here |
|---|---|
| master replica | the leg on the head node (role `master`) |
| slave replica | every other leg, and a replacement leg in flight (role `slave`) |
| `sync` | the head array's member state for that leg's drive |
| `prestage {node}` | replace a slave leg (the leg-move sequence) |
| `bandwidth_class` | the head array's rebuild rate cap |
| `epoch` / `fence` | `DistVolume.epoch`, CAS; carried down to each leg's `/v1` epoch |
| `promote {target_node}` | move the head onto the target's leg node |
| `dual-attach` window | a bounded window on the volume; commit = fence + promote |

## Sync state

After every poll, stormstorage reads each assembled volume's array on its
head (`GET /api/v1/arrays/{id}`). The reading stays in memory, like node
status: it is not persisted and not replicated. Each leg becomes a replica
in **the same JSON as stormblock's `/v1` `Replica`**, so a `/v1` client
parses it unchanged:

```json
{"node": "b", "role": "slave",
 "sync": {"state": "resyncing", "progress_pct": 25.0, "lag_bytes": 805306368}}
```

| Member state on the head | `sync` |
|---|---|
| `active` | `{"state": "in_sync"}` |
| `rebuilding` | `{"state": "resyncing", "progress_pct": rebuilt / member_data × 100, "lag_bytes": member_data − rebuilt}` |
| anything else, a `lost` or `failed` leg, a pending assembly | `{"state": "detached"}` |

**No reading means `detached`.** That covers a head not read yet, a head
that does not answer, or a reading of a different head or array. A
consumer that waits for `in_sync` before a failover is never told a copy
is in sync without evidence. A single-leg volume is one master, `in_sync`.

`health` follows `/v1`: `healthy` when at least `replicas` copies are in
sync, `faulted` when none is, `degraded` otherwise.

`GET /api/v1/volumes/{name}/replicas` returns the volume in this shape:
`{id, name, size_bytes, epoch, fenced, health, replicas[], bandwidth_class,
head, dual_attach, sync_read_at, rebuild_bytes_per_sec}`. The full volume
record (`GET /api/v1/volumes[/{name}]`) carries the same list as
`replica_sync`, plus `health` and `sync_read_at`. Its `replicas` field is
the copy count, as before. The feed shows `in sync n/m`, a `resync node
pct%` metric and the epoch when it has moved.

### Where stormblock-csi reads it (stormblock-csi#29)

**From stormstorage, directly.** Under (b) the engine never holds the
truth: a `/v1` volume on one engine has no cross-node copy, and the head's
array lives on another engine than the one a client might ask. Making the
engine report it would make the engine call up to stormstorage, which
inverts the layering. The answer is also only reachable on stormstorage's
side: a mirrored volume is created here (`POST /api/v1/volumes` with
`replicas: 2`), not by `/v1` create. So a stormblock-csi that wants
mirrored volumes, failover and moves talks to stormstorage for them:

- create: `POST /api/v1/volumes {name, size_bytes, replicas, bandwidth_class?}`
- state: `GET /api/v1/volumes/{name}/replicas`
- prestage, fence, promote, dual-attach: the routes below, which take the
  `/v1` bodies and return the `/v1` error envelope.

How stormblock-csi reaches stormstorage, and with which token, belongs
with its own open decision on how it reaches the engines
(stormblock-csi#32).

## Epoch and fence

Every distributed volume has an `epoch`, starting at 1 like `/v1`.

`POST /api/v1/volumes/{name}/fence {expected_epoch}` → `{epoch,
legs_fenced[], legs_not_fenced[]}`:

1. **CAS on the record.** `expected_epoch` must equal the epoch, else
   **412** `{code: "stale_epoch", current_epoch}`. Two racing tiebreakers
   cannot both fence. The epoch becomes `expected + 1` and the volume
   `fenced: true` (the head at the old epoch has lost its writer role).
   Only a mirrored volume (assembled or degraded) can be fenced; anything
   else is a 409.
2. **Every reachable leg is fenced on its engine**, through that engine's
   existing `/v1` CAS: read the leg's epoch (`GET /v1/volumes/{leg}`),
   then `POST /v1/volumes/{leg}/fence {expected_epoch}`. The leg's new
   epoch is kept on the leg (`legs[].epoch`), and every later attach of
   that leg presents it. An unreachable leg is reported in
   `legs_not_fenced`. It is lost anyway, and it is re-legged.

The fence is local to this instance. With `[replication] peers` the
record is replicated last-writer-wins, like every intent. Only one
instance should take fence and promote calls, as with re-leg (`[recovery]
enabled`).

### The leg attach contract (for stormblock#6)

What a fence must achieve: **a head that has been fenced can no longer
write to any leg.** Today the engine bumps the leg's `/v1` epoch and
enforces nothing. stormblock#6 builds the enforcement to this contract:

1. **The attach carries the epoch.** `POST /v1/volumes/{leg}/attach
   {node, mode: "read_write", transport: "nvme_tcp", host_nqn, epoch}`.
   `epoch` is the leg volume's own `/v1` epoch as the head last saw it
   (after the last fence, the epoch that fence returned). stormstorage
   sends it from this release on. Engines before #6 ignore it.
2. **A stale attach is refused.** If the volume's epoch is not `epoch`,
   the answer is **412** `{code: "stale_epoch", current_epoch}` and
   nothing is attached. Once a volume has been fenced (epoch > 1), an
   attach **without** `epoch` is refused the same way, or a zombie head
   could reattach by leaving the field out. An attach without `epoch` at
   epoch 1 is accepted, for compatibility.
3. **A fence revokes.** `POST /v1/volumes/{leg}/fence` (the existing CAS),
   on success and before it answers, revokes every attachment of that
   volume made at a lower epoch. Its namespace goes from that host's
   subsystem, and that host's controllers on it are disconnected, so
   in-flight and later writes from it fail. The attachment records carry
   their epoch (`GET /v1/volumes/{id}` lists them) so this is decidable.
4. **Per host, not shared.** Fencing means something only when each head
   reaches a leg through a subsystem that admits that head alone:
   `host_nqn` on the attach (stormblock#210). On the shared subsystem,
   the new head's attach would hand the namespace straight back to every
   connected host, the zombie included. stormstorage sends the head's
   `host_nqn` on leg attaches with #27. Until #27, fencing is
   bookkeeping only.
5. **Persisted.** The epoch and the attachment epochs survive an engine
   restart, so a leg engine that restarts does not accept the zombie
   again.

This makes the zombie-master guard a property of the leg, enforced where
the data is. No per-I/O epoch is needed. The engine's self-demotion after
a lease timeout (stormblock#6's other half) is not needed for safety
here either: a head cut off from its legs fails its own writes. So
stormstorage builds no self-demotion. A head that loses its contact with
stormstorage keeps serving until it is fenced, which is the `/v1`
behaviour of a partitioned master.

## Promote

`POST /api/v1/volumes/{name}/promote {target_node, fenced_epoch}` moves
the head onto `target_node`:

- **412** unless the volume is fenced at exactly `fenced_epoch`. A
  promote with no fence before it is a 412 too.
- **409** while a dual-attach window is open (closing it with commit is
  the cutover), while a leg replacement runs, when the target holds no
  created leg or is unreachable, or when the volume is not assembled.
- **409** while the **old head is still reachable.** A handover from a
  live head needs the engine to let go of the array without writing to
  its members. That does not exist yet: deleting the array is refused
  while the served volume is pinned to it, and closing its drives writes
  failure marks into the surviving legs' superblocks. This is
  **stormblock#296**. Promote proceeds when the old head is gone
  (failover).

The steps (the volume's claim is held, so no re-leg or assembly runs
beside it):

1. Every surviving leg (created, node reachable) is attached at its
   fenced epoch and opened on the target as an `nvme-tcp://` drive.
2. `POST /api/v1/arrays/assemble {drive_uuids}` on the target puts **the
   same array** (same uuid) back together from the legs' superblocks, and
   adopts its dedicated slab with the volumes on it (stormblock#252). If
   the array does not come up, the promote fails (502) and the record is
   unchanged.
3. Member uuids are read back, the rebuild rate is set for the volume's
   class, and legs on unreachable nodes are marked `lost`. The volume is
   `degraded` until the reconciler re-legs them, which it now can,
   because the head is reachable.
4. The served volume `<name>-mirror` came across with the slab. `/v1` on
   the new head has no record of it, so it is served through
   `POST /api/v1/volumes/{id}/attach` (`export.adopted: true`; revoke uses
   `DELETE /api/v1/volumes/{id}[/attach]`). **A new, empty served volume
   is never made in its place**: if it did not come across, the export is
   `failed` with that reason. Consumers get new coordinates
   (`coordinates_changed: true`).
5. The old head is recorded in `stale_heads` (`GET /api/v1/stale-heads`).

Promoting the current head (`target_node` = head) keeps it, re-attaches
the legs at their fenced epochs and clears `fenced`.

**Former heads.** When a stale head's engine answers again, the reconciler
lists its arrays. If it no longer holds the array (it restarted: arrays on
runtime-registered `nvme-tcp://` drives are not reassembled on their own),
its leftover leg drives are closed and the record goes. If it still holds
the array (a partition, not a restart), it is **left alone** and reported
once as an error event. Closing its drives would write into the legs.
Its writes are refused only once stormblock#6 lands, and it can let go
only once stormblock#296 does.

Automatic re-head on head loss (#14) is not done here. Promote is the
mechanism #14 will call; until then it is an explicit call (the
tiebreaker's, or an operator's).

## Prestage and bandwidth class

`POST /api/v1/volumes/{name}/prestage {node?, from?, bandwidth_class?}`
replaces a slave leg. This is the leg-move sequence (new leg → attach →
add member → wait active → retire the old one), with `reason:
"prestage"`. `from` defaults to the lost slave, else the only slave; with
several slaves it must be given. `node` defaults to placement's pick.
Naming the head as `node` (anti-affinity) or as `from` (the master moves by
promote only) is a 409. Returns `{replacing, to, bandwidth_class}`.
Progress is in `replicas[].sync` and the events.

`bandwidth_class` (`low | normal | high | unthrottled`, default `normal`)
is stored on the volume, and set at create or by a prestage. It is
applied as the head array's rebuild cap (`PUT
/api/v1/arrays/{id}/rebuild {max_bytes_per_sec}`) at assembly, promote
and prestage. The rates are `[recovery] rate_low` (50 MiB/s),
`rate_normal` (200 MiB/s) and `rate_high` (1 GiB/s); `unthrottled` is 0
(no cap). The cap is per array and applies to every rebuild on it.

## Dual-attach

- `POST /api/v1/volumes/{name}/dual-attach {target_node, ttl_secs}` →
  `{volume_id, epoch, target_node, expires_at_ms}`. The target must hold
  a created slave leg, and the head is not a target (409). Opening it
  again for the same target is idempotent and moves the expiry; another
  target while one is open is a 409. `ttl_secs` is capped by
  `[recovery] max_dual_attach_secs` (3600).
- `POST /api/v1/volumes/{name}/dual-attach/close {epoch, outcome}`:
  `epoch` must be the window's (412 otherwise). `abort` closes it with the
  head unchanged. `commit` closes it, then fences at that epoch and
  promotes the target. With the old head alive, that promote is refused
  until stormblock#296 (above), and the volume is left fenced.
- A window past its expiry is closed by the reconciler (abort), with an
  event.

The window is bookkeeping on the volume. Serving the migration target
needs no special attach: the served volume is an NVMe/TCP namespace that
both hosts can reach. With per-host subsystems (#27) the target's host
is admitted for the window's length.

## Not done

- The live run on real engines: the e2e scripts wait on stormcentral#131
  (a stormblock binary for an sc-build job). Everything above is tested
  against mock engines (`tests/replication.rs`) and in unit tests
  (`src/head.rs`).
- Enforcement at the leg: stormblock#6, to the contract above; `host_nqn`
  on leg attaches: #27.
- Handover from a live head (planned promote, dual-attach commit):
  stormblock#296.
- Automatic re-head when the head is lost: #14, using promote.
- Async catch-up legs for the backup tier: not started. Today every leg
  is a synchronous RAID1 member.
