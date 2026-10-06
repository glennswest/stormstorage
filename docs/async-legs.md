# Async catch-up legs for the backup tier (#46): design pass

Status: **design only; it waits on an owner decision** (posted on #46).
Nothing here is in the code.

## The need

Every leg today is a synchronous RAID1 member on the volume's head (#33). A
write is acked only when every active member has it. A leg on the backup
rung (the PVE cluster, slower and further away) would set the mirror's
write latency. The goal is a leg that **catches up asynchronously**: writes
are acked without it, and it trails by a bounded lag.

## What the engine offers today (stormblock, 2026-10-06)

- **RAID1 on the head** (`/api/v1/arrays`): members are active, rebuilding
  or failed. A member is never write-behind or write-mostly. A v2 superblock
  carries a write-intent bitmap (#252), which is what a catch-up would
  replay from.
- **`/v1` snapshots** of a volume (`POST /v1/snapshots`). There is no API
  that diffs two snapshots and no engine-to-engine copy of changed extents.
- **Live mirror, stormblock#295** (open): mirror an in-use volume to a
  volume on another engine, `copying → synced`, with `bytes_remaining`.
  It was asked for VM-disk migration (#44), not for a standing copy.
- **`cluster/replication.rs`**: the engine's older per-volume sync/async
  write replication between cluster peers. It belongs to the engine-side
  replication model that #179 (option b) set aside for RAID on the head.

So no option below works without engine work.

## Options

### A. A write-behind member in the head's RAID1

The backup leg joins the array as a **write-behind** member. Writes are
acked once the synchronous members have them. The write-behind member gets
them from a bounded queue, and the write-intent bitmap covers whatever the
queue dropped, the same way a rebuild does.

- Where it runs: on the head, inside the array. stormstorage only marks the
  leg `async` (a member flag on add) and reports its lag.
- Sync state (#48): `resyncing {lag_bytes}` while behind, `in_sync` when it
  has caught up. Failover onto it is allowed only when it is `in_sync`, the
  same rule as now.
- RPO: the queue plus the dirty bitmap, usually seconds.
- Engine work: a write-behind member flag and bounded queue in RAID1. This
  is md's `--write-behind` model. It would be a new stormblock issue.
- **What it does not give:** a point in time. A delete or a corruption on
  the primary reaches the backup within seconds.

### B. Periodic snapshot + changed-extent shipping

On a schedule, stormstorage snapshots the served volume on the head. It has
the engine copy the extents changed since the last shipped snapshot to the
backup leg (a plain volume on the backup node), then snapshots the backup
there.

- Where it runs: driven by stormstorage. The copy is engine to engine, and
  stormstorage stays out of the data path.
- The backup holds **points in time** (keep N). A corruption or delete on
  the primary can be rolled back from a snapshot taken before it.
- RPO: the interval, minutes.
- Engine work: a snapshot diff (changed extents between two snapshots of a
  volume) and a copy of extents to a remote volume over NVMe-TCP. These are
  two stormblock issues.
- Failover onto it means promoting a point in time. It is never `in_sync`,
  so #48's rule would never pick it. Restore is an explicit operation.

### C. A standing live mirror (stormblock#295, never cut over)

Use #295's mirror as a permanent async copy: start it to the backup volume
and never call cutover.

- Least new engine surface, if #295 grows a "stay `synced`, keep
  following" mode. As specified it is a migration tool that ends.
- Same semantics as A (no point in time), but outside the array: the head's
  RAID and its sync state don't see it. stormstorage would report its lag
  separately.

## Recommendation

The backup tier is named *backup*, which points to **B**: a copy that
lags by minutes but holds points in time, so it protects against the
primary's mistakes (a delete, a bad write, ransomware) and not only against
losing a node. Losing a node is already covered by the synchronous legs. A
or C give a low-RPO disaster-recovery copy, not a backup.

If both are wanted, B first (it is what the rung is for), with A as the DR
follow-up.

## The decision

Which semantics the backup tier has: point-in-time backups (B), an async
replica (A, or C on #295), or both. That choice decides which stormblock
issues get filed.
