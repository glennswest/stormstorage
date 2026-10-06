# StormStorage

**The storage control plane across Storm nodes and clusters.**

stormblock executes storage on one node; stormdrive knows one node's
hardware; stormstorage decides across all of them. It is **never in the
data path**: it only calls engine management APIs. Data moves
node↔node and client↔node over NVMe-TCP, engine to engine.

One binary, `stormstorage`, serving a REST API, an embedded UI and a
stormview components feed on **:9093**.

## What it does today (v0.3.0)

- **Registry of storage nodes.** Each node is one stormblock engine (an
  SNO cluster). Nodes come from `[[nodes]]` in the config file, register
  themselves through stormblock's existing `[stormfs]` heartbeat
  (see [Self-registration](#self-registration)), or are **adopted**: the
  engine on the machine stormstorage runs on, and that engine's
  stormblock-cluster peers (see [Local adoption](#local-adoption)).
- **Poller.** Every `poll.interval_secs` it calls each engine's
  `GET /v1/nodes/capacity` and records total/free bytes and the engine's
  topology labels. After `poll.fail_threshold` consecutive failures the
  node is marked unhealthy and an event is logged. One success marks it
  healthy again. Engine reads and polls time out after 5 s; engine writes
  (volume create, attach, arrays, deletes) after 300 s, since an array
  create formats a slab through the RAID and a loaded engine can take
  far longer than a poll should wait.
- **Engine token (#38, #12).** Every engine call presents a bearer token.
  The node's own `api_token` or `token_file` from `[[nodes]]` comes first.
  Otherwise stormblock's own rule applies (`token_for`, stormblock#107).
  The cluster's **shared** token (`$STORMBLOCK_API_TOKEN`, else `[local]
  shared_token_file`) goes to every engine. Otherwise the token **this
  machine's engine minted** goes only to an engine on this machine. That
  token is the first readable of `[local] token_file`,
  `$STORMBLOCK_TOKEN_FILE`, `/etc/stormblock/api_token`,
  `/var/lib/stormblock/api_token` and `/run/stormblock/engine/api_token`.
  "On this machine" means a loopback host, this machine's hostname, or an
  IP address held here. A minted token means nothing to a peer, which
  minted its own.
  **Destructive verbs (#47, stormblock#274)** need more than the node
  token on an engine with `admin_gate = enforce`. These are array create,
  delete and forget, member add and remove, and drive close. On those
  calls only, stormstorage presents an admin credential:
  `$STORMBLOCK_ADMIN_TOKEN` (any engine); else `[local] admin_token_file`
  (this machine's engine only); else the `[kubernetes]` bearer, which the
  engine checks with a SubjectAccessReview for `storage.storm.io` (bind
  it to `storage-admin`). If the engine refuses that credential, the call
  is retried once with the node token, so an engine from before #274
  keeps working. A refused destructive DELETE says what to set.
  So a cluster peer adopted or self-registered from
  another machine gets the shared token, or none: then it refuses and is
  backed off, and the log says why. The token is read on every call, so a
  re-minted file is picked up.
  It is never copied into `state.json`. A 401 or 403 is treated as a
  configuration error, not an outage. That engine URL is backed off: no
  calls for twice the poll interval, doubling up to 5 min, and each skipped
  poll counts as a failed one, so the node goes unhealthy at
  `poll.fail_threshold`. The first refusal is one WARN and one `auth`
  error event that names where the token came from (or why there is none).
  After that, a summary is logged at most every 5 min. A changed token is
  tried at once. When the engine accepts the token again, an `auth` info
  event is logged.
- **Node inventory.** Each poll also reads every reachable engine's slabs
  (`GET /api/v1/slabs`, each naming its `drive`: serial, WWN, model, path)
  and all its volumes (`GET /api/v1/volumes?placement=true`). Each volume
  arrives with where it lives (stormblock ≥ v17.1.0, stormblock#136):
  `placement` names its slabs with their drive, node and state
  (`ok`/`failed`/`quarantined`/`draining`/`missing`), its drives, its legs
  (expected, missing, unreadable), `rebuild`, and for drive-level RAID each
  partner with its drive, node and state. From v18.1.0 (stormblock#138) it
  also says its `kind` (`volume`, `golden`, `blank`, `media`, `snapshot`,
  `template`), whether it is `in_use`, its `attachments` and its
  `consumer` (the owner, e.g. a PVC, else a mount). The volume is placed on
  the slabs its placement names (`placed_by: engine`). For an older engine
  that sends no placement, each slab's slot table
  (`GET /api/v1/slabs/{id}/slots`) is read instead: the slabs where the
  volume owns slots, else its parent's (a fresh clone), else the only slab
  of its role, else `unknown`, never guessed. The listing is read in full
  every poll. `?since` is not used, because attaches and slab state changes
  do not bump the engine's generation. Inventory is observed state: in
  memory only, never replicated, and dropped when the node goes
  unhealthy.
- **PV/PVC per node volume (#28).** The engine does not know a volume's
  Kubernetes objects; rustkube-node's mirror (rustkube-node#59) writes a
  PV and its bound PVC for every stormblock volume a node holds (its
  `-data`, `-state` and `-logs` volumes, and the built-in driver's
  claims). Each poll reads `GET /api/v1/persistentvolumes` and
  `/api/v1/persistentvolumeclaims` from the apiserver (`[kubernetes]`,
  default `https://127.0.0.1:6443`) and puts on each node volume its
  `pv`: the PV's name, `phase`, `reclaim`, `capacity`, `volume_kind`
  (`storm.io/volume-kind`: `data`, `state`, `logs`) and `component`
  (`storm.io/component`), and the `claim` its `claimRef` names
  (namespace, name, uid, the PVC's `phase`, and `bound`: the PVC exists,
  names the PV and has the uid the PV carries). A PV belongs to a volume by
  driver `stormblock.storm.io`, `storm.io/volume` (else `volumeHandle`) and
  `storm.io/node` (else its `nodeAffinity` hostname; `n1` matches
  `n1.g8.lo`). Where an old unqualified pair sits beside the
  node-qualified one (rustkube-node#107), the complete pair wins, then the
  node-qualified name. A failed read keeps the last view; the change
  between readable and not is one WARN and one `kubernetes` event.
- **Pools.** Three kinds, all in `GET /api/v1/pools`:
  - `slab`: every slab of every node is a pool, with its tier, role,
    failure domain, total/free/allocated bytes and its volume count;
  - `tier`: the slabs of one tier summed across nodes;
  - `policy`: from `[[pools]]` — a name, a node selector (tier, labels,
    explicit names) and defaults (`replicas`, `rung`) for distributed
    volumes. Policy pools may overlap.
- **Placement.** Picks one node per distinct failure domain at a rung
  (a domain is the node's label chain from the top rung down to that
  rung). Only healthy nodes with `free_bytes ≥ size` count. Within a
  domain, and between domains, the node with the highest score wins. The
  score is `(1−w)·free_ratio + w·(1 − busy/max_busy)`, with
  `w = [placement] io_weight` (0.3), so hot nodes shed new legs (#31).
  `busy` is the node's live NVMe-oF load: each poll reads the engine's
  `stormblock_nvmeof_io_seconds` counters from `/metrics`, and the rate
  between two polls is I/O-seconds a second, the mean number of I/Os in
  flight. It covers every leg-to-head and served-mirror I/O, but not
  local ublk I/O. The node record carries it as `io {iops, busy, at}`,
  and the feed shows it. A node without a reading counts as the mean of
  the known ones. With no readings at all, the order is the free ratio's
  alone, as before. Ties go to the lower name, so the result is
  deterministic. If there are not enough domains, the request fails with
  an explanation.
- **Tier migration (#32).** `POST /api/v1/volumes/{name}/migrate {pool}`
  moves every leg of an assembled volume into another pool, for example
  from the 3.5" cluster's pool to the 2.5" one. It is refused (409) when
  the volume is not assembled or is a single leg, is fenced, replacing a
  leg or in a dual-attach window, or already has every leg in that pool.
  It is also refused when the destination cannot hold every leg in
  distinct domains at its rung now. Asking again for the same pool returns
  the migration in flight. The volume then carries
  `migration {to_pool, from_pool, started_at, state, message}`. After each
  poll, on the instance that acts on recovery, the reconciler moves one
  leg at a time: the next leg outside the pool that is not the head's,
  to the best node of the pool in a domain distinct from the legs that
  stay. It uses the leg-move sequence (reason `tier migration`), so the
  volume never has fewer copies. **The head's own leg moves by promote
  only**, and a handover from a live head needs stormblock#296. So once
  only that leg is outside the pool, the migration is `waiting_handover`,
  with an event saying so. When every leg is in the pool, the volume's
  `pool` and `rung` become the destination's and the migration is
  cleared. A blocked step (a lost leg, no room) is an event once, and a
  move that cannot start waits `recovery.cooldown_secs`.
  `DELETE …/migrate` cancels: legs already moved stay, and a move in
  flight finishes. Rebalance leaves a migrating volume alone. The feed
  shows `migrating → <pool>`.
- **Rebalance (#30).** Opt-in per pool: a `[[pools]]` entry with
  `high_watermark` and `low_watermark` (fractions of a node used) is
  rebalanced; a pool without them never is. After each poll, on the
  instance that acts on recovery (`[recovery] enabled`), the following
  happens for a pool whose nodes all answer (recovery comes first):
  - A node of the pool used above `high_watermark` is a source, fullest
    first.
  - Its legs of idle, assembled, unfenced volumes of that pool are
    candidates, largest volume first. A head's own leg never moves.
  - Each goes to a node of the pool used below `low_watermark` that
    stays at or under `high_watermark` with the leg added, in a domain
    distinct from the volume's other legs, picked by the placement score.
    The gap between the watermarks keeps a move from creating a new
    source.
  - A move is the ordinary leg-move sequence (reason `rebalance`): new
    leg, rebuild, old leg retired. A volume never has fewer copies while
    it moves.
  - At most `max_moves` (default 1) moves per pool are in flight, and one
    per volume. A move that cannot start waits `recovery.cooldown_secs`.
    Each move is an event.

  `GET /api/v1/pools/{name}/rebalance` shows what it would do now, as a
  dry run.
- **Distributed volumes.** `POST /api/v1/volumes` creates one ordinary
  thin volume per leg through each engine's `/v1/volumes`. If any leg
  fails, the legs already created are rolled back. With two or more legs,
  the volume is then **assembled**:
  - every leg is exported with `/v1/volumes/{id}/attach` to the head
    alone: the attach names the head's `host_nqn` (`[legs] host_nqn`,
    #27, stormblock#210), and the reply's nqn is that host's own
    subsystem, with the address and nsid;
  - the head (the first placed node) opens each leg as an `nvme-tcp://`
    drive with `&hostnqn=<the head's NQN>`, so it presents exactly that
    name, including its own leg over loopback;
  - the head builds a RAID1 across those drives with `/api/v1/arrays`.

  If assembly fails, the legs are kept and the volume stays
  `pending_engine_support` with an error event (#7). It is retried
  automatically once every leg's node is healthy and
  `recovery.cooldown_secs` have passed since the last attempt
  (`next_assemble_after`), on the instance where `[recovery]` is active,
  the same rule as re-leg. `POST /api/v1/volumes/{name}/assemble` retries
  at once. A retry resumes: exports already made are kept and drive opens
  are idempotent. Before building the RAID it lists the head's arrays, and
  an array whose members are exactly this volume's leg drives is adopted.
  That array comes from a create whose answer was lost, and stormblock
  would otherwise format a second array over the same drives
  (stormblock#215). If any leg drive is in some other array, the retry
  refuses. Once assembled, the volume is served (#2). An assembly never
  runs beside a re-leg or another assembly of the same volume.
- **Leg move.** `POST /api/v1/volumes/{name}/move` works on an assembled
  or degraded volume and runs these steps:
  1. create a new leg on the target node;
  2. attach it and add it as a RAID member on the head;
  3. a background task waits (up to 1 h) for that member to report
     active;
  4. it then removes the old member, closes the old head drive and
     deletes the old volume.

  Progress is logged to the event feed. The replacement in flight is
  recorded on the volume (`replacing`), so a restart of stormstorage
  resumes the wait rather than adding another member; one replacement per
  volume at a time. If the new member never becomes active within
  `recovery.rebuild_timeout_secs`, the new leg is undone and the old one
  kept.
- **Re-leg on node loss (#1).** After every poll a reconciler checks the
  assembled volumes. A leg whose node has crossed `poll.fail_threshold` is
  marked `lost` and the volume `degraded`. The lost leg is then replaced
  automatically, using the same sequence as a leg move (target from the
  same placement rules, `reason: "node lost"`). Cleanup of the dead side
  never blocks the rebuild. A leg volume that cannot be deleted is
  recorded as an **orphan** (`GET /api/v1/orphans`) and deleted when its
  node answers again. A failed attempt waits `recovery.cooldown_secs`
  before the next. A lost leg stays lost even if its node comes back, so a
  flapping node gives one re-leg. A lost **head** is reported and not
  re-legged automatically, because the array lives there: fence it and
  promote a surviving leg's node (#33, below), or turn on automatic re-head
  (`[recovery] rehead`, #14: off by default, see the key). A head that
  only stalled past the threshold comes back (#26): once it answers again,
  its array is read (`GET /api/v1/arrays/{id}`), and if the head's member
  is active its leg is `created` again and the volume `assembled` (if no
  other leg is lost). If its member is not active, the volume stays
  `degraded`, with one warning event per finding.
  **A head whose engine restarted (#15)** answers but no longer holds the
  array: stormblock reassembles arrays on runtime `nvme-tcp://` legs only
  when asked (stormblock#252). After each poll, the reconciler finds every
  assembled volume whose healthy head was not read and no longer has the
  array (`GET /api/v1/arrays/{id}` 404). It opens the legs again for the
  head, including the head's own leg if it was marked lost, and puts the
  **same** array back together from their superblocks
  (`POST /api/v1/arrays/assemble`). It never creates one, which would
  format over the legs. The served `<name>-mirror` comes back with the
  array's slab and is served again (`export.adopted`). If it does not
  come back, the export is `gone` (#40). A failed attempt waits
  `recovery.cooldown_secs`. Volumes that are fenced, replacing a leg or in
  a dual-attach window are left alone. In the assembly path (#7), a create
  the engine refuses with 409 means the legs already carry an array the
  head lost, so that array is reassembled instead (#43). With
  `[replication] peers` set, only an instance with
  `[recovery] enabled = true` acts, because every peer sees the same loss.
- **Consumer serving (#2).** A consumer attaches **the mirror**, never a
  leg. When an assembled volume is created, a `<name>-mirror` volume is
  carved on the head's array. It is pinned there (`placement.array_id`), so
  every extent is on the array. It is then attached over NVMe-TCP, and the
  coordinates are recorded in the volume's `export`
  (`{state, volume_id, node, master_node, coordinates: {nqn, traddr,
  trsvcid, nsid}, published_at, coordinates_changed, message}`). A
  single-leg volume is served as its leg, in the same field. A consumer
  opens `nvme-tcp://<traddr>:<trsvcid>/<nqn>?nsid=<nsid>`. Leg moves and
  re-legs leave the export as it is, because it is on the array.
  `POST /api/v1/volumes/{name}/export` publishes a volume that is not
  served yet, or retries a failed publish. On a published volume it
  re-attaches and sets `coordinates_changed` when the answer differs. A
  publish on a node that is unreachable is recorded as `failed` with its
  message and an event; a failed export keeps the last coordinates it
  handed out. A node whose engine answers again after being unreachable
  republishes every export it serves, published or failed.
  **A served volume that is gone is never recreated unasked (#40).** If
  the engine answers 404 for the recorded served volume, the export
  becomes `failed` with `gone: true`, an error event, and a message
  saying so. That covers the attach, the adopted attach, a host's attach,
  or the lookup of its engine id. The served volume *is* the consumer's
  data: an empty replacement would come up blank without an error. So
  nothing attaches it again, recovery skips it, and a plain
  `POST …/export` is a 409 that says what to do.
  `POST …/export {"recreate": true}` serves a new, **empty**
  `<name>-mirror` on the array, to the same hosts. It sets
  `coordinates_changed` on the export and on every host. A single-leg
  volume is refused, because its leg is the data: delete the volume. Names ending
  in `-mirror` are refused on create. Needs stormblock ≥ v19.1.1
  (dedicated arrays and pinning #150, NVMe-TCP attach on the master
  #149). An engine with stormblock #210 refuses an attach that names no
  `host_nqn` unless it sets `[nvmeof] allow_any_host`. Leg attaches name
  their head (#27), so assembly, moves, re-leg and promote work on such
  an engine.
- **Serving to named consumer hosts (#51, #53).** A consumer names its
  host: `POST /api/v1/volumes/{name}/export/hosts {host_nqn, dhchap?}`.
  The serving engine serves the volume to that host from a subsystem of
  its own, which admits that host alone
  (`POST /api/v1/volumes/{local id}/attach {transport, host_nqn, dhchap}`).
  The answer is that host's record: `{host_nqn, dhchap, coordinates: {nqn
  (`<nqn>:host:<hex>`), traddr, trsvcid, nsid, host_nqn},
  coordinates_changed, served_at, message}`. It also carries
  `dhchap_secret` when the engine gave the host one. The host connects
  with `--hostnqn` (and `--dhchap-secret`). **The secret is only in that
  answer.** It is not stored, not replicated, not shown in the volume or
  the feed, and not in events. The engine keeps a host's secret, so
  asking again returns the same one; `dhchap` once asked for stays on. The
  call is idempotent.
  `DELETE /api/v1/volumes/{name}/export/hosts/{host_nqn}` withdraws one
  host (`DELETE …/attach?host_nqn=` on the engine) and answers
  `{withdrawn: done | pending | nothing_served}`. The host leaves the
  record at once. If the engine does not answer, the withdrawal is kept in
  `export.withdrawing` and done when it answers (on recovery, or at the
  next poll).
  Hosts can also be named at create (`hosts: [{host_nqn, dhchap?}]`) and
  on `POST …/export`. Once any host is named, the volume is served per
  host for good (`export.per_host`): no shared attach, and `coordinates`
  stays empty. With every host withdrawn it is served to none, never back
  on the shared subsystem. Every republish (promote, a node that answers
  again, `POST …/export`) serves every recorded host again, and on a new
  head it sets each changed host's `coordinates_changed`. A consumer
  whose coordinates changed reconnects; with `dhchap` it asks again for
  its secret. The per-host calls take the engine's own volume id, not the
  `/v1` id. It is found once by name on that engine (`<name>-mirror`, or
  the leg's name) and kept as `export.local_id`. A volume with no host
  named is served on the shared subsystem as before, which a closed
  engine refuses without `allow_any_host`.

  Kubernetes claims on stormcos do not come through here: a PVC is the
  built-in `stormblock` driver, where the kubelet clones a blank on the
  pod's node and attaches it over ublk. CSI (stormblock-csi) is for
  third-party drivers and foreign clusters (#36). Claims replicated across
  servers are wanted (rustkube-node#68), and that path is re-leg (#1).

  Unit and mock-engine tests cover re-leg, serving and assembly retry
  (`tests/export.rs`). The live runs against real engines
  (`scripts/e2e-releg.sh`, `scripts/e2e-export.sh`) have not passed yet:
  they need a built stormblock in the build job (stormcentral#131).
- **Delete.** Revokes the export first: it detaches the served volume and
  deletes it. The head's array refuses deletion while a volume is pinned
  to it, so if this fails on a reachable head, the record is kept and the
  API returns 502. The export is then `revoking` (#24). The engine may
  have deleted the served volume even though its answer never came, so a
  `revoking` export is never attached again. A recovery skips it, and
  `POST …/export` is a 409 that says to delete it again. A retried DELETE
  finishes, because a served volume that is already gone counts as
  deleted. It then tears the assembly down (array, head drives, leg
  exports). This step is best-effort, and problems are logged as a warning
  event. Then it deletes every leg volume, including a replacement leg in
  flight. A leg on an unreachable node is recorded as an orphan to reap
  later. If a leg delete on a reachable node fails, the volume record is
  kept and the API returns 502.
- **Peer replication.** Durable intent (volume records, self-registered
  node configs and orphaned legs to reap) is pushed to every `[replication] peers` URL on each
  change. A peer applies a payload only if its revision is newer. Poll
  status is not replicated: each peer polls the engines itself.
- **Persistence.** State is written to `<data_dir>/state.json`. With no
  `data_dir`, state lives only in memory and a warning is logged.
- **UI and feeds.** The embedded UI at `/` shows:
  - nodes, and pools (policy, slab with its drive, tier);
  - node volumes with their slab and drive, RAID partners, consumer, kind,
    in-use state, and PV / PVC (a warning chip when the pair is not
    complete);
  - distributed volumes with leg states, assembly, copies in sync and
    resync progress, fence and dual-attach state (#33), and export, plus
    buttons to move a leg, publish/republish, assemble a pending volume
    and delete;
  - a create form and the event feed.

  When a write gets a 401 it asks for the API token once and keeps it for
  the tab. `/api/v1/summary` serves a stormd dashboard card.
  `/api/v1/components` and `/ws/components` serve the stormview feed,
  which stormconsole's `stormstorage` plugin and stormd/stormsh render.

- **Replication on the RAID head (#33).** What stormblock-csi's `/v1`
  replica surface asks of an engine is done here, over distributed
  volumes (owner, stormblock#179 option b). Each poll reads the head's
  array: every leg is a replica `{node, role, sync}` in `/v1`'s exact
  JSON (`in_sync`, `resyncing {progress_pct, lag_bytes}`, `detached`; no
  evidence = `detached`). When the head does not answer, sync comes from
  the RAID superblocks in the surviving legs, read on their own engines
  (`sync_source: superblock`, #48, stormblock#309; docs/replication.md has
  the rules). A per-volume `epoch` with a CAS `fence` that also
  fences every leg's `/v1` epoch. `promote` moves the head onto a
  surviving leg's node and re-imports the same array from the legs'
  superblocks. `prestage` replaces a slave at the volume's
  `bandwidth_class`. Dual-attach windows commit by fence + promote.
  A handover from a head that is still alive waits on stormblock#296.
  Enforcement at the legs is stormblock#6, to the contract in
  [docs/replication.md](docs/replication.md).

**Not done yet** (tracked in issues, see [Status](#status)): async backup
legs (#46), HA state (#34), and moving a head by handover (stormblock#296),
which is what a tier migration of the head's own leg waits on.

## Running

```bash
stormstorage --config /etc/stormstorage/stormstorage.toml

curl -s http://localhost:9093/api/v1/nodes | python3 -m json.tool
curl -s -X POST http://localhost:9093/api/v1/volumes \
  -H 'Content-Type: application/json' \
  -d '{"name":"vol1","size_bytes":10737418240,"pool":"protected"}'
```

### Command-line flags

| Flag | Default | Meaning |
|---|---|---|
| `--config <path>` | `/etc/stormstorage/stormstorage.toml` | Config file. A missing file means all defaults. |
| `--listen <addr>` | (from config) | Overrides `listen_addr`. |
| `--data-dir <dir>` | (from config) | Overrides `data_dir`. |
| `--version`, `--help` | | |

Logging uses `RUST_LOG` (tracing `EnvFilter`), default `info`.
Shutdown is on SIGINT (ctrl-c); state is persisted on the way out.

### Configuration (`stormstorage.toml`)

Every key, with the default the code uses when it is absent. See
[deploy/stormstorage.example.toml](deploy/stormstorage.example.toml) for a
worked example.

| Key | Default | Meaning |
|---|---|---|
| `listen_addr` | `"0.0.0.0:9093"` | Socket address to bind. Must parse as `ip:port`. |
| `data_dir` | unset | Directory for `state.json`. Unset means state is in memory only. |
| `[federation] rungs` | `["site","building","room","row","rack","multicluster","cluster","node"]` | Top-down rung order for failure domains. Every pool's `rung` must be in it. |
| `[poll] interval_secs` | `15` | Seconds between engine polls. Must be non-zero. |
| `[poll] fail_threshold` | `3` | Consecutive failed polls before a node is marked unhealthy. |
| `[api] api_token` | `""` | Set: every inbound write needs `Authorization: Bearer <token>` (see *Auth* under API), and outbound replication pushes send it, so replicating peers share one token. Empty: no auth. |
| `[replication] peers` | `[]` | Base URLs of peer instances, e.g. `["http://siteb:9093"]`. |
| `[local] enabled` | `true` | Adopt the engine on this machine, see [Local adoption](#local-adoption). |
| `[local] engine_url` | `"http://127.0.0.1:9090"` | Where this machine's stormblock answers. |
| `[local] name` | unset | Name for the adopted node. Unset: the engine's own name (`local_node` from its `GET /api/v1/discovery`), else this machine's hostname. |
| `[local] cluster_peers` | `true` | Also adopt the live peers in the local engine's stormblock cluster. |
| `[local] token_file` | unset | Engine bearer token file. `$STORMBLOCK_API_TOKEN` wins over it; otherwise the first readable, non-empty file of this, `$STORMBLOCK_TOKEN_FILE`, `/etc/stormblock/api_token`, `/var/lib/stormblock/api_token` and `/run/stormblock/engine/api_token` (the family order, stormdrive#14; the last is where a stormcos unit mounts the minted token) (#42). The token is read on every call and presented to every engine without an `api_token` of its own: the adopted engine and its peers, and nodes stormblock registered. An engine that refuses it (401/403) is backed off up to 5 min and logged once, then at most every 5 min; a changed token is tried at once (#38). The key belongs under `[local]`: a top-level `token_file` is ignored, and every unknown top-level key is logged as a WARN at start. |
| `[local] shared_token_file` | unset | File holding the cluster's **shared** engine token (stormblock's `management.api_token`), presented to every engine, peers included (#12). `$STORMBLOCK_API_TOKEN` wins over it. The `token_file` search above finds a token the engine minted for itself, which goes only to an engine on this machine. |
| `[local] admin_token_file` | unset | The engine's **admin** token file, presented on its destructive verbs (array create/delete, members, drive close; stormblock#274, #47), and only to an engine on this machine. `$STORMBLOCK_ADMIN_TOKEN` wins over it. There is no default: stormblock keeps its admin token (`/run/stormblock-admin/admin_token`) out of what services mount. Without either, the `[kubernetes]` bearer is presented, and the engine reviews it against `storage-admin`. |
| `[local] tier` | unset | Tier role given to adopted nodes. |
| `[recovery] enabled` | unset | Replace lost legs automatically. Unset means on for a lone instance and off when `[replication] peers` is set. With peers, set it `true` on exactly one instance. When off, legs are still marked lost. |
| `[recovery] cooldown_secs` | `300` | Wait after a failed re-leg or assembly attempt before the next automatic one. |
| `[recovery] rebuild_timeout_secs` | `3600` | How long a new member may take to become active before the replacement is undone. |
| `[recovery] rate_low` / `rate_normal` / `rate_high` | `52428800` / `209715200` / `1073741824` | Rebuild cap, bytes a second, for a volume's `bandwidth_class` (#33); applied to the head's array. `unthrottled` is 0 (no cap). |
| `[recovery] max_dual_attach_secs` | `3600` | Longest dual-attach window (#33). |
| `[recovery] rehead` | `false` | Re-head a volume automatically when its head is lost (#14). **Off by default (owner, 2026-10-06). Turn it on only once a lost head is fenced through cluster membership or quorum (stormcluster)**, not merely unreachable from stormstorage: a partitioned head that keeps writing to the legs is split-brain. When on, and only on the instance that acts on recovery, the reconciler re-heads a volume when all of these hold: its head has failed polls for `rehead_after_secs`; it is not already fenced (someone else's failover in flight is left alone); it is idle; and a surviving leg on a healthy node reads `in_sync` (from the head's last reading or the legs' superblocks, #48). It then fences the volume at its epoch and promotes that leg's node. Without an in-sync leg it holds, with one warning: promoting a stale copy would lose writes. A failed promote leaves the volume fenced, names the manual promote in an error event, and waits `cooldown_secs`. A WARN is logged at start when it is on. Until it is on, failover is the consumer's tiebreaker's or an operator's (fence + promote). |
| `[recovery] rehead_after_secs` | `120` | How long the head must have failed its polls before an automatic re-head. |
| `[placement] io_weight` | `0.3` | Weight of live I/O load against free space in placement, 0 to 1 (#31). 0 is free space alone. |
| `[legs] host_nqn` | `nqn.2026-10.lo.storm:stormstorage:{node}` | The host NQN a head presents to its legs; `{node}` is the head's node name and is required. Every leg attach (assembly, move, re-leg, promote) names it, so the leg's engine serves the leg from a subsystem that admits that head alone (stormblock#210), and the head's drive URI carries it as `hostnqn=`. Must start `nqn.` and contain no `& ? / #` or spaces (#27). |
| `[kubernetes] enabled` | `true` | Read each node volume's PV/PVC from the apiserver (#28). |
| `[kubernetes] server` | `"https://127.0.0.1:6443"` | The apiserver. |
| `[kubernetes] token_file` | unset | Bearer token file. `$KUBE_TOKEN` wins over it; otherwise the first readable, non-empty file of this, `/data/stormcert/node-admin.token` (the node-admin token stormcert writes) and `/var/run/secrets/kubernetes.io/serviceaccount/token`. None found: anonymous. Read on every poll. |
| `[kubernetes] ca_file` | unset | CA to verify the apiserver with (e.g. `/data/stormcert/ca.crt`). |
| `[kubernetes] insecure_skip_tls_verify` | unset | Unset: TLS is not verified for a loopback `server` (as stormconsole), and verified otherwise. |
| `[[nodes]]` | none | Static nodes, see below. Names must be unique. |
| `[[pools]]` | none | Pools, see below. |

`[[nodes]]`:

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Node name, also its registry key. |
| `engine_url` | required | stormblock management base URL, e.g. `http://192.168.8.150:9090`. |
| `api_token` | unset | Bearer token for that engine's API. Unset: `token_file`, then the `[local]` rule (shared token; minted token only for an engine on this machine, #12). |
| `token_file` | unset | File holding that engine's token, read on every call (#12). |
| `labels` | `{}` | Rung → value. `node` and `cluster` default to `name` (SNO). Config labels override the labels the engine reports. |
| `tier` | unset | Free-form tier role (`high`, `medium`, `backup`, …). |

`[[pools]]`:

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Pool name. |
| `selector` | `{}` (every node) | `tier`, `labels` (all must match) and `nodes` (explicit names; empty means no restriction). All present conditions must hold. |
| `replicas` | `2` | Default leg count. |
| `rung` | `"node"` | Default spread rung. |
| `high_watermark` | unset | Rebalance (#30): a node of the pool used above this fraction sheds legs. Set with `low_watermark`, or the pool is never rebalanced. |
| `low_watermark` | unset | Rebalance target: nodes of the pool used below this fraction, staying at or under `high_watermark` with the leg added. Must be below `high_watermark`. |
| `max_moves` | `1` | Rebalance moves in flight in this pool at a time. |

A create or plan request with no pool uses `replicas = 1` and
`rung = "node"`, unless the request sets them.

### Self-registration

stormstorage accepts stormblock's own registration heartbeat
(`VolumeAnnouncement` in stormblock's `src/stormfs.rs`). To enrol an
engine with no engine changes, set this in its config:

```toml
[stormfs]
enabled        = true
metadata_url   = "http://<stormstorage-host>:9093"
advertise_addr = "<engine-host>:9090"   # a bare host gets :9090
heartbeat_secs = 30                     # stormblock's default
```

Each heartbeat marks the node healthy and records its volume count. The
first heartbeat adds the node to the registry as `registered` and
replicates it to peers. A deregister (sent by the engine on clean
shutdown) marks the node unhealthy, but the next successful poll marks it
healthy again if the engine still answers. Registered nodes have no labels
or tier until the engine reports topology.

### Local adoption

On a node nothing else tells stormstorage where storage is, so by default it
looks for the engine on the same machine (`[local] engine_url`). Once that
engine answers, every poll:

- registers it under the engine's own name (source `local`);
- reads its `GET /api/v1/discovery` view and adopts every peer in the same
  stormblock cluster (same `cluster_id`, not stale) at its `mgmt_addr`;
- forgets an adopted peer that has left the cluster, and re-adopts the
  local engine if its name changed.

A node already named by `[[nodes]]` or a heartbeat, by name or engine URL,
is left as it is. Adopted nodes are this instance's own: they are not
replicated to peers (every instance adopts its own engine) and adoption does
not bump the replication revision. Where no engine answers, nothing is
adopted and nothing is shown. An adopted local node's URL is loopback, so it
is fine for inventory and single-node volumes; legs for multi-node volumes
want a node whose `engine_url` other engines can reach (`[[nodes]]`).

## API (:9093)

Errors return `{"error": "...", "code": "not_found|bad_request|conflict|engine|unauthorized"}`
with HTTP 404/400/409/502/401.

**Auth (#6).** With `[api] api_token` set, these need
`Authorization: Bearer <token>`, else 401 `unauthorized`: every request
that is not a GET, HEAD or OPTIONS, which today is volume create, delete,
move, export and assemble, and `POST /api/v1/replicate`. Open with or
without a token:
- reads (every GET, `/ws/components`) and the `placement/plan` dry run.
  Anyone who can reach the port may look (the family posture).
- `storage/register` and `storage/deregister`: stormblock's heartbeat sends
  no token yet (stormblock#214).

The embedded UI asks for the token the first time a change gets a 401 and
keeps it for the tab. stormd's proxy and stormconsole send no token, so
with a token set, their Delete/Publish actions get 401.

| Method | Path | What |
|---|---|---|
| GET | `/`, `/ui`, `/ui/` | Embedded UI. |
| GET | `/api/v1/health` | `{"status":"ok","version":"…"}`: the liveness/health check. |
| GET | `/api/v1/nodes` | Registry: name, engine_url, tier, effective labels, status (with `source`: `static`, `registered` or `local`). |
| GET | `/api/v1/nodes/{name}/inventory` | That node's slabs and engine volumes, each volume as the engine reports it (with `placement`, `kind`, `in_use`, `attachments`, `consumer` when the engine sends them) plus `slabs` and `placed_by` (`engine`, else `slots`, `parent`, `role`, `unknown`); each slab with its `drive`; `fetched_at`, `error`. |
| GET | `/api/v1/topology` | Rungs, plus each node's label chain, tier and health. |
| GET | `/api/v1/pools/{name}/rebalance` | Rebalance dry run (#30): `{pool, enabled, high_watermark, low_watermark, max_moves, moves[{volume, from, to, from_used, to_used}], held[]}`. `held` says why nothing moves. 404 for a pool not in the config. |
| GET | `/api/v1/pools` | Every pool with its `kind`. `policy`: matched/healthy node counts and a capacity rollup over healthy nodes. `slab`: `node`, `slab`, `tier`, `role`, `domain`, total/free/allocated bytes, `volumes`. `tier`: `nodes`, `slabs`, summed bytes, `volumes`. |
| POST | `/api/v1/placement/plan` | Dry run. Body `{size_bytes, pool?, replicas?, rung?, tier?}` returns `{replicas, rung, legs:[node…]}`. |
| GET | `/api/v1/volumes` | All distributed volumes. |
| POST | `/api/v1/volumes` | Create. Body `{name, size_bytes, pool?, replicas?, rung?, tier?, bandwidth_class?, hosts?: [{host_nqn, dhchap?}]}` (hosts: served per host, #51). Places, creates the legs and assembles. Returns the volume record. |
| GET | `/api/v1/volumes/{name}` | One volume: legs (node, volume id, state, export, drive/member uuids, `epoch`), head, array id, assembly, `export` (what consumers attach), `epoch`, `fenced`, `bandwidth_class`, `dual_attach`, and from the last reading of the head: `replica_sync` (the `/v1` replica list), `health`, `sync_read_at`, `sync_source` (`head` or `superblock`). `GET /api/v1/volumes` lists the same. |
| GET | `/api/v1/volumes/{name}/replicas` | The volume in `/v1`'s replica shape (#33): `{id, name, size_bytes, epoch, fenced, health, replicas[{node, role, sync}], bandwidth_class, head, dual_attach, sync_read_at, sync_source, head_read_at, rebuild_bytes_per_sec}`. With the head unreachable, sync is read from the legs' superblocks (#48). |
| POST | `/api/v1/volumes/{name}/fence` | `{expected_epoch}` → `{epoch, legs_fenced, legs_not_fenced}`. CAS: 412 `{code: "stale_epoch", current_epoch}` on a mismatch; 409 unless mirrored. |
| POST | `/api/v1/volumes/{name}/promote` | `{target_node, fenced_epoch}` → the volume, headed on the target. 412 unless fenced at that epoch; 409 with a window open, a replacement running, no leg on the target, or the old head still alive (stormblock#296); 502 when the legs do not assemble there. |
| POST | `/api/v1/volumes/{name}/prestage` | `{node?, from?, bandwidth_class?}` → `{replacing, to, bandwidth_class}`. Replaces a slave leg; 409 for the head as `node` or `from`. |
| POST | `/api/v1/volumes/{name}/dual-attach` | `{target_node, ttl_secs}` → `{volume_id, epoch, target_node, expires_at_ms}`. Target must hold a slave; 409 for another target while one is open. |
| POST | `/api/v1/volumes/{name}/dual-attach/close` | `{epoch, outcome: commit\|abort}`. Commit = fence + promote the target; 412 on the wrong epoch, 409 with none open. |
| GET | `/api/v1/stale-heads` | Former heads to clean up when they answer: `{stale_heads: [{node, array_id, drive_uris, of_volume, epoch, since}]}`. |
| POST | `/api/v1/volumes/{name}/export` | Publish, or republish, what consumers attach. Optional body `{hosts: [{host_nqn, dhchap?}]}` adds consumer hosts first (#53); `{recreate: true}` replaces a served volume that is `gone` with a new, empty one (#40; 409 when nothing is gone or on a single leg). Returns the `export` record (`hosts[]`, `per_host`, `local_id`, `withdrawing[]`; never a secret); 409 when it cannot be served (not assembled, node unreachable, engine error, a host refused). |
| POST | `/api/v1/volumes/{name}/export/hosts` | `{host_nqn, dhchap?}` → that host's record plus `dhchap_secret` when it has one (#51). Served from the host's own subsystem; idempotent. 400 for a value that is not a host NQN (`nqn.…`, ≤ 223 bytes), 404 for no such volume, 409 when it cannot be served. |
| DELETE | `/api/v1/volumes/{name}/export/hosts/{host_nqn}` | Stop serving it to that host: `{host_nqn, withdrawn: done\|pending\|nothing_served}`. |
| POST | `/api/v1/volumes/{name}/assemble` | Retry a failed assembly now, then publish. Returns the volume; 409 when it is not pending, has a single leg or is busy, 502 when the engine refuses (the reason is in the error and the events). |
| DELETE | `/api/v1/volumes/{name}` | Revoke the export, tear down the assembly, then delete the legs. |
| POST | `/api/v1/volumes/{name}/migrate` | Body `{pool}`. Tier migration (#32): every leg moves into that pool, one at a time; the head's own leg waits on a handover (stormblock#296). Returns the `migration`. 404 for an unknown volume or pool, 409 when it cannot start (see *Tier migration*). |
| DELETE | `/api/v1/volumes/{name}/migrate` | Cancel the tier migration: `{cancelled}`. Legs already moved stay. |
| POST | `/api/v1/volumes/{name}/move` | Body `{from, to?}`. Moves the leg on `from`; with no `to`, placement picks one. Returns `{moving, to, status:"rebuilding"}`. |
| GET | `/api/v1/orphans` | Leg volumes left on unreachable nodes: `{orphans: [{node, volume_id, master_node, of_volume, reason, since}]}`. Reaped when the node answers. |
| GET | `/api/v1/events?since=<seq>` | Event ring (4096 entries, in memory): `{latest_seq, events}`. |
| GET | `/api/v1/summary` | stormd card: `{health, detail, metrics}`. |
| GET | `/api/v1/components` | stormview feed: `system`, policy pools, `tier:<tier>`, nodes, slab pools `pool:<node>/<slab>` (relations: node, tier, volumes), distributed volumes `volume:<name>` (an `export` metric, Publish/Republish and delete actions) and node volumes `nvol:<node>/<id>` (relations: node, pools; detail names the owner). |
| GET (WS) | `/ws/components` | The same feed. It is checked every 2 s and pushed when it changes. |
| POST | `/api/v1/replicate` | Peer push, `{revision, volumes, registered, orphans, stale_heads}`. Applied only if newer. |
| GET | `/api/v1/replication/status` | `{revision, peers}`. |
| POST | `/api/v1/storage/register` | stormblock heartbeat, `{node_addr, hostname, volumes[]}`. |
| POST | `/api/v1/storage/deregister` | `{node_addr}`. |

There is no metrics endpoint. The health check is `/api/v1/health`.

### Engine calls it makes (stormblock :9090)

- `/v1`: `GET nodes/capacity`, `GET|POST volumes`, `GET|DELETE volumes/{id}`,
  `POST volumes/{id}/attach|detach`, `POST volumes/{id}/fence` (legs, #33).
  Legs are created with `replica_tier` `slaves = 0`. A served mirror is
  also created with `placement: {array_id}`. Every attach sends
  `transport: nvme_tcp`; a leg attach also sends the head's `host_nqn`
  (#27, stormblock#210) and the leg's `epoch` once it has been fenced
  (stormblock#6). `/v1` prestage, promote and dual-attach
  are never called.
- `/api/v1`: `GET slabs`, `GET slabs/{id}/slots`, `GET volumes`,
  `POST|DELETE volumes/{id}/attach` and `DELETE volumes/{id}` (a served
  volume adopted on a promote), `GET discovery` (local adoption),
  `GET|POST drives`, `DELETE drives/{id}`, `POST arrays`,
  `POST arrays/assemble` (promote), `GET|DELETE arrays/{id}`,
  `PUT arrays/{id}/rebuild` (bandwidth class),
  `POST arrays/{id}/members`, `DELETE arrays/{id}/members/{member}`.

Assembly needs a stormblock with stormblock#73 (nvme-tcp drives as RAID
members, 2026-08-28 or later) and an engine NVMe-oF target that is
listening. An engine with no export device returns no nsid on attach, and
assembly fails with that message.

## Building

Builds and tests run on the build box, never on the session VM, and never
as root. Push first, then run from the checkout:

```bash
git push
sc-build                        # cargo build && cargo test on dev.g8.lo
sc-build 'cargo clippy --all-targets'
```

`sc-build` builds the pushed commit in a scratch directory on
`dev.g8.lo` and deletes it afterwards. A failure files a `build-failure`
issue on this repo.

Note that `stormview` is a git dependency, and `Cargo.lock` pins the
exact commit that gets compiled. A fix in stormview does not reach this
binary until `cargo update -p stormview` is committed here.

### Live test: re-leg on node loss

`scripts/e2e-releg.sh` runs three real stormblock engines
(`STORMBLOCK_BIN`) on loopback, unprivileged. It creates
a 2-leg volume, kills the non-head engine, checks the re-leg converges
(both members active, one re-leg, the orphan recorded), then restarts the
engine and checks the orphan is reaped:

```
sc-build 'STORMBLOCK_BIN=/path/to/stormblock scripts/e2e-releg.sh'
```

### Live test: consumer serving

`scripts/e2e-export.sh` runs three storage engines and a fourth engine,
with no drives, as the consumer. It creates a 2-leg volume and checks that
it is published as a volume pinned to the head's dedicated array. The
consumer formats a data slab on the published NVMe-TCP namespace. The
script then kills the non-head leg's engine under it, waits for the re-leg,
and checks that the export is unchanged and the consumer reads its slab
header back through the mirror. It also checks republish, a single-leg
volume, and that delete leaves no served volume, array or leg behind:

```
sc-build 'STORMBLOCK_BIN=/path/to/stormblock scripts/e2e-export.sh'
```

Both scripts test at runtime against a **built** stormblock (≥ v19.1.1)
and never compile one (#25): without `STORMBLOCK_BIN` they stop before
starting anything, and they print the stormblock version they ran
against. An sc-build job has no stormblock binary to point at until
stormblock ships as a golden bin a job can reach (stormcentral#131).

### Test suites on a node (the stormcos test standard)

`test/` is stormstorage's test container, per stormcentral
`docs/test-standard.md` (#8): one static binary, `/test short|medium|long`,
run by stormcentral as a Job (`test/stormstorage-test.yaml`) on every test
machine. It drives the **stormstorage running on the node**
(`STORM_NODE:9093`) through its REST API. It needs no privileges and no
devices, and carries no stormblock of its own.

| suite | budget | what it checks |
|---|---|---|
| `short` | < 2 min | the API answers; a healthy storage node (the adopted local engine); slab pools and the feed; a single-leg volume created, served as its leg, and deleted |
| `medium` | < 30 min | the short checks, plus: the leg is a real volume on the engine and goes on delete; placement dry run (fits, and an explained refusal); refusals (duplicate 409, reserved `-mirror`, unplaceable replicas leave nothing); republish unchanged; assemble and move refused on a single leg; 404s; auth (open, or closed to writes without the token); RAID1 across two nodes (skip with fewer); events; no orphans; cleanup |
| `long` | the night window | waves of 64 MiB volumes sized from the healthy nodes' free bytes (4 to 64, `STORM_WAVE_MAX` caps), 8 at a time: create p50/max per wave, then delete and settle, then count what is left (volumes, engine legs, orphans). A wave with errors, a residue, or a p50 over twice wave 1's fails; `trend` names the first regressed wave |

Every volume is named `t-<run id>-…` and is deleted on success, on failure
and on timeout. With the node's `[api] api_token` set, writes need
`STORM_STORMSTORAGE_TOKEN`; without it they are skips, never passes.
Results are JSON lines on stdout and in `/results/results.jsonl`. Exit
codes: 0 all passed, 1 a test failed, 2 the run could not happen (no
`STORM_NODE`, or stormstorage not answering).

`test/build.sh` builds only the test crate (static musl) and stages it in
`test/.stage/`. `STAGE_ONLY=1` stops there; otherwise it also runs
`podman build` with `test/Containerfile` (`FROM scratch`). When
stormcentral#121 moves tests to golden bins, the Containerfile goes and the
staged binary stays. Against any instance:

```
STORM_STORMSTORAGE_URL=http://host:9093 STORM_RESULTS=tmp/results test/.stage/stormstorage-test short
```

## How it ships

stormstorage is a stormcos **service component** and ships in goldens:

| Golden | Kind | Pallet/slab |
|---|---|---|
| `stormstorage` | service (the static musl binary) | system1 |
| `stormstorage-data` | data (`/data/stormstorage`, survives installs) | data1 |
| `stormstorage-logs` | logs (`/logs/stormstorage`) | system1 |

stormd supervises it on the node with
`--config /etc/stormstorage/stormstorage.toml`. The shipped config sets
`listen_addr = "0.0.0.0:9093"`, `data_dir = "/var/lib/stormstorage"` and
`[local] token_file = "/run/stormblock/engine/api_token"`, the engine's
minted token. stormd's unit mounts the host's `/run/stormblock` read-only.
A stock node has no static nodes or pools. `[local]` is on by default, so
it adopts its own engine, and that engine's cluster peers, once the engine
answers. The health check is `/api/v1/health` on 9093, and the node
gateway routes `storage.storm1.g8.lo` to `127.0.0.1:9093` (stormcos
`deploy/manifests/85-routes.yaml`).

The component entry (port, health, argv, the shipped config text, goldens)
lives in stormcentral's database (stormcentral#185). Read it with
`stormcentral component export`, and change it with
`stormcentral component edit stormstorage --set key=value`, never by a
commit to stormcentral's `components/stormcos.toml`, which is only the
seed. Since 2026-10-06 the entry's config puts `token_file` under
`[local]`; before that it sat at the top level, where stormstorage ignores
it (and warns). The token was found anyway, since
`/run/stormblock/engine/api_token` is one of the default paths (#42).

A commit here does not reach a node until a golden is built and a
release composed. When an issue's work is complete, request the golden
once:

```bash
stormcentral component build stormstorage --url http://stormcentral.g8.lo
```

How goldens are built and composed is documented in one place:
**[stormcos `docs/goldens.md`](https://github.com/glennswest/stormcos/blob/main/docs/goldens.md)**.

For a non-golden host, see [deploy/systemd/stormstorage.service](deploy/systemd/stormstorage.service)
and the stormd snippet [deploy/stormd-ui.toml](deploy/stormd-ui.toml). The
snippet adds a `[process.ui]` block with `proxy` pointing at the UI and
`summary` pointing at the card.

## Documentation

- [docs/architecture.md](docs/architecture.md): the design. Each section
  says whether it is implemented or design only.
- [docs/replication.md](docs/replication.md): replication on the RAID
  head (#33): sync state, fence, promote, prestage, dual-attach, and the
  leg attach contract for stormblock#6.
- [docs/presentation.md](docs/presentation.md): an 11-slide overview deck.
  It is Marp Markdown: `npx @marp-team/marp-cli docs/presentation.md`.
- [CLAUDE.md](CLAUDE.md): work plan, status and project rules.
- [CHANGELOG.md](CHANGELOG.md)

## Status

**v0.3.0.** Phase 1 (registry, pools, placement, volumes), 1.5 (stormview
feed, peer replication) and 2 (leg wiring: assembled RAID1, leg move) are
done. The open work:

- #1, #2: re-leg and consumer serving are in the code; their live runs
  wait on stormcentral#131;
- #8: the test suites are in and run on C2NR0Q2; see the issue for the
  latest run;
- #33: replication on the RAID head is in the code (sync state, fence,
  promote, prestage, dual-attach; mock-engine tested). Its live run waits
  on stormcentral#131; enforcement at the legs is stormblock#6, a handover
  from a live head stormblock#296;
- stormblock#214: a token on self-registration, so register/deregister
  can close too (#6);
- #34–#36: HA
  state, forwarding announcements to stormfs, and the stormblock-csi
  analysis.
