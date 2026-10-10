# Changelog

## [Unreleased]
<!-- New unreleased changes go here -->

### 2026-10-10
- **build:** stormview is pinned to a `rev` (81ef1d2, the commit Cargo.lock already compiled) instead of `branch = "main"` (#64, stormcentral#571: golden builds refuse unpinned git dependencies). Cargo.lock's source reads `?rev=<sha>#<sha>`. Moving stormview forward is now a deliberate rev bump. README and CLAUDE.md say so.
- **fix(test):** The test crate's three `iter().any(|x| *x == …)` checks are `contains` (clippy `manual_contains` under `-D warnings`, #67).

### 2026-10-07
- **fix(test):** The #60 test helper no longer shadows axum's `put` (#62).
- **feat:** A volume's bandwidth class can change after create (#60, for stormblock-csi#31's ControllerModifyVolume). `PUT /api/v1/volumes/{name}/bandwidth-class {bandwidth_class}` records the class, so later prestages, re-legs and promotes use it, and puts its rebuild cap on the head's array at once (`PUT /api/v1/arrays/{id}/rebuild`). It is idempotent (the same class applies the cap again, with no new revision) and answers with the volume plus `rebuild_cap {bytes_per_sec, applied, pending, message?}`. A change is an event and is replicated to peers. If the head does not answer, or the call fails, the class is still recorded and the volume carries `rate_pending`; after each poll, the instance that acts on recovery applies the cap once the head is healthy. A volume with no array (a single leg, or assembly pending) is only recorded, since assembly applies the class. It needs the API token like every mutation. Test: `tests/replication.rs` (applied at once; idempotent; head down → recorded and pending, then applied by one reconcile when it answers; 404 for an unknown volume; an unknown class refused; a single leg recorded with nothing pending).
- **docs:** Builds run on a fresh build VM; dev.g8.lo was retired on 2026-10-07 (README build section, CLAUDE.md).
- **feat:** An optional extent size on create (#59, stormblock#156, for stormblock-csi#37's StorageClass `extentSize`). `extent_size_bytes` on `POST /api/v1/volumes` must be a power of two of 4096 or more (else 400, before any engine is called). It is sent on every leg's `/v1` create and kept on the volume (`extent_size_bytes` in the volume record and `/replicas`), so a replacement leg made by a move, re-leg, prestage, rebalance or tier migration is carved at the same size. Absent, nothing is sent and each node chooses, as before. A node with no pool of that size fails the create with its own message (the sizes it has), and the legs already made are deleted, so extent sizes never mix across legs. The served `<name>-mirror` is carved on the head's array and gets the array slab's slot size, which the engine chooses. Test: `tests/replication.rs` (every leg and the prestaged replacement get the size, the mirror does not; a bad size is a 400; a node without the pool fails the create with its sizes and nothing is left).

### 2026-10-06
- **feat:** A soft head-node preference on create (#50, for stormblock-csi's WaitForFirstConsumer). `prefer_node` on `POST /api/v1/volumes` and `POST /api/v1/placement/plan` puts the first leg on that node when it fits (healthy, in the pool or tier, room for the leg). The first leg is the head, or a single copy's only leg. The other legs go to other domains by the usual rules, and otherwise the plain placement is used. Responses carry `prefer_node_honored`, and the create event says why the node was not used. Tests: unit `a_preferred_node_leads_when_it_fits`, and `tests/replication.rs` (the preferred node becomes the head; an unknown node is not honoured; the dry run).
- **docs:** The stormblock-csi analysis (#36) is recorded as settled. stormblock-csi targets stormstorage, not one engine (owner decisions on stormblock-csi#29 and #32, shipped in stormblock-csi v0.4.0). There is a new *stormblock-csi* section in docs/architecture.md with the reasons and the remaining follow-ups (#49, #50, #56). CLAUDE.md and the README status are updated to match.
- **feat:** Tier migration: a volume's legs move into another pool (#32). `POST /api/v1/volumes/{name}/migrate {pool}` records a migration, but only for assembled, idle, unfenced volumes whose destination can hold every leg in distinct domains at its rung. After each poll, on the instance that acts on recovery, the reconciler moves the next non-head leg outside the pool to the pool's best node in a distinct domain, through the leg-move sequence (reason `tier migration`), so the volume never has fewer copies. The head's own leg moves by promote only, and a live handover waits on stormblock#296, so a migration with only that leg left is `waiting_handover` with an event. With every leg in the pool, the volume's `pool` and `rung` change and the migration clears. Blocked steps are reported once, failed starts wait the cooldown, and `DELETE …/migrate` cancels. Rebalance skips migrating volumes, and the feed shows `migrating → <pool>`. Tests: unit tests of `next_step` (non-head first, then waiting on the head; done; a lost leg or a full pool blocks) and `tests/replication.rs` (four mock engines and two pools: the leg moves into the destination, then `waiting_handover`; cancel; refusals for an unknown pool, a pool already holding every leg, and a single leg).
- **feat:** Legs are rebalanced by pool watermarks (#30). This is opt-in per pool: `[[pools]] high_watermark`, `low_watermark` (fractions used) and `max_moves` (default 1); a pool without watermarks never moves. After each poll, on the instance that acts on recovery, a pool whose nodes all answer moves legs off its nodes above `high`, fullest node first and largest volume first. Only idle, assembled, unfenced volumes of that pool move, and never a head's own leg. Each leg goes to a node below `low` that stays at or under `high` with the leg added, in a distinct domain, picked by the placement score. A move is the leg-move sequence with reason `rebalance`, so a volume never has fewer copies. Each move is an event, and a move that cannot start waits the cooldown. `GET /api/v1/pools/{name}/rebalance` is the dry run, saying why when nothing moves. Tests: unit tests of the planner (the move chosen, off without watermarks, inverted watermarks, a pool node down, a move in flight, busy and cooling-down volumes, a target never pushed over high, `max_moves`).
- **docs:** Design pass for async catch-up legs on the backup tier (#46): docs/async-legs.md. The options are (A) a write-behind member in the head's RAID1, (B) periodic snapshots with changed-extent shipping, and (C) a standing stormblock#295 live mirror. It covers what the engine offers today (none of the three works without engine work) and recommends B. The choice is with the owner on #46.
- **feat:** Placement weighs live I/O load as well as free space (#31). Each poll reads the engine's `stormblock_nvmeof_io_seconds` counters from `/metrics`, summed over labels, and keeps the rate between two polls on the node as `io {iops, busy, at}`. `busy` is I/O-seconds a second, i.e. the mean number of I/Os in flight. It covers leg-to-head and served-mirror I/O, not local ublk I/O. A missing metric, an error or a counter reset never fails the poll. The score is `(1−w)·free_ratio + w·(1 − busy/max_busy)`, with `w` from the new `[placement] io_weight` (0.3). A node without a reading counts as the mean, so with no readings the order is the free ratio's as before. Ties still go by name. The feed shows each node's `io`. Tests: placement units (a hot node sheds new legs, the weight trades load against space, an unread node counts as the mean, weight 0 is the old order), the metrics parse, and the rate (first sample, reset, no elapsed time).
- **feat:** stormstorage now presents an admin credential on the engine's destructive verbs (#47, stormblock#274). Under `admin_gate = enforce` the engine refuses the node token for array create, delete and forget, member add and remove, and drive close. Those calls now carry `$STORMBLOCK_ADMIN_TOKEN` (any engine), else `[local] admin_token_file` (new key; this machine's engine only, no default), else the `[kubernetes]` bearer, which the engine reviews against `storage-admin`. Every other call keeps the node token. A refused admin credential is retried once with the node token, so engines from before #274 keep working. A refused destructive DELETE says what to set. Tests: `tests/token.rs` (a gated mock engine sees the admin credential on exactly those calls and no retries; an old engine is served through the retry; credential order: this machine's admin token locally, the Kubernetes bearer for a peer).
- **feat:** Automatic re-head of a lost head, opt-in and off by default (#14; owner's decision 2026-10-06). New keys: `[recovery] rehead` (`false`) and `rehead_after_secs` (`120`). Turn it on only once a lost head is fenced through cluster membership or quorum, since a partitioned head that keeps writing to the legs is split-brain. When it is on, the reconciler fences a volume whose head has failed its polls for that long, then promotes a surviving leg on a healthy node that reads `in_sync` (#48 evidence). It never promotes a leg without that evidence, and never acts on a volume someone else already fenced. With no in-sync leg it holds, with one warning. A failed promote leaves the volume fenced, names the manual promote, and waits the cooldown. A WARN is logged at start when it is on. Tests: unit `rehead_plan_needs_time_evidence_and_no_fence`, and `tests/replication.rs` (off by default does nothing; on, the in-sync slave's node becomes head at epoch 2 and the volume is served again).
- **feat:** A volume's array is reassembled on its head after the head's engine restarts (#15, #43). stormblock reassembles arrays on runtime `nvme-tcp://` legs only when asked (stormblock#252), so a restarted head used to leave its volume unserved for good. After each poll, the reconciler now finds every assembled volume whose healthy head was not read and answers 404 for the array. It opens the legs again for that head, including the head's own leg if #26 had marked it lost, and puts the same array back together from their superblocks with `POST /api/v1/arrays/assemble`. It never creates an array, which would format over the legs. The served `<name>-mirror` that comes back with the slab is served again (`export.adopted`); if it does not come back, the export is `gone` (#40). A failed attempt waits `recovery.cooldown_secs`, and only the instance that acts on recovery does this. Promote and reassemble now share `head::assemble_on`. In the assembly path (#7), a create refused with 409 means the legs already carry an array the head lost, so it is reassembled instead of retried. Test: `tests/replication.rs` (the mock head forgets its drives and array; one poll reassembles the same array and serves the same volume; the next poll leaves it alone).
- **fix:** The engine token sent to cluster peers now follows stormblock's own rule (#12; stormblock#107 `token_for`). The token this machine's engine minted (found through the `[local] token_file` search) was sent to every engine, peers included, where it meant nothing. It now goes only to an engine on this machine: a loopback host, this hostname, or an IP held here. The cluster's shared token (`$STORMBLOCK_API_TOKEN`, or the new `[local] shared_token_file`) goes to every engine. A node can also name its own token file (`[[nodes]] token_file`, after `api_token`). The refusal log says which rule applied and what to set. The default-path half of #12 was already done by #42 (`/run/stormblock/engine/api_token`, `$STORMBLOCK_TOKEN_FILE`). Tests: unit tests (`is_this_machine`, minted vs shared) and `tests/token.rs` (per-node order; a peer is never sent the minted token).
- **fix:** A served volume gone from its engine is reported and never recreated empty unless asked (#40; owner's decision 2026-10-06). Before, publish attached the recorded served volume on every recovery and got a 404 each time. Now a 404 on it marks the export `failed` with `gone: true`, raises an error event ("its data is not recreated") and sets the feed metric `export: gone`. That applies to the /v1 attach, the adopted attach, a host's attach, and the lookup of its engine id. Recovery skips it, and a plain `POST …/export` is a 409 that says what to do. `POST …/export {"recreate": true}` serves a new, empty `<name>-mirror` on the array, to the same hosts, and flags every consumer to reconnect. A single-leg volume is refused, because its leg is the data. Engine error statuses are now typed (`engine::HttpStatus`, `engine::is_not_found`), so a 404 is told apart from other failures. Tests: `tests/export.rs` (gone → reported, not retried, recreated on request) and `tests/hosts.rs` (per host; recreate refused on a single leg).
- **fix:** A delete that stops while revoking the export is no longer republished on every recovery (#24). In #24's e2e, the head deleted the served volume but did not answer within the timeout. The delete stopped with the record kept as `published`, and every time the head answered again, the recovery attached the vanished served volume and got a 404. Revoke now records `export.state = revoking` before it calls the engine and keeps that state, with the error, when the call fails. `republish_on` skips a `revoking` export, `POST …/export` refuses it (409, "DELETE it again"), and a retried DELETE finishes, since a 404 on the served volume counts as gone. The feed and UI show "delete unfinished". Test: `tests/export.rs` (an engine that deletes but answers 500). The general case, a served volume gone some other way, is asked on #40.
- **feat:** A volume can be served to named consumer hosts (#51, #53; stormblock-csi#34). A closed engine (stormblock#210) refused the export's shared-subsystem attach, and a consumer node had no way in. `POST /api/v1/volumes/{name}/export/hosts {host_nqn, dhchap?}` has the serving engine serve the volume to that host from its own subsystem (`POST /api/v1/volumes/{local id}/attach {host_nqn, dhchap}`). It answers with that host's coordinates (per-host subsystem NQN, address, NSID, `host_nqn`), plus `dhchap_secret` when the host has one. The secret is only in that answer: it is never stored, replicated, shown or logged. `DELETE /api/v1/volumes/{name}/export/hosts/{host_nqn}` withdraws one host. If the engine is unreachable, the withdrawal is kept (`export.withdrawing`) and done when the engine answers. Hosts can also be named at create and on `POST …/export` (`hosts: [{host_nqn, dhchap?}]`). Once a host is named, the volume is served per host for good (`export.per_host`) and never goes back to the shared subsystem. Every republish (promote, recovery, `POST …/export`) serves the recorded hosts again and flags changed coordinates per host. The per-host calls take the engine-local volume id, which is found by name and kept as `export.local_id`. The feed shows a `hosts` metric. Tests: `tests/hosts.rs` (a closed mock engine: serve with DH-HMAC-CHAP, idempotent repeat, bad NQN, a host via `POST …/export`, withdraw, republish serves only the remaining hosts, pending withdrawal done on recovery, no secret in the state, API or events, hosts named at create) and `tests/replication.rs` (a promote serves the hosts again on the new head).
- **fix(test):** The `tests/hosts.rs` helper no longer shadows axum's `post` (#54).
- **fix:** `/replicas` no longer reads every leg `detached` once the head is lost (#48, stormblock-csi#29). A consumer that waits for `in_sync` before failing over could therefore never fail over. When the head's array cannot be read, each surviving leg's RAID superblock is read on the leg's own engine (`GET /v1/volumes/{id}/raid-superblock`, filed as stormblock#309; a 404 is no evidence). A leg is `in_sync` only if its superblock is of this array, is at the newest event count among the legs read, every newest superblock records it active, and it agrees with (or is newer than) the last live reading of the head, which is now kept in memory while the head is away. The answer carries `sync_source` (`head`/`superblock`) and `head_read_at`; the feed shows `sync from: leg superblocks`. The head-leg rejoin (#26) still uses only the head's own reading. The two-leg window (a drop recorded only on the dead head, after its last reading) is documented in docs/replication.md. Tests: unit tests of the rules in `src/head.rs`; `tests/replication.rs` (head lost → slave in sync from its superblock, stale superblock → detached, head back → live).
- **feat:** Each RAID leg is served to its head alone (#27, stormblock#210). A closed engine admits no host on its shared NVMe subsystem and refuses an attach that names none. Every leg attach now names the head's `host_nqn`: in assembly, leg move, re-leg, promote, and the re-attach when the head is promoted in place. The engine serves the leg from that host's own subsystem (`<nqn>:host:<hex>`, taken from the reply), and the head opens it with `&hostnqn=<the same>` on the drive URI, so it presents exactly that name. The engine has no API for its initiator's NQN, so stormstorage names the head itself: new `[legs] host_nqn` template, default `nqn.2026-10.lo.storm:stormstorage:{node}`, checked at start. The NQN is recorded on the leg (`export.host_nqn`). Legs exported before #27 keep their URI, so existing arrays still match. An export made for another head is attached again for this one. Tests: unit tests (URI, template, old records); `tests/export.rs` (attach body, per-host NQN, `hostnqn=` on the opened drive); `tests/replication.rs` (promote attaches for the new head). The consumer export still names no host: filed as #53.
- **fix(test):** The replication mock keys superblocks by the namespace, not by the connecting host (#52, the first #27 build).
- **fix:** The component entry's shipped config puts `token_file` under `[local]` (#42, stormcentral#72). It sat at the top level, where stormstorage ignores it and logs a WARN at start. Changed with `stormcentral component edit stormstorage --set config=…`. A unit test (`shipped_entry_config`) pins the entry's text: it parses, `token_file` lands in `[local]`, and there are no unknown top-level keys. README, docs/presentation.md and CLAUDE.md now describe the corrected entry.
- **fix:** A head leg marked lost comes back when the head answers again (#26). A head that missed `poll.fail_threshold` polls (45 s by default — a loaded box was enough) marked its own leg `lost` and left the volume `degraded` for good, since a head leg is never re-legged. After each poll the reconciler now reads the array of every degraded, unfenced, idle volume whose head leg is lost and whose head is healthy (this poll's reading, else `GET /api/v1/arrays/{id}`). If the head's member is `active`, the leg goes back to `created` and the volume to `assembled` when no other leg is lost, with an info event. If the array is gone (404: engine restart, #15) or the member is not active, the volume stays `degraded`, and a warning event is logged once per finding (the leg's `message` says which). Unit tests in `src/orchestrate.rs`; `tests/rejoin.rs` against mock engines.

### 2026-10-05
- **feat:** Replication on the RAID head (#33). The owner chose on stormblock#179 (option b) that cross-node RAID1 is built on these heads, not in the engine. stormblock #5 and #7 moved here, and the engine's `/v1` prestage/promote/dual-attach stay control-plane only and are never called. Added:
  - **Sync state.** Each poll reads every assembled volume's array on its head (in memory, not replicated), and each leg is a replica in `/v1`'s exact JSON: `in_sync`, `resyncing {progress_pct, lag_bytes}` from the member's `rebuilt_bytes`, or `detached`. No reading means `detached`, never `in_sync`. `GET /api/v1/volumes/{name}/replicas`; the volume view gains `replica_sync`, `health` and `sync_read_at`; the feed gains `in sync`, `resync` and `epoch` metrics.
  - **Epoch and fence.** Per-volume `epoch` (from 1). `POST …/fence {expected_epoch}` is a CAS (412 `stale_epoch` + `current_epoch`) and fences each reachable leg through its engine's `/v1/volumes/{leg}/fence`. Leg attaches send the leg's epoch: the attach contract for stormblock#6, in docs/replication.md.
  - **Promote.** `POST …/promote {target_node, fenced_epoch}` opens the surviving legs on the target, re-imports the same array with `POST /api/v1/arrays/assemble`, and serves the `<name>-mirror` that came across with its slab through `/api/v1/volumes/{id}/attach` (`export.adopted`). It never makes an empty volume in its place. A live old head is refused until stormblock#296 (release an array without writing to its members, filed today). Former heads go to `stale_heads` and are cleaned up only once they no longer hold the array.
  - **Prestage.** `POST …/prestage {node?, from?, bandwidth_class?}` replaces a slave through the leg-move sequence. `bandwidth_class` (low|normal|high|unthrottled, set at create or here) becomes the head array's rebuild cap (`[recovery] rate_*`).
  - **Dual-attach.** `POST …/dual-attach {target_node, ttl_secs}` and `…/dual-attach/close {epoch, outcome}`: commit = fence + promote, abort closes, and expiry aborts in the reconciler. Promote is a 409 while a window is open.
  - Unit tests in `src/head.rs`; `tests/replication.rs` runs mock engines that share the legs' superblocks and slabs.
- **docs:** docs/replication.md, which covers the model, the `/v1` mapping, the leg attach contract for stormblock#6, and where stormblock-csi#29 reads sync state (here). README, architecture.md and CLAUDE.md are updated to match.
- **docs:** #44 (live migration of a VM's disks) triaged. The data path must be stormblock's: only the source engine sees the in-use ublk volume's writes. Filed as stormblock#295, a live mirror of a volume to a peer engine's volume, then released. The orchestration API planned here (`migrate` / `cutover` / `complete` / `abort` per node volume) is written in the work plan. #44 is proposed after stormblock#295.
- **feat:** Each node volume carries its PV and bound PVC (#28). rustkube-node's mirror (rustkube-node#59) writes a PV + PVC for every stormblock volume a node holds; each poll now reads PVs and PVCs from the apiserver (`[kubernetes]`: `server` default `https://127.0.0.1:6443`, TLS unverified on loopback; token `$KUBE_TOKEN`, `token_file`, `/data/stormcert/node-admin.token`, the pod ServiceAccount token, else anonymous; `ca_file`) and joins them by `storm.io/node` and `storm.io/volume` onto the inventory as `pv` {pv, phase, reclaim, capacity, volume_kind, component, claim {namespace, name, uid, phase, bound}}. An old unqualified pair beside a node-qualified one loses to the complete pair. A failed read keeps the last view, with one WARN and `kubernetes` event per change. Feed: `pv`, `claim` and `holds` metrics on `nvol:` (warn tone when the pair is not complete). UI: a PV / PVC column in Node volumes. `src/kube.rs`, `tests/kube.rs`. On a node the token file needs the unit to mount `/data/stormcert` (stormcos#290); until then it reads anonymously, which only `sno`'s anonymous admin allows (stormcos#76).
- **fix:** The engine token is found where a stormcos node mounts it (#42, found by #8's short suite on C2NR0Q2: the node's stormstorage adopted nothing because its engine answered 401 — "no token: /etc/stormblock/api_token: No such file or directory"). The search is now the family order (stormdrive#14): `$STORMBLOCK_API_TOKEN`, then the first readable, non-empty file of `[local] token_file`, `$STORMBLOCK_TOKEN_FILE`, `/etc/stormblock/api_token`, `/var/lib/stormblock/api_token`, `/run/stormblock/engine/api_token`. A set but unreadable `token_file` no longer ends the search, and the `auth` event lists every path tried. Unknown top-level config keys (the registry entry's top-level `token_file`) are logged as a WARN at start.

### 2026-10-02
- **docs:** Refreshed from the code (changes since 2026-09-25; the 2026-09-28 pass covered most of them). README: the engine token on every call and the 401/403 back-off (#38), with `auth` events and refused polls counting as failures; `[local] token_file` must sit under `[local]` (unknown keys are ignored) and `$STORMBLOCK_TOKEN_FILE` is not read; auth guards every non-GET write, assemble included; the replicate payload carries `orphans`; the component entry lives in stormcentral's database (`component export` / `component edit`), not `components/stormcos.toml` (#37). docs/architecture.md: the engine token and back-off. docs/presentation.md: #38, test counts (39 unit, `tests/token.rs`), the shipped config's home. CLAUDE.md: the entry's home, status. Filed #42: the live registry entry ships `token_file` at the top level, where stormstorage ignores it, so a node polls its engine with no token.
- **fix:** Every engine call carries the configured engine token (#38). A node with no `api_token` of its own — one stormblock self-registered, or the adopted local engine and its cluster peers — was polled bare, so a stormblock that requires its token logged a WARN for `GET /api/v1/discovery` and `/v1/nodes/capacity` on the node console every 15 s. Such a node now gets `$STORMBLOCK_API_TOKEN` or `[local] token_file`, read at call time, so a file stormblock re-mints at start is picked up. Adopted nodes no longer copy the token into the state file.
- **fix:** An engine that refuses the token (401/403) is backed off per engine URL: twice the poll interval, doubling, at most 5 min, and a changed token is tried at once. The first refusal is one WARN and an `auth` event naming where the token was looked for (e.g. `no token: /run/stormblock/engine/api_token: Permission denied`); after that a summary at most every 5 min, and an event when the engine accepts again. A refused node counts as a failed poll ("engine unusable after N polls"). `src/refusal.rs`, `tests/token.rs`.

### 2026-09-28
- **docs:** Refreshed from the code (changes since 2026-09-18). README: leg move works on degraded volumes too; replication carries orphans; the UI's columns and buttons and its token prompt; the shipped config's `[local] token_file`, and adoption at start; `cooldown_secs` also paces assembly retries; engines with stormblock #210 need `host_nqn` (#27); the live runs of #1/#2 are not yet passed (stormcentral#131); Kubernetes PVCs on stormcos are the built-in stormblock driver, not stormstorage; the status list is current. docs/architecture.md: the stack's consumers (PVCs are the built-in driver, CSI for third-party), DistVolume fields (`lost`, `replacing`, `next_*_after`), the export and assemble routes, the replicate payload, the UI, phases and the asks on neighbours. docs/presentation.md: what it does, planned, config, API, shipping and status slides. Example config: `[api]`, `[local] tier`, `[recovery]` comments. CLAUDE.md: the CSI section in the owner's terms, #1/#2 status, phases with issues. Doc promises without code filed as #30–#36.
- **test:** Test container per the stormcos test standard (#8). `test/` is a workspace crate, `stormstorage-test`: a static `/test short|medium|long` that drives the node's stormstorage (`STORM_NODE:9093`) through its API. It emits JSON lines, exits 0/1/2, names its volumes `t-<run id>-…` and deletes them on success, failure and timeout, and skips writes when the node's API is closed and no `STORM_STORMSTORAGE_TOKEN` is given. short: up, adopted engine, pools and feed, single-leg lifecycle. medium: plus leg on the engine, dry-run placement, refusals, republish, one-leg refusals, 404s, auth, RAID1 on ≥ 2 nodes (else skip), events and orphans, cleanup. long: capacity-sized waves measuring create latency and residue, with a trend. `test/build.sh`, `test/Containerfile` (`FROM scratch`), `test/stormstorage-test.yaml`.
- **feat:** A failed assembly can be retried (#7). `POST /api/v1/volumes/{name}/assemble` retries at once, and the reconciler retries a pending volume once every leg's node is healthy, `recovery.cooldown_secs` after the last attempt (`next_assemble_after`, gated like re-leg by `[recovery]`). Once assembled the volume is published. Feed: an Assemble action; UI: an assemble button on pending volumes. The create event now names the real retry path (it said "re-create or move", and neither worked).
- **fix:** An assembly retry adopts an array the head already holds over exactly its leg drives (a create whose response was lost), matched by member `device_path`, instead of building a second array over the same members. It refuses if any leg drive is in another array. stormblock's array create does not check this and would format over the first array's data (stormblock#215). Members map to legs by drive path. Assembly takes the per-volume claim, so it never runs beside a re-leg of the same volume.
- **feat:** Node inventory takes placement and consumer from the engine (#11). Volumes are read with `?placement=true` (stormblock#136, v17.1.0): slabs, drives, legs, rebuild and RAID partners with their states. Slabs name their drive (serial, WWN, model, path). Volumes carry `kind`, `in_use`, `attachments` and `consumer` (stormblock#138, v18.1.0). A volume is placed where the engine says (`placed_by: engine`); the slot-table scan now runs only for an engine that sends no placement. Feed: a slab pool names its drive; a node volume shows kind, in-use transports, consumer, drives, partners and rebuild, and a failed slab, a partner that is not active, or a rebuild owed makes it a warning. `/api/v1/pools` slab entries carry `drive`. UI: Node volumes gain Slab / Drive, Partners and Consumer columns, with kind and in-use chips. PV/PVC is #28, waiting on rustkube-node#59.
- **feat:** Inbound API auth (#6). With `[api] api_token` set, volume create, delete, move and export and `POST /api/v1/replicate` need `Authorization: Bearer <token>`, else 401 `{code: "unauthorized"}`. The token is compared in constant time. Reads, the placement dry run and `storage/register|deregister` stay open: stormblock's heartbeat sends no token yet (stormblock#214). An empty token (the default) leaves the API open. The embedded UI asks for the token on a 401. `tests/auth.rs`.
- **fix(test):** e2e-export and e2e-releg never compile stormblock: a fat-LTO stormblock build held a dev build slot for over an hour. They require `STORMBLOCK_BIN` (a built stormblock, from its golden bin once sc-build jobs can reach one: stormcentral#131), stop at once without it, and print the stormblock version they ran against (#25).
- **fix(test):** e2e-releg gets e2e-export's fixes for a loaded build box: `fail_threshold = 15` (was 2, so a head stalled for 4 s read as lost; a killed engine still reads lost in ~30 s), steps stamped with time and load, a `/v1/volumes` list read bare or under `volumes`, and a delete the head did not answer retried once it is healthy. After delete it checks that no engine still holds a leg (#1).

### 2026-09-27
- **fix(test):** e2e-export `fail_threshold = 15`: at load 48–67 on 16 cores a live head missed 5 polls and read as lost; a killed engine still fails fast (connection refused), so detection stays ~30 s. The head-stays-degraded gap it exposed is #26 (#2).
- **test:** e2e-export stamps each step with time and load, probes the head's and another engine's latency during delete, and retries a delete the head did not answer (the documented 502 contract) once the head is healthy again (#2).
- **fix:** A publish on an unreachable node was returned but not recorded: the create response showed `export.state: none` with no message or event. It is now `failed` with the reason and an event, keeps the last coordinates, and a node that answers again retries its failed exports as well as republishing its published ones. `coordinates_changed` compares against the last coordinates handed out, published or not (#2, found by the e2e).
- **fix(test):** e2e-export matches the victim against whole leg names; victim `b` matched the "b" in "assembled" and the converged re-leg read as failed (#2).
- **fix(test):** e2e-export polls with `fail_threshold = 5`, so a slow capacity poll on a busy build box does not mark a live node lost mid-test (#2).
- **fix:** Engine write timeout 60 s → 300 s: an array create (a slab format through the RAID) took 47 s on a loaded engine and the next run timed out at 60 s (#2, #21).
- **fix(test):** e2e-export checks the served volume on the head's array by name: the array lists the engine's volume uuid, not the /v1 id (#2).
- **fix:** Engine writes (volume create, attach, arrays, deletes) time out after 60 s instead of 5 s. On a loaded engine a leg create took longer than 5 s, failed the create, and could leave the engine finishing a volume nobody recorded. Reads and polls keep 5 s (#2, found by the e2e, #19).
- **fix(test):** e2e scripts wait up to 180 s for an engine (slab adoption took 38 s on a busy dev) and fail if it never answers, instead of reading an unminted token.
- **feat:** Consumer serving (#2). A distributed volume is served to
  consumers as `export` {state, volume_id, node, master_node, coordinates,
  published_at, coordinates_changed, message}. An assembled volume is served
  as a `<name>-mirror` /v1 volume pinned to the head's array
  (`placement.array_id`, stormblock#150 v19.0.0), attached over NVMe-TCP. A
  single-leg volume is served as its leg, with the same field. It is
  published at the end of create. `POST /api/v1/volumes/{name}/export`
  publishes or republishes and reports whether the coordinates changed. A
  head whose engine answers again is republished. Delete revokes the export
  first (detach, then delete the pinned volume) and keeps the record if that
  fails, because a dedicated array refuses deletion while a volume is
  pinned. Feed: `export` metric, Publish/Republish action. UI: Export column.
- **BREAKING:** volume names ending in `-mirror` are refused (reserved for
  served mirrors; a /v1 create is name-idempotent).
- **test:** `tests/export.rs`, mock engines keeping stormblock's rules:
  assembled → served from the array, republish unchanged, revoke before
  array delete; single-leg served as its leg; refusals.
- **feat:** Re-leg on node loss (#1). A reconciler runs after every poll: a
  leg whose node is unhealthy becomes `lost` and its volume `degraded`, and
  one replacement per volume is started through the leg-move sequence
  (`reason: node lost`). Dead-side cleanup never blocks it: undeletable old
  legs become orphans (`GET /api/v1/orphans`), reaped when the node answers.
  There is a cooldown after a failed attempt, and a lost leg stays lost, so
  a flapping node gives one re-leg. A lost head is reported, not recovered
  (#14). New `[recovery]` config. With replication peers, only an instance
  with `enabled = true` acts.
- **feat:** A leg replacement (operator move or re-leg) is recorded on the
  volume (`replacing`). A restart resumes the rebuild wait instead of losing
  the new leg, and a second concurrent move is refused. A replacement that
  fails part-way, or never rebuilds, undoes its new leg.
- **feat:** Deleting a volume with a leg on an unreachable node succeeds and
  leaves that leg as an orphan instead of returning 502 forever. The delete
  also removes an in-flight replacement leg.
- **feat:** Feed, summary and UI show `degraded`, lost legs, a re-leg in
  progress and orphans. The move button also works on degraded volumes.
- **fix:** `/v1` attach sends `transport: nvme_tcp` (stormblock#149, v19.1.1),
  so leg exports get NVMe-TCP coordinates on engines that offer ublk.
- **test:** `scripts/e2e-releg.sh`: three real engines, kill the non-head,
  re-leg converges, orphan reaped on restart.

### 2026-09-25
- **fix:** A leg attach answered with ublk now fails with an error that
  names the cause (stormblock#149) and the workaround
  (`[management] ublk_transport = false`), instead of "unexpected
  transport" (#2).
- **docs:** Consumer serving (#2) decided: a volume carved on the array, not
  the array as a namespace. Rationale and shape are in docs/architecture.md.
  Blocked on stormblock#150 (pin a volume to an array's slab) and
  stormblock#149 (/v1 attach returns ublk to the master node since
  stormblock 2337c8a, which also breaks leg assembly where ublk is
  available).
- **feat:** Adopt the local stormblock (#9). With `[local]` (on by
  default) the engine at `http://127.0.0.1:9090` is registered once it
  answers, under its own name from `GET /api/v1/discovery`, with the live
  peers of its stormblock cluster. Adopted nodes (source `local`) are not
  replicated. Token from `$STORMBLOCK_API_TOKEN` or
  `/etc/stormblock/api_token` when readable.
- **feat:** Node inventory (#9). Each poll reads every reachable engine's
  slabs, volumes and slab slot tables and places each volume on its slab(s)
  (by slots, else its parent's, else the only slab of its role, else
  `unknown`). New `GET /api/v1/nodes/{name}/inventory`.
- **feat:** Slabs are pools (#9). `GET /api/v1/pools` now carries `kind`:
  `policy` (the configured pools, as before), `slab` (one per slab per
  node: tier, role, domain, total/free/allocated, volume count) and `tier`
  (slabs summed per tier across nodes).
- **feat:** Components feed shows real counts (#9): `tier:<tier>`, slab pools
  `pool:<node>/<slab>` with their volumes, node volumes `nvol:<node>/<id>`
  with their pool(s) and owner; the system card counts pools and volumes.
  `/api/v1/summary` and the embedded UI show the same (Pools table by kind,
  a Node volumes table; the create form offers only policy pools).
- **docs:** README (local adoption, `[local]` keys, inventory, pool kinds,
  new route), architecture.md, example config.

### 2026-09-24
- **docs:** `docs/presentation.md`: an 11-slide Marp deck on purpose, place in
  stormcos (from stormcentral's graph), how it works, current features,
  planned work, interfaces, shipping and status. Built from the code (#5).
- **docs:** README rewritten from the code (#4). It covers what runs today, every
  CLI flag and config key with its default, the full API route table, the
  engine calls made, self-registration (stormblock `[stormfs]` needs
  `enabled` + `advertise_addr`), building with `sc-build`, and how it ships
  in goldens (stormcos `docs/goldens.md`, closes #3). Gaps found in the code
  are filed as #6 (inbound API auth) and #7 (no assembly retry).
- **docs:** docs/architecture.md checked against the code. Design-only
  sections are marked (head failover, native replication, IO-load placement,
  rebalance, tiering, stormfs forwarding). The API table is corrected (move
  takes `{from, to?}`, plan takes `size_bytes`/`tier`, and the components,
  replicate and replication-status routes are added). The DistVolume shape
  now matches `src/model.rs`.
- **docs:** Stale comments fixed in `src/config.rs` (`api_token` is
  outbound-only), `src/events.rs` (the actual event kinds) and `src/lib.rs`,
  and in the example config (#73 has landed; self-registration keys).
- **docs:** CLAUDE.md now builds with `sc-build` instead of ssh to root@dev,
  records the golden/shipping facts, and has a current work plan.

## [v0.3.0] — 2026-08-28

### Added
- Phase 2 — leg wiring. Multi-leg volumes now assemble into a real
  **RAID1 on the head node**: every leg is exported via `/v1 attach`
  (hot-added NVMe-TCP namespace), the head opens each as an
  `nvme-tcp://` drive (stormblock#73; its own leg over loopback —
  uniform, local fast-path later) and mirrors across them via
  `/api/v1/arrays`. Legs record master node, export coordinates, head
  drive uuid, and RAID member uuid; volumes record head + array id;
  `AssemblyState::Assembled`.
- **Leg move** — `POST /api/v1/volumes/{name}/move {from, to?}`: new leg
  placed (distinct staying domains at the volume's rung, emptiest first,
  or an explicit target), created, attached, added as a member; a
  background task waits for the rebuild to reach active, then retires
  the old member, closes the old head drive, and deletes the old volume.
  The same sequence is failure recovery and evacuation.
- Delete tears the assembly down first (array, head drives, exports),
  best-effort, before removing leg volumes.
- Engine client: attach/detach, arrays create/get/add/remove member,
  idempotent drive open, drive close, drive listing.
- UI: assembly chip (raid1/pending), head shown, per-leg move button.

### Verified
- e2e on dev.g8.lo with three live engines: create → assembled RAID1
  (legs node-b/node-c, member uuids captured); move off node-c →
  converged to node-b/node-d, both members active on the head, old
  volume gone from node-c. Requires stormblock ≥ the 2026-08-28 build
  (array members expose uuid + device_path).

## [v0.2.0] — 2026-08-26

### Added
- stormview integration — `GET /api/v1/components` +
  `/ws/components` serving system/pools/nodes/volumes with relations
  (pool has_many nodes+volumes, volume legs target nodes) and actions
  (volume delete), so stormd/stormsh/stormconsole render and drive the
  federation generically
- Peer replication — `[replication] peers`, revision-guarded
  last-writer-wins push of durable intent (volumes + registered nodes) to
  every peer on change (`POST /api/v1/replicate`,
  `GET /api/v1/replication/status`); poll status deliberately local so
  peers cannot ping-pong. Verified live on dev: volume created on peer A
  visible on peer B at the pushed revision within 2 s

## [v0.1.0] — 2026-08-26

### Added
- Phase 1 scaffold: node registry (static config + stormblock-compatible
  self-registration endpoints), capacity/health poller against /v1,
  pools (selector + replicas + rung), placement engine (label-chain
  domains at any rung, load-balanced by free ratio, deterministic),
  distributed volumes with per-node legs created via /v1 (slaves=0 —
  redundancy is stormstorage's, via legs; rollback on partial failure),
  events, persisted federation state, axum API on :9093, embedded UI,
  stormd summary card, deploy files (testbed-shaped example config,
  systemd unit, stormd [process.ui] snippet)
- End-to-end verified on dev.g8.lo against a live stormblock engine:
  poll → healthy with real capacity, pool create → leg volume visible in
  the engine, delete → both sides clean

### Documentation (bootstrap)
- **docs:** Founding specification (docs/architecture.md): control plane
  across SNO storage clusters — registry (static + stormblock-compatible
  self-registration), federation tree, pools, load-balanced placement at
  rungs, DistVolume = RAID across individual per-node volumes with
  movable NVMe-TCP legs, cross-cluster tiering, stormfs consumption,
  phased plan
- **chore:** Project bootstrap — CLAUDE.md work plan, README, changelog,
  .gitignore
