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
  SNO cluster). Nodes come from `[[nodes]]` in the config file, or
  register themselves through stormblock's existing `[stormfs]` heartbeat
  (see [Self-registration](#self-registration)).
- **Poller.** Every `poll.interval_secs` it calls each engine's
  `GET /v1/nodes/capacity` and records total/free bytes and the engine's
  topology labels. After `poll.fail_threshold` consecutive failures the
  node is marked unhealthy and an event is logged. One success marks it
  healthy again.
- **Pools.** A pool is a name, a node selector (tier, labels, explicit
  names) and defaults (`replicas`, `rung`). Pools may overlap.
- **Placement.** Picks one node per distinct failure domain at a rung
  (a domain is the node's label chain from the top rung down to that
  rung). Only healthy nodes with `free_bytes ≥ size` count. Within a
  domain, and between domains, the node with the highest free ratio wins.
  Ties go to the lower name. The result is deterministic. If there are
  not enough domains, the request fails with an explanation.
- **Distributed volumes.** `POST /api/v1/volumes` creates one ordinary
  thin volume per leg through each engine's `/v1/volumes`. If any leg
  fails, the legs already created are rolled back. With two or more legs,
  the volume is then **assembled**:
  - every leg is exported with `/v1/volumes/{id}/attach`, which returns
    the nqn, address and nsid;
  - the head (the first placed node) opens each leg as an `nvme-tcp://`
    drive, including its own leg over loopback;
  - the head builds a RAID1 across those drives with `/api/v1/arrays`.

  If assembly fails, the legs are kept and the volume stays
  `pending_engine_support` with an error event. There is no retry path yet
  (#7).
- **Leg move.** `POST /api/v1/volumes/{name}/move` works on an assembled
  volume and runs these steps:
  1. create a new leg on the target node;
  2. attach it and add it as a RAID member on the head;
  3. a background task waits (up to 1 h) for that member to report
     active;
  4. it then removes the old member, closes the old head drive and
     deletes the old volume.

  Progress is logged to the event feed.
- **Delete.** Tears the assembly down first (array, head drives, leg
  exports). This step is best-effort, and problems are logged as a warning
  event. Then it deletes every leg volume. If any leg delete fails, the
  volume record is kept and the API returns 502.
- **Peer replication.** Durable intent (volume records and self-registered
  node configs) is pushed to every `[replication] peers` URL on each
  change. A peer applies a payload only if its revision is newer. Poll
  status is not replicated: each peer polls the engines itself.
- **Persistence.** State is written to `<data_dir>/state.json`. With no
  `data_dir`, state lives only in memory and a warning is logged.
- **UI and feeds.** The embedded UI at `/` shows nodes, pools and volumes
  with leg states and assembly, plus a create form, a per-leg move button
  and delete. `/api/v1/summary` serves a stormd dashboard card.
  `/api/v1/components` and `/ws/components` serve the stormview feed,
  which stormconsole's `stormstorage` plugin and stormd/stormsh render.

**Not done yet** (tracked in issues, see [Status](#status)): automatic
re-leg when a node is lost (#1), an attachable export of the assembled
mirror for consumers (#2), rebalancing, tier migration, native `/v1`
replication, and HA state.

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
| `[api] api_token` | `""` | Sent as a bearer token on **outbound** replication pushes to peers. It is **not checked on inbound requests** today: the API has no auth (#6). |
| `[replication] peers` | `[]` | Base URLs of peer instances, e.g. `["http://siteb:9093"]`. |
| `[[nodes]]` | none | Static nodes, see below. Names must be unique. |
| `[[pools]]` | none | Pools, see below. |

`[[nodes]]`:

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Node name, also its registry key. |
| `engine_url` | required | stormblock management base URL, e.g. `http://192.168.8.150:9090`. |
| `api_token` | unset | Bearer token for that engine's API. |
| `labels` | `{}` | Rung → value. `node` and `cluster` default to `name` (SNO). Config labels override the labels the engine reports. |
| `tier` | unset | Free-form tier role (`high`, `medium`, `backup`, …). |

`[[pools]]`:

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Pool name. |
| `selector` | `{}` (every node) | `tier`, `labels` (all must match) and `nodes` (explicit names; empty means no restriction). All present conditions must hold. |
| `replicas` | `2` | Default leg count. |
| `rung` | `"node"` | Default spread rung. |

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

## API (:9093)

Errors return `{"error": "...", "code": "not_found|bad_request|conflict|engine"}`
with HTTP 404/400/409/502.

| Method | Path | What |
|---|---|---|
| GET | `/`, `/ui`, `/ui/` | Embedded UI. |
| GET | `/api/v1/health` | `{"status":"ok","version":"…"}`: the liveness/health check. |
| GET | `/api/v1/nodes` | Registry: name, engine_url, tier, effective labels, status. |
| GET | `/api/v1/topology` | Rungs, plus each node's label chain, tier and health. |
| GET | `/api/v1/pools` | Pools with matched/healthy node counts and a capacity rollup over healthy nodes. |
| POST | `/api/v1/placement/plan` | Dry run. Body `{size_bytes, pool?, replicas?, rung?, tier?}` returns `{replicas, rung, legs:[node…]}`. |
| GET | `/api/v1/volumes` | All distributed volumes. |
| POST | `/api/v1/volumes` | Create. Body `{name, size_bytes, pool?, replicas?, rung?, tier?}`. Places, creates the legs and assembles. Returns the volume record. |
| GET | `/api/v1/volumes/{name}` | One volume: legs (node, volume id, state, export, drive/member uuids), head, array id, assembly. |
| DELETE | `/api/v1/volumes/{name}` | Tear down the assembly, then delete the legs. |
| POST | `/api/v1/volumes/{name}/move` | Body `{from, to?}`. Moves the leg on `from`; with no `to`, placement picks one. Returns `{moving, to, status:"rebuilding"}`. |
| GET | `/api/v1/events?since=<seq>` | Event ring (4096 entries, in memory): `{latest_seq, events}`. |
| GET | `/api/v1/summary` | stormd card: `{health, detail, metrics}`. |
| GET | `/api/v1/components` | stormview feed: system, pools, nodes, volumes with relations. Volumes carry a delete action. |
| GET (WS) | `/ws/components` | The same feed. It is checked every 2 s and pushed when it changes. |
| POST | `/api/v1/replicate` | Peer push, `{revision, volumes, registered}`. Applied only if newer. |
| GET | `/api/v1/replication/status` | `{revision, peers}`. |
| POST | `/api/v1/storage/register` | stormblock heartbeat, `{node_addr, hostname, volumes[]}`. |
| POST | `/api/v1/storage/deregister` | `{node_addr}`. |

There is no metrics endpoint. The health check is `/api/v1/health`.

### Engine calls it makes (stormblock :9090)

- `/v1`: `GET nodes/capacity`, `GET|POST volumes`, `DELETE volumes/{id}`,
  `POST volumes/{id}/attach|detach`. Legs are created with `replica_tier`
  `slaves = 0`.
- `/api/v1`: `GET|POST drives`, `DELETE drives/{id}`,
  `POST arrays`, `GET|DELETE arrays/{id}`,
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

## How it ships

stormstorage is a stormcos **service component** and ships in goldens:

| Golden | Kind | Pallet/slab |
|---|---|---|
| `stormstorage` | service (the static musl binary) | system1 |
| `stormstorage-data` | data (`/data/stormstorage`, survives installs) | data1 |
| `stormstorage-logs` | logs (`/logs/stormstorage`) | system1 |

stormd supervises it on the node with
`--config /etc/stormstorage/stormstorage.toml`. The shipped config sets
only `listen_addr = "0.0.0.0:9093"` and
`data_dir = "/var/lib/stormstorage"`, so a stock node starts with no
static nodes or pools. The health check is `/api/v1/health` on 9093, and
the node gateway routes `storage.storm1.g8.lo` to `127.0.0.1:9093` (stormcos `deploy/manifests/85-routes.yaml`). The
component entry lives in stormcentral's `components/stormcos.toml`.

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
- [CLAUDE.md](CLAUDE.md): work plan, status and project rules.
- [CHANGELOG.md](CHANGELOG.md)

## Status

**v0.3.0.** Phase 1 (registry, pools, placement, volumes), 1.5 (stormview
feed, peer replication) and 2 (leg wiring: assembled RAID1, leg move) are
done. The open work:

- #1: re-leg on node loss;
- #2: a consumer export of the assembled mirror;
- #6: inbound API auth;
- #7: retrying a failed assembly;
- phase 3 onward: rebalance and tier migration, native replication, HA.
