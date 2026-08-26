# StormStorage

**The storage control plane across nodes and clusters.**

stormblock executes storage on one node; stormdrive knows one node's
hardware; StormStorage decides across all of them:

- **Registry** of storage nodes (SNO stormblock clusters) — static config
  or self-registration using stormblock's existing heartbeat
- **Pools** — named capacity groupings spanning nodes, with policy
- **Placement** — legs spread across failure domains at a chosen rung
  (shelf, rack, cluster, site…), load-balanced
- **Distributed volumes** — RAID across individual per-node volumes,
  legs over NVMe-TCP, movable between nodes
- **Tiering across clusters** and the migrations it implies
- The fleet surface **stormfs** walks across for its shared filesystem

Never in the data path — everything node↔node and client↔node is
NVMe over TCP. REST API on **:9093**, embedded UI, stormd dashboard card.

> **Build on `root@dev.g8.lo`, never on a Mac.** Commit, push, pull on
> dev, build there.

## Quick start

```bash
stormstorage --config /etc/stormstorage/stormstorage.toml

curl -s http://localhost:9093/api/v1/nodes | python3 -m json.tool
curl -s -X POST http://localhost:9093/api/v1/volumes \
  -H 'Content-Type: application/json' \
  -d '{"name":"vol1","size_bytes":10737418240,"pool":"fast"}'
```

## Documentation

- [docs/architecture.md](docs/architecture.md) — the founding spec
- [CLAUDE.md](CLAUDE.md) — work plan and project rules

## Status

v0.1.0 — Phase 1: registry, pools, placement, distributed-volume
bookkeeping. Mirror assembly awaits stormblock#73 (NVMe-TCP export as a
RAID member via the management API).
