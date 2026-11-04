#!/usr/bin/env bash
# e2e-engine-tls.sh — stormstorage against a real engine serving TLS (#96,
# stormcos#374), on one host, unprivileged.
#
#   SC_TOOLS=stormblock sc-build scripts/e2e-engine-tls.sh
#
# 1. a node CA, another CA, and a serving pair for 127.0.0.1;
# 2. one engine with tls_cert/tls_key and tls_mode = optional, as stormcos
#    ships it during the move;
# 3. stormstorage over https with the node CA: healthy, a volume created and
#    deleted, and the engine counted no plain request from it;
# 4. stormstorage over https with the other CA: never healthy, says why, and
#    the engine's counts do not move;
# 5. stormstorage over http: still works, and the engine counts it.
#
# stormblock is never compiled here (#25): STORMBLOCK_BIN names a built one.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
W=${WORK:-$ROOT/tmp/e2e-engine-tls}
SS_ADDR=127.0.0.1:9395
SS="http://$SS_ADDR/api/v1"
MGMT=127.0.0.1:9295
NVME=127.0.0.1:14425
ENGINE=
SSPID=
ENGPID=

say() { printf '\n== %s  [%s]\n' "$*" "$(date -u +%H:%M:%S)"; }
ok() { printf '  OK: %s\n' "$*"; }
fail() {
    printf '\nFAIL: %s\n' "$*" >&2
    for f in "$W"/*.log; do [ -f "$f" ] && { echo "--- $f" >&2; tail -25 "$f" >&2; }; done
    exit 1
}
py() { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }
cleanup() {
    [ -n "$ENGPID" ] && kill -9 "$ENGPID" 2>/dev/null || true
    [ -n "$SSPID" ] && kill -9 "$SSPID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT
trap 'fail "line $LINENO: $BASH_COMMAND"' ERR

rm -rf "$W"; mkdir -p "$W/certs" "$W/engine/data"

STORMBLOCK_ADMIN_TOKEN=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
export STORMBLOCK_ADMIN_TOKEN

say "build"
[ -n "${STORMBLOCK_BIN:-}" ] ||
    fail "STORMBLOCK_BIN is not set: this e2e runs a built stormblock and never compiles one (#25)"
(cd "$ROOT" && cargo build --release -q)
SS_BIN="${CARGO_TARGET_DIR:-$ROOT/target}/release/stormstorage"
echo "  stormstorage $(cd "$ROOT" && git rev-parse --short HEAD)"
echo "  stormblock $("$STORMBLOCK_BIN" --version 2>/dev/null || echo "(no --version)")"

say "certificates"
(
    cd "$W/certs"
    for ca in node-ca other-ca; do
        openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
            -keyout "$ca.key" -out "$ca.crt" -subj "/CN=$ca" -days 2 2>/dev/null
    done
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
        -keyout engine.key -out engine.csr -subj /CN=e2e-engine 2>/dev/null
    printf 'subjectAltName=DNS:e2e-engine,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >engine.ext
    openssl x509 -req -in engine.csr -CA node-ca.crt -CAkey node-ca.key -CAcreateserial \
        -out engine.crt -days 2 -extfile engine.ext 2>/dev/null
)
ok "node CA, other CA, engine pair"

say "an engine with tls_mode = optional"
truncate -s 2G "$W/engine/slab.img"
"$STORMBLOCK_BIN" slab format "$W/engine/slab.img" --role data >/dev/null 2>&1 ||
    "$STORMBLOCK_BIN" slab format "$W/engine/slab.img" >/dev/null
cat >"$W/engine/stormblock.toml" <<TOML
[[drives]]
path = "$W/engine/slab.img"

[management]
listen_addr = "$MGMT"
data_dir = "$W/engine/data"
node_name = "e"
discovery_disabled = true
tls_cert = "$W/certs/engine.crt"
tls_key = "$W/certs/engine.key"
tls_client_ca = "$W/certs/node-ca.crt"
tls_mode = "optional"
TOML
RUST_LOG=stormblock=info "$STORMBLOCK_BIN" --config "$W/engine/stormblock.toml" --data-dir "$W/engine/data" \
    --no-iscsi --nvmeof-addr "$NVME" --nvmeof-nqn nqn.2024.io.stormblock:e2e-tls \
    >>"$W/engine.log" 2>&1 &
ENGPID=$!
up=
for _ in $(seq 1 1800); do
    curl -sf -o /dev/null --cacert "$W/certs/node-ca.crt" "https://$MGMT/api/v1/health" && { up=1; break; }
    kill -0 "$ENGPID" 2>/dev/null || fail "engine exited"
    sleep 0.1
done
[ -n "$up" ] || fail "engine not answering over https after 180 s"
TOKEN=$(cat "$W/engine/data/api_token" 2>/dev/null || true)
[ -n "$TOKEN" ] || fail "no minted token"
ok "https answers, verified by curl against the node CA"
if curl -s -o /dev/null --cacert "$W/certs/other-ca.crt" "https://$MGMT/api/v1/health"; then
    fail "curl trusted the engine with the other CA: the fixture is wrong"
fi

# The engine's count of plain requests, summed over caller and route.
plain_count() {
    curl -sf --cacert "$W/certs/node-ca.crt" -H "Authorization: Bearer $TOKEN" "https://$MGMT/metrics" |
        awk '/^stormblock_mgmt_plain_requests_total/ { n += $NF } END { print n + 0 }'
}

# Start stormstorage: <engine url>, <CA file or empty>.
start_ss() {
    [ -n "$SSPID" ] && { kill -9 "$SSPID" 2>/dev/null || true; wait "$SSPID" 2>/dev/null || true; }
    rm -rf "$W/ss"
    cat >"$W/stormstorage.toml" <<TOML
listen_addr = "$SS_ADDR"
data_dir = "$W/ss"

[poll]
interval_secs = 1
fail_threshold = 2

[local]
enabled = false

[kubernetes]
enabled = false

[engine_tls]
ca_file = "$2"

[[nodes]]
name = "e"
engine_url = "$1"
api_token = "$TOKEN"
TOML
    RUST_LOG=stormstorage=info "$SS_BIN" --config "$W/stormstorage.toml" >"$W/stormstorage.log" 2>&1 &
    SSPID=$!
    for _ in $(seq 1 100); do curl -s -o /dev/null "$SS/health" && return 0; sleep 0.1; done
    fail "stormstorage not answering"
}
healthy() { curl -s "$SS/nodes" | py 'print(sum(1 for n in d["nodes"] if n["status"]["healthy"] and n["status"].get("total_bytes", 0) > 0))'; }
wait_healthy() {
    local h=0
    for _ in $(seq 1 60); do
        h=$(healthy); [ "$h" = 1 ] && return 0
        sleep 1
    done
    fail "$1: node not healthy"
}

say "over https with the node CA"
before=$(plain_count)
start_ss "https://$MGMT" "$W/certs/node-ca.crt"
wait_healthy "https with the node CA"
ok "node healthy, capacity read"
NQN=nqn.2026-11.lo.storm:e2e:tls-consumer
curl -s -X POST "$SS/volumes" -H 'Content-Type: application/json' \
    -d "{\"name\":\"tv\",\"size_bytes\":67108864,\"replicas\":1,\"hosts\":[{\"host_nqn\":\"$NQN\"}]}" >"$W/create.json"
[ "$(curl -s "$SS/volumes/tv" | py 'print(d["export"]["state"])')" = published ] ||
    fail "volume not published over https: $(cat "$W/create.json")"
ok "volume created and published (engine writes over https)"
code=$(curl -s -o "$W/delete.json" -w '%{http_code}' -X DELETE "$SS/volumes/tv")
[ "${code:0:1}" = 2 ] || fail "delete: $code $(cat "$W/delete.json")"
ok "volume deleted"
after=$(plain_count)
[ "$after" = "$before" ] || fail "the engine counted $((after - before)) plain requests while stormstorage was on https"
ok "the engine counted no plain request ($after)"

say "over https with another CA"
before=$(plain_count)
start_ss "https://$MGMT" "$W/certs/other-ca.crt"
sleep 8
[ "$(healthy)" = 0 ] || fail "healthy behind a certificate from another CA"
curl -s "$SS/events" | py 'print("\n".join(e["message"] for e in d["events"]))' >"$W/events.txt"
grep -qi certificate "$W/events.txt" || fail "no event names the certificate: $(cat "$W/events.txt")"
grep -i certificate "$W/events.txt" | head -1 | sed 's/^/  event: /'
after=$(plain_count)
[ "$after" = "$before" ] || fail "it fell back to plain ($before -> $after)"
ok "refused, said why, and did not fall back to plain"

say "over http, while the engine is optional"
before=$(plain_count)
start_ss "http://$MGMT" ""
wait_healthy "http"
after=$(plain_count)
[ "$after" -gt "$before" ] || fail "plain polls not counted ($before -> $after)"
ok "http keeps working, and the engine counts it ($before -> $after)"

printf '\nPASS\n'
