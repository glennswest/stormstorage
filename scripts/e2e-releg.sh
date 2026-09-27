#!/usr/bin/env bash
# e2e-releg.sh — re-leg on node loss (#1), against three real stormblock
# engines on one host, unprivileged.
#
#   sc-build scripts/e2e-releg.sh
#
# 1. three engines (a, b, c), each with a file-backed slab and its own
#    NVMe-oF/TCP target on loopback; stormstorage with all three as nodes;
# 2. create a 2-leg volume and wait for the RAID1 on its head;
# 3. kill -9 the engine holding the non-head leg;
# 4. expect: the leg is marked lost, the volume degraded, a replacement leg
#    placed on the third engine and rebuilt, both members active on the
#    head, the volume assembled again, the old leg recorded as an orphan,
#    exactly one re-leg;
# 5. restart the killed engine: the orphaned leg volume is reaped.
#
# stormblock is built from GitHub main unless STORMBLOCK_BIN points at one.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
W=${WORK:-$ROOT/tmp/e2e-releg}
SB_REF=${STORMBLOCK_REF:-main}
SS_ADDR=127.0.0.1:9393
SS="http://$SS_ADDR/api/v1"
NODES=(a b c)
declare -A MGMT=([a]=127.0.0.1:9291 [b]=127.0.0.1:9292 [c]=127.0.0.1:9293)
declare -A NVME=([a]=127.0.0.1:14421 [b]=127.0.0.1:14422 [c]=127.0.0.1:14423)
declare -A PID=() TOKEN=()
SSPID=

say() { printf '\n== %s\n' "$*"; }
ok() { printf '  OK: %s\n' "$*"; }
fail() {
    printf '\nFAIL: %s\n' "$*" >&2
    for f in "$W"/*.log; do [ -f "$f" ] && { echo "--- $f" >&2; tail -25 "$f" >&2; }; done
    curl -s "$SS/events" 2>/dev/null | py 'print("\n".join(e["message"] for e in d["events"][-30:]))' >&2 || true
    exit 1
}
py() { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }
cleanup() {
    for n in "${!PID[@]}"; do kill -9 "${PID[$n]}" 2>/dev/null || true; done
    [ -n "$SSPID" ] && kill -9 "$SSPID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

rm -rf "$W"; mkdir -p "$W"

say "build"
if [ -z "${STORMBLOCK_BIN:-}" ]; then
    git clone -q --depth 1 --branch "$SB_REF" https://github.com/glennswest/stormblock "$W/stormblock-src"
    (cd "$W/stormblock-src" && echo "  stormblock $(git rev-parse --short HEAD)" &&
        CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$W/sb-target}" cargo build --release -q)
    STORMBLOCK_BIN="${CARGO_TARGET_DIR:-$W/sb-target}/release/stormblock"
fi
(cd "$ROOT" && cargo build --release -q)
SS_BIN="${CARGO_TARGET_DIR:-$ROOT/target}/release/stormstorage"
[ -x "$STORMBLOCK_BIN" ] || fail "no stormblock at $STORMBLOCK_BIN"
[ -x "$SS_BIN" ] || fail "no stormstorage at $SS_BIN"
echo "  stormstorage $(cd "$ROOT" && git rev-parse --short HEAD)"

start_engine() {
    local n=$1 d="$W/$1"
    RUST_LOG=stormblock=info "$STORMBLOCK_BIN" --config "$d/stormblock.toml" --data-dir "$d/data" \
        --no-iscsi --nvmeof-addr "${NVME[$n]}" --nvmeof-nqn "nqn.2024.io.stormblock:e2e-$n" \
        >>"$W/engine-$n.log" 2>&1 &
    PID[$n]=$!
    for _ in $(seq 1 300); do
        curl -s -o /dev/null "http://${MGMT[$n]}/api/v1/health" && break
        kill -0 "${PID[$n]}" 2>/dev/null || fail "engine $n exited"
        sleep 0.1
    done
    TOKEN[$n]=$(cat "$d/data/api_token" 2>/dev/null || true)
}

say "three engines"
for n in "${NODES[@]}"; do
    d="$W/$n"; mkdir -p "$d/data"
    truncate -s 4G "$d/slab.img"
    "$STORMBLOCK_BIN" slab format "$d/slab.img" --role data >/dev/null 2>&1 ||
        "$STORMBLOCK_BIN" slab format "$d/slab.img" >/dev/null
    cat >"$d/stormblock.toml" <<EOF
[[drives]]
path = "$d/slab.img"

[management]
listen_addr = "${MGMT[$n]}"
data_dir = "$d/data"
node_name = "$n"
discovery_disabled = true
ublk_transport = false
EOF
    start_engine "$n"
    echo "  $n: mgmt ${MGMT[$n]}, nvme ${NVME[$n]}, auth $([ -n "${TOKEN[$n]}" ] && echo token || echo none)"
done

say "stormstorage"
{
    cat <<EOF
listen_addr = "$SS_ADDR"
data_dir = "$W/ss"

[poll]
interval_secs = 2
fail_threshold = 2

[local]
enabled = false

[recovery]
cooldown_secs = 10
rebuild_timeout_secs = 300
EOF
    for n in "${NODES[@]}"; do
        printf '\n[[nodes]]\nname = "%s"\nengine_url = "http://%s"\n' "$n" "${MGMT[$n]}"
        [ -n "${TOKEN[$n]}" ] && printf 'api_token = "%s"\n' "${TOKEN[$n]}"
    done
} >"$W/stormstorage.toml"
RUST_LOG=stormstorage=info "$SS_BIN" --config "$W/stormstorage.toml" >>"$W/stormstorage.log" 2>&1 &
SSPID=$!
for _ in $(seq 1 100); do curl -s -o /dev/null "$SS/health" && break; sleep 0.1; done
for _ in $(seq 1 30); do
    h=$(curl -s "$SS/nodes" | py 'print(sum(1 for n in d["nodes"] if n["status"]["healthy"]))')
    [ "$h" = 3 ] && break
    sleep 1
done
[ "$h" = 3 ] || fail "only $h of 3 nodes healthy"
ok "3 nodes healthy"

say "create a 2-leg volume"
curl -s -X POST "$SS/volumes" -H 'Content-Type: application/json' \
    -d '{"name":"rv","size_bytes":268435456,"replicas":2}' >"$W/create.json"
vol() { curl -s "$SS/volumes/rv"; }
[ "$(vol | py 'print(d["assembly"])')" = assembled ] || fail "not assembled: $(cat "$W/create.json")"
HEAD=$(vol | py 'print(d["head"])')
ARRAY=$(vol | py 'print(d["array_id"])')
LEGS=$(vol | py 'print(" ".join(l["node"] for l in d["legs"]))')
VICTIM=$(vol | py 'print([l["node"] for l in d["legs"] if l["node"]!=d["head"]][0])')
OLD_VID=$(vol | py 'print([l["volume_id"] for l in d["legs"] if l["node"]!=d["head"]][0])')
SPARE=$(printf '%s\n' "${NODES[@]}" | grep -vxF -e "$HEAD" -e "$VICTIM")
ok "assembled on $HEAD across [$LEGS], array $ARRAY; victim $VICTIM, spare $SPARE"

say "kill -9 $VICTIM"
t0=$SECONDS
kill -9 "${PID[$VICTIM]}"; wait "${PID[$VICTIM]}" 2>/dev/null || true; unset "PID[$VICTIM]"
for _ in $(seq 1 60); do
    [ "$(vol | py 'print(d["assembly"])')" = degraded ] && break
    [ -n "$(vol | py 'print(d.get("replacing") or "")')" ] && break
    sleep 1
done
ok "loss noticed after $((SECONDS - t0)) s: $(vol | py 'print(d["assembly"], [(l["node"], l["state"]) for l in d["legs"]])')"

say "wait for the re-leg to converge"
for _ in $(seq 1 300); do
    s=$(vol | py 'print(d["assembly"], "replacing" if d.get("replacing") else "-", " ".join(sorted(l["node"] for l in d["legs"])))')
    case "$s" in "assembled - "*) break ;; esac
    sleep 1
done
echo "  after $((SECONDS - t0)) s: $s"
case "$s" in "assembled - "*) ;; *) fail "did not converge: $s" ;; esac
NEW_LEGS=$(vol | py 'print(" ".join(sorted(l["node"] for l in d["legs"])))')
[ "$NEW_LEGS" = "$(printf '%s\n' "$HEAD" "$SPARE" | sort | tr '\n' ' ' | sed 's/ $//')" ] ||
    fail "legs are [$NEW_LEGS], expected $HEAD and $SPARE"
ok "legs now [$NEW_LEGS]"

HT=${TOKEN[$HEAD]}
AUTH=(); [ -n "$HT" ] && AUTH=(-H "Authorization: Bearer $HT")
curl -s "${AUTH[@]}" "http://${MGMT[$HEAD]}/api/v1/arrays/$ARRAY" >"$W/array.json"
python3 - "$W/array.json" <<'EOF' || fail "head array: $(cat "$W/array.json")"
import json, sys
a = json.load(open(sys.argv[1]))
states = [m["state"].lower() for m in a["members"]]
print("  head array members:", states)
assert len(states) == 2 and all(s == "active" for s in states), states
EOF
ok "both members active on $HEAD"

EV=$(curl -s "$SS/events" | py 'print("\n".join(e["message"] for e in d["events"]))')
echo "$EV" | grep -F "rv: leg on $VICTIM lost" >/dev/null || fail "no lost-leg event"
echo "$EV" | grep -F "rv: leg $VICTIM → $SPARE complete (node lost)" >/dev/null || fail "no completion event"
n=$(echo "$EV" | grep -cF "rv: replacing leg" || true)
[ "$n" = 1 ] || fail "$n re-legs started, expected exactly 1"
ok "events: lost, one re-leg $VICTIM → $SPARE, complete"
ORPH=$(curl -s "$SS/orphans" | py 'print(" ".join(o["node"]+":"+o["volume_id"] for o in d["orphans"]))')
[ "$ORPH" = "$VICTIM:$OLD_VID" ] || fail "orphans: [$ORPH], expected $VICTIM:$OLD_VID"
ok "old leg recorded as orphan $ORPH"

say "restart $VICTIM: the orphan is reaped"
start_engine "$VICTIM"
for _ in $(seq 1 60); do
    [ -z "$(curl -s "$SS/orphans" | py 'print(" ".join(o["node"] for o in d["orphans"]))')" ] && break
    sleep 1
done
[ -z "$(curl -s "$SS/orphans" | py 'print(" ".join(o["node"] for o in d["orphans"]))')" ] || fail "orphan not reaped"
VT=${TOKEN[$VICTIM]}
VAUTH=(); [ -n "$VT" ] && VAUTH=(-H "Authorization: Bearer $VT")
left=$(curl -s "${VAUTH[@]}" "http://${MGMT[$VICTIM]}/v1/volumes" | py "print(sum(1 for v in d if v.get('id')=='$OLD_VID'))")
[ "$left" = 0 ] || fail "old leg $OLD_VID still on $VICTIM"
ok "old leg volume gone from $VICTIM"
sleep 6
n=$(curl -s "$SS/events" | py 'print(sum(1 for e in d["events"] if "rv: replacing leg" in e["message"]))')
[ "$n" = 1 ] || fail "the victim coming back started another re-leg ($n)"
ok "victim back: still exactly one re-leg"

say "delete the volume"
curl -s -X DELETE "$SS/volumes/rv" | py 'assert d.get("deleted")=="rv", d' || fail "delete"
ok "deleted"
echo
echo "PASS: re-leg on node loss (#1)"
