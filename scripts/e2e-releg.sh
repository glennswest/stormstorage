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
# stormblock is never compiled here (#25): a stormstorage job must not spend
# its build slot on a fat-LTO stormblock build. STORMBLOCK_BIN names a built
# stormblock (from its golden bin once sc-build jobs can reach one,
# stormcentral#131); without it the script stops before doing anything.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
W=${WORK:-$ROOT/tmp/e2e-releg}
SS_ADDR=127.0.0.1:9393
SS="http://$SS_ADDR/api/v1"
NODES=(a b c)
declare -A MGMT=([a]=127.0.0.1:9291 [b]=127.0.0.1:9292 [c]=127.0.0.1:9293)
declare -A NVME=([a]=127.0.0.1:14421 [b]=127.0.0.1:14422 [c]=127.0.0.1:14423)
declare -A PID=() TOKEN=()
SSPID=

say() { printf '\n== %s  [%s load %s]\n' "$*" "$(date -u +%H:%M:%S)" "$(cut -d' ' -f1-3 /proc/loadavg)"; }
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
[ -n "${STORMBLOCK_BIN:-}" ] ||
    fail "STORMBLOCK_BIN is not set: this e2e runs a built stormblock and never compiles one (#25)"
(cd "$ROOT" && cargo build --release -q)
SS_BIN="${CARGO_TARGET_DIR:-$ROOT/target}/release/stormstorage"
[ -x "$STORMBLOCK_BIN" ] || fail "no stormblock at $STORMBLOCK_BIN"
[ -x "$SS_BIN" ] || fail "no stormstorage at $SS_BIN"
echo "  stormstorage $(cd "$ROOT" && git rev-parse --short HEAD)"
echo "  stormblock $("$STORMBLOCK_BIN" --version 2>/dev/null || echo "(no --version)") at $STORMBLOCK_BIN"

start_engine() {
    local n=$1 d="$W/$1"
    RUST_LOG=stormblock=info "$STORMBLOCK_BIN" --config "$d/stormblock.toml" --data-dir "$d/data" \
        --no-iscsi --nvmeof-addr "${NVME[$n]}" --nvmeof-nqn "nqn.2024.io.stormblock:e2e-$n" \
        >>"$W/engine-$n.log" 2>&1 &
    PID[$n]=$!
    # Adopting a slab can take tens of seconds on a busy build box; the
    # token is minted only once the API is up.
    local up=
    for _ in $(seq 1 1800); do
        curl -s -o /dev/null "http://${MGMT[$n]}/api/v1/health" && { up=1; break; }
        kill -0 "${PID[$n]}" 2>/dev/null || fail "engine $n exited"
        sleep 0.1
    done
    [ -n "$up" ] || fail "engine $n not answering after 180 s"
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
# A killed engine refuses connections at once, so 15 polls is still ~30 s
# to detect; a live engine stalled on a loaded build box is not marked lost.
fail_threshold = 15

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

# /v1 volumes on an engine (the list is bare or under "volumes").
eapi_count() {
    local t=${TOKEN[$1]:-} a=()
    [ -n "$t" ] && a=(-H "Authorization: Bearer $t")
    curl -s "${a[@]}" "http://${MGMT[$1]}/v1/volumes" | py 'print(len(d if isinstance(d,list) else d.get("volumes",[])))'
}

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
for _ in $(seq 1 120); do
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
left=$(curl -s "${VAUTH[@]}" "http://${MGMT[$VICTIM]}/v1/volumes" | py "print(sum(1 for v in (d if isinstance(d,list) else d.get('volumes',[])) if v.get('id')=='$OLD_VID'))")
[ "$left" = 0 ] || fail "old leg $OLD_VID still on $VICTIM"
ok "old leg volume gone from $VICTIM"
sleep 6
n=$(curl -s "$SS/events" | py 'print(sum(1 for e in d["events"] if "rv: replacing leg" in e["message"]))')
[ "$n" = 1 ] || fail "the victim coming back started another re-leg ($n)"
ok "victim back: still exactly one re-leg"

say "delete the volume"
if ! curl -s -X DELETE "$SS/volumes/rv" | py 'assert d.get("deleted")=="rv", d' 2>/dev/null; then
    # A teardown the head did not answer keeps the record (502): retry once
    # the head reads healthy again.
    for _ in $(seq 1 300); do
        [ "$(curl -s "$SS/nodes" | py "print(any(n['name']=='$HEAD' and n['status']['healthy'] for n in d['nodes']))")" = True ] && break
        sleep 1
    done
    curl -s -X DELETE "$SS/volumes/rv" | py 'assert d.get("deleted")=="rv", d' || fail "delete (retry)"
fi
for n in "${!PID[@]}"; do
    left=$(eapi_count "$n")
    [ "$left" = 0 ] || fail "$n still has $left /v1 volumes"
done
ok "deleted; no leg left on any engine"
echo
echo "PASS: re-leg on node loss (#1)"
