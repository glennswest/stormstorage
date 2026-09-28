#!/usr/bin/env bash
# e2e-export.sh — consumer serving (#2), against real stormblock engines
# on one host, unprivileged.
#
#   sc-build scripts/e2e-export.sh
#
# 1. three storage engines (a, b, c) with file-backed slabs and NVMe-oF/TCP
#    targets on loopback, stormstorage over them, and a fourth engine (x)
#    with no drives as the consumer;
# 2. create a 2-leg volume: assembled on its head and published — a
#    volume pinned to the head's dedicated array, served over NVMe-TCP;
# 3. the consumer opens the published coordinates and formats a data slab
#    on them (a real write through the mirror);
# 4. kill -9 the non-head leg's engine under the consumer: the re-leg
#    converges, the export is untouched, and the consumer reads back the
#    same slab header through the mirror;
# 5. republish reports unchanged coordinates; a single-leg volume is
#    served as its leg;
# 6. delete revokes the export first: the served volume, the array and
#    the legs are gone from every engine.
#
# stormblock is built from GitHub main unless STORMBLOCK_BIN points at one.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
W=${WORK:-$ROOT/tmp/e2e-export}
SB_REF=${STORMBLOCK_REF:-main}
SS_ADDR=127.0.0.1:9393
SS="http://$SS_ADDR/api/v1"
NODES=(a b c)
declare -A MGMT=([a]=127.0.0.1:9291 [b]=127.0.0.1:9292 [c]=127.0.0.1:9293 [x]=127.0.0.1:9294)
declare -A NVME=([a]=127.0.0.1:14421 [b]=127.0.0.1:14422 [c]=127.0.0.1:14423 [x]=127.0.0.1:14424)
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
trap 'fail "line $LINENO: $BASH_COMMAND"' ERR

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

engine_config() {
    local n=$1 d="$W/$1"
    mkdir -p "$d/data"
    {
        if [ "$n" != x ]; then
            truncate -s 4G "$d/slab.img"
            "$STORMBLOCK_BIN" slab format "$d/slab.img" --role data >/dev/null 2>&1 ||
                "$STORMBLOCK_BIN" slab format "$d/slab.img" >/dev/null
            printf '[[drives]]\npath = "%s"\n\n' "$d/slab.img"
        fi
        cat <<EOF
[management]
listen_addr = "${MGMT[$n]}"
data_dir = "$d/data"
node_name = "$n"
discovery_disabled = true
EOF
    } >"$d/stormblock.toml"
}
# Engine API call: engine, method, path, [json body].
eapi() {
    local n=$1 m=$2 p=$3 t=${TOKEN[$1]:-}
    local a=(); [ -n "$t" ] && a=(-H "Authorization: Bearer $t")
    if [ $# -ge 4 ]; then
        curl -s -X "$m" "${a[@]}" -H 'Content-Type: application/json' -d "$4" "http://${MGMT[$n]}$p"
    else
        curl -s -X "$m" "${a[@]}" "http://${MGMT[$n]}$p"
    fi
}

say "three storage engines and a consumer"
for n in a b c x; do
    engine_config "$n"
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

say "create a 2-leg volume: assembled and published"
curl -s -X POST "$SS/volumes" -H 'Content-Type: application/json' \
    -d '{"name":"ev","size_bytes":536870912,"replicas":2}' >"$W/create.json"
vol() { curl -s "$SS/volumes/ev"; }
[ "$(vol | py 'print(d["assembly"])')" = assembled ] || fail "not assembled: $(cat "$W/create.json")"
[ "$(vol | py 'print(d["export"]["state"])')" = published ] || fail "not published: $(vol)"
HEAD=$(vol | py 'print(d["head"])')
ARRAY=$(vol | py 'print(d["array_id"])')
SERVED=$(vol | py 'print(d["export"]["volume_id"])')
URI=$(vol | py 'c=d["export"]["coordinates"]; print("nvme-tcp://%s:%s/%s?nsid=%s" % (c["traddr"], c["trsvcid"], c["nqn"], c["nsid"]))')
VICTIM=$(vol | py 'print([l["node"] for l in d["legs"] if l["node"]!=d["head"]][0])')
[ "$(vol | py 'print(d["export"]["node"])')" = "$HEAD" ] || fail "served from elsewhere than the head"
vol | py "assert all(l['volume_id']!='$SERVED' for l in d['legs']), 'served volume is a leg'" || fail "served a leg"
ok "published $SERVED on $HEAD at $URI"

eapi "$HEAD" GET "/api/v1/arrays/$ARRAY" >"$W/array.json"
# The array lists the engine's own volume uuid, not the /v1 id, so match
# the served volume by name.
python3 - "$W/array.json" "ev-mirror" <<'EOF' || fail "head array: $(cat "$W/array.json")"
import json, sys
a = json.load(open(sys.argv[1])); served = sys.argv[2]
print("  array slab:", {k: a["slab"].get(k) for k in ("id", "dedicated", "role")})
print("  on it:", a["volumes"])
assert a["slab"]["dedicated"] is True, "array slab is not dedicated"
assert any(v["name"] == served and v["pinned"] for v in a["volumes"]), "served volume not pinned to the array"
EOF
ok "served volume pinned to the dedicated array"

say "consumer x writes through the mirror"
eapi x POST /api/v1/slabs "{\"device_path\":\"$URI\",\"role\":\"data\",\"tier\":\"hot\"}" >"$W/cslab.json"
CSLAB=$(py 'print(d.get("id") or d.get("slab_id") or "")' <"$W/cslab.json")
[ -n "$CSLAB" ] || fail "consumer could not format a slab on $URI: $(cat "$W/cslab.json")"
ok "consumer formatted data slab $CSLAB on the served namespace"

say "kill -9 $VICTIM under the consumer"
t0=$SECONDS
kill -9 "${PID[$VICTIM]}"; wait "${PID[$VICTIM]}" 2>/dev/null || true; unset "PID[$VICTIM]"
for _ in $(seq 1 300); do
    s=$(vol | py 'print(d["assembly"], "replacing" if d.get("replacing") else "-", " ".join(sorted(l["node"] for l in d["legs"])))')
    case "$s" in "assembled - "*) [[ " ${s#assembled - } " != *" $VICTIM "* ]] && break ;; esac
    sleep 1
done
echo "  after $((SECONDS - t0)) s: $s"
case "$s" in "assembled - "*) ;; *) fail "re-leg did not converge: $s" ;; esac
# Match whole leg names: a victim "b" is in "assembled".
[[ " ${s#assembled - } " != *" $VICTIM "* ]] || fail "victim still a leg: $s"
[ "$(vol | py 'print(d["export"]["state"], d["export"]["volume_id"])')" = "published $SERVED" ] ||
    fail "export changed by the re-leg: $(vol | py 'print(d["export"])')"
ok "re-legged; export untouched"

# A second format without role=data is refused because the device already
# holds a data slab — the engine reads the header back through the mirror.
eapi x POST /api/v1/slabs "{\"device_path\":\"$URI\",\"tier\":\"hot\"}" >"$W/reread.json"
grep -qF "$CSLAB" "$W/reread.json" || fail "consumer did not read its slab back: $(cat "$W/reread.json")"
ok "consumer reads slab $CSLAB back through the re-legged mirror"

say "republish"
curl -s -X POST "$SS/volumes/ev/export" -H 'Content-Type: application/json' -d '{}' >"$W/repub.json"
py "assert d['state']=='published' and d['coordinates_changed'] is False and d['volume_id']=='$SERVED', d" \
    <"$W/repub.json" || fail "republish: $(cat "$W/repub.json")"
ok "republished, coordinates unchanged"

say "single-leg volume"
curl -s -X POST "$SS/volumes" -H 'Content-Type: application/json' \
    -d '{"name":"sv","size_bytes":268435456,"replicas":1}' >"$W/single.json"
# On a busy build box the head can miss polls and read as unreachable; a
# publish then fails, is recorded, and is retried when the node answers.
first=$(py 'print(d["export"]["state"], d["export"].get("message") or "")' <"$W/single.json")
for _ in $(seq 1 180); do
    curl -s "$SS/volumes/sv" >"$W/single.json"
    [ "$(py 'print(d["export"]["state"])' <"$W/single.json")" = published ] && break
    sleep 1
done
py 'e=d["export"]; l=d["legs"][0]; assert d["assembly"]=="single_leg" and e["state"]=="published" and e["volume_id"]==l["volume_id"] and e["coordinates"]==l["export"], d' \
    <"$W/single.json" || fail "single leg: $(cat "$W/single.json")"
case "$first" in published*) ok "served as its leg" ;;
    *) ok "served as its leg (on create: $first — published on recovery)" ;; esac

say "delete: the export is revoked first"
eapi x DELETE "/api/v1/slabs/$CSLAB" >/dev/null || true
eapi x DELETE "/api/v1/drives/$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1],safe=""))' "$URI")?force=true" >/dev/null || true
# Probe engine latency while deleting: the head against another live
# engine, to tell an engine stall from a loaded box.
OTHER=$(for n in a b c; do [ "$n" != "$HEAD" ] && [ -n "${PID[$n]:-}" ] && echo "$n"; done | head -1)
( while :; do
    for n in "$HEAD" $OTHER; do
        t=$(curl -s -o /dev/null -w '%{time_total}' -m 30 -H "Authorization: Bearer ${TOKEN[$n]:-}" \
            "http://${MGMT[$n]}/v1/nodes/capacity" || echo timeout)
        printf '%s %s:%s ' "$(date -u +%H:%M:%S)" "$n" "$t"
    done
    echo "load $(cut -d' ' -f1 /proc/loadavg)"
    sleep 5
  done ) >"$W/probe.log" 2>&1 &
PROBE=$!
for v in ev sv; do
    t0=$SECONDS
    curl -s -X DELETE "$SS/volumes/$v" >"$W/del.json"
    if ! py "assert d.get('deleted')=='$v'" <"$W/del.json" 2>/dev/null; then
        # The documented contract: a revoke the head did not answer keeps
        # the record (502) — retry once the head answers again.
        echo "  delete $v: $((SECONDS - t0)) s, not done: $(cat "$W/del.json")"
        echo "  latency (head $HEAD, other $OTHER):"; sed 's/^/    /' "$W/probe.log" | tail -80
        t1=$SECONDS
        for _ in $(seq 1 300); do
            [ "$(curl -s "$SS/nodes" | py "print(any(n['name']=='$HEAD' and n['status']['healthy'] for n in (d if isinstance(d,list) else d.get('nodes',[]))))")" = True ] && break
            sleep 1
        done
        curl -s -X DELETE "$SS/volumes/$v" | py "assert d.get('deleted')=='$v', d" || fail "delete $v (retry)"
        ok "delete $v on retry, $((SECONDS - t1)) s after the first attempt returned"
    else
        echo "  delete $v: $((SECONDS - t0)) s"
    fi
done
kill "$PROBE" 2>/dev/null || true
LIVE=$(for n in a b c; do [ -n "${PID[$n]:-}" ] && echo "$n"; done)
for n in $LIVE; do
    left=$(eapi "$n" GET /v1/volumes | py 'print(len(d if isinstance(d,list) else d.get("volumes",[])))')
    [ "$left" = 0 ] || fail "$n still has $left /v1 volumes"
done
arrays=$(eapi "$HEAD" GET /api/v1/arrays | py 'print(len(d if isinstance(d,list) else d.get("items",d.get("arrays",[]))))')
[ "$arrays" = 0 ] || fail "$HEAD still has $arrays arrays"
EV=$(curl -s "$SS/events" | py 'print("\n".join(e["message"] for e in d["events"]))')
echo "$EV" | grep -F "ev: published on $HEAD" >/dev/null || fail "no publish event"
ok "served volume, array and legs gone"
echo
echo "PASS: consumer serving (#2)"
