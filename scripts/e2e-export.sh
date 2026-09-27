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
python3 - "$W/array.json" "$SERVED" <<'EOF' || fail "head array: $(cat "$W/array.json")"
import json, sys
a = json.load(open(sys.argv[1])); served = sys.argv[2]
print("  array slab:", {k: a["slab"].get(k) for k in ("id", "dedicated", "role")})
print("  on it:", a["volumes"])
assert a["slab"]["dedicated"] is True, "array slab is not dedicated"
assert any(v["id"] == served and v["pinned"] for v in a["volumes"]), "served volume not pinned to the array"
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
    case "$s" in "assembled - "*) [[ "$s" != *"$VICTIM"* ]] && break ;; esac
    sleep 1
done
echo "  after $((SECONDS - t0)) s: $s"
case "$s" in "assembled - "*) ;; *) fail "re-leg did not converge: $s" ;; esac
[[ "$s" != *"$VICTIM"* ]] || fail "victim still a leg: $s"
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
py 'e=d["export"]; l=d["legs"][0]; assert d["assembly"]=="single_leg" and e["state"]=="published" and e["volume_id"]==l["volume_id"] and e["coordinates"]==l["export"], d' \
    <"$W/single.json" || fail "single leg: $(cat "$W/single.json")"
ok "served as its leg"

say "delete: the export is revoked first"
eapi x DELETE "/api/v1/slabs/$CSLAB" >/dev/null || true
eapi x DELETE "/api/v1/drives/$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1],safe=""))' "$URI")?force=true" >/dev/null || true
for v in ev sv; do
    curl -s -X DELETE "$SS/volumes/$v" | py "assert d.get('deleted')=='$v', d" || fail "delete $v"
done
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
