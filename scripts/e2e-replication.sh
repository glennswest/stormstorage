#!/usr/bin/env bash
# e2e-replication.sh — replication on the RAID head (#33), against real
# stormblock engines on one host, unprivileged.
#
#   SC_TOOLS=stormblock sc-build scripts/e2e-replication.sh
#
# 1. three storage engines (a, b, c) and a consumer engine x with no drives;
#    a 2-leg volume served to x by host NQN; x formats a data slab on it;
# 2. /replicas: both legs in_sync; prestage the slave onto the third node
#    at bandwidth class low → in_sync again on the new leg;
# 3. kill -9 the head; fence: a stale expected_epoch is a 412, the fence
#    bumps the epoch and fences the surviving leg's /v1 epoch;
# 4. enforcement at the leg (stormblock#6): the old head's attachment is
#    gone, and an attach at the old epoch or with none is a 412;
# 5. promote onto the surviving leg: the same array uuid on the new head,
#    the served volume adopted (never recreated), new coordinates; a fresh
#    consumer y (same host NQN) reads x's slab back through it;
# 6. the lost leg is re-legged under the new head; the old head restarted
#    is reaped from stale_heads, its orphan leg deleted;
# 7. dual-attach: a stale close is a 412, open + abort leaves the head;
# 8. delete leaves nothing on any engine.
#
# Not here: promote off a live head (stormblock#296) and sync evidence from
# superblocks while the head is gone (stormblock#309).
#
# stormblock is never compiled here (#25): STORMBLOCK_BIN names a built one
# (SC_TOOLS=stormblock sets it from stormblock's golden, stormcentral#168).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
W=${WORK:-$ROOT/tmp/e2e-replication}
SS_ADDR=127.0.0.1:9393
SS="http://$SS_ADDR/api/v1"
NODES=(a b c)
declare -A MGMT=([a]=127.0.0.1:9291 [b]=127.0.0.1:9292 [c]=127.0.0.1:9293 [x]=127.0.0.1:9294 [y]=127.0.0.1:9295)
declare -A NVME=([a]=127.0.0.1:14421 [b]=127.0.0.1:14422 [c]=127.0.0.1:14423 [x]=127.0.0.1:14424 [y]=127.0.0.1:14425)
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

# Engines enforce an admin credential on destructive verbs (array create,
# member add/remove, drive close; stormblock#274). Engines and stormstorage
# share one admin token through the variable both read (stormstorage#47).
STORMBLOCK_ADMIN_TOKEN=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
export STORMBLOCK_ADMIN_TOKEN

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

engine_config() {
    local n=$1 d="$W/$1"
    mkdir -p "$d/data"
    {
        if [ "$n" != x ] && [ "$n" != y ]; then
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
    # The harness's own writes (the consumer's slab format, drive close) are
    # destructive verbs: they take the admin token (stormblock#274).
    [ "$m" != GET ] && t=$STORMBLOCK_ADMIN_TOKEN
    local a=(); [ -n "$t" ] && a=(-H "Authorization: Bearer $t")
    if [ $# -ge 4 ]; then
        curl -s -X "$m" "${a[@]}" -H 'Content-Type: application/json' -d "$4" "http://${MGMT[$n]}$p"
    else
        curl -s -X "$m" "${a[@]}" "http://${MGMT[$n]}$p"
    fi
}

say "three storage engines and consumer x"
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

# stormstorage call: method, path, [json body] → body in $W/r.json, prints
# the HTTP status.
sreq() {
    local a=()
    [ $# -ge 3 ] && a=(-H 'Content-Type: application/json' -d "$3")
    curl -s -o "$W/r.json" -w '%{http_code}' -X "$1" "${a[@]}" "$SS$2"
}
vol() { curl -s "$SS/volumes/rv"; }
legs() { vol | py 'print(" ".join(sorted(l["node"] for l in d["legs"])))'; }
healthy() { curl -s "$SS/nodes" | py "print(any(n['name']=='$1' and n['status']['healthy'] for n in d['nodes']))"; }
# Wait until the volume is assembled, idle, on exactly these legs.
converge() {
    local want=$1 s=
    for _ in $(seq 1 300); do
        s=$(vol | py 'print(d["assembly"], "replacing" if d.get("replacing") else "-", " ".join(sorted(l["node"] for l in d["legs"])))')
        [ "$s" = "assembled - $want" ] && return 0
        sleep 1
    done
    fail "not converged on [$want]: $s"
}
# Every replica's sync state, "node:role:state …".
replicas() { curl -s "$SS/volumes/rv/replicas" | py 'print(" ".join("%s:%s:%s" % (r["node"], r["role"], r["sync"]["state"]) for r in d["replicas"]))'; }
wait_in_sync() {
    local r=
    for _ in $(seq 1 120); do
        r=$(replicas)
        py "import sys; assert all(x.endswith(':in_sync') for x in '$r'.split()) and len('$r'.split())==2" </dev/null 2>/dev/null && { echo "$r"; return 0; }
        sleep 1
    done
    fail "replicas not in sync: $r"
}

say "create a 2-leg volume served to x"
X_NQN=nqn.2026-10.lo.storm:e2e:consumer-x
[ "$(sreq POST /volumes "{\"name\":\"rv\",\"size_bytes\":268435456,\"replicas\":2,\"hosts\":[{\"host_nqn\":\"$X_NQN\"}]}")" = 200 ] ||
    fail "create: $(cat "$W/r.json")"
[ "$(vol | py 'print(d["assembly"], d["export"]["state"])')" = "assembled published" ] || fail "not assembled + published: $(vol)"
HEAD=$(vol | py 'print(d["head"])')
ARRAY=$(vol | py 'print(d["array_id"])')
SERVED=$(vol | py 'print(d["export"]["volume_id"])')
SLAVE=$(vol | py 'print([l["node"] for l in d["legs"] if l["node"]!=d["head"]][0])')
SPARE=$(printf '%s\n' "${NODES[@]}" | grep -vxF -e "$HEAD" -e "$SLAVE")
uri() { vol | py 'c=d["export"]["hosts"][0]["coordinates"]; print("nvme-tcp://%s:%s/%s?nsid=%s&hostnqn=%s" % (c["traddr"], c["trsvcid"], c["nqn"], c["nsid"], "'"$X_NQN"'"))'; }
URI=$(uri)
ok "assembled on $HEAD over [$(legs)], array $ARRAY, served $SERVED at $URI"

eapi x POST /api/v1/slabs "{\"device_path\":\"$URI\",\"role\":\"data\",\"tier\":\"hot\"}" >"$W/cslab.json"
CSLAB=$(py 'print(d.get("id") or d.get("slab_id") or "")' <"$W/cslab.json")
[ -n "$CSLAB" ] || fail "x could not format a slab on $URI: $(cat "$W/cslab.json")"
# x has done its write; a clean stop flushes it. y reads it back later.
kill -TERM "${PID[x]}"; wait "${PID[x]}" 2>/dev/null || true; unset "PID[x]"
ok "x formatted data slab $CSLAB through the mirror, then stopped"

say "sync state"
ok "replicas: $(wait_in_sync)"
[ "$(curl -s "$SS/volumes/rv/replicas" | py 'print(d["epoch"])')" = 1 ] || fail "epoch is not 1"

say "prestage the slave $SLAVE → $SPARE at class low"
[ "$(sreq POST /volumes/rv/prestage "{\"node\":\"$SPARE\",\"from\":\"$SLAVE\",\"bandwidth_class\":\"low\"}")" = 200 ] ||
    fail "prestage: $(cat "$W/r.json")"
echo "  $(cat "$W/r.json")"
seen=
for _ in $(seq 1 20); do r=$(replicas); case "$r" in *resyncing*) seen=$r; break ;; esac; sleep 0.5; done
[ -n "$seen" ] && echo "  during: $seen" || echo "  (rebuild finished before a resyncing read)"
converge "$(printf '%s\n' "$HEAD" "$SPARE" | sort | tr '\n' ' ' | sed 's/ $//')"
ok "legs [$(legs)], class $(vol | py 'print(d["bandwidth_class"])'), replicas: $(wait_in_sync)"
OLD_SLAVE=$SLAVE; SLAVE=$SPARE; SPARE=$OLD_SLAVE
SLAVE_VID=$(vol | py "print([l['volume_id'] for l in d['legs'] if l['node']=='$SLAVE'][0])")
HEAD_VID=$(vol | py "print([l['volume_id'] for l in d['legs'] if l['node']=='$HEAD'][0])")
eapi "$SLAVE" GET "/v1/volumes/$SLAVE_VID" >"$W/leg.json"
py "a=d.get('attachments') or []; print('  slave leg attachments:', [(x.get('host_nqn'), x.get('epoch')) for x in a]); assert any(x.get('epoch')==1 for x in a), a" \
    <"$W/leg.json" || fail "the head's attachment on $SLAVE: $(cat "$W/leg.json")"

say "kill -9 the head $HEAD"
t0=$SECONDS
kill -9 "${PID[$HEAD]}"; wait "${PID[$HEAD]}" 2>/dev/null || true; unset "PID[$HEAD]"
for _ in $(seq 1 120); do [ "$(healthy "$HEAD")" = False ] && break; sleep 1; done
[ "$(healthy "$HEAD")" = False ] || fail "$HEAD still reads healthy"
ok "$HEAD unhealthy after $((SECONDS - t0)) s; volume $(vol | py 'print(d["assembly"])'); replicas: $(replicas)"

say "fence"
[ "$(sreq POST /volumes/rv/fence '{"expected_epoch":7}')" = 412 ] || fail "stale fence not refused: $(cat "$W/r.json")"
py 'assert d["code"]=="stale_epoch" and d["current_epoch"]==1, d' <"$W/r.json" || fail "stale fence body: $(cat "$W/r.json")"
[ "$(sreq POST /volumes/rv/fence '{"expected_epoch":1}')" = 200 ] || fail "fence: $(cat "$W/r.json")"
echo "  $(cat "$W/r.json")"
py "assert d['epoch']==2 and '$SLAVE' in str(d['legs_fenced']), d" <"$W/r.json" || fail "fence result"
[ "$(sreq POST /volumes/rv/fence '{"expected_epoch":1}')" = 412 ] || fail "second fence at 1 not refused"
ok "fenced at epoch 2; a racing fence at 1 is refused"

say "enforcement at the leg on $SLAVE (stormblock#6)"
eapi "$SLAVE" GET "/v1/volumes/$SLAVE_VID" >"$W/leg.json"
py 'a=d.get("attachments") or []; print("  leg epoch", d["epoch"], "attachments", [(x.get("host_nqn"), x.get("epoch")) for x in a]); assert d["epoch"]==2 and all(x.get("epoch",0)>=2 for x in a), d' \
    <"$W/leg.json" || fail "leg after fence: $(cat "$W/leg.json")"
OLD_HOST="nqn.2026-10.lo.storm:stormstorage:$HEAD"
for body in "{\"node\":\"$HEAD\",\"mode\":\"read_write\",\"transport\":\"nvme_tcp\",\"host_nqn\":\"$OLD_HOST\",\"epoch\":1}" \
            "{\"node\":\"$HEAD\",\"mode\":\"read_write\",\"transport\":\"nvme_tcp\",\"host_nqn\":\"$OLD_HOST\"}"; do
    code=$(curl -s -o "$W/att.json" -w '%{http_code}' -X POST -H "Authorization: Bearer ${TOKEN[$SLAVE]}" \
        -H 'Content-Type: application/json' -d "$body" "http://${MGMT[$SLAVE]}/v1/volumes/$SLAVE_VID/attach")
    [ "$code" = 412 ] || fail "zombie attach $body → $code $(cat "$W/att.json")"
    py 'assert d["code"]=="stale_epoch" and d["current_epoch"]==2, d' <"$W/att.json" || fail "zombie attach body"
done
ok "old head's attachment revoked; attach at epoch 1 or with none → 412"

say "promote onto $SLAVE"
[ "$(sreq POST /volumes/rv/promote "{\"target_node\":\"$SLAVE\",\"fenced_epoch\":1}")" = 412 ] || fail "promote at a wrong epoch not refused"
[ "$(sreq POST /volumes/rv/promote "{\"target_node\":\"$SLAVE\",\"fenced_epoch\":2}")" = 200 ] || fail "promote: $(cat "$W/r.json")"
py "assert d['head']=='$SLAVE' and d['array_id']=='$ARRAY' and not d['fenced'], d" <<<"$(vol)" ||
    fail "after promote: $(vol)"
ex=$(vol | py 'e=d["export"]; print(e["state"], e.get("adopted"), e["node"], [h["coordinates_changed"] for h in e["hosts"]])')
[ "$ex" = "published True $SLAVE [True]" ] || fail "export after promote: $ex / $(vol | py 'print(d["export"])')"
NEW_URI=$(uri)
[ "$NEW_URI" != "$URI" ] || fail "coordinates did not change"
[ "$(curl -s "$SS/stale-heads" | py 'print(" ".join(s["node"] for s in d["stale_heads"]))')" = "$HEAD" ] || fail "stale head not recorded"
eapi "$SLAVE" GET "/v1/volumes/$SLAVE_VID" >"$W/leg.json"
py 'a=d.get("attachments") or []; print("  leg attachments now", [(x.get("host_nqn"), x.get("epoch")) for x in a])' <"$W/leg.json"
ok "head $SLAVE, same array $ARRAY, served volume adopted at $NEW_URI"

say "consumer y reads x's slab back through the new head"
engine_config y; start_engine y
eapi y POST /api/v1/slabs "{\"device_path\":\"$NEW_URI\",\"tier\":\"hot\"}" >"$W/reread.json"
grep -qF "$CSLAB" "$W/reread.json" || fail "y did not find slab $CSLAB: $(cat "$W/reread.json")"
ok "slab $CSLAB read back after the failover"

say "the lost leg is re-legged under the new head"
OLD_HEAD=$HEAD; HEAD=$SLAVE
converge "$(printf '%s\n' "$HEAD" "$SPARE" | sort | tr '\n' ' ' | sed 's/ $//')"
ok "legs [$(legs)], replicas: $(wait_in_sync)"

say "restart the old head $OLD_HEAD: stale head and orphan reaped"
start_engine "$OLD_HEAD"
for _ in $(seq 1 90); do
    sh=$(curl -s "$SS/stale-heads" | py 'print(len(d["stale_heads"]))')
    or=$(curl -s "$SS/orphans" | py 'print(len(d["orphans"]))')
    [ "$sh$or" = 00 ] && break
    sleep 1
done
[ "$sh$or" = 00 ] || fail "stale heads $sh, orphans $or left"
left=$(eapi "$OLD_HEAD" GET /v1/volumes | py "print(sum(1 for v in (d if isinstance(d,list) else d.get('volumes',[])) if v.get('id')=='$HEAD_VID'))")
[ "$left" = 0 ] || fail "old head leg $HEAD_VID still on $OLD_HEAD"
[ "$(vol | py 'print(d["head"])')" = "$HEAD" ] || fail "head moved back"
ok "nothing stale; $HEAD still head"

say "dual-attach"
[ "$(sreq POST /volumes/rv/dual-attach "{\"target_node\":\"$SPARE\",\"ttl_secs\":60}")" = 200 ] || fail "open: $(cat "$W/r.json")"
DEP=$(py 'print(d["epoch"])' <"$W/r.json")
[ "$(sreq POST /volumes/rv/dual-attach/close "{\"epoch\":$((DEP + 5)),\"outcome\":\"abort\"}")" = 412 ] || fail "stale close not refused"
[ "$(sreq POST /volumes/rv/dual-attach/close "{\"epoch\":$DEP,\"outcome\":\"abort\"}")" = 200 ] || fail "abort: $(cat "$W/r.json")"
py "assert d['head']=='$HEAD' and d['dual_attach'] is None, d" <<<"$(vol)" || fail "after abort: $(vol)"
ok "window at epoch $DEP opened, stale close 412, aborted; head unchanged"

say "delete"
eapi y DELETE "/api/v1/drives/$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1],safe=""))' "$NEW_URI")?force=true" >/dev/null || true
for _ in 1 2 3; do
    [ "$(sreq DELETE /volumes/rv)" = 200 ] && break
    echo "  delete: $(cat "$W/r.json")"; sleep 5
done
py 'assert d.get("deleted")=="rv", d' <"$W/r.json" || fail "delete: $(cat "$W/r.json")"
for n in "${NODES[@]}"; do
    left=$(eapi "$n" GET /v1/volumes | py 'print(len(d if isinstance(d,list) else d.get("volumes",[])))')
    [ "$left" = 0 ] || fail "$n still has $left /v1 volumes"
done
ok "deleted; no leg left on any engine"
echo
echo "PASS: replication on the RAID head (#33)"
