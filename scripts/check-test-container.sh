#!/usr/bin/env bash
# check-test-container.sh — the test container's own plumbing (#8), on the
# build box, unprivileged:
#
#   sc-build scripts/check-test-container.sh
#
# 1. the workspace builds and tests with --locked, and test/build.sh stages
#    a static test binary (STAGE_ONLY=1, no podman);
# 2. /test short against a real stormstorage of this commit with no storage
#    nodes: api-up passes, engine-adopted fails → exit 1, JSON lines with a
#    summary last, the same lines in results.jsonl;
# 3. against a port nobody answers → exit 2; with no node given → exit 2.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
W=$ROOT/tmp/check-test-container
rm -rf "$W"; mkdir -p "$W"
fail() { echo "FAIL: $*" >&2; [ -f "$W/ss.log" ] && tail -20 "$W/ss.log" >&2; exit 1; }

cd "$ROOT"
cargo build --locked
cargo test --locked
STAGE=$(STAGE_ONLY=1 test/build.sh | tail -1)
T="$STAGE/stormstorage-test"
[ -x "$T" ] || fail "no staged test binary"
if command -v ldd >/dev/null && ldd "$T" 2>&1 | grep -q '=>'; then fail "test binary is dynamic: $(ldd "$T")"; fi
echo "OK: staged $(du -h "$T" | cut -f1) static test binary"

cat >"$W/ss.toml" <<CFG
listen_addr = "127.0.0.1:19093"
data_dir = "$W/data"
[local]
enabled = false
CFG
target/debug/stormstorage --config "$W/ss.toml" >"$W/ss.log" 2>&1 &
SS=$!
trap 'kill $SS 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do curl -s -o /dev/null http://127.0.0.1:19093/api/v1/health && break; sleep 0.1; done

set +e
STORM_STORMSTORAGE_URL=http://127.0.0.1:19093 STORM_RUN_ID=check STORM_RESULTS="$W/r1" "$T" short >"$W/short.out"
rc=$?
set -e
cat "$W/short.out"
[ "$rc" = 1 ] || fail "short with no storage nodes: exit $rc, want 1"
python3 - "$W/short.out" "$W/r1/results.jsonl" <<'PY' || fail "report"
import json, sys
lines = [json.loads(l) for l in open(sys.argv[1])]
st = {l["test"]: l["status"] for l in lines if "test" in l}
assert st.get("api-up") == "pass", st
assert st.get("engine-adopted") == "fail", st
assert st.get("single-leg-lifecycle") == "skip", st
assert "summary" in lines[-1] and lines[-1]["summary"]["fail"] >= 1, lines[-1]
assert open(sys.argv[2]).read().splitlines() == open(sys.argv[1]).read().splitlines()
PY
echo "OK: short → exit 1, api-up pass, engine-adopted fail, lifecycle skip, results.jsonl = stdout"

set +e
STORM_NODE=127.0.0.1:1 STORM_RESULTS="$W/r2" STORM_TIMEOUT=30 "$T" short >"$W/down.out"; rc=$?
set -e
[ "$rc" = 2 ] || fail "unreachable: exit $rc, want 2"
set +e
env -u STORM_NODE -u STORM_STORMSTORAGE_URL STORM_RESULTS="$W/r3" "$T" medium >/dev/null 2>&1; rc=$?
set -e
[ "$rc" = 2 ] || fail "no node: exit $rc, want 2"
echo "OK: unreachable → 2, no node → 2"
echo "PASS: test container plumbing (#8)"
