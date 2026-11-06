#!/usr/bin/env bash
# e2e-fastetcd.sh — stormstorage's store and leader election against a real
# fastetcd (#56, #34): tests/fastetcd.rs, over fastetcd's mutual TLS.
#
#   SC_TOOLS=fastetcd sc-build scripts/e2e-fastetcd.sh
#
# fastetcd is never compiled here (like stormblock, #25): FASTETCD_BIN names a
# built one. Without it the test passes by skipping, so this script refuses.
set -euo pipefail
[ -n "${FASTETCD_BIN:-}" ] || { echo "FASTETCD_BIN is not set: use SC_TOOLS=fastetcd" >&2; exit 2; }
echo "fastetcd: $("$FASTETCD_BIN" --version 2>/dev/null | head -1 || echo '(no --version)')"
cd "$(dirname "$0")/.."
cargo test --locked --test fastetcd -- --nocapture --test-threads=1
