#!/usr/bin/env bash
# Real Binance USD-M smoke test (public market data only, no keys, no orders).
#   scripts/binance-smoke.sh [SECONDS] [SYMBOLS]     e.g. scripts/binance-smoke.sh 300 BTCUSDT,ETHUSDT
# Records live streams, replays the recording, and checks that replayed feature rows equal
# the live ones. TLS is verified normally; if the network or a proxy blocks Binance this
# script fails with exit code 3 instead of working around it.
# SMOKE_WS_URL / SMOKE_REST_URL override the endpoints (only to exercise the script itself
# against a local fake server; a run with them set is not a Binance test).
set -euo pipefail

seconds="${1:-120}"
symbols="${2:-BTCUSDT}"
[[ "$seconds" =~ ^[0-9]{1,6}$ ]] || { echo "SECONDS must be an integer" >&2; exit 2; }
[[ "$symbols" =~ ^[A-Za-z0-9]{2,30}(,[A-Za-z0-9]{2,30}){0,44}$ ]] || { echo "bad SYMBOLS" >&2; exit 2; }

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="$(mktemp -d "${TMPDIR:-/tmp}/binance-smoke.XXXXXX")"
bin="$root/market-data/target/release/market-data"
(cd "$root/market-data" && cargo build --release --locked --quiet)

echo "recording $symbols for ${seconds}s into $out" >&2
set +e
endpoints=()
[[ -n "${SMOKE_WS_URL:-}" ]] && endpoints+=(--ws-url "$SMOKE_WS_URL")
[[ -n "${SMOKE_REST_URL:-}" ]] && endpoints+=(--rest-url "$SMOKE_REST_URL")
timeout -s INT "$seconds" "$bin" record --symbols "$symbols" --out "$out/rec" \
  --features-dir "$out/live-features" "${endpoints[@]}" 2> "$out/record.log"
code=$?
set -e
# timeout exits 124 after delivering SIGINT; the recorder then shuts down cleanly.
if [[ $code -ne 0 && $code -ne 124 ]]; then
  echo "recorder failed with exit $code, see $out/record.log" >&2
  exit 1
fi

"$bin" replay --input "$out/rec" --features-dir "$out/replay-features" > "$out/replay.json"

python3 - "$out" "${#endpoints[@]}" <<'PY'
import gzip, json, pathlib, sys
out = pathlib.Path(sys.argv[1])
report = json.loads((out / "replay.json").read_text())
events = report["state"]["events"]
books = report["state"]["books"]
def rows(d):
    files = sorted(p for p in (out / d).glob("features-*.ndjson.gz"))
    return [line for f in files for line in gzip.open(f, "rt")]
live, replayed = rows("live-features"), rows("replay-features")
summary = {
    "dir": str(out),
    "events": events["events"],
    "feature_rows_live": len(live),
    "feature_rows_replay": len(replayed),
    "replay_equals_live": live == replayed,
    "books": {s: {"synced": b["synced"], "gaps": b["stats"]["gaps"],
                  "snapshots": b["stats"]["snapshots_applied"]} for s, b in books.items()},
    "unparsed": events["unparsed"], "malformed": events["malformed"],
}
print(json.dumps(summary, indent=2))
if not books or events["events"] <= len(books) + 2:
    print("FAIL: no market data received; Binance unreachable from here (TLS/proxy/network). "
          "Nothing was bypassed. See record.log.", file=sys.stderr)
    sys.exit(3)
if not live or live != replayed:
    print("FAIL: replayed feature rows differ from live rows", file=sys.stderr)
    sys.exit(1)
if sys.argv[2] != "0":
    print("PASS against overridden endpoints: this was NOT a Binance test", file=sys.stderr)
else:
    print("PASS: live Binance data recorded, synced and replayed identically", file=sys.stderr)
PY
