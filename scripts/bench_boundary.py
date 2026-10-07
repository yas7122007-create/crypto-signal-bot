"""Developer benchmark of the Rust -> Python boundary and what it feeds (not part of the bot).

    python scripts/bench_boundary.py STATE_FILE

STATE_FILE is a `<SYMBOL>.json` bar-window file written by the Rust recorder (the bench
`cargo bench --bench hotpath` leaves one in /tmp/claude-0/perf). Measures, per call:
reading + validating the state file (the whole boundary), building the model window, one
PatchTST prediction on a small model trained here on SYNTHETIC bars (latency only, no
claim about accuracy), the full bridge.forecast(), and one Toto-style worker subprocess
round trip carrying the same bars. No network, no orders.
"""
import json
import statistics
import sys
import tempfile
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2 import bridge as BR  # noqa: E402
from v2 import workers  # noqa: E402
from v2.bars import validate  # noqa: E402
from v2.dataset import DatasetSpec, build, window_matrix  # noqa: E402


def timed(name, fn, n):
    fn()  # warm-up
    samples = []
    for _ in range(n):
        t = time.perf_counter_ns()
        fn()
        samples.append(time.perf_counter_ns() - t)
    samples.sort()
    q = lambda p: samples[min(len(samples) - 1, int(p * (len(samples) - 1)))] / 1e6  # noqa: E731
    print(f"| {name} | {n} | {statistics.fmean(samples) / 1e6:.3f} | {q(.5):.3f} | {q(.95):.3f} | {q(.99):.3f} |")


def main(state_file):
    from tests.synthetic import raw_bars
    from v2.patchtst import Forecaster, configure

    path = Path(state_file)
    raw = json.loads(path.read_text())
    # The bench stream repeats a short session, so some bars are incomplete; mark a copy
    # complete so every stage below runs its full path (sizes and parsing are unchanged).
    for bar in raw["bars"]:
        bar.update(complete=True, incomplete_reason=None,
                   buy_qty=bar["buy_qty"] or "1", sell_qty=bar["sell_qty"] or "1")
    tmp = tempfile.TemporaryDirectory()
    path = Path(tmp.name) / path.name
    path.write_text(json.dumps(raw))
    print(f"state file: {path.stat().st_size} bytes, {len(raw['bars'])} bars\n")
    print("| stage | n | mean ms | p50 ms | p95 ms | p99 ms |")
    print("|---|---|---|---|---|---|")
    timed("json.loads only", lambda: json.loads(path.read_text()), 300)
    timed("read_state (read + parse + validate)", lambda: BR.read_state(path), 300)
    _, bars = BR.read_state(path)

    spec = DatasetSpec(window=32, horizon=15)
    window = bars[-(spec.window + 1):]
    timed("window_matrix (32 x channels)", lambda: window_matrix(window, spec), 300)

    configure(threads=1, seed=0)
    train = [validate(b) for b in raw_bars(600, seed=1, signal=0.3)]
    x, y, _ = build({("BTCUSDT", 1): train}, spec)
    model = Forecaster(spec, dict(dropout=0.0)).fit(x[:400], y[:400], x[400:], y[400:],
                                                     epochs=1, seed=0)
    x1 = window_matrix(window, spec)[None]
    timed("PatchTST predict (1 window, 1 thread)", lambda: model.predict(x1), 300)

    with tempfile.TemporaryDirectory() as d:
        model.save(d)
        config = BR.BridgeConfig(model_dir=d)
        now = bars[-1]["end_ms"]
        BR._MODEL_CACHE.clear()
        timed("bridge.forecast (cached model)",
              lambda: BR.forecast(path, "BTCUSDT", 15 * 60_000, config, now_ms=now), 300)
        out = BR.forecast(path, "BTCUSDT", 15 * 60_000, config, now_ms=now)
        print(f"\nforecast status: {out['status']} {out.get('reason') or ''}\n")

        worker = Path(d) / "worker.py"
        worker.write_text(textwrap.dedent("""
            import json, sys
            payload = json.load(sys.stdin)
            print(json.dumps({"n": len(payload["bars"])}))
        """))
        argv = [sys.executable, str(worker)]
        print("| stage | n | mean ms | p50 ms | p95 ms | p99 ms |")
        print("|---|---|---|---|---|---|")
        timed("worker subprocess round trip (256 bars)",
              lambda: workers.run(argv, dict(bars=bars, forecast=out), 30), 30)
        timed("worker subprocess round trip (empty)",
              lambda: workers.run(argv, dict(bars=[], forecast={}), 30), 30)


if __name__ == "__main__":
    main(sys.argv[1])
