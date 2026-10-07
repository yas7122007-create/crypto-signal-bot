"""SYNTHETIC bar fixtures for tests only. Never a substitute for market data.

Produces raw bar dicts in the market-data bar schema (docs/bar-schema.md). With `signal`,
next-minute returns depend on the current close OBI, a planted relationship that lets tests
check the training machinery can find a learnable pattern. It says nothing about markets.
"""
import gzip
import json
import math
import random
from pathlib import Path

T0 = 1_700_000_040_000
BAR_MS = 60_000


def dec(x):
    return format(round(x, 8), "f").rstrip("0").rstrip(".") or "0"


def raw_bars(n, symbol="BTCUSDT", session=1, seed=0, signal=0.0, start=T0):
    rng = random.Random(seed)
    price, bars, obi = 100.0, [], 0.0
    for i in range(n):
        ret = signal * obi * 4.0 + rng.gauss(0, 2.0)        # bps
        price *= math.exp(ret / 1e4)
        obi = max(-0.95, min(0.95, 0.5 * obi + rng.gauss(0, 0.4)))
        hi, lo = price * (1 + abs(rng.gauss(0, 2e-4))), price * (1 - abs(rng.gauss(0, 2e-4)))
        buy, sell = rng.expovariate(1.0), rng.expovariate(1.0)
        s = start + i * BAR_MS
        bars.append({
            "v": 1, "feature_v": 2, "symbol": symbol, "session_id": session,
            "start_ms": s, "end_ms": s + BAR_MS, "rows": 600, "complete": True,
            "incomplete_reason": None, "synced_since_seq": 3, "close_seq": 600 * (i + 1),
            "open_mid": dec(price), "high_mid": dec(hi), "low_mid": dec(lo),
            "close_mid": dec(price), "close_microprice": dec(price * (1 + obi * 1e-5)),
            "mean_spread_bps": "0.5", "close_spread_bps": "0.5",
            "close_obi": [{"levels": 10, "value": dec(obi)}, {"levels": 50, "value": dec(obi / 2)}],
            "close_trade_state": "active",
            "buy_qty": None if i == 0 else dec(buy), "sell_qty": None if i == 0 else dec(sell),
        })
    return bars


def write_dir(directory, bars, marker=True):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with gzip.open(directory / "bars-0000000000001-1-0-000000.ndjson.gz", "wt") as f:
        for bar in bars:
            f.write(json.dumps(bar) + "\n")
    if marker:
        (directory / "SYNTHETIC").write_text("test fixture, not market data\n")
    return directory
