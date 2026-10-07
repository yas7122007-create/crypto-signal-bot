"""Deterministic forecasting dataset from one-minute bars (requires numpy).

A sample is made at `asof_ms`, the end of bar t. Its input is the `window` bars ending at
t, all complete and adjacent, plus bar t-window for the first return. Its target is the
log return of the mid from the close of bar t to the close of bar t+horizon, in basis
points. Inputs never use a bar ending after `asof_ms`; the target never uses a bar ending
at or before it. Splits are by time, never shuffled, with an embargo of one horizon so no
training target overlaps a later split.

The same `window_matrix` builds training inputs and live inference inputs, so features
cannot drift between the two.
"""
from dataclasses import asdict, dataclass
import math

import numpy as np

from v2.bars import BAR_MS, BAR_SCHEMA, FEATURE_SCHEMA

DATASET_SCHEMA = 1
# Small, explainable input set (docs/forecasting.md explains each choice).
CHANNELS = ("ret_bps", "range_bps", "spread_bps", "micro_disp_bps", "obi",
            "flow_imbalance", "log_volume")


@dataclass(frozen=True)
class DatasetSpec:
    window: int = 32        # bars of input (32 minutes)
    horizon: int = 15       # bars ahead (15 minutes, the bot's signal candle)
    obi_levels: int = 10    # which OBI depth to use

    def __post_init__(self):
        if not (8 <= self.window <= 512 and 1 <= self.horizon <= 240 and self.obi_levels >= 1):
            raise ValueError("dataset spec out of range")

    def as_dict(self):
        return dict(asdict(self), channels=list(CHANNELS), dataset_schema=DATASET_SCHEMA,
                    bar_schema=BAR_SCHEMA, feature_schema=FEATURE_SCHEMA, bar_ms=BAR_MS)


def bar_vector(prev, bar, spec):
    """One bar's inputs, or None if any is unavailable (never imputed)."""
    obi = bar["obi"].get(spec.obi_levels)
    if obi is None or bar["buy_qty"] is None:
        return None
    close, mid_prev = float(bar["close_mid"]), float(prev["close_mid"])
    buy, sell = float(bar["buy_qty"]), float(bar["sell_qty"])
    volume = buy + sell
    return [
        1e4 * math.log(close / mid_prev),
        1e4 * float(bar["high_mid"] - bar["low_mid"]) / close,
        float(bar["mean_spread_bps"]),
        1e4 * float(bar["close_microprice"] - bar["close_mid"]) / close,
        float(obi),
        (buy - sell) / volume if volume > 0 else 0.0,  # No trades is a real zero.
        math.log1p(volume),
    ]


def contiguous(bars):
    return all(b["complete"] for b in bars) and all(
        a["end_ms"] == b["start_ms"] and a["session_id"] == b["session_id"]
        and a["symbol"] == b["symbol"] for a, b in zip(bars, bars[1:]))


def window_matrix(bars, spec):
    """`window + 1` bars (oldest first) -> (window, channels) float32, or None."""
    if len(bars) != spec.window + 1 or not contiguous(bars):
        return None
    rows = []
    for prev, bar in zip(bars, bars[1:]):
        vector = bar_vector(prev, bar, spec)
        if vector is None:
            return None
        rows.append(vector)
    matrix = np.asarray(rows, dtype=np.float32)
    return matrix if np.isfinite(matrix).all() else None


def build(groups, spec):
    """groups from v2.bars.series -> (x, y, meta). Deterministic order: symbol, session, time."""
    xs, ys, meta = [], [], []
    for (symbol, session), bars in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)):
        by_start = {b["start_ms"]: b for b in bars}
        for t in range(spec.window, len(bars)):
            bar = bars[t]
            target = by_start.get(bar["start_ms"] + spec.horizon * BAR_MS)
            if target is None or not target["complete"]:
                continue
            x = window_matrix(bars[t - spec.window:t + 1], spec)
            if x is None:
                continue
            y = 1e4 * math.log(float(target["close_mid"]) / float(bar["close_mid"]))
            xs.append(x)
            ys.append(y)
            meta.append(dict(symbol=symbol, session_id=session, asof_ms=bar["end_ms"],
                             input_start_ms=bars[t - spec.window + 1]["start_ms"],
                             target_end_ms=target["end_ms"]))
    x = np.stack(xs) if xs else np.zeros((0, spec.window, len(CHANNELS)), np.float32)
    return x, np.asarray(ys, dtype=np.float32), meta


def time_split(meta, spec, fractions=(0.7, 0.15)):
    """Index arrays (train, val, test) by asof time, with a one-horizon embargo."""
    asof = np.asarray([m["asof_ms"] for m in meta], dtype=np.int64)
    if len(asof) == 0:
        return (np.array([], int),) * 3
    times = np.unique(asof)
    t1 = times[min(len(times) - 1, int(len(times) * fractions[0]))]
    t2 = times[min(len(times) - 1, int(len(times) * (fractions[0] + fractions[1])))]
    embargo = spec.horizon * BAR_MS
    train = np.flatnonzero(asof + embargo <= t1)
    val = np.flatnonzero((asof >= t1) & (asof + embargo <= t2))
    test = np.flatnonzero(asof >= t2)
    return train, val, test


def walk_forward(meta, spec, folds=4):
    """Expanding-window folds [(train_idx, test_idx)], each test block later than its train."""
    asof = np.asarray([m["asof_ms"] for m in meta], dtype=np.int64)
    times = np.unique(asof)
    if len(times) < folds + 1:
        return []
    edges = [times[int(len(times) * k / (folds + 1))] for k in range(1, folds + 1)] + [times[-1] + 1]
    embargo = spec.horizon * BAR_MS
    out = []
    for start, stop in zip(edges, edges[1:]):
        train = np.flatnonzero(asof + embargo <= start)
        test = np.flatnonzero((asof >= start) & (asof + embargo <= stop) if stop != edges[-1]
                              else (asof >= start))
        if len(train) and len(test):
            out.append((train, test))
    return out
