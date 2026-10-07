"""Read and validate one-minute bars written by market-data (docs/bar-schema.md)."""
import gzip
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re

BAR_SCHEMA = 1
FEATURE_SCHEMA = 2
BAR_MS = 60_000
SYMBOL = re.compile(r"[A-Z0-9]{1,30}")
DECIMALS = ("open_mid", "high_mid", "low_mid", "close_mid", "close_microprice",
            "mean_spread_bps", "close_spread_bps")


class BarError(ValueError):
    pass


def number(value, field, nullable=False):
    """Exact decimal text from Rust -> Decimal; floats and garbage are rejected."""
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise BarError(f"{field}: expected decimal string")
    try:
        d = Decimal(value)
    except InvalidOperation:
        raise BarError(f"{field}: not a decimal") from None
    if not d.is_finite():
        raise BarError(f"{field}: not finite")
    return d


def integer(value, field, nullable=False):
    if value is None and nullable:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise BarError(f"{field}: expected integer")
    return value


def validate(raw):
    """One bar dict -> validated dict with Decimal prices. Raises BarError."""
    if not isinstance(raw, dict):
        raise BarError("bar must be an object")
    if raw.get("v") != BAR_SCHEMA or raw.get("feature_v") != FEATURE_SCHEMA:
        raise BarError(f"unsupported bar/feature schema {raw.get('v')}/{raw.get('feature_v')}")
    symbol = raw.get("symbol")
    if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
        raise BarError("bad symbol")
    start = integer(raw.get("start_ms"), "start_ms")
    end = integer(raw.get("end_ms"), "end_ms")
    if end - start != BAR_MS or start % BAR_MS:
        raise BarError("bar window is not one aligned minute")
    if not isinstance(raw.get("complete"), bool):
        raise BarError("complete: expected bool")
    bar = dict(symbol=symbol, session_id=integer(raw.get("session_id"), "session_id", True),
               start_ms=start, end_ms=end, complete=raw["complete"],
               rows=integer(raw.get("rows"), "rows"),
               incomplete_reason=raw.get("incomplete_reason"),
               trade_state=raw.get("close_trade_state"))
    for field in DECIMALS:
        bar[field] = number(raw.get(field), field)
    if not bar["low_mid"] <= bar["close_mid"] <= bar["high_mid"] or bar["low_mid"] <= 0:
        raise BarError("mid path inconsistent")
    obi = raw.get("close_obi")
    if not isinstance(obi, list):
        raise BarError("close_obi: expected list")
    if not all(isinstance(o, dict) for o in obi):
        raise BarError("close_obi: expected objects")
    bar["obi"] = {integer(o.get("levels"), "obi.levels"): number(o.get("value"), "obi.value", True)
                  for o in obi}
    if bar["rows"] < 1:
        raise BarError("rows: expected at least one row")
    if raw.get("complete") and raw.get("incomplete_reason") is not None:
        raise BarError("complete bar with an incomplete_reason")
    bar["buy_qty"] = number(raw.get("buy_qty"), "buy_qty", True)
    bar["sell_qty"] = number(raw.get("sell_qty"), "sell_qty", True)
    if (bar["buy_qty"] is None) != (bar["sell_qty"] is None):
        raise BarError("buy_qty and sell_qty must be both null or both set")
    if bar["buy_qty"] is not None and (bar["buy_qty"] < 0 or bar["sell_qty"] < 0):
        raise BarError("negative flow")
    return bar


def read_dir(directory):
    """All complete-file bars in a --bars-dir, validated. `.partial` files are skipped."""
    bars = []
    for path in sorted(Path(directory).glob("bars-*.ndjson.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for n, line in enumerate(handle, 1):
                try:
                    bars.append(validate(json.loads(line)))
                except (BarError, json.JSONDecodeError) as exc:
                    raise BarError(f"{path.name}:{n}: {exc}") from None
    return bars


def series(bars):
    """Group by (symbol, session) and sort by time; duplicates are an error."""
    groups = {}
    for bar in bars:
        groups.setdefault((bar["symbol"], bar["session_id"]), []).append(bar)
    for key, items in groups.items():
        items.sort(key=lambda b: b["start_ms"])
        for a, b in zip(items, items[1:]):
            if a["start_ms"] == b["start_ms"]:
                raise BarError(f"duplicate bar {key} {a['start_ms']}")
    return groups
