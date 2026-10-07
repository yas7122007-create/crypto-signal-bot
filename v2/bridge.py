"""Python integration boundary (Phase 4): turns a market-data bar-state file into a
forecast.v1 object, or a documented reason it cannot. Stdlib plus v2.bars/v2.contracts only;
PatchTST (v2.patchtst, torch) is imported lazily, so this module works even where torch is
not installed -- it then always reports status "unavailable".

Fail-closed by construction: every check below returns early with a reason instead of
raising, and `forecast()` never returns a value for `expected_return_bps` unless every check
passed. A caller that gets "unavailable" has exactly one correct response: treat it as no
forecast, and let the existing deterministic engine continue to HOLD or trade without it.
Nothing here calls Binance, places orders, or writes files.
"""
from dataclasses import dataclass
import json
import logging
import math
from pathlib import Path
import time

from v2 import contracts as C
from v2.bars import BAR_MS, BarError, validate
from v2.dataset import DatasetSpec, window_matrix

LOG = logging.getLogger("signalbot")  # Same logger name as services.py; no import of it,
# so v2/ stays usable without the bot's own dependencies (httpx, pymysql, ...).

DEFAULT_MAX_STALE_MS = 180_000  # 3 bars: a live state file older than this is not "now".
# A state file timestamped after "now" cannot be real: either the state writer's clock is
# wrong, or the file is fabricated/replayed. A small tolerance absorbs ordinary clock skew
# between the Rust recorder's host and this process without opening a window for either.
MAX_FUTURE_SKEW_MS = 5_000
_MODEL_CACHE = {}


@dataclass(frozen=True)
class BridgeConfig:
    model_dir: str
    max_stale_ms: int = DEFAULT_MAX_STALE_MS

    def __post_init__(self):
        if self.max_stale_ms <= 0:
            raise ValueError("max_stale_ms must be positive")


def _load_model(model_dir):
    """Cached by resolved path once a load succeeds (a process then reuses those weights
    for its lifetime). A failed load is never cached: model files mid-write, or a transient
    OSError, must not pin "unavailable" for the rest of the process once they become valid
    -- only a successful Forecaster.load is treated as a fact that cannot change."""
    key = str(Path(model_dir).resolve())
    if key in _MODEL_CACHE:
        return _MODEL_CACHE[key]
    try:
        from v2.patchtst import Forecaster
    except ImportError:
        return None  # Not cached either: a future call in the same process still checks.
    try:
        model = Forecaster.load(model_dir)
    except Exception as exc:
        # Trust-boundary safety net (Hardening 7): a corrupt or mid-write model artifact
        # must become "unavailable", not an exception that reaches the live caller. This is
        # deliberately broad -- torch.load on bad bytes can raise pickle.UnpicklingError,
        # RuntimeError, EOFError, zipfile.BadZipFile, or others that are not enumerable in
        # advance -- but it only wraps model deserialization, not the rest of the bridge:
        # every other function below still raises its own specific, catchable errors.
        LOG.warning("v2 patchtst model load failed (%s): %s", type(exc).__name__, model_dir)
        return None
    _MODEL_CACHE[key] = model
    return model


def _unavailable(symbol, asof_ms, horizon_ms, spec, reason):
    window = spec.window if spec else 1  # placeholder: the contract requires window >= 1
    return C.forecast(symbol=symbol, asof_ms=max(asof_ms, 1), horizon_ms=horizon_ms,
                      model="patchtst", model_version="unavailable",
                      input_info=dict(dataset_schema=1, bar_schema=1, feature_schema=2,
                                       window=window, channels=[]),
                      reason=reason)


def read_state(path):
    """One bar_window state file (docs/bar-schema.md): -> (symbol, [validated bars]).
    Raises BridgeError; callers turn that into an "unavailable" forecast."""
    try:
        raw = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BridgeError(f"cannot read state file: {exc}") from None
    if raw.get("v") != 1 or raw.get("kind") != "bar_window":
        raise BridgeError("unsupported state file schema")
    symbol, bars = raw.get("symbol"), raw.get("bars")
    if not isinstance(symbol, str) or not isinstance(bars, list):
        raise BridgeError("malformed state file")
    try:
        validated = [validate(b) for b in bars]
    except BarError as exc:
        raise BridgeError(f"invalid bar: {exc}") from None
    # Every bar must actually be this symbol's own data: checking only adjacent bars against
    # each other (below) would accept a state file whose *every* bar is uniformly mislabeled
    # -- e.g. a top-level "symbol": "BTCUSDT" over bars that are all really ETHUSDT -- since
    # such bars are internally consistent with each other. The top-level field is the one
    # being claimed; each bar's own field is the one actually used to compute anything, so
    # both must agree, for every bar, not just checked once.
    if any(b["symbol"] != symbol for b in validated):
        raise BridgeError("a bar's symbol does not match the state file's own symbol")
    for a, b in zip(validated, validated[1:]):
        if b["start_ms"] - a["start_ms"] != BAR_MS or a["session_id"] != b["session_id"]:
            raise BridgeError("state file bars are not one contiguous session")
    return symbol, validated


class BridgeError(ValueError):
    pass


def forecast(state_path, symbol, horizon_ms, config, now_ms=None):
    """The Phase 4 entry point. `symbol` and `horizon_ms` are what the caller (the engine)
    is asking about; everything is checked against them, never silently substituted."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    model = _load_model(config.model_dir)
    spec = model.spec if model else None
    if model is None:
        return _unavailable(symbol, now_ms, horizon_ms, spec, "model_unavailable")
    if horizon_ms != model.spec.horizon * BAR_MS:
        return _unavailable(symbol, now_ms, horizon_ms, spec, "horizon_mismatch")
    try:
        state_symbol, bars = read_state(state_path)
    except BridgeError:
        return _unavailable(symbol, now_ms, horizon_ms, spec, "state_unreadable")
    if state_symbol != symbol or not bars:
        return _unavailable(symbol, now_ms, horizon_ms, spec, "symbol_mismatch")
    asof_ms = bars[-1]["end_ms"]
    if asof_ms - now_ms > MAX_FUTURE_SKEW_MS:
        return _unavailable(symbol, asof_ms, horizon_ms, spec, "future_input")
    if now_ms - asof_ms > config.max_stale_ms:
        return _unavailable(symbol, asof_ms, horizon_ms, spec, "stale_input")
    if len(bars) < spec.window + 1:
        return _unavailable(symbol, asof_ms, horizon_ms, spec, "insufficient_history")
    x = window_matrix(bars[-(spec.window + 1):], spec)
    if x is None:
        return _unavailable(symbol, asof_ms, horizon_ms, spec, "incomplete_window")
    import numpy as np
    mu, sigma = model.predict(np.expand_dims(x, 0))
    mu, sigma = float(mu[0]), float(sigma[0])
    if not (math.isfinite(mu) and math.isfinite(sigma) and sigma > 0):
        return _unavailable(symbol, asof_ms, horizon_ms, spec, "model_output_invalid")
    p_up = 0.5 * (1.0 + math.erf((mu / sigma) / math.sqrt(2.0)))
    input_info = dict(dataset_schema=1, bar_schema=1, feature_schema=2, window=spec.window,
                      channels=list(spec.as_dict()["channels"]))
    try:
        return C.forecast(symbol=symbol, asof_ms=asof_ms, horizon_ms=horizon_ms,
                          model="patchtst", model_version=model.version, input_info=input_info,
                          mu=mu, sigma=sigma, p_up=p_up)
    except C.ContractError:
        return _unavailable(symbol, asof_ms, horizon_ms, spec, "model_output_invalid")
