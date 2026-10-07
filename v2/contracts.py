"""Machine-readable contracts between V2 stages (stdlib only).

Every stage output is a small JSON object with a `contract` name and version. Validators
raise ContractError on anything outside the documented bounds, and callers turn that into
HOLD: an invalid or missing model output never becomes a trade signal.
"""
import math
import re

FORECAST = "forecast.v1"
TARGET = "mid_log_return_bps"
MAX_BPS = 5_000.0          # |expected return| and sigma bound: 50 % in one horizon is garbage.
VERSION = re.compile(r"[0-9a-f]{8,64}|[A-Za-z0-9._:/-]{1,80}")
SYMBOL = re.compile(r"[A-Z0-9]{2,30}")
REASON = re.compile(r"[a-z0-9_]{1,64}")


class ContractError(ValueError):
    pass


TOTO = "toto.v1"
DECISIONS = ("CONFIRM", "REJECT", "HOLD")


def finite(value, field, lo=-math.inf, hi=math.inf):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ContractError(f"{field}: expected a finite number")
    if not lo <= value <= hi:
        raise ContractError(f"{field}: {value} outside [{lo}, {hi}]")
    return float(value)


def integer(value, field, lo=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < lo:
        raise ContractError(f"{field}: expected an integer >= {lo}")
    return value


def text(value, field, pattern):
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ContractError(f"{field}: invalid")
    return value


def exact_keys(obj, required, optional=()):
    if not isinstance(obj, dict):
        raise ContractError("expected an object")
    missing = set(required) - obj.keys()
    extra = obj.keys() - set(required) - set(optional)
    if missing or extra:
        raise ContractError(f"fields: missing {sorted(missing)}, unexpected {sorted(extra)}")


TOTO_KEYS = ("contract", "status", "reason", "symbol", "asof_ms", "model", "model_version",
            "decision", "p_up", "confidence", "disagreement_reason")


def toto(symbol, asof_ms, model, model_version, decision=None, p_up=None, confidence=None,
         disagreement_reason=None, reason=None):
    """Builds a toto.v1 object. A disagreement_reason may accompany CONFIRM or REJECT alike
    (it always says why Toto's own read differs from the input, not whether it gated); it is
    required on REJECT and optional elsewhere."""
    ok = reason is None
    if ok:
        if decision not in DECISIONS:
            raise ContractError("decision: expected CONFIRM, REJECT, or HOLD")
        p_up, confidence = (finite(v, f, 0.0, 1.0) for v, f in ((p_up, "p_up"), (confidence, "confidence")))
        if decision == "REJECT" and not disagreement_reason:
            raise ContractError("disagreement_reason required on REJECT")
    out = dict(contract=TOTO, status="ok" if ok else "unavailable", reason=reason, symbol=symbol,
               asof_ms=asof_ms, model=model, model_version=model_version,
               decision=decision if ok else None, p_up=round(p_up, 6) if ok else None,
               confidence=round(confidence, 6) if ok else None,
               disagreement_reason=disagreement_reason if ok else None)
    return validate_toto(out)


def validate_toto(obj):
    exact_keys(obj, TOTO_KEYS)
    if obj["contract"] != TOTO:
        raise ContractError("not a toto.v1 object")
    text(obj["symbol"], "symbol", SYMBOL)
    integer(obj["asof_ms"], "asof_ms", 1)
    text(obj["model"], "model", REASON)
    text(obj["model_version"], "model_version", VERSION)
    if obj["status"] == "ok":
        if obj["reason"] is not None:
            raise ContractError("reason must be null when ok")
        if obj["decision"] not in DECISIONS:
            raise ContractError("decision: expected CONFIRM, REJECT, or HOLD")
        finite(obj["p_up"], "p_up", 0.0, 1.0)
        finite(obj["confidence"], "confidence", 0.0, 1.0)
        if obj["decision"] == "REJECT" and not obj["disagreement_reason"]:
            raise ContractError("disagreement_reason required on REJECT")
        if obj["disagreement_reason"] is not None:
            text(obj["disagreement_reason"], "disagreement_reason", REASON)
    elif obj["status"] == "unavailable":
        text(obj["reason"], "reason", REASON)
        if any(obj[k] is not None for k in ("decision", "p_up", "confidence", "disagreement_reason")):
            raise ContractError("an unavailable result carries no decision fields")
    else:
        raise ContractError("status: expected ok or unavailable")
    return obj


FORECAST_KEYS = ("contract", "status", "reason", "symbol", "asof_ms", "horizon_ms", "target",
                 "model", "model_version", "input", "expected_return_bps", "sigma_bps", "p_up")
INPUT_KEYS = ("dataset_schema", "bar_schema", "feature_schema", "window", "channels")


def forecast(symbol, asof_ms, horizon_ms, model, model_version, input_info, mu=None, sigma=None,
             p_up=None, reason=None):
    """Builds a forecast.v1 object; status follows from whether a value is present."""
    ok = reason is None
    if ok:  # Check the raw values: rounding would turn True into 1.0.
        mu, sigma, p_up = (finite(v, f) for v, f in ((mu, "mu"), (sigma, "sigma"), (p_up, "p_up")))
    out = dict(contract=FORECAST, status="ok" if ok else "unavailable", reason=reason,
               symbol=symbol, asof_ms=asof_ms, horizon_ms=horizon_ms, target=TARGET, model=model,
               model_version=model_version, input=input_info,
               expected_return_bps=round(float(mu), 4) if ok else None,
               sigma_bps=round(float(sigma), 4) if ok else None,
               p_up=round(float(p_up), 6) if ok else None)
    return validate_forecast(out)


def validate_forecast(obj):
    exact_keys(obj, FORECAST_KEYS)
    if obj["contract"] != FORECAST or obj["target"] != TARGET:
        raise ContractError("not a forecast.v1 object")
    text(obj["symbol"], "symbol", SYMBOL)
    integer(obj["asof_ms"], "asof_ms", 1)
    integer(obj["horizon_ms"], "horizon_ms", 1)
    text(obj["model"], "model", REASON)
    text(obj["model_version"], "model_version", VERSION)
    exact_keys(obj["input"], INPUT_KEYS)
    for key in ("dataset_schema", "bar_schema", "feature_schema", "window"):
        integer(obj["input"][key], f"input.{key}", 1)
    if not isinstance(obj["input"]["channels"], list):
        raise ContractError("input.channels: expected list")
    values = (obj["expected_return_bps"], obj["sigma_bps"], obj["p_up"])
    if obj["status"] == "ok":
        if obj["reason"] is not None:
            raise ContractError("reason must be null when ok")
        finite(values[0], "expected_return_bps", -MAX_BPS, MAX_BPS)
        finite(values[1], "sigma_bps", 1e-6, MAX_BPS)
        finite(values[2], "p_up", 0.0, 1.0)
    elif obj["status"] == "unavailable":
        text(obj["reason"], "reason", REASON)
        if any(v is not None for v in values):
            raise ContractError("an unavailable forecast carries no values")
    else:
        raise ContractError("status: expected ok or unavailable")
    return obj
