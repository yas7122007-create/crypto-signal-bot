"""Phase 6: deterministic candidate ranking and gating. Pure functions on plain dicts, no
network and no database, so this is directly unit-testable and has nothing to do with
Telegram delivery (that happens after this, downstream, and a Telegram failure cannot
reach back into these results -- see docs/forecasting.md).

A candidate here is one that already passed engine.analyze() and engine.validate_market():
the deterministic engine's own rules (trend alignment, sweep/breakout, volume, taker
imbalance, ATR-based stop, spread, funding, net R/R) are not re-derived or re-weighed here.
This module adds exactly two things on top of that, both optional and additive:

1. Hard gates on V2 evidence, applied only to the evidence actually present on a candidate
   (so a candidate with no "toto"/"gate"/"forecast" key -- i.e. V2_MODE="off" -- is gated
   exactly as before: untouched).
2. A bounded confidence modifier on the existing `rank` score, documented below, that can
   only ever lower a candidate's score, never raise or zero it, so V2 can make the ranking
   more conservative but never invent a reason to prefer a worse setup.

`model_confidence` here is an uncalibrated, 0..1 summary of what the models themselves
report (PatchTST's distance of p_up from 0.5, Toto's and Nemotron's own `confidence`
fields). `historical_hit_rate` is the only number in this module backed by actual paper
outcomes (`journal["positive_fraction"]`, and only once there are `evaluation_samples` of
them). The two are never combined into one number and never presented as the same kind of
thing: a Telegram message or report must label them separately.
"""
from dataclasses import dataclass

MAX_CANDIDATES = 3


def patchtst_confidence(forecast):
    """0..1: how far PatchTST's P(up) sits from a coin flip. Not a probability of being
    correct -- an uncalibrated model-confidence proxy, same as Toto's and Nemotron's own
    `confidence` fields."""
    if not isinstance(forecast, dict) or forecast.get("status") != "ok":
        return None
    return min(1.0, abs(forecast.get("p_up", 0.5) - 0.5) * 2.0)


def model_confidence(candidate):
    """Mean of whichever of PatchTST/Toto/Nemotron confidences are actually available; None
    (never 0) when none are, so callers can tell "no V2 evidence" from "zero confidence"."""
    values = [v for v in (
        patchtst_confidence(candidate.get("forecast")),
        candidate.get("toto", {}).get("confidence") if candidate.get("toto", {}).get("status") == "ok" else None,
        candidate.get("gate", {}).get("confidence") if candidate.get("gate", {}).get("status") == "OK" else None,
    ) if v is not None]
    return sum(values) / len(values) if values else None


def historical_hit_rate(candidate, min_samples):
    """The one empirically calibrated number here: the paper journal's own positive
    fraction, and only once there is enough of it to mean something."""
    journal = candidate.get("journal") or {}
    samples = journal.get("sample_count") or 0
    if samples < min_samples or journal.get("positive_fraction") is None:
        return None
    return journal["positive_fraction"]


@dataclass(frozen=True)
class GateRules:
    """What each piece of V2 evidence is allowed to veto, applied only when that evidence
    is present on the candidate at all (see the module docstring)."""
    reject_stale_forecast: bool = True
    reject_on_toto_reject: bool = True
    require_gate_confirm: bool = True

    def __post_init__(self):
        for field in ("reject_stale_forecast", "reject_on_toto_reject", "require_gate_confirm"):
            if not isinstance(getattr(self, field), bool):
                raise ValueError(f"{field} must be a bool")


def gate(candidate, rules=GateRules()):
    """(passed, reason). The first failing check wins; an absent evidence key never fails
    anything, so a V2_MODE="off" candidate (no forecast/toto/gate keys at all) always
    passes here exactly as engine.analyze/validate_market left it."""
    if candidate.get("action") not in ("LONG", "SHORT"):
        return False, "candidate bukan LONG/SHORT"
    if "market" not in candidate:
        return False, "data market belum divalidasi"
    forecast = candidate.get("forecast")
    if rules.reject_stale_forecast and isinstance(forecast, dict) \
            and forecast.get("status") == "unavailable" and forecast.get("reason") == "stale_input":
        return False, "forecast PatchTST basi (stale_input)"
    toto = candidate.get("toto")
    if rules.reject_on_toto_reject and isinstance(toto, dict) and toto.get("decision") == "REJECT":
        return False, f"Toto REJECT: {toto.get('disagreement_reason', '?')}"
    nemotron_gate = candidate.get("gate")
    if rules.require_gate_confirm and isinstance(nemotron_gate, dict) \
            and nemotron_gate.get("decision") != "CONFIRM":
        return False, f"Nemotron gate bukan CONFIRM: {nemotron_gate.get('status', '?')}"
    return True, None


def score(candidate, min_samples):
    """base * (0.5 + 0.5 * model_confidence). model_confidence in [0, 1], so the modifier is
    in [0.5, 1.0]: V2 evidence can halve a candidate's score at worst, never raise it above
    the deterministic engine's own `rank`, and never zero it out on a mere disagreement (a
    hard disagreement is gate()'s job, not score()'s)."""
    base = candidate["rank"]
    confidence = model_confidence(candidate)
    modifier = 0.5 + 0.5 * confidence if confidence is not None else 1.0
    return base * modifier


def rank_and_select(candidates, rules=GateRules(), min_samples=20, max_candidates=MAX_CANDIDATES):
    """Gates every candidate, scores the survivors, and returns at most `max_candidates`,
    highest score first (ties broken by historical_hit_rate, then by symbol for a
    deterministic order). Each returned candidate carries its own `v2_score`,
    `v2_model_confidence` and `v2_historical_hit_rate` -- the engine's own fields
    (action/entry/stop/target/...) are passed through unchanged."""
    if not isinstance(max_candidates, int) or max_candidates < 1:
        raise ValueError("max_candidates must be a positive int")
    survivors = []
    for candidate in candidates:
        passed, reason = gate(candidate, rules)
        if not passed:
            continue
        hit_rate = historical_hit_rate(candidate, min_samples)
        survivors.append(dict(candidate, v2_score=score(candidate, min_samples),
                              v2_model_confidence=model_confidence(candidate),
                              v2_historical_hit_rate=hit_rate))
    survivors.sort(key=lambda c: (c["v2_score"], c["v2_historical_hit_rate"] or 0.0, c["symbol"]),
                   reverse=True)
    return survivors[:max_candidates]
