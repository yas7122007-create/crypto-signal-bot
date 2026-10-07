"""Phase 7: the full V2 decision path as an append-only NDJSON journal, plus the metrics
that answer the brief's own questions (did PatchTST help, did Toto help, did Nemotron
reject good or bad candidates, what happens on disagreement). Decoupled from the live
bot's MySQL-backed signals/outcomes tables on purpose: this runs offline, in CI, and on
synthetic or paper fixtures alike, with no database and no network.

Two record kinds, one file each, joined by `id` (engine.signal_id, or any stable string for
a HOLD that never became a signal):

- decision.v1: one row per candidate the engine reached a final action on (issued or HOLD),
  with whatever V2 evidence was attached (forecast/toto/gate), V2_MODE, and the reason a
  candidate was held, if any.
- outcome.v1: one row per signal that reached a terminal paper outcome
  (engine.settle()'s output: net_r, outcome, exit_ms, ...).

Nothing here computes P&L or touches a real order; outcomes come from the existing
engine.paper_update/settle simulation, recorded, not re-derived.
"""
from dataclasses import dataclass
import json
from pathlib import Path

from engine import signal_id
from v2.metrics import calibration, evaluate as forecast_metrics

DECISION = "decision.v1"
OUTCOME = "outcome.v1"
DIRECTIONS = ("LONG", "SHORT")
HOLD_ID_PREFIX = "hold:"  # engine.signal_id() returns a 24-char sha256 hex digest, which
# can never start with this, so a real signal's id and a no-candidate HOLD's id can never
# collide even if compared without checking which kind they are.


class JournalError(ValueError):
    pass


def _append(path, record):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")


def decision_id(candidate):
    """The same id `engine.new_signal()` would assign this exact candidate if it becomes a
    real paper signal (Fix 4, adversarial-audit corrective pass): `engine.signal_id()` is a
    pure function of the candidate's own `version`/`symbol`/`candle_ms`/`action`, computed
    before any gate runs, so using it here -- rather than inventing a second ID scheme --
    guarantees `record_decision()` and `engine.new_signal()` agree on a signal's id whenever
    both see the same candidate, including when V2 held a candidate the engine proposed
    (final_action="HOLD" but a real signal_id-eligible candidate underneath). Never joins by
    symbol alone: two different candles or sides on the same symbol get different ids.
    A candidate with nothing actionable proposed (no `action`/`candle_ms`/`version` at all)
    has no real signal to join against; it gets its own documented, clearly-prefixed id."""
    try:
        return signal_id(candidate)
    except KeyError:
        return f"{HOLD_ID_PREFIX}{candidate.get('symbol')}:{candidate.get('candle_ms')}"


def record_decision(path, candidate, final_action, rejection_reason=None, v2_mode="off"):
    """`final_action` is what was actually done (LONG/SHORT/HOLD) after every gate; a
    candidate the engine itself proposed but V2 held carries both `proposed_action` and
    `rejection_reason`, so "did the gate reject a good setup" is answerable later."""
    if final_action not in DIRECTIONS + ("HOLD",):
        raise JournalError("final_action must be LONG, SHORT, or HOLD")
    record = dict(
        record=DECISION, id=decision_id(candidate),
        symbol=candidate["symbol"], candle_ms=candidate.get("candle_ms"),
        proposed_action=candidate.get("action"), final_action=final_action,
        rejection_reason=rejection_reason, v2_mode=v2_mode,
        forecast=candidate.get("forecast"), toto=candidate.get("toto"), gate=candidate.get("gate"))
    _append(path, record)
    return record


def record_outcome(path, signal):
    if signal.get("status") != "CLOSED":
        raise JournalError("only a CLOSED signal has a paper outcome")
    record = dict(record=OUTCOME, id=signal["id"], symbol=signal["symbol"],
                 action=signal["action"], outcome=signal["outcome"], net_r=signal["net_r"],
                 net_return=signal["net_return"], candle_ms=signal["candle_ms"],
                 fill_ms=signal.get("fill_ms"), exit_ms=signal["exit_ms"])
    _append(path, record)
    return record


def load(path):
    """-> (decisions, outcomes). Raises JournalError naming the line on anything malformed:
    an evaluation report is only as trustworthy as the journal it reads, so this does not
    silently skip bad rows."""
    decisions, outcomes = [], []
    for n, line in enumerate(Path(path).read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise JournalError(f"{path}:{n}: {exc}") from None
        if not isinstance(row, dict) or "record" not in row:
            raise JournalError(f"{path}:{n}: missing 'record' kind")
        if row["record"] == DECISION:
            decisions.append(row)
        elif row["record"] == OUTCOME:
            outcomes.append(row)
        else:
            raise JournalError(f"{path}:{n}: unknown record kind {row['record']!r}")
    return decisions, outcomes


def _rate(numerator, denominator):
    return numerator / denominator if denominator else None


def counts_by_action(decisions):
    counts = {"LONG": 0, "SHORT": 0, "HOLD": 0}
    for d in decisions:
        counts[d["final_action"]] += 1
    return counts


def precision_by_direction(outcomes):
    """Fraction of closed trades with net_r > 0, per direction and overall."""
    result = {}
    for side in DIRECTIONS + (None,):
        rows = [o for o in outcomes if side is None or o["action"] == side]
        result[side or "overall"] = _rate(sum(o["net_r"] > 0 for o in rows), len(rows))
    return result


def average_net_r(outcomes):
    return _rate(sum(o["net_r"] for o in outcomes), len(outcomes))


def expectancy(outcomes):
    """Classic win_rate*avg_win - loss_rate*avg_loss, computed, not assumed. Over the same
    population this is algebraically identical to average_net_r (every trade is counted on
    one side or the other); the two are reported separately only because the brief asks for
    an explicit expectancy figure, not because they can differ here."""
    if not outcomes:
        return None
    wins = [o["net_r"] for o in outcomes if o["net_r"] > 0]
    losses = [o["net_r"] for o in outcomes if o["net_r"] <= 0]
    win_rate = len(wins) / len(outcomes)
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    return win_rate * avg_win + (1 - win_rate) * avg_loss


def max_drawdown(outcomes):
    """Largest peak-to-trough drop in cumulative net_r, in outcome order (exit_ms); 0.0 for
    an empty or monotonically non-decreasing series, never None (a drawdown of "none" would
    be misread as "unknown" rather than "never drew down")."""
    equity, peak, worst = 0.0, 0.0, 0.0
    for o in sorted(outcomes, key=lambda o: o["exit_ms"]):
        equity += o["net_r"]
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def rejection_reasons(decisions):
    """Counts of `rejection_reason` among held candidates; a proposed LONG/SHORT that V2
    turned into HOLD, grouped by why."""
    counts = {}
    for d in decisions:
        if d["final_action"] == "HOLD" and d.get("proposed_action") in DIRECTIONS and d.get("rejection_reason"):
            counts[d["rejection_reason"]] = counts.get(d["rejection_reason"], 0) + 1
    return counts


def provider_failures(decisions):
    """Counts of Toto/Nemotron being unavailable rather than reaching a real decision --
    distinct from a real REJECT/HOLD, which is a model opinion, not a failure."""
    toto_failed = sum(1 for d in decisions if isinstance(d.get("toto"), dict)
                      and d["toto"].get("status") == "unavailable")
    gate_failed = sum(1 for d in decisions if isinstance(d.get("gate"), dict)
                      and d["gate"].get("status") in ("DISABLED", "DEGRADED", "ERROR"))
    return {"toto": toto_failed, "gate": gate_failed}


def stale_rejections(decisions):
    return sum(1 for d in decisions if isinstance(d.get("forecast"), dict)
              and d["forecast"].get("status") == "unavailable"
              and d["forecast"].get("reason") == "stale_input")


def forecast_quality(decisions, outcomes_by_id):
    """MAE/RMSE/directional accuracy/NLL/Brier/calibration of PatchTST's forecast against
    the realized net_return (in bps), for decisions that both carry an `ok` forecast and
    later closed. Reuses v2.metrics, the same scoring Phase 3 uses offline, so a live
    forecast is judged the identical way its training-time baselines were."""
    y, mu, sigma = [], [], []
    for d in decisions:
        forecast, outcome = d.get("forecast"), outcomes_by_id.get(d["id"])
        if isinstance(forecast, dict) and forecast.get("status") == "ok" and outcome:
            y.append(outcome["net_return"] * 1e4)  # fraction -> bps, matching the contract's unit.
            mu.append(forecast["expected_return_bps"])
            sigma.append(forecast["sigma_bps"])
    if not y:
        return dict(n=0)
    return forecast_metrics(y, mu, sigma)


@dataclass(frozen=True)
class Report:
    counts: dict
    precision_by_direction: dict
    average_net_r: float | None
    positive_fraction: float | None
    expectancy: float | None
    max_drawdown: float
    rejection_reasons: dict
    provider_failures: dict
    stale_rejections: int
    forecast_quality: dict
    n_decisions: int
    n_outcomes: int

    def as_dict(self):
        return dict(report="v2.evaluate.v1", counts=self.counts,
                   precision_by_direction=self.precision_by_direction,
                   average_net_r=self.average_net_r, positive_fraction=self.positive_fraction,
                   expectancy=self.expectancy, max_drawdown=self.max_drawdown,
                   rejection_reasons=self.rejection_reasons,
                   provider_failures=self.provider_failures,
                   stale_rejections=self.stale_rejections,
                   forecast_quality=self.forecast_quality,
                   n_decisions=self.n_decisions, n_outcomes=self.n_outcomes)


def build_report(decisions, outcomes):
    outcomes_by_id = {o["id"]: o for o in outcomes}
    return Report(
        counts=counts_by_action(decisions),
        precision_by_direction=precision_by_direction(outcomes),
        average_net_r=average_net_r(outcomes),
        positive_fraction=_rate(sum(o["net_r"] > 0 for o in outcomes), len(outcomes)),
        expectancy=expectancy(outcomes),
        max_drawdown=max_drawdown(outcomes),
        rejection_reasons=rejection_reasons(decisions),
        provider_failures=provider_failures(decisions),
        stale_rejections=stale_rejections(decisions),
        forecast_quality=forecast_quality(decisions, outcomes_by_id),
        n_decisions=len(decisions), n_outcomes=len(outcomes))


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="V2 paper-evaluation report (no network, no orders).")
    ap.add_argument("--decisions", required=True, help="decision.v1 NDJSON journal")
    ap.add_argument("--outcomes", required=True, help="outcome.v1 NDJSON journal")
    ap.add_argument("--out", help="write the report as JSON here (default: stdout)")
    args = ap.parse_args(argv)
    decisions = load(args.decisions)[0] if Path(args.decisions).exists() else []
    outcomes = load(args.outcomes)[1] if Path(args.outcomes).exists() else []
    report = build_report(decisions, outcomes).as_dict()
    text = json.dumps(report, indent=1, sort_keys=True)
    if args.out:
        Path(args.out).write_text(text)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
