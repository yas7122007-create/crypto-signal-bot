"""Toto validator (Phase 5): a second, independent read on the same window, used to confirm
or reject a PatchTST forecast before it can reach Nemotron or ranking. Not a third source of
authority: it validates, and a deterministic risk gate upstream and downstream of it is
never bypassed by anything in this module.

The real `toto-ts` package needs HuggingFace to fetch its weights, which this container
cannot reach (network.environment), and pins a torch version that would conflict with the
one PatchTST uses here. So there is no real Toto model wired up in this environment: `Adapter`
is the interface a real one would implement (in-process, or over `v2.workers.run` if its
dependencies must stay isolated), and `FakeAdapter` is a clearly-marked stand-in used only to
test that `validate()`'s fail-closed plumbing -- timeout, invalid output, an adapter that
raises, model unavailable -- actually produces HOLD/unavailable rather than a value, and
never CONFIRM on failure. No result from `FakeAdapter` may be treated as a real validation.
"""
import math

from v2 import contracts as C
from v2.workers import WorkerError, run as run_worker

MODEL_NAME = "toto"


class Adapter:
    """validate(bars, forecast) -> dict with decision, p_up, confidence, and, on REJECT (or
    optionally CONFIRM), disagreement_reason -- the raw fields `contracts.toto()` wraps. Must
    not raise for a disagreement; raise only for a genuine failure (`validate()` catches it).

    Optional `model_version` key (Hardening 8, adversarial-audit corrective pass): a real
    adapter SHOULD include its own model identity here -- a weight hash, manifest digest, or
    revision string that changes whenever the underlying model artifact does -- and
    `validate()` below uses it in place of whatever static version its caller supplied.
    Without one, `validate()` falls back to the caller's version (e.g. a hash of the launch
    command), which identifies the *invocation*, not the *model*: a changed checkpoint behind
    an unchanged command would silently keep the old version. `FakeAdapter` deliberately never
    sets this, so it can never be mistaken for a real model's provenance."""

    def validate(self, bars, forecast):
        raise NotImplementedError


class FakeAdapter(Adapter):
    """FAKE: a deterministic stand-in for tests only, not a real Toto model. It "agrees" with
    PatchTST's direction when the close OBI points the same way with the configured margin,
    otherwise REJECTs; this is a toy rule to exercise the contract and the fail-closed
    wrapper, not a validation anyone should trust."""

    def __init__(self, obi_levels=10, margin=0.05, confidence=0.6, fail=None):
        self.obi_levels, self.margin, self.confidence = obi_levels, margin, confidence
        self.fail = fail  # Exception instance or class, to simulate a failing adapter.

    def validate(self, bars, forecast):
        if self.fail is not None:
            raise self.fail
        obi = float(bars[-1]["obi"].get(self.obi_levels) or 0.0)
        p_up = 0.5 * (1.0 + math.copysign(min(abs(obi), 1.0), obi)) if obi else 0.5
        agree = (obi > self.margin and forecast["p_up"] > 0.5) \
            or (obi < -self.margin and forecast["p_up"] < 0.5)
        if agree:
            return dict(decision="CONFIRM", p_up=p_up, confidence=self.confidence)
        return dict(decision="REJECT", p_up=p_up, confidence=self.confidence,
                    disagreement_reason="obi_direction_disagrees")


class WorkerAdapter(Adapter):
    """Runs a real Toto model out of process (see the module docstring for why). `argv` is
    fixed and must not come from untrusted input; the worker is sent `{bars, forecast}` and
    must answer with the same fields `Adapter.validate` returns."""

    def __init__(self, argv, timeout_s=20):
        self.argv, self.timeout_s = argv, timeout_s

    def validate(self, bars, forecast):
        return run_worker(self.argv, dict(bars=bars, forecast=forecast), self.timeout_s)


def validate(symbol, asof_ms, bars, forecast, adapter, model_version="unavailable"):
    """The Phase 5 entry point: never raises, always returns a toto.v1 object. `forecast`
    must be a forecast.v1 object with status "ok" (an unavailable forecast has nothing for
    Toto to validate, so Toto itself is reported unavailable, not run)."""
    if adapter is None:
        return C.toto(symbol, asof_ms, MODEL_NAME, model_version, reason="model_unavailable")
    if forecast.get("status") != "ok":
        return C.toto(symbol, asof_ms, MODEL_NAME, model_version, reason="no_forecast_to_validate")
    try:
        result = adapter.validate(bars, forecast)
    except WorkerError as exc:
        reason = "timeout" if "timed out" in str(exc) else "worker_failed"
        return C.toto(symbol, asof_ms, MODEL_NAME, model_version, reason=reason)
    except Exception:  # Any adapter failure is unavailable, never a crash or a fabricated CONFIRM.
        return C.toto(symbol, asof_ms, MODEL_NAME, model_version, reason="adapter_failed")
    if not isinstance(result, dict) or result.get("decision") not in C.DECISIONS:
        return C.toto(symbol, asof_ms, MODEL_NAME, model_version, reason="invalid_output")
    # Hardening 8: prefer the adapter's own reported model identity over the caller's
    # static fallback, so a changed model artifact behind the same launch command is
    # reported as a different version. Only a non-empty string is trusted; anything else
    # (missing, not a string, blank) keeps the caller's version unchanged.
    reported = result.get("model_version")
    version = reported if isinstance(reported, str) and reported.strip() else model_version
    try:
        return C.toto(symbol, asof_ms, MODEL_NAME, version, decision=result["decision"],
                      p_up=result.get("p_up"), confidence=result.get("confidence"),
                      disagreement_reason=result.get("disagreement_reason"))
    except C.ContractError:
        return C.toto(symbol, asof_ms, MODEL_NAME, model_version, reason="invalid_output")
