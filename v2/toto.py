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
import re

from v2 import contracts as C
from v2.workers import WorkerError, run as run_worker

MODEL_NAME = "toto"
UNAVAILABLE_VERSION = "unavailable"
# An immutable model identity, reported by the adapter itself: the SHA-256 of the weights or
# of an immutable manifest, or a full 40-hex upstream revision (e.g. a HuggingFace commit).
# Anything else -- mutable labels like "latest", free-form names, a launch-command hash, the
# fake identity below -- is rejected. Bounded and ASCII-only by construction.
MODEL_IDENTITY = re.compile(r"sha256:[0-9a-f]{64}|revision:[0-9a-f]{40}")
FAKE_MODEL_VERSION = "test:fake-adapter-v1"  # Never matches MODEL_IDENTITY.


class Adapter:
    """validate(bars, forecast) -> dict with decision, p_up, confidence, and, on REJECT (or
    optionally CONFIRM), disagreement_reason -- the raw fields `contracts.toto()` wraps. Must
    not raise for a disagreement; raise only for a genuine failure (`validate()` catches it).

    Required `model_version` key for every real adapter: an immutable identity of the model
    artifact that produced this result, matching `MODEL_IDENTITY`. `validate()` treats a
    real adapter's result without one as unavailable (`model_version_missing` /
    `invalid_model_version`); there is no fallback, because a launch command or config can
    stay the same while the checkpoint behind it changes. Test-only adapters set
    `TEST_ONLY = True` instead and are always labelled `FAKE_MODEL_VERSION`.
    """

    def validate(self, bars, forecast):
        raise NotImplementedError


class FakeAdapter(Adapter):
    """FAKE: a deterministic stand-in for tests only, not a real Toto model. It "agrees" with
    PatchTST's direction when the close OBI points the same way with the configured margin,
    otherwise REJECTs; this is a toy rule to exercise the contract and the fail-closed
    wrapper, not a validation anyone should trust. Its results are always labelled
    `FAKE_MODEL_VERSION`, whatever it returns, so it can never pass for a real checkpoint."""

    TEST_ONLY = True

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


def _identity(adapter, result):
    """-> (model_version, None) or (None, reason). See `Adapter` for the policy."""
    if getattr(adapter, "TEST_ONLY", False):
        return FAKE_MODEL_VERSION, None
    reported = result.get("model_version")
    if reported is None:
        return None, "model_version_missing"
    if not isinstance(reported, str) or not MODEL_IDENTITY.fullmatch(reported):
        return None, "invalid_model_version"
    return reported, None


def validate(symbol, asof_ms, bars, forecast, adapter):
    """The Phase 5 entry point: never raises, always returns a toto.v1 object. `forecast`
    must be a forecast.v1 object with status "ok" (an unavailable forecast has nothing for
    Toto to validate, so Toto itself is reported unavailable, not run). An `ok` result
    requires a valid immutable model identity from the adapter itself (see `Adapter`)."""
    def unavailable(reason, version=UNAVAILABLE_VERSION):
        return C.toto(symbol, asof_ms, MODEL_NAME, version, reason=reason)

    if adapter is None:
        return unavailable("model_unavailable")
    if forecast.get("status") != "ok":
        return unavailable("no_forecast_to_validate")
    try:
        result = adapter.validate(bars, forecast)
    except WorkerError as exc:
        return unavailable("timeout" if "timed out" in str(exc) else "worker_failed")
    except Exception:  # Any adapter failure is unavailable, never a crash or a fabricated CONFIRM.
        return unavailable("adapter_failed")
    if not isinstance(result, dict) or result.get("decision") not in C.DECISIONS:
        return unavailable("invalid_output")
    version, problem = _identity(adapter, result)
    if problem:
        return unavailable(problem)
    try:
        return C.toto(symbol, asof_ms, MODEL_NAME, version, decision=result["decision"],
                      p_up=result.get("p_up"), confidence=result.get("confidence"),
                      disagreement_reason=result.get("disagreement_reason"))
    except C.ContractError:
        return unavailable("invalid_output")
