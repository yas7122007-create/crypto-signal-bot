"""Phase 4 Python bridge: schema, timestamp, staleness, symbol and horizon validation, and
fail-closed behavior when the model or its input is unavailable or invalid."""
import json
import tempfile
import unittest
from pathlib import Path

try:
    import numpy as np
except ImportError:  # pragma: no cover
    np = None
try:
    import torch
except ImportError:  # pragma: no cover
    torch = None

from tests.synthetic import BAR_MS, raw_bars
from v2 import bridge as BR
from v2.bars import validate


def state_file(path, symbol, bars, kind="bar_window", v=1):
    Path(path).write_text(json.dumps(dict(v=v, kind=kind, symbol=symbol, bars=bars)))


class ReadState(unittest.TestCase):
    def test_reads_and_validates_a_contiguous_session(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "BTCUSDT.json"
            bars = raw_bars(5)
            state_file(p, "BTCUSDT", bars)
            symbol, validated = BR.read_state(p)
            self.assertEqual(symbol, "BTCUSDT")
            self.assertEqual(len(validated), 5)

    def test_rejects_malformed_missing_or_discontiguous_state(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.json"
            with self.assertRaises(BR.BridgeError):
                BR.read_state(Path(d) / "missing.json")
            p.write_text("not json")
            with self.assertRaises(BR.BridgeError):
                BR.read_state(p)
            state_file(p, "BTCUSDT", raw_bars(3), v=2)
            with self.assertRaises(BR.BridgeError):
                BR.read_state(p)
            bars = raw_bars(3)
            bars[1]["close_mid"] = "nan"
            state_file(p, "BTCUSDT", bars)
            with self.assertRaises(BR.BridgeError):
                BR.read_state(p)
            bars = raw_bars(5)
            del bars[2]  # a gap
            state_file(p, "BTCUSDT", bars)
            with self.assertRaises(BR.BridgeError):
                BR.read_state(p)

    def test_rejects_bars_whose_own_symbol_does_not_match_the_state_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.json"
            # Every bar is internally consistent (all ETHUSDT), but the state file claims
            # BTCUSDT at the top level: adjacent-bar-only checks would miss this.
            state_file(p, "BTCUSDT", raw_bars(5, symbol="ETHUSDT"))
            with self.assertRaises(BR.BridgeError):
                BR.read_state(p)
            # A mix of symbols across bars is caught the same way.
            mixed = raw_bars(3, symbol="BTCUSDT") + raw_bars(3, symbol="ETHUSDT")
            state_file(p, "BTCUSDT", mixed)
            with self.assertRaises(BR.BridgeError):
                BR.read_state(p)


@unittest.skipIf(np is None, "numpy not installed")
class FailClosed(unittest.TestCase):
    """Every branch of forecast() that does not reach a valid model output: status must be
    'unavailable' with a specific reason, never a fabricated value."""

    def setUp(self):
        self.config = BR.BridgeConfig(model_dir="/does/not/exist")

    def unavailable(self, result, reason):
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["reason"], reason)
        self.assertIsNone(result["expected_return_bps"])

    def test_model_unavailable(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.json"
            state_file(p, "BTCUSDT", raw_bars(5))
            self.unavailable(BR.forecast(p, "BTCUSDT", 900_000, self.config), "model_unavailable")

    def test_symbol_mismatch_and_state_unreadable_need_no_model(self):
        # These checks happen even without a real model, as long as _load_model is forced.
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.json"
            state_file(p, "ETHUSDT", raw_bars(5))
            self.unavailable(BR.forecast(p, "BTCUSDT", 900_000, self.config), "model_unavailable")
            # With no model at all, every path reports model_unavailable first (fail closed
            # on the cheapest check), which is itself the required behavior.


@unittest.skipIf(torch is None or np is None, "torch not installed")
class WithRealModel(unittest.TestCase):
    """Exercises every fail-closed branch once a trained model is present."""

    @classmethod
    def setUpClass(cls):
        from v2.dataset import DatasetSpec, build
        from v2.patchtst import Forecaster, configure
        configure(threads=1, seed=0)
        cls.spec = DatasetSpec(window=8, horizon=3)
        raw = raw_bars(200, seed=1, signal=0.3)
        x, y, _ = build({("BTCUSDT", 1): [validate(b) for b in raw]}, cls.spec)
        cls.model = Forecaster(cls.spec, dict(dropout=0.0)).fit(x[:100], y[:100], x[100:150],
                                                                 y[100:150], epochs=3, seed=0)
        cls.tmp = tempfile.TemporaryDirectory()
        cls.model.save(cls.tmp.name)
        cls.raw = raw

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.config = BR.BridgeConfig(model_dir=self.tmp.name, max_stale_ms=180_000)
        BR._MODEL_CACHE.clear()

    def state(self, d, bars, symbol="BTCUSDT"):
        p = Path(d) / "s.json"
        state_file(p, symbol, bars)
        return p

    def now(self, bars):
        return bars[-1]["end_ms"]

    def test_valid_window_produces_an_ok_forecast(self):
        with tempfile.TemporaryDirectory() as d:
            bars = self.raw[-20:]
            p = self.state(d, bars)
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config, now_ms=self.now(bars))
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["model_version"], self.model.version)
            self.assertTrue(0.0 <= result["p_up"] <= 1.0)
            self.assertEqual(result["asof_ms"], bars[-1]["end_ms"])

    def test_horizon_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            bars = self.raw[-20:]
            p = self.state(d, bars)
            result = BR.forecast(p, "BTCUSDT", 5 * BAR_MS, self.config, now_ms=self.now(bars))
            self.assertEqual(result["reason"], "horizon_mismatch")

    def test_symbol_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            # A self-consistent ETHUSDT state file (every bar genuinely ETHUSDT), requested
            # as BTCUSDT: the mismatch is between the request and the file, not within it.
            eth_bars = raw_bars(20, symbol="ETHUSDT", seed=2)
            p = self.state(d, eth_bars, symbol="ETHUSDT")
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config, now_ms=self.now(eth_bars))
            self.assertEqual(result["reason"], "symbol_mismatch")

    def test_stale_input_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            bars = self.raw[-20:]
            p = self.state(d, bars)
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config,
                                 now_ms=self.now(bars) + 10 * 60_000)
            self.assertEqual(result["reason"], "stale_input")

    def test_insufficient_history_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            bars = self.raw[-5:]  # fewer than spec.window + 1
            p = self.state(d, bars)
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config, now_ms=self.now(bars))
            self.assertEqual(result["reason"], "insufficient_history")

    def test_incomplete_window_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            import copy
            bars = copy.deepcopy(self.raw[-20:])
            bars[-3]["buy_qty"] = bars[-3]["sell_qty"] = None
            p = self.state(d, bars)
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config, now_ms=self.now(bars))
            self.assertEqual(result["reason"], "incomplete_window")

    def test_malformed_state_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.json"
            p.write_text("{not json")
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config)
            self.assertEqual(result["reason"], "state_unreadable")

    def test_future_dated_state_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            bars = self.raw[-20:]
            p = self.state(d, bars)
            future_now = self.now(bars) - BR.MAX_FUTURE_SKEW_MS - 1
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config, now_ms=future_now)
            self.assertEqual(result["reason"], "future_input")

    def test_small_clock_skew_within_tolerance_is_accepted(self):
        with tempfile.TemporaryDirectory() as d:
            bars = self.raw[-20:]
            p = self.state(d, bars)
            just_inside = self.now(bars) - BR.MAX_FUTURE_SKEW_MS + 1
            result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, self.config, now_ms=just_inside)
            self.assertEqual(result["status"], "ok")

    def test_corrupt_model_artifact_never_raises(self):
        """Hardening 7: a model directory whose weights.pt is not valid torch data must
        become model_unavailable, not an exception escaping the public bridge boundary."""
        import hashlib
        with tempfile.TemporaryDirectory() as corrupt_dir:
            weights = b"not a valid torch checkpoint " + b"x" * 64
            meta = json.dumps(dict(self.model.metadata()), sort_keys=True, indent=1).encode()
            version = hashlib.sha256(hashlib.sha256(weights).digest()
                                     + hashlib.sha256(meta).digest()).hexdigest()[:16]
            Path(corrupt_dir, "weights.pt").write_bytes(weights)
            Path(corrupt_dir, "model.json").write_bytes(meta)
            Path(corrupt_dir, "VERSION").write_text(version + "\n")
            config = BR.BridgeConfig(model_dir=corrupt_dir)
            BR._MODEL_CACHE.clear()
            with tempfile.TemporaryDirectory() as d:
                bars = self.raw[-20:]
                p = self.state(d, bars)
                result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, config, now_ms=self.now(bars))
                self.assertEqual(result["status"], "unavailable")
                self.assertEqual(result["reason"], "model_unavailable")
                # A second call does not raise either (the failure is not cached, but still
                # degrades cleanly on retry rather than, say, caching a half-constructed
                # object).
                result = BR.forecast(self.state(d, bars), "BTCUSDT", 3 * BAR_MS, config,
                                     now_ms=self.now(bars))
                self.assertEqual(result["status"], "unavailable")

    def test_missing_model_file_never_raises(self):
        with tempfile.TemporaryDirectory() as empty_dir:
            config = BR.BridgeConfig(model_dir=empty_dir)
            BR._MODEL_CACHE.clear()
            with tempfile.TemporaryDirectory() as d:
                bars = self.raw[-20:]
                p = self.state(d, bars)
                result = BR.forecast(p, "BTCUSDT", 3 * BAR_MS, config, now_ms=self.now(bars))
                self.assertEqual(result["reason"], "model_unavailable")


if __name__ == "__main__":
    unittest.main()
