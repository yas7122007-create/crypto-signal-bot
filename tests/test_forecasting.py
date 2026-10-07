"""Baselines, metrics, the forecast contract, PatchTST and the training CLI."""
import json
import math
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

from tests.synthetic import raw_bars, write_dir
from v2 import bars as B
from v2 import contracts as C

INPUT = dict(dataset_schema=1, bar_schema=1, feature_schema=2, window=32, channels=["ret_bps"])


class ForecastContract(unittest.TestCase):
    def make(self, **kw):
        args = dict(symbol="BTCUSDT", asof_ms=1, horizon_ms=900_000, model="patchtst",
                    model_version="0123456789abcdef", input_info=INPUT)
        args.update(kw)
        return C.forecast(**args)

    def test_ok_and_unavailable(self):
        ok = self.make(mu=3.2, sigma=10.0, p_up=0.62)
        self.assertEqual(ok["status"], "ok")
        down = self.make(reason="stale_input")
        self.assertEqual((down["status"], down["p_up"]), ("unavailable", None))

    def test_out_of_bounds_values_are_rejected(self):
        for kw in (dict(mu=float("nan"), sigma=1, p_up=0.5), dict(mu=1, sigma=0, p_up=0.5),
                   dict(mu=1, sigma=1, p_up=1.2), dict(mu=1e6, sigma=1, p_up=0.5),
                   dict(mu=True, sigma=1, p_up=0.5)):
            with self.assertRaises(C.ContractError):
                self.make(**kw)
        with self.assertRaises(C.ContractError):
            self.make(symbol="btc;rm -rf", mu=1, sigma=1, p_up=0.5)
        with self.assertRaises(C.ContractError):
            self.make(reason="Free form text from a model")

    def test_extra_or_missing_fields_are_rejected(self):
        ok = self.make(mu=1, sigma=1, p_up=0.5)
        with self.assertRaises(C.ContractError):
            C.validate_forecast(dict(ok, note="free text"))
        partial = dict(ok)
        del partial["sigma_bps"]
        with self.assertRaises(C.ContractError):
            C.validate_forecast(partial)
        with self.assertRaises(C.ContractError):
            C.validate_forecast(dict(ok, status="unavailable", reason="x"))  # values present


@unittest.skipIf(np is None, "numpy not installed")
class BaselinesAndMetrics(unittest.TestCase):
    def setUp(self):
        from v2.dataset import DatasetSpec, build, time_split
        self.spec = DatasetSpec(window=16, horizon=1)
        raw = raw_bars(1500, seed=1, signal=1.0)
        self.x, self.y, meta = build(B.series([B.validate(b) for b in raw]), self.spec)
        self.train, _, self.test = time_split(meta, self.spec)

    def test_metrics_on_known_values(self):
        from v2.metrics import evaluate, p_up
        self.assertAlmostEqual(float(p_up([0.0], [1.0])[0]), 0.5)
        self.assertAlmostEqual(float(p_up([1.0], [1.0])[0]), 0.8413447, places=6)
        m = evaluate([1.0, -1.0, 0.0], [2.0, 1.0, 0.0], [1.0, 1.0, 1.0])
        self.assertAlmostEqual(m["mae_bps"], (1 + 2 + 0) / 3)
        self.assertEqual(m["directional_accuracy"], 0.5)   # zero return excluded
        nll0 = 0.5 * math.log(2 * math.pi)
        self.assertAlmostEqual(m["nll"], nll0 + (0.5 + 2.0 + 0) / 3)
        self.assertEqual(evaluate([], [], [])["n"], 0)

    def test_baselines_fit_on_train_and_ridge_finds_a_planted_signal(self):
        from v2.baselines import all_baselines
        from v2.metrics import evaluate
        scores = {}
        for model in all_baselines():
            model.fit(self.x[self.train], self.y[self.train], self.spec)
            mu, sigma = model.predict(self.x[self.test])
            self.assertTrue(np.isfinite(mu).all() and (sigma > 0).all())
            scores[model.name] = evaluate(self.y[self.test], mu, sigma)
        self.assertEqual(scores["zero"]["directional_accuracy"], 0.0)  # sign(0) never matches
        # The planted OBI effect is learnable: ridge must beat the random walk out of sample.
        self.assertLess(scores["ridge"]["nll"], scores["zero"]["nll"])
        self.assertGreater(scores["ridge"]["directional_accuracy"], 0.6)


@unittest.skipIf(torch is None or np is None, "torch not installed")
class PatchTSTModel(unittest.TestCase):
    def test_trains_deterministically_saves_verifies_and_loads(self):
        from v2.dataset import DatasetSpec, build, time_split
        from v2.metrics import evaluate
        from v2.patchtst import Forecaster, configure
        spec = DatasetSpec(window=16, horizon=1)
        raw = raw_bars(1500, seed=2, signal=1.0)
        x, y, meta = build(B.series([B.validate(b) for b in raw]), spec)
        train, val, test = time_split(meta, spec)

        def run():
            configure(threads=1, seed=0)
            return Forecaster(spec, dict(dropout=0.0)).fit(x[train], y[train], x[val], y[val],
                                                           epochs=8, seed=0)
        a, b = run(), run()
        mu_a, sig_a = a.predict(x[test])
        mu_b, _ = b.predict(x[test])
        np.testing.assert_array_equal(mu_a, mu_b)          # same seed, same weights
        self.assertTrue((sig_a > 0).all())
        zero = evaluate(y[test], np.zeros(len(test)), np.full(len(test), y[train].std()))
        self.assertLess(evaluate(y[test], mu_a, sig_a)["nll"], zero["nll"])

        with tempfile.TemporaryDirectory() as d:
            version = a.save(d)
            loaded = Forecaster.load(d)
            self.assertEqual(loaded.version, version)
            np.testing.assert_allclose(loaded.predict(x[test])[0], mu_a, rtol=1e-6)
            meta_path = Path(d) / "model.json"
            info = json.loads(meta_path.read_text())
            info["y_std"] *= 2                               # tampered scaler
            meta_path.write_text(json.dumps(info))
            with self.assertRaises(ValueError):
                Forecaster.load(d)


@unittest.skipIf(torch is None or np is None, "torch not installed")
class TrainCli(unittest.TestCase):
    def test_reports_every_model_on_the_same_split_and_labels_synthetic_data(self):
        from v2 import train
        with tempfile.TemporaryDirectory() as d:
            src = write_dir(Path(d) / "bars", raw_bars(900, seed=5, signal=1.0))
            out = Path(d) / "out"
            code = train.main(["--bars-dir", str(src), "--out", str(out), "--window", "16",
                               "--horizon", "1", "--epochs", "2", "--threads", "1",
                               "--folds", "2"])
            self.assertEqual(code, 0)
            report = json.loads((out / "report.json").read_text())
            self.assertTrue(report["synthetic_data"])
            self.assertEqual(set(report["test"]),
                             {"zero", "persistence", "moving_average", "ridge", "patchtst"})
            n = {r["n"] for r in report["test"].values()}
            self.assertEqual(len(n), 1)
            self.assertEqual(len(report["walk_forward"]), 2)
            self.assertTrue((out / "model" / "VERSION").exists())

    def test_refuses_to_train_on_too_little_data(self):
        from v2 import train
        with tempfile.TemporaryDirectory() as d:
            src = write_dir(Path(d) / "bars", raw_bars(60), marker=False)
            self.assertEqual(train.main(["--bars-dir", str(src), "--out", str(Path(d) / "o"),
                                         "--skip-patchtst"]), 2)


if __name__ == "__main__":
    unittest.main()
