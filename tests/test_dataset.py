"""Bar validation, dataset alignment, leakage and split tests (numpy)."""
import copy
import unittest

try:
    import numpy as np
except ImportError:  # pragma: no cover
    np = None

from tests.synthetic import BAR_MS, raw_bars
from v2 import bars as B

if np is not None:
    from v2.dataset import CHANNELS, DatasetSpec, build, time_split, walk_forward, window_matrix


def groups_of(raw):
    return B.series([B.validate(b) for b in raw])


class BarValidation(unittest.TestCase):
    def setUp(self):
        self.bar = raw_bars(2)[1]

    def bad(self, **change):
        bar = dict(copy.deepcopy(self.bar), **change)
        with self.assertRaises(B.BarError):
            B.validate(bar)

    def test_accepts_a_valid_bar(self):
        bar = B.validate(self.bar)
        self.assertEqual(bar["symbol"], "BTCUSDT")
        self.assertEqual(str(bar["obi"][10]), self.bar["close_obi"][0]["value"])

    def test_rejects_malformed_and_mismatched_bars(self):
        self.bad(v=2)
        self.bad(feature_v=1)
        self.bad(symbol="btc/usdt")
        self.bad(end_ms=self.bar["end_ms"] + 1)
        self.bad(start_ms=self.bar["start_ms"] + 1, end_ms=self.bar["end_ms"] + 1)
        self.bad(close_mid=1.5)                       # float, not exact decimal text
        self.bad(close_mid="NaN")
        self.bad(close_mid="1e9")                     # outside [low, high]
        self.bad(buy_qty=None)                        # one of the pair null
        self.bad(sell_qty="-1")
        self.bad(close_obi=["x"])
        self.bad(rows=0)
        self.bad(incomplete_reason="row_gap")         # complete bar with a reason
        self.bad(complete="yes")

    def test_duplicate_bars_are_an_error(self):
        raw = raw_bars(3)
        with self.assertRaises(B.BarError):
            groups_of(raw + [raw[1]])


@unittest.skipIf(np is None, "numpy not installed")
class Dataset(unittest.TestCase):
    spec = DatasetSpec(window=8, horizon=3) if np is not None else None

    def test_alignment_is_explicit_and_auditable(self):
        raw = raw_bars(40)
        x, y, meta = build(groups_of(raw), self.spec)
        self.assertEqual(x.shape, (len(meta), 8, len(CHANNELS)))
        first = meta[0]
        # The first sample needs bar 0 (for its first return) and bars 1..8 as input.
        self.assertEqual(first["asof_ms"], raw[8]["end_ms"])
        self.assertEqual(first["input_start_ms"], raw[1]["start_ms"])
        self.assertEqual(first["target_end_ms"] - first["asof_ms"], 3 * BAR_MS)
        close = lambda i: float(raw[i]["close_mid"])
        self.assertAlmostEqual(y[0], 1e4 * np.log(close(11) / close(8)), places=2)
        self.assertEqual(len(meta), 40 - 8 - 3)
        for m in meta:
            self.assertEqual(m["symbol"], "BTCUSDT")
            self.assertEqual(m["session_id"], 1)

    def test_inputs_never_see_the_future(self):
        """Changing every bar after asof leaves the input unchanged; changing the target bar
        changes only the target."""
        raw = raw_bars(40, seed=3)
        x, y, meta = build(groups_of(raw), self.spec)
        k = 10
        asof = meta[k]["asof_ms"]
        future = copy.deepcopy(raw)
        for bar in future:
            if bar["end_ms"] > asof:
                for f in ("open_mid", "high_mid", "low_mid", "close_mid", "close_microprice"):
                    bar[f] = str(float(bar[f]) * 1.5)
                bar["close_obi"] = [{"levels": 10, "value": "0.9"}, {"levels": 50, "value": "0.9"}]
                bar["buy_qty"], bar["sell_qty"] = "77", "1"
        x2, y2, meta2 = build(groups_of(future), self.spec)
        np.testing.assert_array_equal(x2[k], x[k])
        self.assertNotAlmostEqual(float(y2[k]), float(y[k]), places=3)

    def test_the_target_never_uses_data_at_or_before_asof_except_the_reference_close(self):
        raw = raw_bars(40, seed=4)
        x, y, meta = build(groups_of(raw), self.spec)
        k = 10
        changed = copy.deepcopy(raw)
        for bar in changed:
            if bar["end_ms"] < meta[k]["asof_ms"]:
                bar["close_obi"] = [{"levels": 10, "value": "-0.9"}, {"levels": 50, "value": "0"}]
        _, y2, _ = build(groups_of(changed), self.spec)
        self.assertEqual(float(y2[k]), float(y[k]))

    def test_incomplete_bars_and_gaps_never_enter_a_sample(self):
        raw = raw_bars(40)
        raw[20]["complete"], raw[20]["incomplete_reason"] = False, "resync"
        del raw[30]                                   # a missing minute
        x, y, meta = build(groups_of(raw), self.spec)
        start20 = raw[20]["start_ms"]
        for m in meta:
            self.assertFalse(m["input_start_ms"] - BAR_MS <= start20 < m["asof_ms"])
            self.assertNotEqual(m["target_end_ms"], raw[20]["end_ms"])
            gap = raw[29]["end_ms"]                   # bar 30 is gone
            self.assertFalse(m["input_start_ms"] - BAR_MS < gap + BAR_MS <= m["asof_ms"])

    def test_missing_flow_or_obi_is_never_imputed(self):
        raw = raw_bars(20)
        raw[12]["buy_qty"] = raw[12]["sell_qty"] = None
        bars = groups_of(raw)[("BTCUSDT", 1)]
        self.assertIsNone(window_matrix(bars[4:13], self.spec))
        raw[12]["buy_qty"] = raw[12]["sell_qty"] = "1"
        raw[12]["close_obi"] = [{"levels": 10, "value": None}]
        self.assertIsNone(window_matrix(groups_of(raw)[("BTCUSDT", 1)][4:13], self.spec))

    def test_sessions_and_symbols_never_mix_in_a_window(self):
        a = raw_bars(15, session=1)
        b = raw_bars(15, session=2, start=a[-1]["end_ms"])   # adjacent in time, new session
        _, _, meta = build(groups_of(a + b), self.spec)
        self.assertEqual(len(meta), 2 * (15 - 8 - 3))
        # window_matrix itself refuses a mixed window (live inference calls it directly).
        b0 = dict(b[0], buy_qty="1", sell_qty="1")    # flow present, so only the session differs
        mixed = [B.validate(r) for r in a[-4:] + [b0] + b[1:5]]
        self.assertIsNone(window_matrix(mixed, self.spec))
        eth = raw_bars(15, symbol="ETHUSDT")
        _, _, meta = build(groups_of(a + eth), self.spec)
        self.assertEqual([m["symbol"] for m in meta][:4], ["BTCUSDT"] * 4)
        self.assertEqual(len(meta), 2 * 4)

    def test_build_is_deterministic(self):
        raw = raw_bars(60, seed=9)
        x1, y1, m1 = build(groups_of(raw), self.spec)
        x2, y2, m2 = build(groups_of(list(reversed(raw))), self.spec)
        np.testing.assert_array_equal(x1, x2)
        np.testing.assert_array_equal(y1, y2)
        self.assertEqual(m1, m2)

    def test_time_split_is_ordered_with_an_embargo(self):
        _, _, meta = build(groups_of(raw_bars(400)), self.spec)
        train, val, test = time_split(meta, self.spec)
        asof = np.array([m["asof_ms"] for m in meta])
        target_end = np.array([m["target_end_ms"] for m in meta])
        self.assertTrue(len(train) and len(val) and len(test))
        self.assertLessEqual(target_end[train].max(), asof[val].min())
        self.assertLessEqual(target_end[val].max(), asof[test].min())
        self.assertFalse(set(train) & set(val) or set(val) & set(test))

    def test_walk_forward_trains_only_on_the_past(self):
        _, _, meta = build(groups_of(raw_bars(400)), self.spec)
        folds = walk_forward(meta, self.spec, folds=4)
        self.assertEqual(len(folds), 4)
        asof = np.array([m["asof_ms"] for m in meta])
        target_end = np.array([m["target_end_ms"] for m in meta])
        previous = -1
        for train, test in folds:
            self.assertLessEqual(target_end[train].max(), asof[test].min())
            self.assertGreater(asof[test].min(), previous)
            previous = asof[test].max()

    def test_spec_bounds(self):
        with self.assertRaises(ValueError):
            DatasetSpec(window=4)
        with self.assertRaises(ValueError):
            DatasetSpec(horizon=0)


if __name__ == "__main__":
    unittest.main()
