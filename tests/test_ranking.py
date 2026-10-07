"""Phase 6 ranking and gating: V2_MODE="off" (no evidence keys) is untouched; each hard
gate fires only on its own evidence; the score modifier is bounded; at most 3 survive."""
import unittest

from v2 import ranking as R

FORECAST_OK = dict(status="ok", p_up=0.9)
FORECAST_STALE = dict(status="unavailable", reason="stale_input")
TOTO_CONFIRM = dict(status="ok", decision="CONFIRM", confidence=0.8)
TOTO_REJECT = dict(status="ok", decision="REJECT", disagreement_reason="x", confidence=0.8)
GATE_CONFIRM = dict(status="OK", decision="CONFIRM", confidence=0.9)
GATE_HOLD = dict(status="OK", decision="HOLD", confidence=0.9)


def candidate(symbol="BTCUSDT", rank=2.0, **extra):
    base = dict(symbol=symbol, action="LONG", rank=rank, market={"spread_bps": 1.0})
    base.update(extra)
    return base


class WithoutV2Evidence(unittest.TestCase):
    """No forecast/toto/gate keys at all: gate always passes, score == rank unchanged."""

    def test_v1_candidates_pass_through_unchanged(self):
        c = candidate()
        self.assertEqual(R.gate(c), (True, None))
        self.assertEqual(R.score(c, min_samples=20), c["rank"])
        self.assertIsNone(R.model_confidence(c))

    def test_hold_and_missing_market_are_rejected(self):
        self.assertFalse(R.gate(candidate(action="HOLD"))[0])
        c = candidate()
        del c["market"]
        self.assertFalse(R.gate(c)[0])


class HardGates(unittest.TestCase):
    def test_stale_forecast_is_rejected(self):
        passed, reason = R.gate(candidate(forecast=FORECAST_STALE))
        self.assertFalse(passed)
        self.assertIn("stale_input", reason)

    def test_non_stale_unavailable_forecast_is_not_rejected_by_this_gate(self):
        other = dict(status="unavailable", reason="model_unavailable")
        self.assertTrue(R.gate(candidate(forecast=other))[0])

    def test_toto_reject_is_rejected(self):
        passed, reason = R.gate(candidate(toto=TOTO_REJECT))
        self.assertFalse(passed)
        self.assertIn("Toto REJECT", reason)

    def test_toto_confirm_or_hold_is_not_rejected(self):
        self.assertTrue(R.gate(candidate(toto=TOTO_CONFIRM))[0])
        hold = dict(status="ok", decision="HOLD", confidence=0.5)
        self.assertTrue(R.gate(candidate(toto=hold))[0])

    def test_gate_not_confirm_is_rejected(self):
        passed, reason = R.gate(candidate(gate=GATE_HOLD))
        self.assertFalse(passed)
        self.assertIn("bukan CONFIRM", reason)
        disabled = dict(status="DISABLED", decision="HOLD", confidence=0.0)
        self.assertFalse(R.gate(candidate(gate=disabled))[0])

    def test_gate_confirm_passes(self):
        self.assertTrue(R.gate(candidate(gate=GATE_CONFIRM))[0])

    def test_rules_can_disable_each_gate_independently(self):
        rules = R.GateRules(reject_stale_forecast=False, reject_on_toto_reject=False,
                            require_gate_confirm=False)
        c = candidate(forecast=FORECAST_STALE, toto=TOTO_REJECT, gate=GATE_HOLD)
        self.assertTrue(R.gate(c, rules)[0])


class Scoring(unittest.TestCase):
    def test_model_confidence_is_the_mean_of_available_signals(self):
        c = candidate(forecast=FORECAST_OK, toto=TOTO_CONFIRM, gate=GATE_CONFIRM)
        expected = (R.patchtst_confidence(FORECAST_OK) + 0.8 + 0.9) / 3
        self.assertAlmostEqual(R.model_confidence(c), expected)

    def test_score_modifier_is_bounded_to_half_at_worst_never_above_base(self):
        low_conf = candidate(rank=2.0, forecast=dict(status="ok", p_up=0.5),  # confidence 0
                             toto=dict(status="ok", decision="CONFIRM", confidence=0.0))
        self.assertAlmostEqual(R.score(low_conf, 20), 1.0)  # 2.0 * 0.5
        high_conf = candidate(rank=2.0, forecast=dict(status="ok", p_up=1.0),
                              toto=dict(status="ok", decision="CONFIRM", confidence=1.0))
        self.assertAlmostEqual(R.score(high_conf, 20), 2.0)  # 2.0 * 1.0, never exceeds base
        self.assertEqual(R.score(candidate(rank=2.0), 20), 2.0)  # no evidence: unchanged

    def test_historical_hit_rate_requires_enough_samples(self):
        thin = candidate(journal={"sample_count": 5, "positive_fraction": 0.9})
        self.assertIsNone(R.historical_hit_rate(thin, min_samples=20))
        enough = candidate(journal={"sample_count": 25, "positive_fraction": 0.6})
        self.assertEqual(R.historical_hit_rate(enough, min_samples=20), 0.6)


class RankAndSelect(unittest.TestCase):
    def test_at_most_three_survive_highest_score_first(self):
        cands = [candidate(symbol=f"SYM{i}USDT", rank=float(i)) for i in range(5)]
        top = R.rank_and_select(cands)
        self.assertEqual(len(top), 3)
        self.assertEqual([c["symbol"] for c in top], ["SYM4USDT", "SYM3USDT", "SYM2USDT"])
        for c in top:
            self.assertIn("v2_score", c)

    def test_gated_candidates_never_survive(self):
        cands = [candidate(symbol="A", rank=10.0, toto=TOTO_REJECT),
                 candidate(symbol="B", rank=1.0)]
        top = R.rank_and_select(cands)
        self.assertEqual([c["symbol"] for c in top], ["B"])

    def test_ties_break_on_historical_hit_rate_then_symbol(self):
        a = candidate(symbol="AAAUSDT", rank=1.0, journal={"sample_count": 25, "positive_fraction": 0.3})
        b = candidate(symbol="BBBUSDT", rank=1.0, journal={"sample_count": 25, "positive_fraction": 0.7})
        top = R.rank_and_select([a, b], max_candidates=2)
        self.assertEqual([c["symbol"] for c in top], ["BBBUSDT", "AAAUSDT"])
        c = candidate(symbol="AAAUSDT", rank=1.0)
        d = candidate(symbol="BBBUSDT", rank=1.0)
        top = R.rank_and_select([d, c], max_candidates=2)
        self.assertEqual([x["symbol"] for x in top], ["BBBUSDT", "AAAUSDT"])  # symbol tiebreak

    def test_max_candidates_is_validated(self):
        with self.assertRaises(ValueError):
            R.rank_and_select([], max_candidates=0)


if __name__ == "__main__":
    unittest.main()
