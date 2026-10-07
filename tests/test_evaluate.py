"""Phase 7: journal round-trip, every metric on known values, and the CLI."""
import json
import tempfile
import unittest
from pathlib import Path

from v2 import evaluate as E


def decision(id_, symbol, proposed, final, reason=None, forecast=None, toto=None, gate=None):
    return dict(record=E.DECISION, id=id_, symbol=symbol, candle_ms=1, proposed_action=proposed,
                final_action=final, rejection_reason=reason, v2_mode="on",
                forecast=forecast, toto=toto, gate=gate)


def outcome(id_, symbol, action, net_r, net_return=None, exit_ms=1000):
    return dict(record=E.OUTCOME, id=id_, symbol=symbol, action=action, outcome="TP",
                net_r=net_r, net_return=net_return if net_return is not None else net_r / 10,
                candle_ms=1, fill_ms=1, exit_ms=exit_ms)


class JournalRoundTrip(unittest.TestCase):
    def test_record_and_load_decisions_and_outcomes(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "j.ndjson"
            E.record_decision(path, dict(symbol="BTCUSDT", action="LONG", candle_ms=1), "LONG")
            signal = dict(id="s1", symbol="BTCUSDT", action="LONG", status="CLOSED", outcome="TP",
                         net_r=1.5, net_return=0.01, candle_ms=1, fill_ms=1, exit_ms=2)
            E.record_outcome(path, signal)
            decisions, outcomes = E.load(path)
            self.assertEqual(len(decisions), 1)
            self.assertEqual(len(outcomes), 1)
            self.assertEqual(outcomes[0]["net_r"], 1.5)

    def test_rejects_malformed_or_unopen_outcome(self):
        with self.assertRaises(E.JournalError):
            E.record_outcome("/dev/null", dict(status="OPEN"))
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "bad.ndjson"
            path.write_text('{"record": "decision.v1"}\nnot json\n')
            with self.assertRaises(E.JournalError):
                E.load(path)
            path.write_text('{"record": "mystery"}\n')
            with self.assertRaises(E.JournalError):
                E.load(path)

    def test_final_action_is_validated(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(E.JournalError):
                E.record_decision(Path(d) / "j.ndjson", dict(symbol="X"), "MAYBE")


class Metrics(unittest.TestCase):
    def test_counts_by_action(self):
        decisions = [decision(1, "A", "LONG", "LONG"), decision(2, "B", "SHORT", "HOLD", "x"),
                    decision(3, "C", None, "HOLD")]
        self.assertEqual(E.counts_by_action(decisions), {"LONG": 1, "SHORT": 0, "HOLD": 2})

    def test_precision_by_direction_and_average_and_positive_fraction(self):
        outcomes = [outcome(1, "A", "LONG", 1.0), outcome(2, "B", "LONG", -0.5),
                   outcome(3, "C", "SHORT", 2.0)]
        prec = E.precision_by_direction(outcomes)
        self.assertAlmostEqual(prec["LONG"], 0.5)
        self.assertAlmostEqual(prec["SHORT"], 1.0)
        self.assertAlmostEqual(prec["overall"], 2 / 3)
        self.assertAlmostEqual(E.average_net_r(outcomes), (1.0 - 0.5 + 2.0) / 3)
        self.assertIsNone(E.average_net_r([]))

    def test_expectancy_equals_average_net_r_over_the_same_population(self):
        outcomes = [outcome(1, "A", "LONG", 2.0), outcome(2, "B", "LONG", -1.0),
                   outcome(3, "C", "LONG", -1.0), outcome(4, "D", "LONG", 3.0)]
        self.assertAlmostEqual(E.expectancy(outcomes), E.average_net_r(outcomes))
        self.assertIsNone(E.expectancy([]))

    def test_max_drawdown_on_a_known_equity_curve(self):
        # Cumulative: 1, 3, 1, 2, -1, 1 -> drawdown from peak 3 to trough -1 is -4.
        outcomes = [outcome(i, "A", "LONG", r, exit_ms=i) for i, r in
                   enumerate([1, 2, -2, 1, -3, 2], start=1)]
        self.assertAlmostEqual(E.max_drawdown(outcomes), -4.0)
        self.assertEqual(E.max_drawdown([]), 0.0)
        rising = [outcome(i, "A", "LONG", 1.0, exit_ms=i) for i in range(1, 4)]
        self.assertEqual(E.max_drawdown(rising), 0.0)

    def test_rejection_reasons_counts_only_held_proposals(self):
        decisions = [decision(1, "A", "LONG", "HOLD", "toto_reject"),
                    decision(2, "B", "LONG", "HOLD", "toto_reject"),
                    decision(3, "C", "SHORT", "HOLD", "stale"),
                    decision(4, "D", "LONG", "LONG"),       # issued, not held
                    decision(5, "E", None, "HOLD")]         # never a candidate at all
        self.assertEqual(E.rejection_reasons(decisions), {"toto_reject": 2, "stale": 1})

    def test_provider_failures_distinct_from_real_decisions(self):
        decisions = [
            decision(1, "A", "LONG", "HOLD", toto={"status": "unavailable"}),
            decision(2, "B", "LONG", "LONG", toto={"status": "ok", "decision": "CONFIRM"}),
            decision(3, "C", "LONG", "HOLD", gate={"status": "DISABLED"}),
            decision(4, "D", "LONG", "LONG", gate={"status": "OK", "decision": "CONFIRM"}),
        ]
        self.assertEqual(E.provider_failures(decisions), {"toto": 1, "gate": 1})

    def test_stale_rejections(self):
        decisions = [decision(1, "A", "LONG", "HOLD",
                              forecast={"status": "unavailable", "reason": "stale_input"}),
                    decision(2, "B", "LONG", "HOLD",
                              forecast={"status": "unavailable", "reason": "model_unavailable"})]
        self.assertEqual(E.stale_rejections(decisions), 1)

    def test_forecast_quality_only_on_ok_forecasts_with_a_closed_outcome(self):
        decisions = [decision(1, "A", "LONG", "LONG", forecast={"status": "ok",
                                                                 "expected_return_bps": 5.0,
                                                                 "sigma_bps": 10.0}),
                    decision(2, "B", "LONG", "HOLD", forecast={"status": "unavailable"})]
        outcomes_by_id = {1: outcome(1, "A", "LONG", 1.0, net_return=0.001)}  # 10 bps realized
        quality = E.forecast_quality(decisions, outcomes_by_id)
        self.assertEqual(quality["n"], 1)
        self.assertAlmostEqual(quality["mae_bps"], 5.0)
        self.assertEqual(E.forecast_quality(decisions, {})["n"], 0)


class BuildReportAndCli(unittest.TestCase):
    def test_build_report_combines_everything(self):
        decisions = [decision(1, "A", "LONG", "LONG"), decision(2, "B", "LONG", "HOLD", "toto_reject",
                                                                 toto={"status": "ok", "decision": "REJECT"})]
        outcomes = [outcome(1, "A", "LONG", 1.0)]
        report = E.build_report(decisions, outcomes).as_dict()
        self.assertEqual(report["counts"], {"LONG": 1, "SHORT": 0, "HOLD": 1})
        self.assertEqual(report["n_decisions"], 2)
        self.assertEqual(report["n_outcomes"], 1)
        self.assertEqual(report["rejection_reasons"], {"toto_reject": 1})

    def test_cli_reads_journals_and_writes_a_report(self):
        with tempfile.TemporaryDirectory() as d:
            dpath, opath, out = Path(d) / "d.ndjson", Path(d) / "o.ndjson", Path(d) / "report.json"
            E.record_decision(dpath, dict(symbol="BTCUSDT", action="LONG", candle_ms=1), "LONG")
            E.record_outcome(opath, dict(id="BTCUSDT", symbol="BTCUSDT", action="LONG",
                                        status="CLOSED", outcome="TP", net_r=1.0, net_return=0.01,
                                        candle_ms=1, fill_ms=1, exit_ms=2))
            code = E.main(["--decisions", str(dpath), "--outcomes", str(opath), "--out", str(out)])
            self.assertEqual(code, 0)
            report = json.loads(out.read_text())
            self.assertEqual(report["report"], "v2.evaluate.v1")
            self.assertEqual(report["n_decisions"], 1)

    def test_cli_tolerates_a_missing_file(self):
        with tempfile.TemporaryDirectory() as d:
            code = E.main(["--decisions", str(Path(d) / "missing.ndjson"),
                           "--outcomes", str(Path(d) / "also-missing.ndjson"),
                           "--out", str(Path(d) / "report.json")])
            self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
