"""Toto validator: contract bounds, the subprocess worker runner, and fail-closed
validate() (timeout, invalid output, a raising adapter, no model, no forecast)."""
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from tests.synthetic import raw_bars
from v2 import contracts as C
from v2 import toto as T
from v2 import workers as W
from v2.bars import validate as validate_bar

OK_FORECAST = dict(status="ok", p_up=0.8)
DOWN_FORECAST = dict(status="ok", p_up=0.2)
UNAVAILABLE_FORECAST = dict(status="unavailable")


class TotoContract(unittest.TestCase):
    def test_confirm_and_unavailable(self):
        ok = C.toto("BTCUSDT", 1, "toto", "v1", "CONFIRM", 0.7, 0.8)
        self.assertEqual(ok["status"], "ok")
        down = C.toto("BTCUSDT", 1, "toto", "v1", reason="timeout")
        self.assertEqual((down["status"], down["decision"]), ("unavailable", None))

    def test_reject_requires_a_disagreement_reason(self):
        with self.assertRaises(C.ContractError):
            C.toto("BTCUSDT", 1, "toto", "v1", "REJECT", 0.3, 0.5)
        ok = C.toto("BTCUSDT", 1, "toto", "v1", "REJECT", 0.3, 0.5,
                   disagreement_reason="obi_direction_disagrees")
        self.assertEqual(ok["decision"], "REJECT")

    def test_out_of_bounds_and_bad_decision_rejected(self):
        with self.assertRaises(C.ContractError):
            C.toto("BTCUSDT", 1, "toto", "v1", "MAYBE", 0.5, 0.5)
        with self.assertRaises(C.ContractError):
            C.toto("BTCUSDT", 1, "toto", "v1", "CONFIRM", 1.5, 0.5)
        with self.assertRaises(C.ContractError):
            C.validate_toto(dict(C.toto("BTCUSDT", 1, "toto", "v1", "CONFIRM", 0.5, 0.5),
                                 extra="nope"))


class WorkerRunner(unittest.TestCase):
    def test_runs_a_real_subprocess_and_round_trips_json(self):
        script = textwrap.dedent("""
            import json, sys
            payload = json.load(sys.stdin)
            print(json.dumps({"echo": payload["x"]}))
        """)
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write(script)
            path = f.name
        out = W.run([sys.executable, path], {"x": 42}, timeout_s=10)
        self.assertEqual(out, {"echo": 42})

    def test_timeout_nonzero_exit_and_bad_json_all_raise(self):
        sleepy = [sys.executable, "-c", "import time; time.sleep(5)"]
        with self.assertRaises(W.WorkerError):
            W.run(sleepy, {}, timeout_s=0.2)
        failing = [sys.executable, "-c", "import sys; sys.exit(3)"]
        with self.assertRaises(W.WorkerError):
            W.run(failing, {}, timeout_s=10)
        garbage = [sys.executable, "-c", "print('not json')"]
        with self.assertRaises(W.WorkerError):
            W.run(garbage, {}, timeout_s=10)
        with self.assertRaises(W.WorkerError):
            W.run(["/no/such/binary"], {}, timeout_s=10)
        with self.assertRaises(W.WorkerError):
            W.run("not a list", {}, timeout_s=10)


class ValidateFailClosed(unittest.TestCase):
    def setUp(self):
        self.bars = [validate_bar(b) for b in raw_bars(5)]

    def test_no_adapter_is_unavailable(self):
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=None)
        self.assertEqual((r["status"], r["reason"]), ("unavailable", "model_unavailable"))

    def test_unavailable_forecast_is_never_validated(self):
        r = T.validate("BTCUSDT", 1, self.bars, UNAVAILABLE_FORECAST, adapter=T.FakeAdapter())
        self.assertEqual(r["reason"], "no_forecast_to_validate")

    def test_a_raising_adapter_degrades_not_crashes(self):
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=T.FakeAdapter(fail=RuntimeError()))
        self.assertEqual((r["status"], r["reason"]), ("unavailable", "adapter_failed"))

    def test_a_timing_out_worker_adapter_is_unavailable_never_confirm(self):
        adapter = T.WorkerAdapter([sys.executable, "-c", "import time; time.sleep(5)"], timeout_s=0.2)
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=adapter)
        self.assertEqual((r["status"], r["reason"]), ("unavailable", "timeout"))

    def test_invalid_adapter_output_is_rejected(self):
        class BadAdapter(T.Adapter):
            def validate(self, bars, forecast):
                return {"decision": "MAYBE"}
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=BadAdapter())
        self.assertEqual(r["reason"], "invalid_output")

        class OutOfRangeAdapter(T.Adapter):
            def validate(self, bars, forecast):
                return {"decision": "CONFIRM", "p_up": 5.0, "confidence": 0.5}
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=OutOfRangeAdapter())
        self.assertEqual(r["reason"], "invalid_output")

    def test_fake_adapter_confirms_or_rejects_by_the_toy_rule(self):
        bars = [dict(b, obi={10: 0.5, 50: 0.25}) for b in self.bars]
        r = T.validate("BTCUSDT", 1, bars, OK_FORECAST, adapter=T.FakeAdapter())
        self.assertEqual(r["decision"], "CONFIRM")
        r = T.validate("BTCUSDT", 1, bars, DOWN_FORECAST, adapter=T.FakeAdapter())
        self.assertEqual((r["decision"], r["disagreement_reason"]),
                         ("REJECT", "obi_direction_disagrees"))


if __name__ == "__main__":
    unittest.main()
