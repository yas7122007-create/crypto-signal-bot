"""Fix 6 (adversarial-audit corrective pass): mutual exclusion, graduated skipping under
pressure, fail-closed behavior on the gate's own internal failure, and that none of this
affects anything when a caller never invokes it (V2_MODE="off")."""
from collections import deque
import gc
import threading
import time
import unittest
from unittest.mock import patch

from v2 import resource_gate as RG


class MutualExclusion(unittest.TestCase):
    def test_a_single_job_runs_and_returns_its_value(self):
        ran, result = RG.guarded("patchtst", lambda: 42)
        self.assertEqual((ran, result), (True, 42))

    def test_a_second_job_is_skipped_not_queued_while_one_is_in_flight(self):
        """One heavy job at a time: a second call arriving while the first holds the mutex
        must be skipped immediately (BUSY), never blocked waiting for a turn -- there is no
        queue for it to wait in."""
        order = []

        def slow():
            order.append("start")
            time.sleep(0.3)
            order.append("end")
            return "slow-done"

        results = []

        def run_slow():
            results.append(RG.guarded("patchtst", slow))

        t = threading.Thread(target=run_slow)
        t.start()
        time.sleep(0.05)  # let the slow job acquire the mutex first.
        second = RG.guarded("toto", lambda: "second-ran")
        t.join()
        self.assertEqual(second, (False, RG.BUSY))
        self.assertEqual(results[0], (True, "slow-done"))
        # The second call never ran concurrently with the first -- it was skipped outright.
        self.assertEqual(order, ["start", "end"])

    def test_the_mutex_is_released_after_a_run_so_the_next_call_is_not_stuck_forever(self):
        RG.guarded("patchtst", lambda: None)
        ran, result = RG.guarded("patchtst", lambda: "again")
        self.assertEqual((ran, result), (True, "again"))

    def test_the_mutex_is_released_even_when_fn_raises(self):
        with self.assertRaises(ValueError):
            RG.guarded("patchtst", lambda: (_ for _ in ()).throw(ValueError("boom")))
        # Still released: a later call is not permanently BUSY because of the failure above.
        ran, result = RG.guarded("patchtst", lambda: "ok")
        self.assertEqual((ran, result), (True, "ok"))


class GraduatedSkippingUnderPressure(unittest.TestCase):
    def test_toto_is_skipped_before_even_attempting_the_mutex(self):
        with patch.object(RG, "under_pressure", return_value=True):
            ran, reason = RG.guarded("toto", lambda: "should not run")
        self.assertEqual((ran, reason), (False, RG.PRESSURE))

    def test_patchtst_is_skipped_too_once_toto_already_was(self):
        """Escalating pressure: with pressure still present, PatchTST (checked once the
        mutex is held) is skipped the same way Toto already was."""
        with patch.object(RG, "under_pressure", return_value=True):
            toto = RG.guarded("toto", lambda: "should not run")
            patchtst = RG.guarded("patchtst", lambda: "should not run either")
        self.assertEqual(toto, (False, RG.PRESSURE))
        self.assertEqual(patchtst, (False, RG.PRESSURE))

    def test_without_pressure_both_jobs_run_normally(self):
        with patch.object(RG, "under_pressure", return_value=False):
            self.assertEqual(RG.guarded("toto", lambda: "t"), (True, "t"))
            self.assertEqual(RG.guarded("patchtst", lambda: "p"), (True, "p"))

    def test_a_pressure_source_that_cannot_be_measured_is_not_treated_as_pressure(self):
        """getloadavg()/the meminfo file not existing on this host is a normal, expected
        condition (e.g. in a sandboxed container), not pressure."""
        with patch("os.getloadavg", side_effect=AttributeError("not supported")), \
             patch("builtins.open", side_effect=OSError("no such file")):
            self.assertFalse(RG.under_pressure())


class FailClosedOnInternalFailure(unittest.TestCase):
    def test_an_internal_exception_skips_the_job_rather_than_running_it_unguarded(self):
        ran_fn = []
        with patch.object(RG, "under_pressure", side_effect=RuntimeError("gate bug")):
            result = RG.guarded("patchtst", lambda: ran_fn.append(1))
        self.assertEqual(result, (False, RG.GATE_ERROR))
        self.assertEqual(ran_fn, [])  # fn() was never called.

    def test_an_internal_exception_never_propagates_to_the_caller(self):
        with patch.object(RG, "under_pressure", side_effect=RuntimeError("gate bug")):
            try:
                RG.guarded("patchtst", lambda: "x")
            except Exception as exc:  # pragma: no cover - the assertion is that this never fires.
                self.fail(f"guarded() must not raise on its own account, raised {exc!r}")

    def test_fn_raising_once_the_gate_itself_is_fine_still_propagates(self):
        """The gate's job is to fail closed on ITS OWN bugs, not to swallow the model call's
        own exceptions -- callers already handle those (patchtst_forecast()/
        toto_evidence() both wrap their bridge calls)."""
        with self.assertRaises(ValueError):
            RG.guarded("patchtst", lambda: (_ for _ in ()).throw(ValueError("model broke")))

    def test_the_mutex_is_not_left_held_after_a_gate_error(self):
        with patch.object(RG, "under_pressure", side_effect=RuntimeError("gate bug")):
            RG.guarded("patchtst", lambda: None)
        ran, result = RG.guarded("patchtst", lambda: "still works")
        self.assertEqual((ran, result), (True, "still works"))


class V1Unaffected(unittest.TestCase):
    def test_v2_mode_off_never_reaches_this_module(self):
        """reasoning.patchtst_forecast()/toto_evidence() check V2_MODE before importing
        v2.resource_gate at all; this gate has no code path that runs when V2_MODE="off"."""
        import os
        import reasoning
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("V2_MODE", None)
            with patch.object(RG, "guarded", side_effect=AssertionError("resource_gate touched under off")):
                self.assertEqual(reasoning.patchtst_forecast({"symbol": "BTCUSDT"}, 0), "NOT_AVAILABLE")
                self.assertEqual(reasoning.toto_evidence({"symbol": "BTCUSDT"}, {"status": "ok"}, 0),
                                 "NOT_AVAILABLE")

    def test_no_unbounded_queue_exists(self):
        """There is exactly one mutex and no growable container anywhere in this module: a
        skipped job leaves no trace to grow unbounded. Checks by TYPE, not by name (a
        name-only check would pass trivially if an unbounded structure existed under some
        other name), and proves it behaviorally by running many more jobs than any
        plausible queue size and confirming nothing accumulates."""
        growable = (list, dict, set, deque)
        module_containers = {name: value for name, value in vars(RG).items()
                             if not name.startswith("__") and isinstance(value, growable)}
        self.assertEqual(module_containers, {})
        self.assertIsInstance(RG._LOCK, type(threading.Lock()))
        gc.collect()
        before = len(gc.get_objects())
        for _ in range(500):
            RG.guarded("patchtst", lambda: None)
        with patch.object(RG, "under_pressure", return_value=True):
            for _ in range(500):
                RG.guarded("toto", lambda: self.fail("must not run under pressure"))
        gc.collect()
        after = len(gc.get_objects())
        # Heap object count after 500 runs/skips, once garbage-collected, should not have
        # grown by anything close to 500 -- a real queue holding one entry per call would
        # show a clear linear trend; noise from the test harness itself is tolerated with a
        # generous margin.
        self.assertLess(after - before, 100)


if __name__ == "__main__":
    unittest.main()
