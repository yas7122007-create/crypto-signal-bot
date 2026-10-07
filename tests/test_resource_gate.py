"""ResourceGate: one heavy model job at a time, graduated skipping under host pressure, and
fail-closed on every gate failure -- invalid configuration, unexpected telemetry errors, an
unsupported platform, or a bug in the gate itself never lets a heavy job run."""
from collections import deque
import gc
import math
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from v2 import resource_gate as RG

HEALTHY = dict(load=0.1, free_mb=64_000.0)


def host(load, free_mb):
    """Patch both measurements to fixed values (load per core, free RAM in MB)."""
    return patch.multiple(RG, _load_per_core=lambda: load, _free_mb=lambda: free_mb,
                          _linux=lambda: True)


def healthy():
    return host(**HEALTHY)


class Ran:
    """A heavy-job stand-in that records whether it was executed."""

    def __init__(self, value="ran"):
        self.calls, self.value = 0, value

    def __call__(self):
        self.calls += 1
        return self.value


def env(**values):
    return patch.dict(os.environ, {k: str(v) for k, v in values.items()})


class InvalidConfigFailsClosed(unittest.TestCase):
    def assert_blocked(self, **values):
        job = Ran()
        with env(**values), healthy():
            self.assertEqual(RG.check(), RG.INVALID_CONFIG)
            for kind in RG.JOBS:
                self.assertEqual(RG.guarded(kind, job), (False, RG.INVALID_CONFIG))
        self.assertEqual(job.calls, 0)

    def test_non_numeric_cpu_threshold(self):
        self.assert_blocked(V2_RESOURCE_MAX_LOAD_PER_CORE="bogus")

    def test_non_numeric_ram_threshold(self):
        self.assert_blocked(V2_RESOURCE_MIN_FREE_MB="bogus")

    def test_nan_infinity_zero_negative_and_blank_thresholds(self):
        for name in ("V2_RESOURCE_MAX_LOAD_PER_CORE", "V2_RESOURCE_MIN_FREE_MB"):
            for bad in ("nan", "NaN", "inf", "-inf", "Infinity", "0", "-1", "", "  ", "1e400"):
                with self.subTest(name=name, value=bad):
                    self.assert_blocked(**{name: bad})

    def test_defaults_and_valid_overrides_are_accepted(self):
        job = Ran()
        with healthy():
            self.assertEqual(RG.check(), RG.OK)
        with env(V2_RESOURCE_MAX_LOAD_PER_CORE="2.5", V2_RESOURCE_MIN_FREE_MB="256"), healthy():
            self.assertEqual(RG.guarded("patchtst", job), (True, "ran"))
        self.assertEqual(job.calls, 1)


class TelemetryErrorFailsClosed(unittest.TestCase):
    def assert_blocked(self, reason=RG.TELEMETRY_ERROR):
        job = Ran()
        self.assertEqual(RG.check(), reason)
        for kind in RG.JOBS:
            self.assertEqual(RG.guarded(kind, job), (False, reason))
        self.assertEqual(job.calls, 0)

    def test_unexpected_load_measurement_exception(self):
        for exc in (OSError("load unobtainable"), RuntimeError("boom"), ZeroDivisionError()):
            with self.subTest(exc=exc), patch.object(RG, "_linux", return_value=True), \
                    patch("os.getloadavg", side_effect=exc), \
                    patch.object(RG, "_free_mb", return_value=64_000.0):
                self.assert_blocked()

    def test_unexpected_ram_measurement_exception(self):
        with patch.object(RG, "_linux", return_value=True), \
                patch.object(RG, "_load_per_core", return_value=0.1), \
                patch.object(RG, "MEMINFO", "/nonexistent/meminfo"):
            self.assert_blocked()

    def test_malformed_proc_meminfo(self):
        cases = ["", "MemTotal: 100 kB\n", "MemAvailable: lots kB\n", "MemAvailable:\n",
                 "MemAvailable: -5 kB\n", "MemAvailable: nan kB\n", "\x00\xff garbage"]
        for content in cases:
            with self.subTest(content=content), tempfile.NamedTemporaryFile("w", delete=False) as f:
                f.write(content)
            try:
                with patch.object(RG, "_linux", return_value=True), \
                        patch.object(RG, "_load_per_core", return_value=0.1), \
                        patch.object(RG, "MEMINFO", f.name):
                    self.assert_blocked()
            finally:
                os.unlink(f.name)

    def test_non_finite_or_negative_measurements(self):
        for load, free in ((math.nan, 1e5), (math.inf, 1e5), (-1.0, 1e5), (0.1, math.nan),
                           (0.1, -1.0), (0.1, math.inf)):
            with self.subTest(load=load, free=free), host(load, free):
                self.assert_blocked()

    def test_real_linux_telemetry_parses(self):
        """On this (Linux) host, the real measurement path reaches a verdict, not an error."""
        if not RG._linux():
            self.skipTest("Linux-only")
        self.assertIn(RG.check(), (RG.OK, RG.PRESSURE, RG.SEVERE))


class UnsupportedPlatform(unittest.TestCase):
    def test_unsupported_source_is_distinct_and_fails_closed(self):
        job = Ran()
        with patch.object(RG, "_linux", return_value=False):
            self.assertEqual(RG.check(), RG.UNSUPPORTED)
            self.assertEqual(RG.guarded("patchtst", job), (False, RG.UNSUPPORTED))
        self.assertEqual(job.calls, 0)
        self.assertNotEqual(RG.UNSUPPORTED, RG.TELEMETRY_ERROR)


class GraduatedPressure(unittest.TestCase):
    def test_healthy_host_runs_both_jobs(self):
        with healthy():
            self.assertEqual(RG.guarded("toto", Ran("t")), (True, "t"))
            self.assertEqual(RG.guarded("patchtst", Ran("p")), (True, "p"))

    def test_pressure_skips_toto_but_not_patchtst(self):
        toto, patchtst = Ran(), Ran()
        for load, free in ((2.0, 64_000.0), (0.1, 400.0)):  # CPU pressure, then RAM pressure
            with self.subTest(load=load, free=free), host(load, free):
                self.assertEqual(RG.check(), RG.PRESSURE)
                self.assertEqual(RG.guarded("toto", toto), (False, RG.PRESSURE))
                self.assertEqual(RG.guarded("patchtst", patchtst), (True, "ran"))
        self.assertEqual(toto.calls, 0)

    def test_severe_pressure_skips_patchtst_too(self):
        job = Ran()
        for load, free in ((4.0, 64_000.0), (0.1, 100.0)):
            with self.subTest(load=load, free=free), host(load, free):
                self.assertEqual(RG.check(), RG.SEVERE)
                self.assertEqual(RG.guarded("toto", job), (False, RG.SEVERE))
                self.assertEqual(RG.guarded("patchtst", job), (False, RG.SEVERE))
        self.assertEqual(job.calls, 0)


class MutualExclusion(unittest.TestCase):
    def test_a_second_job_is_skipped_not_queued_while_one_is_in_flight(self):
        order, results = [], []

        def slow():
            order.append("start")
            time.sleep(0.3)
            order.append("end")
            return "slow-done"

        with healthy():
            t = threading.Thread(target=lambda: results.append(RG.guarded("patchtst", slow)))
            t.start()
            time.sleep(0.05)
            second = Ran()
            self.assertEqual(RG.guarded("patchtst", second), (False, RG.BUSY))
            t.join()
        self.assertEqual(results, [(True, "slow-done")])
        self.assertEqual(order, ["start", "end"])
        self.assertEqual(second.calls, 0)

    def assert_mutex_free(self):
        self.assertTrue(RG._LOCK.acquire(blocking=False), "mutex left held")
        RG._LOCK.release()

    def test_released_after_success(self):
        with healthy():
            RG.guarded("patchtst", Ran())
        self.assert_mutex_free()

    def test_released_after_model_exception_which_propagates(self):
        def broken():
            raise ValueError("model broke")
        with healthy(), self.assertRaises(ValueError):
            RG.guarded("patchtst", broken)
        self.assert_mutex_free()

    def test_released_after_gate_error_and_job_not_run(self):
        job = Ran()
        with healthy(), patch.object(RG, "check", side_effect=RuntimeError("gate bug")):
            self.assertEqual(RG.guarded("toto", job), (False, RG.GATE_ERROR))
        self.assertEqual(job.calls, 0)
        self.assert_mutex_free()

    def test_released_after_every_rejection(self):
        with env(V2_RESOURCE_MIN_FREE_MB="bogus"), healthy():
            RG.guarded("patchtst", Ran())
        with host(9.0, 1.0):
            RG.guarded("patchtst", Ran())
        self.assert_mutex_free()

    def test_unknown_job_kind_is_a_gate_error(self):
        job = Ran()
        with healthy():
            self.assertEqual(RG.guarded("mystery", job), (False, RG.GATE_ERROR))
        self.assertEqual(job.calls, 0)


class V1UnaffectedAndBounded(unittest.TestCase):
    def test_v2_mode_off_never_reaches_this_module(self):
        import reasoning
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("V2_MODE", None)
            with patch.object(RG, "guarded", side_effect=AssertionError("gate touched under off")), \
                    patch.object(RG, "check", side_effect=AssertionError("gate touched under off")):
                self.assertEqual(reasoning.patchtst_forecast({"symbol": "BTCUSDT"}, 0), "NOT_AVAILABLE")
                self.assertEqual(reasoning.toto_evidence({"symbol": "BTCUSDT"}, {"status": "ok"}, 0),
                                 "NOT_AVAILABLE")

    def test_no_growable_state_in_the_module(self):
        growable = (list, dict, set, deque)
        found = {n: v for n, v in vars(RG).items() if not n.startswith("__") and isinstance(v, growable)}
        self.assertEqual(found, {})

    def test_500_rejected_calls_accumulate_nothing(self):
        job = Ran()
        gc.collect()
        before_objects, before_threads = len(gc.get_objects()), threading.active_count()
        with env(V2_RESOURCE_MAX_LOAD_PER_CORE="bogus"), healthy():
            for _ in range(250):
                RG.guarded("patchtst", job)
        with host(9.0, 1.0):
            for _ in range(250):
                RG.guarded("toto", job)
        gc.collect()
        self.assertEqual(job.calls, 0)
        self.assertLess(len(gc.get_objects()) - before_objects, 100)
        self.assertEqual(threading.active_count(), before_threads)


if __name__ == "__main__":
    unittest.main()
