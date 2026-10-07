"""Toto validator: contract bounds, the subprocess worker runner, and fail-closed
validate() (timeout, invalid output, a raising adapter, no model, no forecast)."""
import json
import os
import sys
import tempfile
import textwrap
import time
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
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "worker.py"
            path.write_text(script)
            out = W.run([sys.executable, str(path)], {"x": 42}, timeout_s=10)
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


class WorkerTimeoutKillsTheWholeTree(unittest.TestCase):
    """Review finding: a timeout must not orphan processes a wrapper-style worker command
    started (e.g. a shell script running the real model), or a model keeps running after
    ResourceGate has released its mutex."""

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX-only")
    def test_grandchild_is_killed_on_timeout(self):
        with tempfile.TemporaryDirectory() as d:
            pidfile = Path(d) / "grandchild.pid"
            wrapper = Path(d) / "wrapper.sh"
            wrapper.write_text(f"#!/bin/sh\n{sys.executable} -c 'import os, time; "
                               f"open(\"{pidfile}\", \"w\").write(str(os.getpid())); time.sleep(30)'\n")
            wrapper.chmod(0o755)
            with self.assertRaises(W.WorkerError):
                W.run([str(wrapper)], {}, timeout_s=1)
            pid = int(pidfile.read_text())
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and _alive(pid):
                time.sleep(0.05)
            self.assertFalse(_alive(pid), "grandchild model process survived the timeout")


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A killed-but-unreaped process is a zombie: dead for our purposes.
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


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
                return {"decision": "CONFIRM", "p_up": 5.0, "confidence": 0.5,
                        "model_version": "sha256:" + "a" * 64}
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=OutOfRangeAdapter())
        self.assertEqual(r["reason"], "invalid_output")

    def test_fake_adapter_confirms_or_rejects_by_the_toy_rule(self):
        bars = [dict(b, obi={10: 0.5, 50: 0.25}) for b in self.bars]
        r = T.validate("BTCUSDT", 1, bars, OK_FORECAST, adapter=T.FakeAdapter())
        self.assertEqual(r["decision"], "CONFIRM")
        r = T.validate("BTCUSDT", 1, bars, DOWN_FORECAST, adapter=T.FakeAdapter())
        self.assertEqual((r["decision"], r["disagreement_reason"]),
                         ("REJECT", "obi_direction_disagrees"))


SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64
REVISION = "revision:" + "0123456789abcdef0123456789abcdef01234567"


class ModelVersionProvenance(unittest.TestCase):
    """Final hardening, Finding 2: a real (non-test) adapter's result is only valid when the
    adapter itself reports an immutable model identity. There is no fallback: a missing,
    blank, non-string, overlong or malformed identity makes the result unavailable, and the
    worker's launch command can never stand in for the model's identity."""

    def setUp(self):
        self.bars = [validate_bar(b) for b in raw_bars(5)]
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "worker.py"
        # One fixed argv for every test: "same command", only the checkpoint changes.
        self.adapter = T.WorkerAdapter([sys.executable, str(self.path)], timeout_s=10)

    def tearDown(self):
        self.tmp.cleanup()

    def run_worker_reporting(self, reply):
        self.path.write_text(textwrap.dedent(f"""
            import json, sys
            json.load(sys.stdin)
            print(json.dumps({reply!r}))
        """))
        return T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=self.adapter)

    def reply(self, **extra):
        return dict(decision="CONFIRM", p_up=0.7, confidence=0.8, **extra)

    def test_same_command_different_checkpoint_yields_different_version(self):
        r1 = self.run_worker_reporting(self.reply(model_version=SHA_A))
        r2 = self.run_worker_reporting(self.reply(model_version=SHA_B))
        self.assertEqual((r1["status"], r1["model_version"]), ("ok", SHA_A))
        self.assertEqual((r2["status"], r2["model_version"]), ("ok", SHA_B))

    def test_valid_revision_identity_is_accepted(self):
        r = self.run_worker_reporting(self.reply(model_version=REVISION))
        self.assertEqual((r["status"], r["decision"], r["model_version"]), ("ok", "CONFIRM", REVISION))

    def test_missing_model_version_fails_closed(self):
        r = self.run_worker_reporting(self.reply())
        self.assertEqual((r["status"], r["reason"], r["decision"]),
                         ("unavailable", "model_version_missing", None))

    def test_blank_non_string_overlong_or_malformed_model_version_fails_closed(self):
        bad_values = ["", "   ", 123, None, ["x"], {"sha": 1},
                      "sha256:" + "a" * 63,              # too short
                      "sha256:" + "a" * 65,              # too long
                      "sha256:" + "A" * 64,              # uppercase hex
                      "sha256:" + "a" * 64 + "\n",      # trailing control char
                      "x" * 10_000,                      # overlong
                      "weights-v2", "latest", "main",    # mutable/free-form labels
                      "test:fake-adapter-v1"]            # the fake identity, from a real worker
        for bad in bad_values:
            reply = self.reply(model_version=bad)
            r = self.run_worker_reporting(reply)
            expected = "model_version_missing" if bad is None else "invalid_model_version"
            with self.subTest(model_version=bad):
                self.assertEqual((r["status"], r["reason"]), ("unavailable", expected))
                self.assertIsNone(r["decision"])

    def test_command_hash_alone_cannot_satisfy_real_identity(self):
        """The command hash reasoning.toto_evidence() used to substitute is 16 hex chars:
        neither a worker reporting it nor validate()'s own signature can turn it into an
        accepted model identity any more."""
        import hashlib
        cmd_hash = hashlib.sha256(b"python worker.py").hexdigest()[:16]
        r = self.run_worker_reporting(self.reply(model_version=cmd_hash))
        self.assertEqual((r["status"], r["reason"]), ("unavailable", "invalid_model_version"))
        r = self.run_worker_reporting(self.reply())
        self.assertNotEqual(r["model_version"], cmd_hash)
        import inspect
        self.assertNotIn("model_version", inspect.signature(T.validate).parameters)

    def test_fake_adapter_is_explicitly_labelled_test_only(self):
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=T.FakeAdapter())
        self.assertEqual(r["model_version"], T.FAKE_MODEL_VERSION)
        self.assertTrue(r["model_version"].startswith("test:"))
        self.assertIsNone(T.MODEL_IDENTITY.fullmatch(T.FAKE_MODEL_VERSION))

    def test_fake_adapter_cannot_claim_a_real_identity(self):
        class SneakyFake(T.FakeAdapter):
            def validate(self, bars, forecast):
                return dict(decision="CONFIRM", p_up=0.7, confidence=0.8, model_version=SHA_A)
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=SneakyFake())
        self.assertEqual(r["model_version"], T.FAKE_MODEL_VERSION)

    def test_worker_timeout_and_error_still_fail_closed(self):
        sleepy = T.WorkerAdapter([sys.executable, "-c", "import time; time.sleep(5)"], timeout_s=0.2)
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=sleepy)
        self.assertEqual((r["status"], r["reason"]), ("unavailable", "timeout"))
        failing = T.WorkerAdapter([sys.executable, "-c", "import sys; sys.exit(2)"], timeout_s=10)
        r = T.validate("BTCUSDT", 1, self.bars, OK_FORECAST, adapter=failing)
        self.assertEqual((r["status"], r["reason"]), ("unavailable", "worker_failed"))


if __name__ == "__main__":
    unittest.main()
