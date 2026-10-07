"""Developer-tool regressions; no market, network, or trained model calls."""
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SH = shutil.which("sh") or str(Path(os.environ.get("LOCALAPPDATA", "")) / "hermes/git/bin/sh.exe")


@unittest.skipUnless(Path(SH).is_file(), "sh not installed")
class AssemblyTool(unittest.TestCase):
    def run_script(self, selector="book_features", **overrides):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "scripts").mkdir()
            (root / "bin").mkdir()
            shutil.copy(ROOT / "market-data/scripts/inspect-asm.sh", root / "scripts")
            tools = {
                "cargo": """#!/bin/sh
exit_code=${CARGO_FAILURE:-0}
[ "$exit_code" -eq 0 ] || exit "$exit_code"
target=${CARGO_TARGET_DIR:-target}
mkdir -p "$target/release/deps"
touch "$target/release/deps/market_data-fixture.s" "$target/release/deps/market_data-fixture.o"
""",
                "llvm-objdump": """#!/bin/sh
exit_code=${OBJDUMP_FAILURE:-0}
[ "$exit_code" -eq 0 ] || exit "$exit_code"
if [ "$1" = -f ]; then
    echo "fixture.o: file format ${OBJECT_FORMAT:-elf64-x86-64}"
else
    printf '%s\n' 'fixture.o: file format elf64-x86-64' '' \
      '0000000000000000 <market_data::features::book_features>:' \
      '       0: callq 0x0' '                1: R_X86_64_PLT32 __rust_alloc-0x4' '' \
      '0000000000000020 <unrelated>:' '      20: retq' ''
fi
""",
            }
            for name, contents in tools.items():
                path = root / "bin" / name
                path.write_text(contents, encoding="utf-8", newline="\n")
                path.chmod(0o755)
            env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"])
            for name in ("CARGO_TARGET_DIR", "CARGO_BUILD_TARGET"):
                env.pop(name, None)
            env.update(overrides)
            return subprocess.run([SH, str(root / "scripts/inspect-asm.sh"), selector],
                                  env=env, capture_output=True, text=True, timeout=10)

    def test_substring_selects_function_and_counts_elf_allocation(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("alloc calls: 1", result.stdout)
        self.assertIn("== book_features: 1 instructions", result.stdout)

    def test_unmatched_selector_and_coff_are_errors(self):
        for options in (dict(selector="missing"), dict(OBJECT_FORMAT="coff-x86-64")):
            with self.subTest(options=options):
                result = self.run_script(**options)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertTrue(result.stderr.strip())

    def test_target_directory_and_command_failures(self):
        result = self.run_script(selector="<market_data::features::book_features>",
                                 CARGO_TARGET_DIR="custom-target")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("custom-target/asm-audit", result.stdout)
        for failure, code in (("CARGO_FAILURE", 17), ("OBJDUMP_FAILURE", 19)):
            result = self.run_script(**{failure: str(code)})
            self.assertEqual(result.returncode, code, result.stderr)


try:
    from scripts import bench_boundary as BOUNDARY
except ImportError:
    BOUNDARY = None


@unittest.skipIf(BOUNDARY is None, "numpy not installed")
class BoundaryTool(unittest.TestCase):
    def test_parse_stage_uses_preloaded_text_and_rejects_discontiguous_input(self):
        from tests.synthetic import raw_bars
        # Avoid loading/training the optional model: stop after the first real timing callable.
        model = types.SimpleNamespace(Forecaster=None, configure=None)
        with tempfile.TemporaryDirectory() as d, mock.patch.dict("sys.modules", {"v2.patchtst": model}):
            path = Path(d) / "BTCUSDT.json"
            bars = raw_bars(256)
            path.write_text(json.dumps(dict(v=1, kind="bar_window", symbol="BTCUSDT", bars=bars)))

            class Parsed(Exception):
                pass

            def first_stage(name, fn, n):
                self.assertEqual(name, "json.loads only")
                with mock.patch.object(Path, "read_text", side_effect=AssertionError("parse stage read a file")):
                    self.assertEqual(len(fn()["bars"]), 256)
                raise Parsed

            with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(BOUNDARY, "timed", first_stage):
                with self.assertRaises(Parsed):
                    BOUNDARY.main(path)
            bars[212]["start_ms"] = bars[211]["start_ms"] - 43 * 60_000
            bars[212]["end_ms"] = bars[212]["start_ms"] + 60_000
            path.write_text(json.dumps(dict(v=1, kind="bar_window", symbol="BTCUSDT", bars=bars)))
            with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(BOUNDARY, "timed", first_stage):
                with self.assertRaises(BOUNDARY.BR.BridgeError):
                    BOUNDARY.main(path)


@unittest.skipUnless(os.environ.get("HOTPATH_BENCH"), "set HOTPATH_BENCH to the built benchmark")
class HotpathTool(unittest.TestCase):
    def test_export_is_a_contiguous_256_bar_session(self):
        from v2.bars import validate
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "BTCUSDT.json"
            env = dict(os.environ, HOTPATH_DIFFS="401", HOTPATH_STATE_OUT=str(path))
            result = subprocess.run([os.environ["HOTPATH_BENCH"]], env=env,
                                    capture_output=True, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stderr)
            bars = [validate(b) for b in json.loads(path.read_text())["bars"]]
            self.assertEqual(len(bars), 256)
            self.assertIsNotNone(bars[0]["session_id"])
            self.assertEqual(len({b["session_id"] for b in bars}), 1)
            self.assertTrue(all(b["start_ms"] - a["start_ms"] == 60_000
                                for a, b in zip(bars, bars[1:])))
            self.assertIn("mean ns/op", result.stdout)

    def test_small_or_invalid_workloads_fail_with_clear_error(self):
        for value in ("0", "1", "400", "bogus"):
            result = subprocess.run([os.environ["HOTPATH_BENCH"]],
                                    env=dict(os.environ, HOTPATH_DIFFS=value),
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("HOTPATH_DIFFS", result.stderr)
            self.assertIn("401", result.stderr)


if __name__ == "__main__":
    unittest.main()
