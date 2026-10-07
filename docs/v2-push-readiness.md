# V2 native refinement: GitHub readiness

Verified locally on 2026-10-07. Ready to push `fix/v2-push-readiness` for review. GitHub Actions results require publishing the branch and opening a pull request.

Starting refinement: `758165ae3701003b555e43f30cd50e4c03b35b1d`. Runtime comparison baseline: `4089182480f696712cc1beb4a258b4332341e337`. Fetched `origin/main` (`fa732e78439f10354c4b5c0bee78439e70a2dbd8`) is an ancestor of this branch.

## Corrections

- Each state-write benchmark pass has its own identically warmed state. Exported bars remain contiguous.
- Invalid `HOTPATH_DIFFS` values and workloads below 401 exit with a clear error. Timing labels distinguish individual timer overhead from batch measurements.
- Assembly substring selectors work, unmatched selectors fail, and unsupported object formats are rejected. Instruction counts exclude relocation rows. The script supports native x86-64 ELF and respects `CARGO_TARGET_DIR`.
- Python boundary timing parses preloaded text and validates the original export before reporting. Temporary benchmark copies are cleaned up.
- Windows worker fixtures use quoted argv, the load-average mock supports Windows, and the echo-worker fixture cleans up its temporary directory.
- The ML CI job installs runtime and ML dependencies. Git preserves LF in shell scripts and NDJSON fixtures, keeping the golden byte assertion intact.

The readiness correction changes developer tooling, tests, CI, and documentation. Bot authority, V2 modules, runtime Rust sources, dependency requirements, and the lockfile retain their audited contents. V2 still defaults to off.

## Verification

| Check | Result |
|---|---|
| Rust formatting | PASS |
| Clippy, locked, all targets, warnings denied | PASS |
| Rust tests | 64 passed; one throughput test excluded from the normal run |
| Explicit release throughput test | PASS: 1,200,000 events, 200,000 features, 2.202 seconds |
| Python with CPU Torch | 119 tests, OK; four platform/native skips |
| Supplemental WSL tests | Seven passed; one NumPy skip. All four Windows skips executed successfully here |
| `check.py` with NumPy and Torch | ALL CHECKS PASSED |
| Python syntax | 32 files passed |
| Native benchmark export and invalid workloads | PASS; original timestamp reversal and zero-workload panic reproduced before correction |
| Actual ELF assembly inspection | Substring, default selectors, and unmatched-selector rejection passed |
| Credential-pattern scan / tracked `.env` files | Zero matches / zero files |
| Independent diff review and whitespace check | PASS |

Rust ran in Ubuntu WSL2 using Rust 1.99.0, LLVM 23.1.1, and GCC 15.2.0. Python ran on Windows using Python 3.12.10, NumPy 2.5.3, and Torch 2.14.1+cpu. The Python runs together cover all 119 tests; the Windows run alone retains four skips.

### Native replay and byte compatibility

Both optimized CLI binaries replayed the same deterministic recording: 20,000 depth diffs, 60,000 aggregate trades, 100,000 book tickers, one connection, and one snapshot. Each side produced 20,000 feature rows and 33 closed bars from 180,002 events. CLI reports matched raw bytes without normalization; feature and bar files matched after decompression.

| Artifact | SHA-256 |
|---|---|
| Decompressed recording | `2e0101ae416ddf7d955101ffbf2e451a19e513ac1cf0f96436cac0c28e61471a` |
| Decompressed features | `343c440381af9ec30aa42480457fa116ce18ea0bfbf3c9bb2f02c421dd8f38f9` |
| Decompressed bars | `c0f2099036d2eecd410f09e18b44b8ea827d87fdfa7703704aa8c19c01158eb7` |
| Raw CLI report | `1f00da70e42aee19994d8e4d06f37ca62b1bbf9bf561663c386b906b1d695b3a` |
| State export, all six runs | `f55d2c32dd547eb6dd828e787d3a03e64ee3004b911d25b659745ddaa1bccab1` |

Each state export contains 256 contiguous BTCUSDT bars and is 147,253 bytes. State compatibility was checked through both runtimes' benchmark writers; the replay CLI does not publish a state file.

### Local performance measurements

Three paired runs used the same corrected harness on both runtimes, separate release build directories, `HOTPATH_DIFFS=20000`, and the WSL `/tmp` filesystem. Pair order was baseline/refined, refined/baseline, baseline/refined. `RUSTFLAGS` and `CARGO_ENCODED_RUSTFLAGS` were absent. Baseline production sources and lockfile were unchanged; only the benchmark harness and its Cargo declaration were added to the comparison checkout.

| Range of three run means | Baseline | Refined |
|---|---:|---:|
| State write, 256 bars, fsync | 13.394–14.414 ms | 1.111–1.245 ms |
| L2 update, 20 levels | 1.821–2.020 µs | 1.780–4.739 µs |
| Pipeline, all events | 1.007–1.327 µs | 1.006–1.370 µs |
| Benchmark wall time | 9.27–9.50 s | 5.13–5.18 s |
| Peak RSS | 181,936–181,964 KiB | 181,924–181,952 KiB |

These measurements support a faster local state write. L2 and pipeline ranges support no improvement claim. Total benchmark time includes hundreds of state warm-up writes; it does not establish the same reduction in live ingestion time or VPS disk latency.

The Python boundary benchmark accepted the actual Rust export and exited successfully. Mean JSON parsing was 1.107 ms; read/parse/validation 4.601 ms; window construction 0.237 ms; PatchTST prediction 0.686 ms; cached bridge 5.167 ms. Echo-worker process round trips averaged 124.225 ms with 256 bars and 110.021 ms empty; these exclude real Toto inference.

The cached bridge returned `unavailable / model_output_invalid`: the benchmark's synthetic training distribution differs from the Rust workload, producing an out-of-bounds expected return. The contract correctly rejected it. Successful trained-model forecast behavior is covered by the unit suite; these timings establish no model accuracy or profitability.

## Reproduction and scope

From the repository root, run `python -B check.py` and `python -B -m unittest discover -s tests -v` in an environment with runtime and optional ML requirements. From `market-data`, run:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test --release --locked --test features throughput_on_synthetic_stream -- --ignored --nocapture
HOTPATH_DIFFS=20000 HOTPATH_KEEP=1 HOTPATH_STATE_OUT=/tmp/v2-state.json cargo bench --locked --bench hotpath
```

The benchmark prints its retained output directory. From `market-data`, replay its `rec` directory with `cargo run --release --locked -- replay --input DIR/rec --audit --features-dir FEATURES --bars-dir BARS`. To repeat the baseline comparison, use the baseline commit above with the same current harness and bench declaration, and a separate `CARGO_TARGET_DIR`. `scripts/inspect-asm.sh book_features` requires `cargo` and `llvm-objdump` on PATH. From the repository root, `python scripts/bench_boundary.py /tmp/v2-state.json` exercises the Python boundary.

Detailed local logs, commands, hashes, exported states, and native recordings are retained under gitignored `runtime/verification/`. The original WSL temporary recording disappeared before archival; a separate persistent regeneration reproduced its recording digest and replay/state hashes. Regeneration timings are excluded from the paired measurements. The Windows application-control policy was preserved; native verification used WSL.

This signoff covers a GitHub review branch. It does not enable V2 or deploy services. Existing research/operational limits remain: standalone ranking/gating/evaluation is unwired; evaluation target/sign/horizon alignment needs separate review; worker output is uncapped; PatchTST has no hard inference timeout; Windows worker cleanup covers the direct child. Real model identity, model quality, live public-stream rates, and deployment disk/queue behavior need their own evidence before operational use.
