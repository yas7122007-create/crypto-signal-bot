# Analysis Bot V2.1: target architecture and status

Paper trading only. No component may create, cancel or modify orders, change leverage or margin, or withdraw.

## Pipeline

```
Binance Futures WebSocket
  -> Rust market data (market-data/)          Phase 1   implemented; live Binance run not yet verified
  -> Local L2 order book                      Phase 1   sync implemented
  -> Order-flow engine (CVD, OBI, microprice) Phase 2   implemented; live Binance run not yet verified
  -> Recorder + deterministic replay          Phase 3   implemented (gzip NDJSON)
  -> Python/Rust interface                    Phase 4   implemented (v2/bridge.py); reachable from bot.scan() as advisory evidence when V2_MODE is shadow/on
  -> PatchTST forecasting                     Phase 3   implemented (v2/patchtst.py); live-reachable advisory evidence behind V2_MODE + ResourceGate
  -> Deterministic strategy / risk gate       existing  engine.py, validate_market
  -> Toto final validation                    Phase 5   implemented (v2/toto.py); no real model available here (needs HuggingFace + a conflicting torch version); live-reachable advisory evidence behind V2_MODE + V2_TOTO_WORKER_CMD + ResourceGate
  -> Nemotron reasoning / explanation         existing  advisory, implemented (reasoning.explain)
  -> Nemotron CONFIRM/HOLD gate               Phase 5B  implemented (reasoning.confirm_gate); research/shadow only, not called anywhere
  -> Deterministic ranking (max 3)            Phase 6   implemented (v2/ranking.py); not wired into bot.scan()
  -> Paper evaluation journal + metrics       Phase 7   implemented (v2/evaluate.py); not wired into bot.scan()
  -> Fresh market revalidation -> paper signal -> Telegram / observability (Phase 9)
```

See "Runtime status" below for exactly which of these are reachable from the live loop.

The phase order is a dependency order. Toto and PatchTST come later because their output has no measurable meaning without reliable, replayable upstream data, not because they are optional.

## Layer roles (no duplicated jobs)

| Layer | Job | Must not |
|---|---|---|
| Rust market data / order flow | Market-data truth: ingestion, L2 sync, sequence integrity, features | Make trading decisions |
| Python strategy / risk | Deterministic candidate, hard risk gate, paper simulation | Process raw 100 ms book streams |
| PatchTST | Time-series forecast: direction probability, expected move, volatility, confidence, horizon | Explain or validate |
| Toto | Machine-oriented validation that candidate + forecast are mutually consistent: validation score, directional consistency, contradiction score, uncertainty, reason codes | Resurrect a candidate that failed a hard rule |
| Nemotron 3 Super | Cross-evidence reasoning, contradiction and anomaly interpretation, operator explanation | Invent market facts, change action/entry/stop/target, block a deterministic signal on API failure |

## Authority order

1. Data integrity: stale data, sequence gaps, corrupted or crossed book
2. Hard deterministic risk rules: spread, funding, staleness, R/R after costs
3. Deterministic quantitative strategy
4. PatchTST forecast evidence
5. Toto validation (may lower confidence or veto per policy; never overrides 1 or 2)
6. Nemotron reasoning (advisory only)

A rejection at levels 1 or 2 is final; no model output can revive it.

## Evaluation before trusting a layer

Once enough replay data exists, measure each configuration on the same replayed events:

1. Quant only
2. Quant + PatchTST
3. Quant + PatchTST + Toto
4. Quant + PatchTST + Toto + Nemotron

Metrics: candidate precision, false-positive reduction, win rate, expectancy, average net R, drawdown, calibration, confidence reliability, rejection quality, contradiction detection, latency, and behavior in degraded modes. A layer that does not improve these stays off by default.

## Phase 2 order-flow features

Computed in `market-data/src/features.rs` inside the one `Pipeline` that both live ingestion and replay use, so replaying a recording reproduces the feature rows byte for byte. All arithmetic is exact `Decimal`; each division is rounded half-to-even to 16 decimals. A value that cannot be computed from trustworthy state is `null`.

**When a row is emitted.** After each depth diff or snapshot that leaves the book synced and uncrossed. No row while the book is buffering, after a sequence gap, a crossed or malformed update, a disconnect, or 30 s without a depth diff. Each row carries `seq` (recorder sequence of the depth event), `recv_ts_ns`, `exchange_ts_ms`, `feature_ts_ms`, `book_update_id` and `synced_since_seq` (the snapshot that started the current sync epoch).

| Feature | Definition |
|---|---|
| `mid` | `(best_bid + best_ask) / 2` |
| `spread`, `spread_bps` | `best_ask - best_bid`; `spread / mid * 10 000` |
| `microprice` | `(bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)`, top of book |
| `obi[N]` | `(B_N - A_N) / (B_N + A_N)`, `B_N`/`A_N` = total quantity of the best N price levels per side |
| `cvd` | Signed aggressor quantity since `cvd_since_ms`. `aggTrade.m == false` (buyer aggressed) adds `q`; `m == true` subtracts `q` |
| `deltas[W]` | Signed aggressor quantity of the trades received so far with `T` in `(t - W, t]`, `t = feature_ts_ms`. Depth `E` can run slightly ahead of trade delivery, so a trade still in flight lands in the next row; replay reproduces this exactly |

**Depth policy (OBI).** N counts price levels, not a price distance. `obi[N]` is `null` unless both sides hold at least N levels, so a thin or rebuilding book never reads as balanced. N is capped at 500. When the REST snapshot is cut at its 1000-level limit, the book remembers that side's deepest snapshot price; levels beyond it are known only if they changed since, so they are never counted. If price moves through that range, `obi[N]` becomes `null`, and the whole row stops once the best bid or ask itself lies outside it, until the next resync. Defaults are N = 10 and 50 (PRD `obi_top10`, `obi_top50`).

**CVD epochs and windows.** The epoch restarts at every point where trades may have been missed: session start, disconnect, an `aggTrade` id gap, or a malformed trade. Duplicate ids are ignored. `t` is the latest exchange time seen for the symbol (trade `T` or depth `E`) and never moves back. A window is reported only when `t - W` is at or after the start of contiguous coverage: the first trade of the epoch, moved forward when the memory cap evicts old trades. It never mixes epochs. Only a depth event that parsed moves the clock. `session_start` clears all per-symbol trade state, so replaying a directory with several runs gives each run the rows it wrote live. Defaults are 1 s, 5 s, 15 s and 1 m (PRD feature table).

**Bounds.** Each window keeps a running sum over one shared deque per symbol, so a trade or clock step costs amortized O(windows). The deque holds at most 100 000 trades; when it overflows, windows that still needed the evicted trades report `null` until they are covered again. OBI costs O(max N) per row. Measured on a synthetic stream in release mode: about 490 000 events/s including JSON parsing (`cargo test --release --test features -- --ignored`).

**Output, integrity, staleness.** Frozen in [feature-schema.md](feature-schema.md) (schema v2): rotated gzip storage with a file cap, the feature config recorded in `session_start` and enforced by replay, and `trade_state` so a stalled trade stream withholds CVD instead of reporting zeros.

**Known limits.** Trade silence thresholds are a policy, not proof of a stall. Features are consumed by Python only via v2/bridge.py (Phase 4), behind V2_MODE; with V2_MODE=shadow/on, bot.scan() reaches it through reasoning.evidence() as advisory input only (see "Runtime status").

## Current Nemotron evidence

`reasoning.evidence()` sends the engine features, entry/SL/TP, market gate output, rules and journal summary, plus `forecast.patchtst` (Phase 4's bridge) and `forecast.toto` (Phase 5's validator). Both are the string `"NOT_AVAILABLE"` when `V2_MODE` is `off` (the default), when the required configuration (`V2_STATE_DIR`, `V2_MODEL_DIR`, `V2_TOTO_WORKER_CMD`) is not set, when the ResourceGate skips the job, or on any failure. No real PatchTST deployment or Toto worker is configured anywhere today, so in practice both are `"NOT_AVAILABLE"`; that is a deployment fact, not a code-path guarantee. The explicit conflict Phase 5B's CONFIRM/HOLD gate raises against this document's "Nemotron... never overrides" rule is discussed in docs/forecasting.md and remains for the project owner/Astra to resolve; nothing calls that gate.

## Runtime status (authoritative; checked against the call graph)

Live call path: `bot.scan()` -> `reasoning.explain()` (for each candidate that passed the deterministic gates, non-legacy providers only) -> `reasoning.evidence()` -> `reasoning.patchtst_forecast()` and `reasoning.toto_evidence()`. `bot.py` imports nothing from `v2/` and does not call `confirm_gate`, ranking or paper evaluation.

| Category | Components | What it means |
|---|---|---|
| Live authoritative | `engine.analyze`, `validate_market`, risk rules, `new_signal`, the V1 paper journal | The only code that decides whether a signal exists, its action, entry, stop and target. Unchanged by any V2_MODE value. |
| Live-reachable advisory | PatchTST forecast, Toto validation | With `V2_MODE=shadow` or `on` and the required config set, their output is added to the evidence Nemotron explains. It is text Nemotron reads; it cannot change the action or levels. Its only indirect effect is latency (bounded by `V2_TOTO_TIMEOUT_SECONDS` and the gate), which runs before the fresh market revalidation and can therefore make that revalidation HOLD a signal. |
| Implemented but not wired | `reasoning.confirm_gate`, `v2/ranking.py` (max 3), `v2/evaluate.py` (paper evaluation) | Tested in isolation; no production caller. |
| Research/shadow only | Nemotron CONFIRM/HOLD | Not a production veto. Wiring it would need the owner's explicit decision (see docs/forecasting.md). |

`V2_MODE` semantics: `off` (default, and the value any invalid setting degrades to with a warning) is exactly V1: nothing in `v2/` is imported and both evidence fields are `"NOT_AVAILABLE"`. `shadow` and `on` currently behave identically on the live path: PatchTST and Toto may run and appear as advisory evidence. The only place the code distinguishes them is `confirm_gate`, which nothing calls.

ResourceGate (`v2/resource_gate.py`) runs every PatchTST and Toto call: one heavy job at a time process-wide (a second is skipped, never queued); Toto runs only on a confirmed-healthy host, PatchTST also under ordinary pressure, neither under severe pressure; invalid thresholds (`INVALID_CONFIG`), telemetry failures on Linux (`TELEMETRY_ERROR`) and non-Linux hosts (`UNSUPPORTED`) all skip both jobs. A skipped job is `"NOT_AVAILABLE"`, never an error in the scan.
