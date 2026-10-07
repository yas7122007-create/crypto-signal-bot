# Analysis Bot V2.1: target architecture and status

Paper trading only. No component may create, cancel or modify orders, change leverage or margin, or withdraw.

## Pipeline

```
Binance Futures WebSocket
  -> Rust market data (market-data/)          Phase 1   implemented; live Binance run not yet verified
  -> Local L2 order book                      Phase 1   sync implemented
  -> Order-flow engine (CVD, OBI, microprice) Phase 2   implemented; live Binance run not yet verified
  -> Recorder + deterministic replay          Phase 3   implemented (gzip NDJSON)
  -> Python/Rust interface                    Phase 4   not started (gRPC only if justified)
  -> Feature engine + data-quality checks     Phase 5   not started
  -> PatchTST forecasting                     Phase 6   not started
  -> Deterministic strategy / risk gate       existing  engine.py, validate_market
  -> Toto final validation                    Phase 7   not started
  -> Nemotron reasoning / explanation         Phase 8   implemented on current evidence (reasoning.py)
  -> Fresh market revalidation -> paper signal -> Telegram / observability (Phase 9)
```

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

**Known limits.** Trade silence thresholds are a policy, not proof of a stall. Features are not yet consumed by Python (Phase 4).

## Current Nemotron evidence

`reasoning.evidence()` sends the engine features, entry/SL/TP, market gate output, rules and journal summary. `forecast.patchtst` and `forecast.toto` are `NOT_AVAILABLE` until Phases 6 and 7 land; the prompt forbids inventing them.
