# Analysis Bot V2.1: target architecture and status

Paper trading only. No component may create, cancel or modify orders, change leverage or margin, or withdraw.

## Pipeline

```
Binance Futures WebSocket
  -> Rust market data (market-data/)          Phase 1   implemented; live Binance run not yet verified
  -> Local L2 order book                      Phase 1/2 sync implemented, order-flow features pending
  -> Order-flow engine (CVD, OBI, microprice) Phase 2   aggressor volume only
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

## Current Nemotron evidence

`reasoning.evidence()` sends the engine features, entry/SL/TP, market gate output, rules and journal summary. `forecast.patchtst` and `forecast.toto` are `NOT_AVAILABLE` until Phases 6 and 7 land; the prompt forbids inventing them.
