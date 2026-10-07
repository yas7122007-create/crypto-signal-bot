# PatchTST forecasting (Phase 3), offline only

`v2/` is a standalone package that forecasts the mid-price return over a fixed horizon from
recorded one-minute bars ([bar-schema.md](bar-schema.md)). It does not run live, does not
place orders, and does not yet feed the bot; Phase 4 defines the boundary that will. Install
`requirements-ml.txt` to use it; `v2/bars.py`, `v2/contracts.py` and `v2/dataset.py`'s
non-numpy pieces need only the standard library, so they can be imported anywhere.

## Dataset (`v2/dataset.py`)

A sample is made at `asof_ms`, the close of bar *t*. Its input is bars *t-window+1..t*
(`window`, default 32, minutes); its target is the log return in bps of the mid from bar
*t*'s close to bar *t+horizon*'s close (`horizon`, default 15 minutes, matching the bot's
signal candle). Only complete, time-contiguous bars of one symbol and one session are used;
a gap, a session boundary, or a bar marked incomplete removes every sample whose window or
target would cross it. Missing OBI or trade flow is never imputed: such a bar makes the whole
window unusable. `tests/test_dataset.py` checks this by mutating bars after `asof_ms` and
confirming the input is unchanged, and by mutating bars before it and confirming the target
is unchanged except through the one bar it is allowed to use.

Splits are by time, never shuffled (`time_split`, with a `walk_forward` alternative of
expanding folds). Both leave an embargo of one horizon between splits, since a training
target that extends into the validation period would leak validation-period information
backward.

## Input channels

Seven per-bar values (`CHANNELS` in `v2/dataset.py`), chosen to be few and explainable
rather than exhaustive:

| Channel | From |
|---|---|
| `ret_bps` | log return of the mid from the previous bar's close |
| `range_bps` | high-low range over the close |
| `spread_bps` | mean spread over the bar |
| `micro_disp_bps` | microprice displacement from the mid at the close |
| `obi` | order-book imbalance at the configured depth (default top 10) |
| `flow_imbalance` | `(buy - sell) / (buy + sell)` aggressor volume in the bar, 0 when no trades |
| `log_volume` | `log1p(buy + sell)` |

## Baselines (`v2/baselines.py`)

Zero (random walk), persistence (recent return continues), a moving average, and a ridge
regression on per-channel summaries (last value and window mean), all fit on the training
split only, with the residual RMS as a Gaussian sigma so every baseline is scored the same
way as PatchTST. A model that cannot beat `zero` and `persistence` on held-out NLL is not
useful.

## PatchTST (`v2/patchtst.py`)

A small encoder (patch 8, stride 4 over the 32-bar window; 2 layers, 4 heads, 32-dim
embedding; channel-independent, so each of the 7 channels is patched and encoded the same
way) whose head outputs mu and log-sigma of the standardized target, trained with Gaussian
NLL and early-stopped on validation NLL. P(up) = Phi(mu/sigma). Inputs and target are
standardized with train-only statistics saved with the model. `configure()` fixes the thread
count, the random seeds and deterministic algorithms, so two runs with the same seed produce
identical weights (checked in `tests/test_forecasting.py`).

A saved model is a directory of `weights.pt` (tensors only, loaded with `weights_only=True`),
`model.json` (spec, hyperparameters, scalers, training record) and `VERSION`, the SHA-256 of
both files; loading recomputes and compares the hash and refuses a model trained for a
different dataset spec or channel set.

## Output contract (`v2/contracts.py`, `forecast.v1`)

```json
{"contract": "forecast.v1", "status": "ok", "reason": null,
 "symbol": "BTCUSDT", "asof_ms": 1700000100000, "horizon_ms": 900000,
 "target": "mid_log_return_bps", "model": "patchtst", "model_version": "<16-hex>",
 "input": {"dataset_schema": 1, "bar_schema": 1, "feature_schema": 2, "window": 32,
           "channels": ["ret_bps", "..."]},
 "expected_return_bps": 3.1, "sigma_bps": 9.4, "p_up": 0.62}
```

`status` is `ok` or `unavailable` (with a short machine-readable `reason`, never free text);
an unavailable forecast carries no numeric fields, and no field outside this fixed set is
allowed, so a caller can rely on the schema without parsing prose. Bounds: `|expected_return_bps|`
and `sigma_bps` under 5000 bps, `p_up` in `[0, 1]`. `v2/contracts.py` is pure stdlib, usable
from Phase 4's Python bridge without pulling in torch.

## Running it

```
pip install -r requirements-ml.txt
python -m v2.train --bars-dir RECORDED_BARS_DIR --out OUT_DIR
```

Reports every baseline and PatchTST on the same held-out test split plus walk-forward folds,
as `OUT_DIR/report.json`, and saves the trained model to `OUT_DIR/model/`. `--skip-patchtst`
runs only the baselines (no torch needed). A `--bars-dir` containing a `SYNTHETIC` marker
file is reported with `"synthetic_data": true`; such a report evaluates a planted toy
relationship between OBI and next-minute return, used only to check the training and
evaluation code work, and says nothing about real markets.

## Measured, not claimed

On synthetic bars with the planted signal, one training run (`--epochs 30 --threads 2`, this
container, 6000 one-minute bars, dataset settled at window 32 / horizon 15 defaults replaced
by `--window 16 --horizon 1` for a smaller test): 5953 samples (4153 train / 879 val / 893
test), 7 epochs before early stop, 15.0 s training wall time, 0.10 ms/sample inference, 20738
parameters, peak RSS 795 MB. [Low confidence beyond "it ran and beat the baselines on this
synthetic set"]: test NLL 3.67 vs. 3.89 (moving average) and persistence's 3.9x; directional
accuracy 0.554 vs. 0.465-0.489 for the baselines. These numbers describe the toy fixture in
`tests/synthetic.py`, not Binance data; no real-market benchmark has been run (no recorded
bars exist in this container; see Phase 2.5's smoke-test result for why).

## What this is not

No live loop, no Binance calls, no order placement, no claim that any market relationship
PatchTST finds in real data would hold. Phase 4 defines how (and whether) a forecast from
this module reaches the existing Python engine, with fail-closed validation of everything
in the contract above.

## Phase 4: the Python integration boundary (`v2/bridge.py`)

`v2.bridge.forecast(state_path, symbol, horizon_ms, config, now_ms=None)` turns a
`--state-dir` bar-window file (docs/bar-schema.md) into a validated `forecast.v1` object, or
an `unavailable` one with a specific `reason`. It never raises into its caller for bad input;
only programmer errors (a wrong `BridgeConfig`) raise. Checks, in order: model present and
loadable (`model_unavailable`) -> requested horizon matches the model's trained horizon
(`horizon_mismatch`) -> state file readable, schema-valid, and one contiguous session
(`state_unreadable`) -> state's symbol matches the request (`symbol_mismatch`) -> state not
older than `BridgeConfig.max_stale_ms` (default 180 s, `stale_input`) -> enough bars for the
model's window (`insufficient_history`) -> the window itself has no missing OBI/flow
(`incomplete_window`) -> the model's own output is finite with positive sigma
(`model_output_invalid`). `tests/test_bridge.py` has one test per branch, including with a
real trained model.

### Wiring into the existing engine (`reasoning.py`)

`reasoning.evidence()` (used by Nemotron's explanation step; see Phase 5B) gained a
`patchtst_forecast()` call gated by `V2_MODE` (env var, default `off`):

- `off` (the default): `evidence()["forecast"]["patchtst"]` is exactly `"NOT_AVAILABLE"`,
  identical to Phase 2's behavior. `check.py` passes unchanged; nothing in `engine.py` or the
  deterministic decision changed.
- `shadow` / `on`: if `V2_STATE_DIR` and `V2_MODEL_DIR` are set, calls the bridge above for
  `candidate["symbol"]` with `V2_HORIZON_MS` (default 900000) and `V2_MAX_STALE_MS` (default
  180000). Any exception, including numpy/torch not being installed (`requirements-ml.txt`
  is optional), is caught and logged, never raised into Nemotron; the result is
  `"NOT_AVAILABLE"` or a `forecast.v1` dict.

This is wiring only: the deterministic engine (`engine.analyze`) is untouched, and Nemotron
still cannot change the action (Phase 5B keeps that true by construction). `shadow`/`on`
reaching an actual decision is Phase 6's job (ranking/gating); until then this only changes
what Nemotron is shown, and Nemotron is advisory. `check.py`'s new
`v2_bridge_checks()` covers `V2_MODE` validation and that every failure mode degrades to
`"NOT_AVAILABLE"` rather than crashing the scan loop, on hosts with and without the ML
extras installed.
