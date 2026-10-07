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

## Phase 5: Toto validator (`v2/toto.py`, `v2/workers.py`)

A second, independent read on the same window and the PatchTST forecast, producing a
`toto.v1` object (`v2/contracts.py`): `CONFIRM`, `REJECT` (with a `disagreement_reason`), or
`HOLD`, plus `p_up` and `confidence`. It validates; it is never given veto power beyond what
Phase 6's ranking assigns it, and it never runs when there is no `ok` forecast to check
(`reason: "no_forecast_to_validate"`).

**Not wired up to a real model in this environment**, and this is reported accurately rather
than worked around: the `toto-ts` package needs HuggingFace to fetch its weights (blocked by
this container's network policy -- see Phase 2.5's smoke test) and pins a torch version that
would conflict with the one PatchTST already uses here. `Adapter` is the interface a real
model would implement; `WorkerAdapter` runs one out of process via `v2.workers.run` (a fixed
argv, JSON on stdin/stdout, a timeout -- exactly the isolation a conflicting torch version
would need) so a future real adapter does not have to share PatchTST's environment.
`FakeAdapter` is explicitly marked as a test-only stand-in (a toy OBI-direction rule) used to
prove `validate()`'s fail-closed wrapper actually works: a raising adapter, a timing-out
worker, and invalid or out-of-range output all become `status: "unavailable"` with a specific
`reason`, never a crash and never a fabricated decision. **NOT VERIFIED against a real Toto
model or real market data** -- only the plumbing is tested.

Running PatchTST and Toto "sequentially to reduce CPU contention" (the Phase 5 requirement)
falls out of the design rather than needing scheduling code: `v2.bridge.forecast()` and
`v2.toto.validate()` are both synchronous, in-process calls from the same caller, so nothing
here introduces concurrency between them; `v2.patchtst.configure()` already bounds
PyTorch's own thread count.

## Phase 5B: Nemotron CONFIRM/HOLD gate (`reasoning.confirm_gate`)

A second Nemotron entry point, `confirm_gate(candidate, history, now_ms=None)`, alongside the
existing `explain()`. It reuses `NemotronProvider`'s HTTP, retry, backoff and 202-polling
code (now parameterized by `system` and `parse`, `explain()`'s own default behavior and
tests unchanged) with a different system prompt (`GATE_SYSTEM`) and a different strict schema
(`decision` CONFIRM/HOLD, `confidence`, `rationale`, `risk_flags`; `parse_gate_result`). Every
failure path -- missing key (`DISABLED`), an unsupported provider (`ERROR`), a legacy
provider not wired to gating (`DEGRADED`), a timeout (`DEGRADED`), malformed JSON or an
API error (`ERROR`) -- returns `decision: "HOLD"`; the only way to get `"CONFIRM"` is a
parsed, schema-valid `CONFIRM` from the provider. `check.py`'s `gate_checks()` exercises
every one of these paths including the two real-looking HTTP replies (CONFIRM and HOLD).

### A documented conflict with the existing architecture

`reasoning.py`'s module docstring and the original PRD say the AI layer only explains a
decision that is "final and bukan wewenang Anda" (not its authority) -- Nemotron, through
`explain()`, can never change the action. Phase 5B's brief asks for the opposite: Nemotron as
"the final reasoning/validation layer" whose CONFIRM/HOLD gates whether a signal proceeds.

This is resolved the same way as the rest of V2: `confirm_gate()` exists and is fully tested,
but nothing calls it yet, and `V2_MODE` stays `off` by default. `engine.analyze()` and
`explain()`'s advisory behavior are completely unchanged; `check.py` passes exactly as before
Phase 5B. Phase 6 is where a `V2_MODE=on` ranking layer would actually call `confirm_gate()`
and use its decision to gate a candidate -- at which point the project is explicitly choosing
to let AI output affect which candidates reach Telegram, which the original PRD's wording
ruled out for `explain()`. Astra should treat this as a design decision that needs the
project owner's explicit sign-off before `V2_MODE` is ever set to anything but `off` in a
place a real signal could be issued, paper or otherwise.

## Phase 6: ranking and gating (`v2/ranking.py`)

Pure functions on plain dicts; no network, no database, nothing here can be affected by a
later Telegram failure because Telegram delivery happens strictly after and separately
(`services.deliver()` already persists the signal before attempting the network call and
only ever changes its own `delivery` field on failure -- see that function; this was true
before Phase 6 and is unchanged by it).

A candidate is whatever `engine.analyze()` and `engine.validate_market() already produced,
optionally carrying `forecast` (Phase 4), `toto` (Phase 5), and `gate` (Phase 5B, Nemotron's
`confirm_gate()`) dicts. A candidate with none of those three keys -- today's exact V1
output -- gates and scores identically to before: `gate()` always passes it and `score()`
returns its existing `rank` unchanged. This is checked directly
(`WithoutV2Evidence` in `tests/test_ranking.py`).

**Gates** (hard, each independently switchable in `GateRules`, each firing only on its own
evidence key): a stale PatchTST forecast (`reason: "stale_input"`), a Toto `REJECT`, or a
Nemotron gate decision other than `CONFIRM` (including the gate being unavailable --
fail-closed per Phase 5B) all reject the candidate outright. `engine.validate_market()`'s own
checks (spread, funding, net R/R, timing) are not re-derived here; `gate()` only confirms a
`market` key is present.

**Score**: `rank * (0.5 + 0.5 * model_confidence)`, where `model_confidence` is the mean of
whichever of PatchTST's `|p_up - 0.5| * 2`, Toto's `confidence`, and Nemotron's `confidence`
are actually present (`ok`/`OK` status). The modifier is bounded to `[0.5, 1.0]`: V2 evidence
can only lower a candidate below the deterministic engine's own score, by at most half, and
only when there is a reason to be less confident, not more. A real disagreement is `gate()`'s
job, not a score penalty.

**`model_confidence` vs `historical_hit_rate`, kept explicitly separate**: `model_confidence`
is an uncalibrated 0..1 summary of what the models themselves report; it has not been
checked against outcomes and must never be shown as a win probability.
`historical_hit_rate()` is the only calibrated number here -- the paper journal's own
`positive_fraction` -- and only once there are at least `rules.evaluation_samples` of them
(`None` otherwise). `rank_and_select()` returns at most 3 candidates (`MAX_CANDIDATES = 3`,
overridable), sorted by score, ties broken by `historical_hit_rate` then symbol.

**Not wired into `bot.scan()`.** `bot.py`'s live scan loop issues each candidate as it is
evaluated, one at a time, so adding a batch gate/rank/select step ahead of that loop is a
structural change to the one file every production run depends on. Given Phase 5B's flagged
conflict (confirm_gate can veto, where the current design says AI never does) and this
module's new hard gates, wiring this in is a deliberate decision for the project owner, not
an "ordinary implementation decision" to make unasked. The integration point, if adopted,
is: build the full `candidates` list exactly as `bot.scan()` does today, attach each
candidate's `forecast`/`toto`/`gate` (Phase 4/5/5B, when `V2_MODE` is `shadow`/`on`), then
call `rank_and_select()` once in place of the existing per-candidate loop's `sorted(...,
key=rank, reverse=True)` ordering, and only issue signals for what survives.

Telegram's 70%-style threshold, if ever added, must read `historical_hit_rate`, never
`model_confidence` -- the two are not interchangeable, and this module keeps them as
separate, separately-documented fields precisely so a future change cannot blur them
without touching this file's tests.
