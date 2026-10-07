# Bar contract, schema v1

One-minute bars aggregated from FeatureSnapshot rows (schema v2, [feature-schema.md](feature-schema.md)) inside the same pipeline that live ingestion and replay use, so replay reproduces them exactly. They are the input of the forecasting dataset and of the Python bridge. Implementation: `market-data/src/bars.rs`.

## Timing

A bar covers `[start_ms, end_ms)`, `end_ms = start_ms + 60000`, on the feature clock (`feature_ts_ms`, exchange time). It is emitted when the first feature row of a later bar arrives, so it never contains data from after `end_ms`. Close values are those of the bar's last row. A `session_start` discards open bars without emitting them, exactly as a stopped live process never emitted its last bar.

## Completeness

`complete` is true only when the whole bar came from one valid book:

| `incomplete_reason` | Cause |
|---|---|
| `row_gap` | More than 10 s without a feature row inside the bar, including from `start_ms` to the first row and from the last row to `end_ms`. Rows stop while the book is invalid (gap, resync, disconnect, stale depth) |
| `resync` | The book was rebuilt from a new snapshot inside the bar (`synced_since_seq` changed) |

Consumers must not use incomplete bars as model input or targets.

## Fields

| Field | Type | Null | Meaning |
|---|---|---|---|
| `v` | int | no | Bar schema version, `1` |
| `feature_v` | int | no | FeatureSnapshot schema version of the source rows |
| `symbol` | string | no | Uppercase symbol |
| `session_id` | int | yes | Recording session (see feature schema) |
| `start_ms`, `end_ms` | int | no | Bar window on exchange time |
| `rows` | int | no | Feature rows in the bar |
| `complete` | bool | no | See above |
| `incomplete_reason` | string | yes | First reason found |
| `synced_since_seq`, `close_seq` | int | no | Sync epoch and `seq` of the closing row |
| `open_mid`, `high_mid`, `low_mid`, `close_mid` | decimal | no | Mid price path |
| `close_microprice` | decimal | no | Microprice at the close |
| `mean_spread_bps` | decimal | no | Mean of `spread_bps` over the bar's rows |
| `close_spread_bps` | decimal | no | `spread_bps` at the close |
| `close_obi` | array | no | OBI at the close, as in the feature row |
| `close_trade_state` | string | no | `trade_state` at the close |
| `buy_qty`, `sell_qty` | decimal | yes | Aggressor buy and sell quantity in `(previous bar close, this close]`. Null unless the previous bar is the adjacent one, both closes are in the same CVD epoch, and the trade stream is not stale |

## Files

- `--bars-dir DIR` (record and replay): rotated gzip NDJSON `bars-<run ms>-<pid>-<counter>-<index>.ndjson.gz`, with the same `.partial` and cap rules as feature files.
- `--state-dir DIR` (record): `DIR/<SYMBOL>.json` = `{"v":1,"kind":"bar_window","symbol":...,"bars":[...]}` with the latest 256 bars of the current session, replaced atomically (temporary file plus rename) at every bar close. This is the live input of the Python bridge, which rejects it when stale.
