# FeatureSnapshot contract, schema v2

The contract between the Rust market-data engine and anything that consumes order-flow features: the dataset builder, the Python bridge and evaluation. It is frozen at `v = 2`. Changing a field name, type, unit or meaning requires a new version number. `market-data/tests/features.rs::schema_doc_matches_serialized_fields` fails if this table and the serialized row disagree.

Rows are one JSON object per line (NDJSON), stored as rotated gzip files `features-<run ms>-<index>.ndjson.gz` (see "Storage"). Decimal values are JSON strings holding exact decimals, never floats, so no precision is lost; parse them with a decimal type or knowingly convert.

## When a row exists

A row is emitted after a depth diff or depth snapshot only if, after applying it, the book is synced and uncrossed. There is no row:

- while the book is buffering diffs before its snapshot;
- after a sequence gap (`pu` mismatch), until the resync snapshot is applied;
- after a crossed, malformed or out-of-range update;
- after a disconnect, until reconnect and resync;
- after 30 s without a depth diff (stale depth stream), by recorded receive time.

So a row's book features are always from a valid book. The absence of rows is itself a signal: consumers must treat a gap in rows as "no valid data", never interpolate across it.

## Fields

| Field | Type | Null | Meaning |
|---|---|---|---|
| `v` | int | no | Schema version, `2` |
| `symbol` | string | no | Uppercase USD-M symbol |
| `session_id` | int | yes | `recv_ts_ns` of the `session_start` that opened the recording session; null only when the stream had no `session_start` |
| `seq` | int | no | Recorder sequence of the depth event behind this row. Unique within a session, restarts at 0 per session |
| `recv_ts_ns` | int | no | Local receive time of that event, ns since epoch (recorded, so identical in replay) |
| `exchange_ts_ms` | int | yes | Exchange event time `E` of that event |
| `feature_ts_ms` | int | yes | Feature clock: the latest exchange time seen for this symbol (depth `E` or trade `T`). Never moves backwards within a session. Rolling windows are evaluated at this time |
| `book_update_id` | int | no | Last applied update id `u` (or the snapshot id if no diff applied yet) |
| `synced_since_seq` | int | no | `seq` of the snapshot that started the current sync epoch. A change means the book was rebuilt |
| `best_bid` | decimal | no | Best bid price |
| `best_bid_qty` | decimal | no | Quantity at best bid |
| `best_ask` | decimal | no | Best ask price |
| `best_ask_qty` | decimal | no | Quantity at best ask |
| `mid` | decimal | no | `(best_bid + best_ask) / 2` |
| `spread` | decimal | no | `best_ask - best_bid` |
| `spread_bps` | decimal | no | `spread / mid * 10000` |
| `microprice` | decimal | no | `(bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)` |
| `obi` | array | no | One `{levels, value}` per configured depth N; `value` (decimal, nullable) is `(B_N - A_N) / (B_N + A_N)` |
| `trade_state` | string | no | `none`, `active`, `quiet` or `stale` (see "Trade stream health") |
| `cvd` | decimal | yes | Signed aggressor quantity since `cvd_since_ms` |
| `cvd_since_ms` | int | yes | Trade time of the first trade of the current CVD epoch |
| `buy_qty` | decimal | yes | Aggressor buy quantity in the current epoch |
| `sell_qty` | decimal | yes | Aggressor sell quantity in the current epoch |
| `deltas` | array | no | One `{window_ms, delta}` per configured window; `delta` (decimal, nullable) is the signed aggressor quantity with `T` in `(feature_ts_ms - window_ms, feature_ts_ms]` |
| `last_trade_ms` | int | yes | Trade time of the last trade seen this session (any epoch) |
| `trade_gaps` | int | no | aggTrade id gaps seen this session |

Divisions are rounded half-to-even to 16 decimals; all decimal text is normalized (no trailing zeros).

## Book guarantees and OBI depth policy

The local book follows the Binance USD-M rules: buffer diffs, fetch a 1000-level snapshot, drop diffs with `u < lastUpdateId`, require the first applied diff to bracket `lastUpdateId`, then require `pu` equal to the previous `u`. Any violation invalidates the book.

`obi[N]` counts price levels, not a price distance. It is null unless both sides hold at least N levels inside the range the book mirrors completely. When the snapshot was cut at its level limit, levels beyond its deepest price are known only if they changed since, so they are never counted; if the top of book itself moves outside that range, rows stop until the next resync. N is at most 500. Defaults: 10 and 50.

## CVD sign, epochs and coverage

Sign follows Binance `aggTrade.m` ("buyer is the maker"): `m = false` means the buyer aggressed, `+qty`; `m = true` means the seller aggressed, `-qty`.

An epoch restarts wherever trades may have been missed: `session_start`, a disconnect, an aggTrade id gap, a malformed trade. `cvd`, `buy_qty`, `sell_qty` and every window cover one epoch only; they never sum across a boundary. Duplicate trade ids are ignored.

`delta` for window W is null until the window is fully covered: `feature_ts_ms - W` must be at or after the start of contiguous trade coverage, which is `cvd_since_ms`, moved forward when the per-symbol memory cap (100 000 trades) evicts old trades. Trades are counted as of arrival: depth `E` can run slightly ahead of trade delivery, so a trade still in flight lands in the next row. Replay reproduces this exactly.

Defaults: windows of 1 s, 5 s, 15 s and 60 s.

## Trade stream health

Silence is `feature_ts_ms - (time of the epoch's last trade)`, on exchange time:

| `trade_state` | Condition | Flow fields |
|---|---|---|
| `none` | no trade yet in this epoch (start, reconnect, after a gap) | `cvd`, `buy_qty`, `sell_qty` null; deltas null (not covered) |
| `active` | silence <= `trade_quiet_ms` (default 10 s) | reported |
| `quiet` | silence <= `trade_stale_ms` (default 120 s) | reported: a lull is a real observation |
| `stale` | longer silence while depth keeps flowing | `cvd`, `buy_qty`, `sell_qty` and every delta null |

Silence cannot prove a stall, so the thresholds are a policy, tunable per symbol liquidity with `--trade-quiet-ms` and `--trade-stale-ms`. When trades resume with a contiguous id, nothing was missed and values return; a non-contiguous id starts a new epoch.

## Configuration integrity

The feature config (windows, OBI depths, silence thresholds) is written into each recording's `session_start` event. `market-data replay` refuses a recording whose config differs from the replay flags unless `--allow-config-mismatch` is given; recordings from before this check are counted as `config_unverified` in the replay report.

## Storage

`--features-dir DIR` writes gzip NDJSON, rotated every `--features-rotate-mb` (default 256) MB of uncompressed rows, at most `--features-max-files` (default 64) files per run. Past the cap the store stops writing and counts `feature_rows_dropped` in the status line; it never deletes older files. The file being written ends in `.partial` until rotation or a clean finish, so only complete files lack that suffix. A crashed run leaves the `.partial` file readable up to its last flush.

## Session boundaries

`session_start` clears all per-symbol state: replaying a directory with several runs yields, for each run, exactly the rows that run wrote live. `seq` restarts per session; `(session_id, seq)` identifies a row.
