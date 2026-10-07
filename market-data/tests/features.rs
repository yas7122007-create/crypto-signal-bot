//! Feature engine through the shared pipeline: a golden session with hand-computed values,
//! a committed golden file, and live/replay parity of the serialized feature rows.

use std::time::Duration;

use market_data::binance::{envelope_from_snapshot, envelope_from_stream};
use market_data::event::{connection, session_start, Envelope};
use market_data::features::{FeatureConfig, FeatureSnapshot};
use market_data::pipeline::Pipeline;
use market_data::recorder::{replay, FeatureWriter, Recorder};

const S: &str = "BTCUSDT";
const GOLDEN: &str = "tests/fixtures/golden_features.ndjson";

fn levels(levels: &[(&str, &str)]) -> String {
    let items: Vec<String> = levels
        .iter()
        .map(|(p, q)| format!(r#"["{p}","{q}"]"#))
        .collect();
    format!("[{}]", items.join(","))
}

fn depth(id: u64, prev: u64, e: i64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Envelope {
    let frame = format!(
        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{e},"T":{e},"s":"{S}","U":{id},"u":{id},"pu":{prev},"b":{},"a":{}}}}}"#,
        levels(bids),
        levels(asks)
    );
    envelope_from_stream(&frame, e * 1_000_000).unwrap()
}

fn snapshot(id: u64, e: i64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Envelope {
    let body = format!(
        r#"{{"lastUpdateId":{id},"E":{e},"T":{e},"bids":{},"asks":{}}}"#,
        levels(bids),
        levels(asks)
    );
    envelope_from_snapshot(S, &body, e * 1_000_000).unwrap()
}

fn trade(id: u64, t: i64, qty: &str, buyer_is_maker: bool) -> Envelope {
    let frame = format!(
        r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":{t},"s":"{S}","a":{id},"p":"100.5","q":"{qty}","f":1,"l":1,"T":{t},"m":{buyer_is_maker}}}}}"#
    );
    envelope_from_stream(&frame, t * 1_000_000).unwrap()
}

/// Index in this list = recorder seq. Comments give the state the golden values rely on.
fn session() -> Vec<Envelope> {
    vec![
        connection("session_start", "test", 0),    // 0
        connection("connected", "", 1),            // 1
        depth(10, 9, 1_000, &[("100", "2")], &[]), // 2 buffered
        snapshot(
            10,
            1_000,
            &[("100", "1"), ("99", "2")],
            &[("101", "1"), ("102", "3")],
        ), // 3 sync, diff 10 replays: bid 100 -> 2
        trade(1, 1_200, "0.5", false),             // 4 +0.5
        trade(2, 1_500, "2", true),                // 5 -2
        depth(11, 10, 2_600, &[], &[("101", "4")]), // 6
        trade(3, 2_700, "1", false),               // 7 +1
        trade(5, 2_800, "1", false),               // 8 gap (4 missing): epoch restarts, +1
        depth(12, 11, 3_000, &[("99", "0")], &[]), // 9 bid side now one level
        depth(14, 13, 3_100, &[("100", "5")], &[]), // 10 pu gap: invalidated, buffered
        snapshot(
            14,
            3_100,
            &[("100", "1"), ("99", "1")],
            &[("101", "1"), ("102", "1")],
        ), // 11 resync, diff 14 replays
        connection("disconnected", "test drop", 3_200_000_000), // 12
        connection("connected", "", 3_300_000_000), // 13
        trade(9, 9_000, "3", false),               // 14 new epoch, +3
        depth(20, 19, 9_100, &[("100", "1")], &[]), // 15 buffered
        snapshot(20, 9_100, &[("100", "1")], &[("101", "3")]), // 16 sync
        depth(21, 20, 10_500, &[], &[]),           // 17
        trade(10, 10_600, "0.25", true),           // 18 -0.25
        depth(22, 21, 14_100, &[], &[]),           // 19
    ]
}

fn config() -> FeatureConfig {
    FeatureConfig::new(vec![1_000, 5_000], vec![1, 2]).unwrap()
}

fn run(events: Vec<Envelope>) -> Vec<FeatureSnapshot> {
    let mut pipeline = Pipeline::new(config());
    events
        .into_iter()
        .enumerate()
        .filter_map(|(seq, mut env)| {
            env.seq = seq as u64;
            pipeline.handle(&env).feature
        })
        .collect()
}

fn ndjson(rows: &[FeatureSnapshot]) -> String {
    rows.iter()
        .map(|r| serde_json::to_string(r).unwrap() + "\n")
        .collect()
}

type Row = (
    u64,
    &'static str,
    Option<&'static str>,
    [Option<&'static str>; 2],
    [Option<&'static str>; 2],
);

/// (seq, microprice, cvd, [obi1, obi2], [delta_1s, delta_5s]), worked by hand.
const EXPECTED: [Row; 7] = [
    // Book 100x2 99x2 / 101x1 102x3, no trades yet.
    (
        3,
        "100.6666666666666667",
        None,
        [Some("0.3333333333333333"), Some("0")],
        [None, None],
    ),
    // Asks 101x4: micro (100*4+101*2)/6. CVD -1.5 since 1200. 1 s (1600, 2600] holds no
    // trade; 5 s reaches before coverage.
    (
        6,
        "100.3333333333333333",
        Some("-1.5"),
        [Some("-0.3333333333333333"), Some("-0.2727272727272727")],
        [Some("0"), None],
    ),
    // Trade gap at 2800 restarted the epoch: CVD 1. One bid level, so OBI(2) is None.
    (
        9,
        "100.3333333333333333",
        Some("1"),
        [Some("-0.3333333333333333"), None],
        [None, None],
    ),
    // Resynced book 100x5 99x1 / 101x1 102x1.
    (
        11,
        "100.8333333333333333",
        Some("1"),
        [Some("0.6666666666666667"), Some("0.5")],
        [None, None],
    ),
    // After the disconnect: new epoch from 9000, book 100x1 / 101x3.
    (16, "100.25", Some("3"), [Some("-0.5"), None], [None, None]),
    (
        17,
        "100.25",
        Some("3"),
        [Some("-0.5"), None],
        [Some("0"), None],
    ),
    // 5 s (9100, 14100] holds only the -0.25 trade.
    (
        19,
        "100.25",
        Some("2.75"),
        [Some("-0.5"), None],
        [Some("0"), Some("-0.25")],
    ),
];

#[test]
fn golden_session_matches_hand_computed_values() {
    let rows = run(session());
    let s = |v: Option<rust_decimal::Decimal>| v.map(|d| d.to_string());
    let got: Vec<_> = rows
        .iter()
        .map(|r| {
            (
                r.seq,
                r.book.microprice.to_string(),
                s(r.cvd),
                [s(r.book.obi[0].value), s(r.book.obi[1].value)],
                [s(r.deltas[0].delta), s(r.deltas[1].delta)],
            )
        })
        .collect();
    let want: Vec<_> = EXPECTED
        .iter()
        .map(|(seq, micro, cvd, obi, deltas)| {
            let o = |v: &Option<&str>| v.map(str::to_string);
            (
                *seq,
                micro.to_string(),
                o(cvd),
                [o(&obi[0]), o(&obi[1])],
                [o(&deltas[0]), o(&deltas[1])],
            )
        })
        .collect();
    assert_eq!(got, want);

    // Audit metadata ties each row back to the recording and the sync epoch.
    assert_eq!(rows[3].synced_since_seq, 11);
    assert_eq!(rows[3].book_update_id, 14);
    assert_eq!(rows[2].trade_gaps, 1);
    assert_eq!(rows[4].cvd_since_ms, Some(9_000));
    assert_eq!(rows[6].feature_ts_ms, Some(14_100));
    assert_eq!(rows[0].book.spread_bps.to_string(), "99.5024875621890547");
    assert_eq!(rows[0].book.mid.to_string(), "100.5");
}

/// No row while the book is unsynced, after a gap, or after a disconnect.
#[test]
fn no_features_from_invalid_book_state() {
    let seqs: Vec<u64> = run(session()).iter().map(|r| r.seq).collect();
    for invalid in [2, 10, 15] {
        assert!(!seqs.contains(&invalid), "row emitted at seq {invalid}");
    }
    // A crossed book is invalidated, never featured.
    let mut events = session()[..4].to_vec();
    events.push(depth(11, 10, 1_100, &[("101.5", "1")], &[]));
    assert_eq!(run(events).len(), 1);
}

/// A malformed trade means an unknown amount of flow was lost: the CVD epoch restarts.
#[test]
fn malformed_trade_restarts_the_cvd_epoch() {
    let mut events = session()[..6].to_vec(); // Synced, trades 1 and 2.
    events.push(
        envelope_from_stream(
            r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":3,"p":"x"}}"#,
            1_600_000_000,
        )
        .unwrap(),
    );
    // Even if the next id looks contiguous, the unparsed frame's content is unknown.
    events.push(trade(3, 1_700, "1", false));
    events.push(depth(11, 10, 1_800, &[], &[]));
    let last = run(events).pop().unwrap();
    assert_eq!(last.cvd.map(|d| d.to_string()), Some("1".into()));
    assert_eq!(last.cvd_since_ms, Some(1_700));
}

/// Replaying a directory that holds several runs yields, for each run, exactly the rows
/// that run wrote live: nothing per symbol leaks across `session_start`.
#[test]
fn multi_session_replay_matches_each_live_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut live_rows = Vec::new();
    // Run 1 has a trade gap; run 2 is the clean tail of the session at earlier times.
    let second: Vec<Envelope> = std::iter::once(connection("session_start", "run 2", 0))
        .chain(session()[1..4].iter().cloned())
        .chain(std::iter::once(trade(1, 900, "1", false)))
        .chain(std::iter::once(depth(11, 10, 950, &[], &[])))
        .collect();
    for (run, events) in [session(), second].into_iter().enumerate() {
        if run > 0 {
            std::thread::sleep(Duration::from_millis(5)); // Distinct file timestamps.
        }
        let mut recorder =
            Recorder::create(dir.path(), Duration::from_secs(3600), u64::MAX).unwrap();
        let mut live = Pipeline::new(config());
        for env in events {
            let env = recorder.record(env).unwrap();
            live_rows.extend(live.handle(&env).feature);
        }
        recorder.finish().unwrap();
    }
    let mut replayed = Pipeline::new(config());
    let mut rows = Vec::new();
    replay(dir.path(), |env| rows.extend(replayed.handle(&env).feature)).unwrap();
    assert_eq!(ndjson(&rows), ndjson(&live_rows));
    let last = rows.last().unwrap();
    assert_eq!((last.trade_gaps, last.feature_ts_ms), (0, Some(1_000)));
}

/// A malformed diff must not move the feature clock: its header time is untrusted.
#[test]
fn malformed_depth_does_not_move_the_clock() {
    let mut events = session()[..4].to_vec();
    events.push(
        envelope_from_stream(
            r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":9999999999,"s":"BTCUSDT","U":11,"u":11,"pu":10,"b":[["x","1"]],"a":[]}}"#,
            1_100_000_000,
        )
        .unwrap(),
    );
    events.push(depth(12, 11, 1_200, &[], &[]));
    events.push(snapshot(12, 1_200, &[("100", "1")], &[("101", "1")]));
    let rows = run(events);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].feature_ts_ms, Some(1_200));
}

#[test]
fn feature_file_is_partial_until_finished_and_never_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.ndjson");
    let mut out = FeatureWriter::create(&path).unwrap();
    out.write(&run(session())[0]).unwrap();
    out.flush().unwrap();
    assert!(!path.exists());
    assert!(dir.path().join("f.ndjson.partial").exists());
    out.finish().unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);
    assert!(!dir.path().join("f.ndjson.partial").exists());
    let err = FeatureWriter::create(&path).err().unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
}

/// Depth keeps flowing while trades stop: past the stale threshold the row withholds CVD
/// instead of reporting zeros, and the values return once a contiguous trade arrives.
#[test]
fn stale_trade_stream_withholds_cvd() {
    let cfg = FeatureConfig::new(vec![1_000], vec![1])
        .unwrap()
        .with_trade_silence(2_000, 5_000)
        .unwrap();
    let mut pipeline = Pipeline::new(cfg);
    let mut events = session()[..5].to_vec(); // Synced at 1000, trade 1 at 1200.
    for (i, e) in [3_300, 7_000, 7_100].into_iter().enumerate() {
        events.push(depth(11 + i as u64, 10 + i as u64, e, &[], &[]));
    }
    events.push(trade(2, 7_150, "2", true));
    events.push(depth(14, 13, 7_200, &[], &[]));
    let rows: Vec<FeatureSnapshot> = events
        .into_iter()
        .enumerate()
        .filter_map(|(seq, mut env)| {
            env.seq = seq as u64;
            pipeline.handle(&env).feature
        })
        .collect();
    let states: Vec<_> = rows
        .iter()
        .map(|r| serde_json::to_value(r.trade_state).unwrap())
        .collect();
    assert_eq!(states, ["none", "quiet", "stale", "stale", "active"]);
    let stale = &rows[2];
    assert_eq!(
        (stale.cvd, stale.buy_qty, stale.deltas[0].delta),
        (None, None, None)
    );
    assert_eq!(stale.last_trade_ms, Some(1_200)); // Still reported so a consumer can judge.
    let back = &rows[4];
    assert_eq!(back.cvd.map(|d| d.to_string()), Some("-1.5".into()));
    assert_eq!(back.buy_qty.map(|d| d.to_string()), Some("0.5".into()));
    assert_eq!(back.sell_qty.map(|d| d.to_string()), Some("2".into()));
}

#[test]
fn replay_detects_feature_config_mismatch() {
    let recorded = config();
    let other = FeatureConfig::new(vec![1_000], vec![1, 2]).unwrap();
    for (pipeline_config, start, mismatch, unverified) in [
        (recorded.clone(), session_start("t", &recorded, 0), 0, 0),
        (other.clone(), session_start("t", &recorded, 0), 1, 0),
        (other, connection("session_start", "old", 0), 0, 1),
    ] {
        let mut pipeline = Pipeline::new(pipeline_config);
        pipeline.handle(&start);
        assert_eq!(
            (
                pipeline.stats.config_mismatch,
                pipeline.stats.config_unverified
            ),
            (mismatch, unverified)
        );
    }
}

/// The committed file pins the full serialized format, not just the hand-checked values.
/// Regenerate with `UPDATE_GOLDEN=1 cargo test` and review the diff.
#[test]
fn golden_file_is_unchanged() {
    let text = ndjson(&run(session()));
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(GOLDEN, &text).unwrap();
    }
    assert_eq!(text, std::fs::read_to_string(GOLDEN).unwrap());
}

#[test]
fn replayed_features_match_live_byte_for_byte() {
    for rotate_bytes in [u64::MAX, 300] {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder =
            Recorder::create(dir.path(), Duration::from_secs(3600), rotate_bytes).unwrap();
        let mut live = Pipeline::new(config());
        let mut live_rows = Vec::new();
        for env in session() {
            let env = recorder.record(env).unwrap();
            live_rows.extend(live.handle(&env).feature);
        }
        recorder.finish().unwrap();
        let mut replayed = Pipeline::new(config());
        let mut replay_rows = Vec::new();
        replay(dir.path(), |env| {
            replay_rows.extend(replayed.handle(&env).feature)
        })
        .unwrap();
        assert_eq!(live_rows.len(), EXPECTED.len());
        assert_eq!(ndjson(&replay_rows), ndjson(&live_rows));
        assert_eq!(replayed.summary(), live.summary());
    }
}

fn refs(v: &[(String, String)]) -> Vec<(&str, &str)> {
    v.iter().map(|(p, q)| (p.as_str(), q.as_str())).collect()
}

/// Throughput on a long synthetic stream (frame building and parsing included). Ignored by default; run with
/// `cargo test --release -- --ignored --nocapture`.
#[test]
#[ignore]
fn throughput_on_synthetic_stream() {
    let mut pipeline = Pipeline::new(FeatureConfig::default());
    let start = std::time::Instant::now();
    let mut seq = 0u64;
    let mut handle = |mut env: Envelope| {
        env.seq = seq;
        seq += 1;
        pipeline.handle(&env).feature.is_some()
    };
    handle(connection("session_start", "bench", 0));
    let book: Vec<(String, String)> = (0..1_000)
        .map(|i| (format!("{}", 50_000 - i), "1".into()))
        .collect();
    let asks: Vec<(String, String)> = (0..1_000)
        .map(|i| (format!("{}", 50_001 + i), "1".into()))
        .collect();
    handle(depth(1, 0, 1, &[], &[]));
    handle(snapshot(1, 1, &refs(&book), &refs(&asks)));
    let (mut rows, mut trades) = (0u64, 0u64);
    for i in 0..200_000u64 {
        let t = 2 + i as i64 * 10;
        for k in 0..5 {
            trades += 1;
            handle(trade(trades, t, "0.01", k % 2 == 0));
        }
        let qty = format!("{}", 1 + i % 7);
        if handle(depth(
            i + 2,
            i + 1,
            t,
            &[("50000", &qty)],
            &[("50001", &qty)],
        )) {
            rows += 1;
        }
    }
    let elapsed = start.elapsed();
    let events = 200_000 * 6;
    println!(
        "{events} events, {rows} feature rows in {elapsed:?} ({:.0} events/s)",
        events as f64 / elapsed.as_secs_f64()
    );
    assert_eq!(rows, 200_000);
}
