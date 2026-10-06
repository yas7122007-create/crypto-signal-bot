//! Live path vs recorded replay: the same envelopes must yield identical book state,
//! trade flow and audit trail, across file rotation and after a crash-truncated file.

use std::io::Write;
use std::time::Duration;

use market_data::binance::{envelope_from_snapshot, envelope_from_stream};
use market_data::event::{connection, Envelope, Kind};
use market_data::pipeline::{Action, AuditEvent, Pipeline};
use market_data::recorder::{recording_files, replay, Recorder};

fn depth(
    symbol: &str,
    first: u64,
    last: u64,
    prev: u64,
    bid: (&str, &str),
    ask: (&str, &str),
) -> Envelope {
    let frame = format!(
        r#"{{"stream":"{}@depth@100ms","data":{{"e":"depthUpdate","E":{},"T":{},"s":"{symbol}","U":{first},"u":{last},"pu":{prev},"b":[["{}","{}"]],"a":[["{}","{}"]]}}}}"#,
        symbol.to_lowercase(),
        1_700_000_000_000 + last,
        1_700_000_000_000 + last,
        bid.0,
        bid.1,
        ask.0,
        ask.1
    );
    envelope_from_stream(&frame, last as i64).unwrap()
}

fn trade(symbol: &str, id: u64, qty: &str, buyer_is_maker: bool) -> Envelope {
    let frame = format!(
        r#"{{"stream":"{}@aggTrade","data":{{"e":"aggTrade","E":1,"s":"{symbol}","a":{id},"p":"100.5","q":"{qty}","f":1,"l":1,"T":1,"m":{buyer_is_maker}}}}}"#,
        symbol.to_lowercase()
    );
    envelope_from_stream(&frame, id as i64).unwrap()
}

fn snapshot(symbol: &str, id: u64) -> Envelope {
    let body = format!(
        r#"{{"lastUpdateId":{id},"E":1,"T":1,"bids":[["100","1"],["99","2"]],"asks":[["101","1"],["102","2"]]}}"#
    );
    envelope_from_snapshot(symbol, &body, id as i64).unwrap()
}

/// A session with buffering, a sequence gap, a resync, a trade gap, a malformed diff,
/// an unparsed frame and a reconnect.
fn session() -> Vec<Envelope> {
    let s = "BTCUSDT";
    vec![
        connection("session_start", "test", 0),
        connection("connected", "", 1),
        depth(s, 95, 99, 94, ("100", "3"), ("101", "1")),
        depth(s, 100, 103, 99, ("100", "4"), ("101", "2")),
        snapshot(s, 101),
        depth(s, 104, 106, 103, ("99.5", "1"), ("102", "0")),
        trade(s, 10, "1.5", false),
        trade(s, 11, "0.5", true),
        trade(s, 13, "2", false), // trade 12 missing
        trade(s, 13, "2", false), // duplicate ignored
        depth(s, 110, 112, 109, ("100", "9"), ("101", "9")), // pu gap: expected 106
        depth(s, 113, 115, 112, ("98", "1"), ("103", "1")),
        snapshot(s, 112),
        depth(s, 116, 117, 115, ("100", "6"), ("101", "6")),
        envelope_from_stream(r#"{"stream":"ethusdt@depth@100ms","data":{"s":"ETHUSDT","U":1,"u":2,"pu":0,"b":[["x","1"]],"a":[]}}"#, 9).unwrap(),
        market_data::binance::unparsed("not json at all", 10),
        connection("disconnected", "test drop", 11),
        connection("connected", "", 12),
        depth(s, 200, 201, 199, ("100", "1"), ("101", "1")),
        snapshot(s, 200),
        depth(s, 202, 203, 201, ("100.5", "2"), ("101", "1")),
    ]
}

fn live(dir: &std::path::Path, rotate_bytes: u64) -> (Pipeline, Vec<Action>) {
    let mut recorder = Recorder::create(dir, Duration::from_secs(3600), rotate_bytes).unwrap();
    let mut pipeline = Pipeline::default();
    let mut actions = Vec::new();
    for env in session() {
        let env = recorder.record(env).unwrap();
        actions.extend(pipeline.handle(&env).actions);
    }
    recorder.finish().unwrap();
    (pipeline, actions)
}

fn replayed(dir: &std::path::Path) -> (Pipeline, Vec<Action>, market_data::recorder::ReplayStats) {
    let mut pipeline = Pipeline::default();
    let mut actions = Vec::new();
    let stats = replay(dir, |env| actions.extend(pipeline.handle(&env).actions)).unwrap();
    (pipeline, actions, stats)
}

#[test]
fn replay_reproduces_live_state_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let (live_pipeline, live_actions) = live(dir.path(), u64::MAX);
    let (first, first_actions, stats) = replayed(dir.path());
    let (second, _, _) = replayed(dir.path());
    assert_eq!(stats.events, session().len() as u64);
    assert_eq!(stats.truncated_files, 0);
    assert_eq!(first, live_pipeline);
    assert_eq!(second, live_pipeline);
    assert_eq!(first_actions, live_actions);
    assert_eq!(first.summary(), live_pipeline.summary());
    assert_eq!(live_pipeline.stats.out_of_order_seq, 0);

    let book = live_pipeline.book("BTCUSDT").unwrap();
    assert!(book.is_synced());
    assert_eq!(book.last_update_id(), Some(203));
    let top = book.book().unwrap();
    assert_eq!(top.best_bid().unwrap().0.to_string(), "100.5");
    assert_eq!(top.best_ask().unwrap().0.to_string(), "101");
    assert_eq!((book.stats.gaps, book.stats.snapshots_applied), (1, 3));

    let audit: Vec<_> = live_pipeline.audit().map(|e| &e.event).collect();
    assert!(audit.iter().any(|e| matches!(
        e,
        AuditEvent::TradeGap {
            expected: 12,
            got: 13
        }
    )));
    assert!(audit.iter().any(|e| matches!(
        e,
        AuditEvent::Malformed {
            kind: Kind::Depth,
            ..
        }
    )));
    assert!(audit.iter().any(|e| matches!(
        e,
        AuditEvent::Book(market_data::book::SyncEvent::Gap {
            expected_prev: 106,
            ..
        })
    )));
    let summary = live_pipeline.summary();
    assert_eq!(summary["trades"]["BTCUSDT"]["aggressive_buy_qty"], "3.5");
    assert_eq!(summary["trades"]["BTCUSDT"]["aggressive_sell_qty"], "0.5");
    assert_eq!(summary["events"]["unparsed"], 1);
    assert!(live_actions.contains(&Action::RequestSnapshot("BTCUSDT".into())));
}

#[test]
fn rotation_does_not_change_replay() {
    let single = tempfile::tempdir().unwrap();
    let rotated = tempfile::tempdir().unwrap();
    let (expected, _) = live(single.path(), u64::MAX);
    live(rotated.path(), 400); // Roughly one event per file.
    assert!(recording_files(rotated.path()).unwrap().len() > 5);
    let (pipeline, _, stats) = replayed(rotated.path());
    assert_eq!(stats.events, session().len() as u64);
    assert_eq!(pipeline, expected);
}

#[test]
fn crash_truncated_file_replays_up_to_last_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::create(dir.path(), Duration::from_secs(3600), u64::MAX).unwrap();
    let events = session();
    for env in events.iter().take(8).cloned() {
        recorder.record(env).unwrap();
    }
    recorder.flush().unwrap();
    for env in events.iter().skip(8).cloned() {
        recorder.record(env).unwrap();
    }
    std::mem::forget(recorder); // Simulated crash: no gzip trailer, unflushed tail lost.
    let file = recording_files(dir.path()).unwrap().remove(0);
    let len = std::fs::metadata(&file).unwrap().len();
    let handle = std::fs::OpenOptions::new().write(true).open(&file).unwrap();
    handle.set_len(len.saturating_sub(3)).unwrap(); // Torn final write.
    drop(handle);
    let mut seen = Vec::new();
    let stats = replay(dir.path(), |env| seen.push(env.seq)).unwrap();
    assert_eq!(stats.truncated_files, 1);
    assert!(
        seen.len() >= 8,
        "flushed events must survive, got {}",
        seen.len()
    );
    assert_eq!(seen, (0..seen.len() as u64).collect::<Vec<_>>());
}

#[test]
fn corrupt_line_in_middle_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("events-0000000000001-000000000000.ndjson.gz");
    let mut gz = flate2::write::GzEncoder::new(
        std::fs::File::create(&path).unwrap(),
        flate2::Compression::fast(),
    );
    let good = serde_json::to_string(&connection("connected", "", 1)).unwrap();
    writeln!(gz, "{good}\n{{broken\n{good}").unwrap();
    gz.finish().unwrap();
    let err = replay(dir.path(), |_| {}).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn envelope_round_trips_byte_for_byte() {
    for env in session() {
        let text = serde_json::to_string(&env).unwrap();
        let back: Envelope = serde_json::from_str(&text).unwrap();
        assert_eq!(back, env);
        assert_eq!(serde_json::to_string(&back).unwrap(), text);
    }
}

#[test]
fn consecutive_sessions_replay_in_order() {
    let dir = tempfile::tempdir().unwrap();
    live(dir.path(), u64::MAX);
    std::thread::sleep(Duration::from_millis(5)); // Distinct file timestamps.
    let (expected, _) = live(dir.path(), u64::MAX);
    let (pipeline, _, stats) = replayed(dir.path());
    assert_eq!((stats.files, stats.events), (2, 2 * session().len() as u64));
    assert_eq!(pipeline.stats.out_of_order_seq, 0);
    // Counters accumulate across sessions; the book itself must match a single session.
    let (got, want) = (
        pipeline.book("BTCUSDT").unwrap(),
        expected.book("BTCUSDT").unwrap(),
    );
    assert_eq!(got.book(), want.book());
    assert_eq!(got.last_update_id(), want.last_update_id());
}

#[test]
fn stalled_depth_stream_expires_the_book() {
    let mut pipeline = Pipeline::default();
    for env in session().into_iter().take(6) {
        pipeline.handle(&env);
    }
    assert!(pipeline.book("BTCUSDT").unwrap().is_synced());
    let mut late = trade("BTCUSDT", 99, "1", false); // Socket alive, depth silent for 31 s.
    late.recv_ts_ns = 106 + 31_000_000_000;
    late.seq = 100;
    pipeline.handle(&late);
    assert!(!pipeline.book("BTCUSDT").unwrap().is_synced());
    assert!(pipeline.audit().any(|e| e.event
        == AuditEvent::Book(market_data::book::SyncEvent::Invalidated {
            reason: "stale depth stream".into()
        })));
}
