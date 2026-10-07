//! One-minute bars through the shared pipeline: completeness, flow differences, session
//! boundaries and replay parity.

use std::time::Duration;

use market_data::bars::Bar;
use market_data::binance::{envelope_from_snapshot, envelope_from_stream};
use market_data::event::{connection, session_start, Envelope};
use market_data::features::FeatureConfig;
use market_data::pipeline::Pipeline;
use market_data::recorder::{replay, Recorder};

const T0: i64 = 1_700_000_040_000; // A minute boundary.

/// Sets `bid` as the only bid among 100..=106 (the others are removed).
fn depth(id: u64, e: i64, bid: &str) -> Envelope {
    let bids: Vec<String> = (100..=106)
        .map(|p| format!(r#"["{p}","{}"]"#, if p.to_string() == bid { 1 } else { 0 }))
        .collect();
    let frame = format!(
        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{e},"T":{e},"s":"BTCUSDT","U":{id},"u":{id},"pu":{},"b":[{}],"a":[]}}}}"#,
        id - 1,
        bids.join(",")
    );
    envelope_from_stream(&frame, e * 1_000_000).unwrap()
}

fn snapshot(id: u64, e: i64) -> Envelope {
    let body = format!(
        r#"{{"lastUpdateId":{id},"E":{e},"T":{e},"bids":[["100","1"]],"asks":[["110","1"]]}}"#
    );
    envelope_from_snapshot("BTCUSDT", &body, e * 1_000_000).unwrap()
}

fn trade(id: u64, t: i64, qty: &str, sell: bool) -> Envelope {
    let frame = format!(
        r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":{t},"s":"BTCUSDT","a":{id},"p":"105","q":"{qty}","f":1,"l":1,"T":{t},"m":{sell}}}}}"#
    );
    envelope_from_stream(&frame, t * 1_000_000).unwrap()
}

struct Stream {
    events: Vec<Envelope>,
    id: u64,
    trade: u64,
}

impl Stream {
    fn new() -> Self {
        let config = FeatureConfig::default();
        let mut s = Self {
            events: vec![
                session_start("test", &config, 0),
                connection("connected", "", 1),
            ],
            id: 10,
            trade: 0,
        };
        s.events.push(depth(10, T0, "100"));
        s.events.push(snapshot(10, T0));
        s
    }

    /// One diff per second over `[from, to)` seconds after T0; the best bid tracks time.
    fn diffs(&mut self, from: i64, to: i64) -> &mut Self {
        for sec in from..to {
            self.id += 1;
            let bid = format!("{}", 100 + sec % 7);
            self.events.push(depth(self.id, T0 + sec * 1_000, &bid));
        }
        self
    }

    fn trade(&mut self, at_ms: i64, qty: &str, sell: bool) -> &mut Self {
        self.trade += 1;
        self.events.push(trade(self.trade, T0 + at_ms, qty, sell));
        self
    }

    fn push(&mut self, env: Envelope) -> &mut Self {
        self.events.push(env);
        self
    }

    fn bars(&self) -> Vec<Bar> {
        let mut pipeline = Pipeline::default();
        self.events
            .iter()
            .enumerate()
            .filter_map(|(seq, env)| {
                let mut env = env.clone();
                env.seq = seq as u64;
                pipeline.handle(&env).bar
            })
            .collect()
    }
}

#[test]
fn adjacent_complete_bars_carry_flow_differences() {
    let mut s = Stream::new();
    s.trade(500, "1", false)
        .diffs(1, 31)
        .trade(30_500, "2", true);
    s.diffs(31, 90).trade(70_000, "0.5", false).diffs(90, 125);
    let bars = s.bars();
    assert_eq!(bars.len(), 2); // The third bar is still open: never emitted early.
    let (first, second) = (&bars[0], &bars[1]);
    assert!(first.complete && second.complete);
    assert_eq!((first.start_ms, first.end_ms), (T0, T0 + 60_000));
    // No previous close to measure from: the first bar's flow is unknown, not zero.
    assert_eq!((first.buy_qty, first.sell_qty), (None, None));
    assert_eq!(second.buy_qty.unwrap().to_string(), "0.5");
    assert_eq!(second.sell_qty.unwrap().to_string(), "0");
    // Rows at 0..=59 s: the bar closes on its last row, never on data after its end.
    assert_eq!(first.rows, 60);
    assert_eq!(first.close_mid.to_string(), "106.5"); // bid 100 + 59 % 7 = 103, ask 110.
    assert_eq!(first.high_mid.to_string(), "108");
    assert_eq!(first.low_mid.to_string(), "105");
}

#[test]
fn a_long_row_gap_makes_the_bar_incomplete() {
    let mut s = Stream::new();
    s.diffs(1, 20).diffs(35, 70); // 15 s without rows inside the first bar.
    let bars = s.bars();
    assert_eq!(bars[0].incomplete_reason, Some("row_gap"));
    assert!(!bars[0].complete);
    let mut tail = Stream::new();
    tail.diffs(1, 45).diffs(61, 70); // Last row at 44 s: 16 s uncovered before the end.
    assert_eq!(tail.bars()[0].incomplete_reason, Some("row_gap"));
}

#[test]
fn a_resync_inside_the_bar_makes_it_incomplete() {
    let mut s = Stream::new();
    s.diffs(1, 20);
    s.push(depth(s.id + 5, T0 + 20_500, "100")); // pu gap: book invalidated.
    s.id += 5;
    s.push(snapshot(s.id, T0 + 21_000));
    s.diffs(22, 65);
    let bars = s.bars();
    assert_eq!(bars[0].incomplete_reason, Some("resync"));
}

#[test]
fn a_new_session_drops_the_open_bar() {
    let mut s = Stream::new();
    s.diffs(1, 50);
    let mut second = Stream::new();
    second.diffs(1, 65);
    let mut events = s.events.clone();
    events.extend(second.events.iter().cloned());
    let combined = Stream {
        events,
        id: 0,
        trade: 0,
    };
    let bars = combined.bars();
    assert_eq!(bars.len(), 1); // Only the second run's first bar.
    assert_eq!(bars[0].rows, 60);
}

#[test]
fn stale_trades_leave_flow_unknown() {
    let mut s = Stream::new();
    s.trade(500, "1", false).diffs(1, 125); // No trade for > 120 s by the third bar.
    let bars = s.bars();
    assert_eq!(bars[1].buy_qty.unwrap().to_string(), "0"); // Quiet: a real zero.
    let mut late = s.events.clone();
    late.push(depth(s.id + 1, T0 + 185_000, "100"));
    let bars = Stream {
        events: late,
        id: 0,
        trade: 0,
    }
    .bars();
    let third = &bars[2];
    assert_eq!(
        serde_json::to_value(third.close_trade_state).unwrap(),
        "stale"
    );
    assert_eq!(third.buy_qty, None);
}

#[test]
fn replayed_bars_match_live() {
    let mut s = Stream::new();
    s.trade(500, "1", false)
        .diffs(1, 200)
        .trade(150_000, "3", true);
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::create(dir.path(), Duration::from_secs(3600), 2_000).unwrap();
    let mut live = Pipeline::default();
    let mut live_bars = Vec::new();
    for env in s.events.clone() {
        let env = recorder.record(env).unwrap();
        live_bars.extend(live.handle(&env).bar);
    }
    recorder.finish().unwrap();
    let mut replayed = Pipeline::default();
    let mut replay_bars = Vec::new();
    replay(dir.path(), |env| {
        replay_bars.extend(replayed.handle(&env).bar)
    })
    .unwrap();
    assert_eq!(live_bars.len(), 3);
    assert_eq!(
        serde_json::to_string(&replay_bars).unwrap(),
        serde_json::to_string(&live_bars).unwrap()
    );
}
