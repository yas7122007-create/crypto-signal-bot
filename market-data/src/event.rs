//! Recorded event envelope: the single unit that both live ingestion and replay feed
//! into [`crate::pipeline::Pipeline`]. Replay order is the recorder-assigned `seq`.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

pub const SCHEMA_VERSION: u16 = 1;
pub const SOURCE: &str = "binance-usdm";

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Depth,
    DepthSnapshot,
    AggTrade,
    BookTicker,
    MarkPrice,
    /// Connection lifecycle (`session_start`, `connected`, `disconnected`). Replayed so
    /// books are invalidated at exactly the same point as they were live.
    Connection,
    /// A message we could not parse. Kept for debugging instead of being dropped.
    Unparsed,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    pub v: u16,
    pub seq: u64,
    pub source: String,
    pub kind: Kind,
    /// Normalized uppercase symbol, empty for connection-wide events.
    pub symbol: String,
    /// Raw stream name (`btcusdt@depth@100ms`) or REST path for snapshots.
    pub stream: String,
    /// Exchange event time (`E`), milliseconds since the Unix epoch.
    pub exchange_ts_ms: Option<i64>,
    /// Local wall clock at receive, nanoseconds since the Unix epoch.
    pub recv_ts_ns: i64,
    pub first_update_id: Option<u64>,
    pub final_update_id: Option<u64>,
    pub prev_update_id: Option<u64>,
    /// Exchange payload exactly as received (the `data` object of a combined stream).
    pub payload: Box<RawValue>,
}

impl PartialEq for Envelope {
    fn eq(&self, other: &Self) -> bool {
        self.v == other.v
            && self.seq == other.seq
            && self.source == other.source
            && self.kind == other.kind
            && self.symbol == other.symbol
            && self.stream == other.stream
            && self.exchange_ts_ms == other.exchange_ts_ms
            && self.recv_ts_ns == other.recv_ts_ns
            && self.first_update_id == other.first_update_id
            && self.final_update_id == other.final_update_id
            && self.prev_update_id == other.prev_update_id
            && self.payload.get() == other.payload.get()
    }
}

pub fn now_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Builds a connection lifecycle envelope (`seq` is assigned by the recorder loop).
pub fn connection(event: &str, detail: &str, recv_ts_ns: i64) -> Envelope {
    connection_with(event, detail, None, recv_ts_ns)
}

/// `session_start` carries the feature configuration, so replay can verify it.
pub fn session_start(
    detail: &str,
    feature_config: &crate::features::FeatureConfig,
    recv_ts_ns: i64,
) -> Envelope {
    let config = serde_json::to_value(feature_config).unwrap_or_default();
    connection_with("session_start", detail, Some(config), recv_ts_ns)
}

fn connection_with(
    event: &str,
    detail: &str,
    feature_config: Option<serde_json::Value>,
    recv_ts_ns: i64,
) -> Envelope {
    let mut payload = serde_json::json!({ "event": event, "detail": detail });
    if let Some(config) = feature_config {
        payload["feature_config"] = config;
    }
    let payload = payload.to_string();
    Envelope {
        v: SCHEMA_VERSION,
        seq: 0,
        source: SOURCE.to_string(),
        kind: Kind::Connection,
        symbol: String::new(),
        stream: "connection".to_string(),
        exchange_ts_ms: None,
        recv_ts_ns,
        first_update_id: None,
        final_update_id: None,
        prev_update_id: None,
        payload: RawValue::from_string(payload).expect("json! output is valid JSON"),
    }
}
