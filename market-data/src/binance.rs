//! Binance USD-M Futures public wire formats. Public market data only: this crate has
//! no API keys, no signed endpoints and no order, leverage, margin or withdrawal calls.

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::event::{Envelope, Kind, SCHEMA_VERSION, SOURCE};

pub const WS_URL: &str = "wss://fstream.binance.com/stream";
pub const REST_URL: &str = "https://fapi.binance.com";
pub const SNAPSHOT_PATH: &str = "/fapi/v1/depth";
pub const SNAPSHOT_LIMIT: u32 = 1000;
const STREAMS: [&str; 4] = ["depth@100ms", "aggTrade", "bookTicker", "markPrice@1s"];
/// Binance USD-M allows 200 streams per combined connection; keep headroom below it.
pub const MAX_STREAMS: usize = 200;
pub const MAX_SYMBOLS: usize = MAX_STREAMS / STREAMS.len() - 5;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ParseError {
    #[error("invalid symbol {0:?}")]
    Symbol(String),
    #[error("malformed message: {0}")]
    Malformed(String),
    #[error("unsupported stream {0:?}")]
    Stream(String),
}

/// Accepts `btcusdt` or `BTCUSDT`; returns the uppercase exchange symbol.
pub fn normalize_symbol(raw: &str) -> Result<String, ParseError> {
    let symbol = raw.trim().to_ascii_uppercase();
    let base = symbol.strip_suffix("USDT").unwrap_or("");
    if base.is_empty() || base.len() > 28 || !base.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(ParseError::Symbol(raw.chars().take(40).collect()));
    }
    Ok(symbol)
}

pub fn stream_url(base: &str, symbols: &[String]) -> String {
    let streams: Vec<String> = symbols
        .iter()
        .flat_map(|s| {
            STREAMS
                .iter()
                .map(move |stream| format!("{}@{stream}", s.to_ascii_lowercase()))
        })
        .collect();
    format!("{base}?streams={}", streams.join("/"))
}

pub fn snapshot_url(base: &str, symbol: &str) -> String {
    format!("{base}{SNAPSHOT_PATH}?symbol={symbol}&limit={SNAPSHOT_LIMIT}")
}

#[derive(Deserialize)]
struct Combined<'a> {
    stream: String,
    #[serde(borrow)]
    data: &'a RawValue,
}

#[derive(Deserialize)]
struct Header {
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "E")]
    event_ms: Option<i64>,
    #[serde(rename = "U")]
    first: Option<u64>,
    #[serde(rename = "u")]
    last: Option<u64>,
    #[serde(rename = "pu")]
    prev: Option<u64>,
}

/// Wraps one combined-stream text frame into an envelope (`seq` assigned later).
pub fn envelope_from_stream(text: &str, recv_ts_ns: i64) -> Result<Envelope, ParseError> {
    let combined: Combined =
        serde_json::from_str(text).map_err(|e| ParseError::Malformed(e.to_string()))?;
    let kind = stream_kind(&combined.stream)?;
    let header: Header = serde_json::from_str(combined.data.get())
        .map_err(|e| ParseError::Malformed(e.to_string()))?;
    let symbol = normalize_symbol(header.symbol.as_deref().unwrap_or(""))?;
    let depth = kind == Kind::Depth;
    if depth && (header.first.is_none() || header.last.is_none() || header.prev.is_none()) {
        return Err(ParseError::Malformed("depth update without U/u/pu".into()));
    }
    Ok(Envelope {
        v: SCHEMA_VERSION,
        seq: 0,
        source: SOURCE.to_string(),
        kind,
        symbol,
        stream: combined.stream,
        exchange_ts_ms: header.event_ms,
        recv_ts_ns,
        first_update_id: if depth { header.first } else { None },
        final_update_id: if depth || kind == Kind::BookTicker {
            header.last
        } else {
            None
        },
        prev_update_id: if depth { header.prev } else { None },
        payload: combined.data.to_owned(),
    })
}

/// Keeps a frame we could not parse, so nothing received is silently discarded.
pub fn unparsed(text: &str, recv_ts_ns: i64) -> Envelope {
    let clipped: String = text.chars().take(65_536).collect();
    let payload = serde_json::to_string(&clipped).unwrap_or_else(|_| "\"\"".to_string());
    Envelope {
        v: SCHEMA_VERSION,
        seq: 0,
        source: SOURCE.to_string(),
        kind: Kind::Unparsed,
        symbol: String::new(),
        stream: "unparsed".to_string(),
        exchange_ts_ms: None,
        recv_ts_ns,
        first_update_id: None,
        final_update_id: None,
        prev_update_id: None,
        payload: RawValue::from_string(payload).unwrap_or_else(|_| empty_payload()),
    }
}

/// Wraps a REST depth snapshot so it is recorded and replayed in stream order.
pub fn envelope_from_snapshot(
    symbol: &str,
    body: &str,
    recv_ts_ns: i64,
) -> Result<Envelope, ParseError> {
    let snapshot = parse_snapshot(body)?;
    Ok(Envelope {
        v: SCHEMA_VERSION,
        seq: 0,
        source: SOURCE.to_string(),
        kind: Kind::DepthSnapshot,
        symbol: normalize_symbol(symbol)?,
        stream: format!("rest:{SNAPSHOT_PATH}"),
        exchange_ts_ms: snapshot.event_ms,
        recv_ts_ns,
        first_update_id: None,
        final_update_id: Some(snapshot.last_update_id),
        prev_update_id: None,
        payload: RawValue::from_string(body.to_string())
            .map_err(|e| ParseError::Malformed(e.to_string()))?,
    })
}

fn empty_payload() -> Box<RawValue> {
    RawValue::from_string("null".to_string()).expect("null is valid JSON")
}

fn stream_kind(stream: &str) -> Result<Kind, ParseError> {
    let suffix = stream.split_once('@').map(|(_, s)| s).unwrap_or("");
    match suffix {
        s if s.starts_with("depth") => Ok(Kind::Depth),
        "aggTrade" => Ok(Kind::AggTrade),
        "bookTicker" => Ok(Kind::BookTicker),
        s if s.starts_with("markPrice") => Ok(Kind::MarkPrice),
        _ => Err(ParseError::Stream(stream.chars().take(80).collect())),
    }
}

pub type Level = (Decimal, Decimal);

#[derive(Debug, Clone, PartialEq)]
pub struct DepthUpdate {
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub prev_final_update_id: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub last_update_id: u64,
    pub event_ms: Option<i64>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggTrade {
    pub id: u64,
    pub price: Decimal,
    pub qty: Decimal,
    pub trade_ms: i64,
    /// `m`: buyer is maker, so the aggressor sold.
    pub buyer_is_maker: bool,
}

#[derive(Deserialize)]
struct RawDepth<'a> {
    #[serde(rename = "U")]
    first: u64,
    #[serde(rename = "u")]
    last: u64,
    #[serde(rename = "pu")]
    prev: u64,
    #[serde(rename = "b", borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(rename = "a", borrow)]
    asks: Vec<[&'a str; 2]>,
}

#[derive(Deserialize)]
struct RawSnapshot<'a> {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "E")]
    event_ms: Option<i64>,
    #[serde(borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    asks: Vec<[&'a str; 2]>,
}

#[derive(Deserialize)]
struct RawAggTrade<'a> {
    #[serde(rename = "a")]
    id: u64,
    #[serde(rename = "p")]
    price: &'a str,
    #[serde(rename = "q")]
    qty: &'a str,
    #[serde(rename = "T")]
    trade_ms: i64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

fn levels(raw: &[[&str; 2]]) -> Result<Vec<Level>, ParseError> {
    raw.iter()
        .map(|[p, q]| {
            let price = decimal(p)?;
            let qty = decimal(q)?;
            if price <= Decimal::ZERO || qty < Decimal::ZERO {
                return Err(ParseError::Malformed(format!("invalid level {p}/{q}")));
            }
            Ok((price, qty))
        })
        .collect()
}

fn decimal(raw: &str) -> Result<Decimal, ParseError> {
    raw.parse::<Decimal>().map_err(|_| {
        ParseError::Malformed(format!(
            "bad decimal {:?}",
            raw.chars().take(40).collect::<String>()
        ))
    })
}

pub fn parse_depth(payload: &str) -> Result<DepthUpdate, ParseError> {
    let raw: RawDepth =
        serde_json::from_str(payload).map_err(|e| ParseError::Malformed(e.to_string()))?;
    if raw.last < raw.first {
        return Err(ParseError::Malformed("depth u < U".into()));
    }
    Ok(DepthUpdate {
        first_update_id: raw.first,
        final_update_id: raw.last,
        prev_final_update_id: raw.prev,
        bids: levels(&raw.bids)?,
        asks: levels(&raw.asks)?,
    })
}

pub fn parse_snapshot(payload: &str) -> Result<Snapshot, ParseError> {
    let raw: RawSnapshot =
        serde_json::from_str(payload).map_err(|e| ParseError::Malformed(e.to_string()))?;
    Ok(Snapshot {
        last_update_id: raw.last_update_id,
        event_ms: raw.event_ms,
        bids: levels(&raw.bids)?,
        asks: levels(&raw.asks)?,
    })
}

pub fn parse_agg_trade(payload: &str) -> Result<AggTrade, ParseError> {
    let raw: RawAggTrade =
        serde_json::from_str(payload).map_err(|e| ParseError::Malformed(e.to_string()))?;
    let (price, qty) = (decimal(raw.price)?, decimal(raw.qty)?);
    if price <= Decimal::ZERO || qty <= Decimal::ZERO {
        return Err(ParseError::Malformed("non-positive trade".into()));
    }
    Ok(AggTrade {
        id: raw.id,
        price,
        qty,
        trade_ms: raw.trade_ms,
        buyer_is_maker: raw.buyer_is_maker,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEPTH: &str = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000123,"T":1700000000120,"s":"BTCUSDT","U":101,"u":105,"pu":100,"b":[["50000.10","1.5"],["49999.00","0"]],"a":[["50001.00","2.25"]]}}"#;

    #[test]
    fn symbols_are_normalized_and_validated() {
        assert_eq!(normalize_symbol(" btcusdt ").unwrap(), "BTCUSDT");
        assert_eq!(normalize_symbol("1000PEPEUSDT").unwrap(), "1000PEPEUSDT");
        for bad in ["", "USDT", "../USDT", "BTCUSD", "BTC-USDT", "BTC USDT"] {
            assert!(normalize_symbol(bad).is_err(), "{bad}");
        }
        assert!(normalize_symbol(&format!("{}USDT", "A".repeat(29))).is_err());
    }

    #[test]
    fn combined_depth_frame_becomes_envelope() {
        let env = envelope_from_stream(DEPTH, 7).unwrap();
        assert_eq!(env.kind, Kind::Depth);
        assert_eq!(env.symbol, "BTCUSDT");
        assert_eq!(
            (env.first_update_id, env.final_update_id, env.prev_update_id),
            (Some(101), Some(105), Some(100))
        );
        assert_eq!(env.exchange_ts_ms, Some(1_700_000_000_123));
        let depth = parse_depth(env.payload.get()).unwrap();
        assert_eq!(depth.bids[1].1, Decimal::ZERO);
        assert_eq!(depth.asks[0].0, "50001.00".parse::<Decimal>().unwrap());
    }

    #[test]
    fn malformed_frames_are_rejected_not_panicking() {
        for bad in [
            "",
            "{}",
            "not json",
            r#"{"stream":"btcusdt@depth@100ms","data":{"s":"BTCUSDT","U":1,"u":2}}"#,
            r#"{"stream":"btcusdt@forceOrder","data":{"s":"BTCUSDT"}}"#,
            r#"{"stream":"x@aggTrade","data":{"s":"../../etc"}}"#,
        ] {
            assert!(envelope_from_stream(bad, 0).is_err(), "{bad}");
        }
        assert!(parse_depth(r#"{"U":5,"u":4,"pu":3,"b":[],"a":[]}"#).is_err());
        assert!(parse_depth(r#"{"U":1,"u":2,"pu":0,"b":[["-1","1"]],"a":[]}"#).is_err());
        assert!(parse_depth(r#"{"U":1,"u":2,"pu":0,"b":[["abc","1"]],"a":[]}"#).is_err());
        assert!(parse_agg_trade(r#"{"a":1,"p":"0","q":"1","T":1,"m":true}"#).is_err());
        let kept = unparsed("garbage\u{0}", 1);
        assert_eq!(kept.kind, Kind::Unparsed);
        assert_eq!(
            serde_json::from_str::<String>(kept.payload.get()).unwrap(),
            "garbage\u{0}"
        );
    }

    #[test]
    fn urls_cover_all_streams() {
        let url = stream_url(WS_URL, &["BTCUSDT".to_string()]);
        assert_eq!(
            url,
            "wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms/btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@markPrice@1s"
        );
        assert_eq!(
            snapshot_url(REST_URL, "BTCUSDT"),
            "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
        );
    }
}
