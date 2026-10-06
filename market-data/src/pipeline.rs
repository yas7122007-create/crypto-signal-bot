//! The one processing path shared by live ingestion and replay. It only reads envelopes,
//! so replaying a recording reproduces the exact same book states and audit trail.

use std::collections::{BTreeMap, VecDeque};

use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};

use crate::binance::{parse_agg_trade, parse_depth, parse_snapshot};
use crate::book::{DepthSync, SyncEvent};
use crate::event::{Envelope, Kind};

pub const MAX_AUDIT: usize = 1_000;
/// A synced book with no diff for this long (by recorded receive time) is no longer trusted.
/// Other streams can keep the socket alive while one symbol's depth stream stalls.
pub const STALE_DEPTH_NS: i64 = 30_000_000_000;

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    RequestSnapshot(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuditEvent {
    Book(SyncEvent),
    TradeGap { expected: u64, got: u64 },
    Malformed { kind: Kind, error: String },
    Connection { event: String },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AuditEntry {
    pub seq: u64,
    pub symbol: String,
    pub event: AuditEvent,
}

/// Aggressor flow since the session started: the raw material for CVD later.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct TradeState {
    pub last_id: Option<u64>,
    pub trades: u64,
    pub gaps: u64,
    pub aggressive_buy_qty: Decimal,
    pub aggressive_sell_qty: Decimal,
    pub last_price: Option<Decimal>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct PipelineStats {
    pub events: u64,
    pub book_tickers: u64,
    pub mark_prices: u64,
    pub unparsed: u64,
    pub malformed: u64,
    pub out_of_order_seq: u64,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Pipeline {
    books: BTreeMap<String, DepthSync>,
    trades: BTreeMap<String, TradeState>,
    audit: VecDeque<AuditEntry>,
    last_seq: Option<u64>,
    last_depth_ns: BTreeMap<String, i64>,
    pub stats: PipelineStats,
}

impl Pipeline {
    pub fn handle(&mut self, env: &Envelope) -> Vec<Action> {
        self.stats.events += 1;
        if env.kind == Kind::Connection && env.payload.get().contains("\"session_start\"") {
            self.last_seq = None; // Each recorder session numbers its events from zero.
        }
        if self.last_seq.is_some_and(|last| env.seq <= last) {
            self.stats.out_of_order_seq += 1;
        }
        self.last_seq = Some(env.seq);
        let mut actions = Vec::new();
        let payload = env.payload.get();
        if matches!(env.kind, Kind::Depth | Kind::DepthSnapshot) {
            self.last_depth_ns
                .insert(env.symbol.clone(), env.recv_ts_ns);
        }
        match env.kind {
            Kind::Depth => match parse_depth(payload) {
                Ok(update) => {
                    let outcome = self
                        .books
                        .entry(env.symbol.clone())
                        .or_default()
                        .on_update(update);
                    self.book_outcome(env, outcome, &mut actions);
                }
                Err(e) => {
                    // A lost diff breaks the sequence; never keep serving that book.
                    self.malformed(env, e.to_string());
                    let outcome = self
                        .books
                        .entry(env.symbol.clone())
                        .or_default()
                        .reset("malformed diff");
                    self.book_outcome(env, outcome, &mut actions);
                }
            },
            Kind::DepthSnapshot => match parse_snapshot(payload) {
                Ok(snapshot) => {
                    let outcome = self
                        .books
                        .entry(env.symbol.clone())
                        .or_default()
                        .on_snapshot(&snapshot);
                    self.book_outcome(env, outcome, &mut actions);
                }
                Err(e) => {
                    self.malformed(env, e.to_string());
                    if let Some(book) = self.books.get_mut(&env.symbol) {
                        book.snapshot_failed();
                    }
                }
            },
            Kind::AggTrade => match parse_agg_trade(payload) {
                Ok(trade) => {
                    let state = self.trades.entry(env.symbol.clone()).or_default();
                    let gap = state
                        .last_id
                        .filter(|last| trade.id != last + 1)
                        .map(|last| last + 1);
                    if state.last_id.is_some_and(|last| trade.id <= last) {
                        return actions; // Duplicate or replayed trade; never double count flow.
                    }
                    state.trades += 1;
                    state.last_id = Some(trade.id);
                    state.last_price = Some(trade.price);
                    if trade.buyer_is_maker {
                        state.aggressive_sell_qty += trade.qty;
                    } else {
                        state.aggressive_buy_qty += trade.qty;
                    }
                    if let Some(expected) = gap {
                        state.gaps += 1;
                        self.push(
                            env,
                            AuditEvent::TradeGap {
                                expected,
                                got: trade.id,
                            },
                        );
                    }
                }
                Err(e) => self.malformed(env, e.to_string()),
            },
            Kind::BookTicker => self.stats.book_tickers += 1,
            Kind::MarkPrice => self.stats.mark_prices += 1,
            Kind::Unparsed => self.stats.unparsed += 1,
            Kind::Connection => {
                let event = serde_json::from_str::<Value>(payload)
                    .ok()
                    .and_then(|v| v.get("event").and_then(Value::as_str).map(str::to_string))
                    .unwrap_or_default();
                if event == "disconnected" || event == "session_start" {
                    for (symbol, book) in self.books.iter_mut() {
                        for e in book.reset(&event).events {
                            push_bounded(&mut self.audit, env.seq, symbol, AuditEvent::Book(e));
                        }
                    }
                    for trade in self.trades.values_mut() {
                        trade.last_id = None; // Trades missed while offline are a known, logged gap.
                    }
                }
                self.push(env, AuditEvent::Connection { event });
            }
        }
        self.expire_stale_books(env);
        actions
    }

    /// Uses recorded receive times, so live and replay expire books at the same event.
    fn expire_stale_books(&mut self, env: &Envelope) {
        for (symbol, book) in self.books.iter_mut() {
            let last = self
                .last_depth_ns
                .get(symbol)
                .copied()
                .unwrap_or(env.recv_ts_ns);
            if book.is_synced() && env.recv_ts_ns.saturating_sub(last) > STALE_DEPTH_NS {
                for e in book.reset("stale depth stream").events {
                    push_bounded(&mut self.audit, env.seq, symbol, AuditEvent::Book(e));
                }
            }
        }
    }

    pub fn book(&self, symbol: &str) -> Option<&DepthSync> {
        self.books.get(symbol)
    }

    pub fn audit(&self) -> impl Iterator<Item = &AuditEntry> {
        self.audit.iter()
    }

    /// Deterministic, ordered state summary (identical for live and replay of the same events).
    pub fn summary(&self) -> Value {
        let books: BTreeMap<&String, Value> = self
            .books
            .iter()
            .map(|(symbol, sync)| {
                let book = sync.book();
                let level = |l: Option<(Decimal, Decimal)>| {
                    l.map(|(p, q)| json!([p.to_string(), q.to_string()]))
                };
                (
                    symbol,
                    json!({
                        "synced": sync.is_synced(),
                        "last_update_id": sync.last_update_id(),
                        "best_bid": level(book.and_then(|b| b.best_bid())),
                        "best_ask": level(book.and_then(|b| b.best_ask())),
                        "depth": book.map(|b| b.depth()),
                        "stats": sync.stats,
                    }),
                )
            })
            .collect();
        json!({ "events": self.stats, "books": books, "trades": self.trades })
    }

    fn book_outcome(
        &mut self,
        env: &Envelope,
        outcome: crate::book::Outcome,
        actions: &mut Vec<Action>,
    ) {
        for e in outcome.events {
            self.push(env, AuditEvent::Book(e));
        }
        if outcome.request_snapshot {
            actions.push(Action::RequestSnapshot(env.symbol.clone()));
        }
    }

    fn malformed(&mut self, env: &Envelope, error: String) {
        self.stats.malformed += 1;
        self.push(
            env,
            AuditEvent::Malformed {
                kind: env.kind,
                error: error.chars().take(200).collect(),
            },
        );
    }

    fn push(&mut self, env: &Envelope, event: AuditEvent) {
        push_bounded(&mut self.audit, env.seq, &env.symbol, event);
    }
}

fn push_bounded(audit: &mut VecDeque<AuditEntry>, seq: u64, symbol: &str, event: AuditEvent) {
    if audit.len() >= MAX_AUDIT {
        audit.pop_front();
    }
    audit.push_back(AuditEntry {
        seq,
        symbol: symbol.to_string(),
        event,
    });
}
