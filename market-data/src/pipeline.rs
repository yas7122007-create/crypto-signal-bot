//! The one processing path shared by live ingestion and replay. It only reads envelopes,
//! so replaying a recording reproduces the exact same book states and audit trail.

use std::collections::{BTreeMap, VecDeque};

use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};

use crate::binance::{parse_agg_trade, parse_depth, parse_snapshot};
use crate::book::{DepthSync, SyncEvent};
use crate::event::{Envelope, Kind};
use crate::features::{
    book_features, FeatureConfig, FeatureSnapshot, TradeFlow, TradeOutcome, TradeState,
    FEATURE_SCHEMA_VERSION,
};

pub const MAX_AUDIT: usize = 1_000;
/// A synced book with no diff for this long (by recorded receive time) is no longer trusted.
/// Other streams can keep the socket alive while one symbol's depth stream stalls.
pub const STALE_DEPTH_NS: i64 = 30_000_000_000;

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    RequestSnapshot(String),
}

/// Result of handling one envelope.
#[derive(Debug, Default, PartialEq)]
pub struct Step {
    pub actions: Vec<Action>,
    pub feature: Option<FeatureSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuditEvent {
    Book(SyncEvent),
    TradeGap {
        expected: u64,
        got: u64,
    },
    Malformed {
        kind: Kind,
        error: String,
    },
    Connection {
        event: String,
    },
    /// The recording was made with a different feature configuration than this pipeline.
    ConfigMismatch {
        recorded: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AuditEntry {
    pub seq: u64,
    pub symbol: String,
    pub event: AuditEvent,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct PipelineStats {
    pub events: u64,
    pub book_tickers: u64,
    pub mark_prices: u64,
    pub unparsed: u64,
    pub malformed: u64,
    pub out_of_order_seq: u64,
    pub feature_rows: u64,
    /// Sessions whose recorded feature config differs from this pipeline's.
    pub config_mismatch: u64,
    /// Sessions recorded without a feature config (older recordings): not checkable.
    pub config_unverified: u64,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Pipeline {
    config: FeatureConfig,
    books: BTreeMap<String, DepthSync>,
    trades: BTreeMap<String, TradeFlow>,
    synced_since: BTreeMap<String, u64>,
    session_id: Option<i64>,
    audit: VecDeque<AuditEntry>,
    last_seq: Option<u64>,
    last_depth_ns: BTreeMap<String, i64>,
    pub stats: PipelineStats,
}

impl Pipeline {
    pub fn new(config: FeatureConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    pub fn handle(&mut self, env: &Envelope) -> Step {
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
        let is_depth = matches!(env.kind, Kind::Depth | Kind::DepthSnapshot);
        if is_depth {
            self.last_depth_ns
                .insert(env.symbol.clone(), env.recv_ts_ns);
        }
        match env.kind {
            Kind::Depth => match parse_depth(payload) {
                Ok(update) => {
                    self.depth_clock(env);
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
                    self.depth_clock(env);
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
                    if let TradeOutcome::Gap { expected } = self.flow(&env.symbol).on_trade(&trade)
                    {
                        self.push(
                            env,
                            AuditEvent::TradeGap {
                                expected,
                                got: trade.id,
                            },
                        );
                    }
                }
                Err(e) => {
                    // The lost trade's flow is unknown: restart the CVD epoch.
                    self.malformed(env, e.to_string());
                    self.flow(&env.symbol).invalidate();
                }
            },
            Kind::BookTicker => self.stats.book_tickers += 1,
            Kind::MarkPrice => self.stats.mark_prices += 1,
            Kind::Unparsed => self.stats.unparsed += 1,
            Kind::Connection => {
                let event = serde_json::from_str::<Value>(payload)
                    .ok()
                    .and_then(|v| v.get("event").and_then(Value::as_str).map(str::to_string))
                    .unwrap_or_default();
                if event == "session_start" {
                    // A new process: nothing per symbol carries over, so replaying many
                    // sessions yields the same rows each live run wrote.
                    self.trades.clear();
                    self.synced_since.clear();
                    self.session_id = Some(env.recv_ts_ns);
                    self.check_config(env, payload);
                }
                if event == "disconnected" || event == "session_start" {
                    for (symbol, book) in self.books.iter_mut() {
                        for e in book.reset(&event).events {
                            push_bounded(&mut self.audit, env.seq, symbol, AuditEvent::Book(e));
                        }
                    }
                    for flow in self.trades.values_mut() {
                        flow.reset_session(); // Trades missed while offline: new CVD epoch.
                    }
                }
                self.push(env, AuditEvent::Connection { event });
            }
        }
        self.expire_stale_books(env);
        let feature = if is_depth { self.feature(env) } else { None };
        if feature.is_some() {
            self.stats.feature_rows += 1;
        }
        Step { actions, feature }
    }

    fn check_config(&mut self, env: &Envelope, payload: &str) {
        let recorded = serde_json::from_str::<Value>(payload)
            .ok()
            .and_then(|v| v.get("feature_config").cloned());
        match recorded {
            None => self.stats.config_unverified += 1,
            Some(recorded) => {
                let same = serde_json::from_value::<FeatureConfig>(recorded.clone())
                    .is_ok_and(|c| c == self.config);
                if !same {
                    self.stats.config_mismatch += 1;
                    self.push(env, AuditEvent::ConfigMismatch { recorded });
                }
            }
        }
    }

    /// Only a depth event that parsed may move the feature clock (it never moves back).
    fn depth_clock(&mut self, env: &Envelope) {
        let flow = self.flow(&env.symbol);
        if let Some(ts) = env.exchange_ts_ms {
            flow.advance(ts);
        }
    }

    fn flow(&mut self, symbol: &str) -> &mut TradeFlow {
        let windows = self.config.cvd_windows_ms();
        self.trades
            .entry(symbol.to_string())
            .or_insert_with(|| TradeFlow::new(windows))
    }

    /// Features only from a synced, uncrossed, fresh book; otherwise no row at all.
    fn feature(&self, env: &Envelope) -> Option<FeatureSnapshot> {
        let sync = self.books.get(&env.symbol)?;
        let book = book_features(sync.book()?, self.config.obi_levels())?;
        let flow = self.trades.get(&env.symbol)?;
        let (quiet, stale) = self.config.trade_silence_ms();
        let trade_state = flow.trade_state(quiet, stale);
        // A stale stream may be missing trades: withhold flow instead of reporting zeros.
        let fresh = trade_state != TradeState::Stale;
        let epoch = |v: Decimal| (fresh && flow.cvd_since_ms.is_some()).then(|| v.normalize());
        let mut deltas = flow.deltas();
        if !fresh {
            deltas.iter_mut().for_each(|d| d.delta = None);
        }
        Some(FeatureSnapshot {
            v: FEATURE_SCHEMA_VERSION,
            symbol: env.symbol.clone(),
            session_id: self.session_id,
            seq: env.seq,
            recv_ts_ns: env.recv_ts_ns,
            exchange_ts_ms: env.exchange_ts_ms,
            feature_ts_ms: flow.clock_ms(),
            book_update_id: sync.last_update_id()?,
            synced_since_seq: self.synced_since.get(&env.symbol).copied()?,
            book,
            trade_state,
            cvd: flow.epoch_cvd().filter(|_| fresh),
            cvd_since_ms: flow.cvd_since_ms,
            buy_qty: epoch(flow.epoch_buy_qty),
            sell_qty: epoch(flow.epoch_sell_qty),
            deltas,
            last_trade_ms: flow.last_trade_ms,
            trade_gaps: flow.gaps,
        })
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
        json!({
            "events": self.stats,
            "books": books,
            "trades": self.trades,
        })
    }

    fn book_outcome(
        &mut self,
        env: &Envelope,
        outcome: crate::book::Outcome,
        actions: &mut Vec<Action>,
    ) {
        for e in outcome.events {
            if matches!(e, SyncEvent::Synced { .. }) {
                self.synced_since.insert(env.symbol.clone(), env.seq);
            }
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
