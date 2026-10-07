//! One-minute bars aggregated from feature rows: the input of the forecasting dataset and
//! of the Python bridge. Built inside the shared pipeline, so replay reproduces them.
//!
//! A bar covers `[start_ms, start_ms + BAR_MS)` on the feature clock and is emitted when the
//! first row of a later bar arrives, so a bar never contains data from after its end.
//! It is `complete` only if every part of it came from one valid book: one session, one
//! sync epoch, and no stretch longer than `MAX_ROW_GAP_MS` without a feature row (rows stop
//! while the book is invalid). Flow fields cover `(previous close, this close]` and are
//! null unless both closes are in the same CVD epoch, the previous bar is the adjacent one,
//! and the trade stream is not stale.

use std::collections::BTreeMap;

use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;

use crate::features::{FeatureSnapshot, Obi, TradeState, DIV_DP};

pub const BAR_SCHEMA_VERSION: u16 = 1;
pub const BAR_MS: i64 = 60_000;
/// Longest stretch inside a bar without a feature row before the bar is incomplete.
/// Liquid USD-M books change every 100 ms; ten seconds of silence means the book was
/// invalid or the depth stream stalled.
pub const MAX_ROW_GAP_MS: i64 = 10_000;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Bar {
    pub v: u16,
    pub feature_v: u16,
    pub symbol: String,
    pub session_id: Option<i64>,
    pub start_ms: i64,
    pub end_ms: i64,
    pub rows: u32,
    pub complete: bool,
    /// Why the bar is incomplete: `row_gap` or `resync`; null when complete.
    pub incomplete_reason: Option<&'static str>,
    pub synced_since_seq: u64,
    pub close_seq: u64,
    pub open_mid: Decimal,
    pub high_mid: Decimal,
    pub low_mid: Decimal,
    pub close_mid: Decimal,
    pub close_microprice: Decimal,
    pub mean_spread_bps: Decimal,
    pub close_spread_bps: Decimal,
    pub close_obi: Vec<Obi>,
    pub close_trade_state: TradeState,
    /// Aggressor buy and sell quantity in `(previous close, this close]`.
    pub buy_qty: Option<Decimal>,
    pub sell_qty: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq)]
struct Open {
    bar: Bar,
    spread_sum: Decimal,
    last_ts: i64,
    /// Epoch and totals at the previous bar's close, for flow differences.
    base: Option<FlowMark>,
    /// Epoch and totals at this bar's latest row: the base for the next bar.
    pending_mark: Option<FlowMark>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct FlowMark {
    end_ms: i64,
    cvd_since_ms: i64,
    buy: Decimal,
    sell: Decimal,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct BarBuilder {
    open: BTreeMap<String, Open>,
    /// Flow mark of each symbol's last emitted bar close.
    marks: BTreeMap<String, Option<FlowMark>>,
}

impl BarBuilder {
    /// Feeds one feature row; returns the bar it closed, if any.
    pub fn on_row(&mut self, row: &FeatureSnapshot) -> Option<Bar> {
        let ts = row.feature_ts_ms?;
        let start = ts.div_euclid(BAR_MS) * BAR_MS;
        let closed = match self.open.get(&row.symbol) {
            Some(open) if open.bar.start_ms == start => None,
            Some(_) => self.close(&row.symbol),
            None => None,
        };
        match self.open.get_mut(&row.symbol) {
            Some(open) => open.add(row, ts),
            None => {
                let base = self.marks.get(&row.symbol).copied().flatten();
                self.open
                    .insert(row.symbol.clone(), Open::new(row, ts, start, base));
            }
        }
        closed
    }

    /// Session boundary: open bars belong to a run that has ended and are never emitted,
    /// exactly as a live process that stopped would not have emitted them.
    pub fn reset(&mut self) {
        self.open.clear();
        self.marks.clear();
    }

    fn close(&mut self, symbol: &str) -> Option<Bar> {
        let mut open = self.open.remove(symbol)?;
        // The stretch from the last row to the end of the bar must be covered too: no row
        // for that long means the book was invalid or the depth stream stalled.
        if open.bar.end_ms.saturating_sub(open.last_ts) > MAX_ROW_GAP_MS {
            open.mark_incomplete("row_gap");
        }
        let bar = open.finish();
        self.marks.insert(symbol.to_string(), open.mark_after());
        Some(bar)
    }
}

impl Open {
    fn new(row: &FeatureSnapshot, ts: i64, start: i64, base: Option<FlowMark>) -> Self {
        let mid = row.book.mid;
        let mut open = Self {
            bar: Bar {
                v: BAR_SCHEMA_VERSION,
                feature_v: row.v,
                symbol: row.symbol.clone(),
                session_id: row.session_id,
                start_ms: start,
                end_ms: start + BAR_MS,
                rows: 0,
                complete: true,
                incomplete_reason: None,
                synced_since_seq: row.synced_since_seq,
                close_seq: row.seq,
                open_mid: mid,
                high_mid: mid,
                low_mid: mid,
                close_mid: mid,
                close_microprice: row.book.microprice,
                mean_spread_bps: row.book.spread_bps,
                close_spread_bps: row.book.spread_bps,
                close_obi: row.book.obi.clone(),
                close_trade_state: row.trade_state,
                buy_qty: None,
                sell_qty: None,
            },
            spread_sum: Decimal::ZERO,
            last_ts: start,
            base,
            pending_mark: None,
        };
        open.add(row, ts);
        open
    }

    fn add(&mut self, row: &FeatureSnapshot, ts: i64) {
        if ts.saturating_sub(self.last_ts) > MAX_ROW_GAP_MS {
            self.mark_incomplete("row_gap");
        }
        if row.synced_since_seq != self.bar.synced_since_seq {
            self.mark_incomplete("resync");
        }
        let b = &mut self.bar;
        let mid = row.book.mid;
        b.rows += 1;
        b.high_mid = b.high_mid.max(mid);
        b.low_mid = b.low_mid.min(mid);
        b.close_mid = mid;
        b.close_microprice = row.book.microprice;
        b.close_spread_bps = row.book.spread_bps;
        b.close_obi = row.book.obi.clone();
        b.close_trade_state = row.trade_state;
        b.close_seq = row.seq;
        b.synced_since_seq = row.synced_since_seq;
        self.spread_sum += row.book.spread_bps;
        self.last_ts = ts;
        self.close_flow(row);
    }

    /// Flow at the latest row, measured from the previous bar's close when possible.
    fn close_flow(&mut self, row: &FeatureSnapshot) {
        let now = match (row.cvd_since_ms, row.buy_qty, row.sell_qty) {
            (Some(since), Some(buy), Some(sell)) => Some((since, buy, sell)),
            _ => None,
        };
        let flow = match (self.base, now) {
            (Some(base), Some((since, buy, sell)))
                if base.cvd_since_ms == since && base.end_ms == self.bar.start_ms =>
            {
                Some((buy - base.buy, sell - base.sell))
            }
            _ => None,
        };
        self.bar.buy_qty = flow.map(|f| f.0.normalize());
        self.bar.sell_qty = flow.map(|f| f.1.normalize());
        self.pending_mark = now.map(|(since, buy, sell)| FlowMark {
            end_ms: self.bar.end_ms,
            cvd_since_ms: since,
            buy,
            sell,
        });
    }

    fn mark_incomplete(&mut self, reason: &'static str) {
        self.bar.complete = false;
        self.bar.incomplete_reason.get_or_insert(reason);
    }

    fn finish(&mut self) -> Bar {
        let rows = Decimal::from(self.bar.rows.max(1));
        self.bar.mean_spread_bps = self
            .spread_sum
            .checked_div(rows)
            .map(|v| {
                v.round_dp_with_strategy(DIV_DP, RoundingStrategy::MidpointNearestEven)
                    .normalize()
            })
            .unwrap_or(self.bar.close_spread_bps);
        self.bar.clone()
    }

    fn mark_after(&self) -> Option<FlowMark> {
        self.pending_mark
    }
}
