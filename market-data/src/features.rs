//! Order-flow features from the synced book and the aggregate-trade stream.
//!
//! Every value is exact decimal arithmetic over recorded envelopes, so live and replay
//! produce identical features. A value that cannot be computed from trustworthy state is
//! `None`; it is never estimated.
//!
//! Aggressor sign (Binance `aggTrade.m` = "buyer is the maker"): `m == false` means the
//! buyer lifted the ask, `+qty`; `m == true` means the seller hit the bid, `-qty`.

use std::collections::VecDeque;

use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;

use crate::binance::AggTrade;
use crate::book::OrderBook;

pub const FEATURE_SCHEMA_VERSION: u16 = 1;
/// Trades kept per symbol for rolling windows. Past this the oldest trade is evicted and
/// windows that still needed it report `None` until they are fully covered again.
pub const MAX_WINDOW_TRADES: usize = 100_000;
pub const MAX_WINDOW_MS: i64 = 3_600_000;
pub const MAX_WINDOWS: usize = 8;
/// OBI depth stays well inside the 1000-level REST snapshot: beyond it the local book holds
/// only levels that changed since the snapshot, so it is not a complete picture.
pub const MAX_OBI_LEVELS: usize = 500;
/// Division results are rounded half-to-even to this many decimals: stable text, and far
/// finer than any USD-M tick size.
pub const DIV_DP: u32 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureConfig {
    /// Rolling CVD windows, ascending, unique (only `new` and `default` construct this).
    cvd_windows_ms: Vec<i64>,
    /// OBI depths in price levels per side, ascending, unique.
    obi_levels: Vec<usize>,
}

impl Default for FeatureConfig {
    /// PRD V2.1: CVD over 1 s, 5 s, 15 s and 1 m; OBI over the top 10 and top 50 levels.
    fn default() -> Self {
        Self {
            cvd_windows_ms: vec![1_000, 5_000, 15_000, 60_000],
            obi_levels: vec![10, 50],
        }
    }
}

impl FeatureConfig {
    pub fn new(mut cvd_windows_ms: Vec<i64>, mut obi_levels: Vec<usize>) -> Result<Self, String> {
        cvd_windows_ms.sort_unstable();
        cvd_windows_ms.dedup();
        obi_levels.sort_unstable();
        obi_levels.dedup();
        if obi_levels.is_empty()
            || obi_levels.len() > MAX_WINDOWS
            || obi_levels.iter().any(|n| !(1..=MAX_OBI_LEVELS).contains(n))
        {
            return Err(format!(
                "1 to {MAX_WINDOWS} OBI depths of 1..={MAX_OBI_LEVELS} levels required"
            ));
        }
        if cvd_windows_ms.is_empty() || cvd_windows_ms.len() > MAX_WINDOWS {
            return Err(format!("1 to {MAX_WINDOWS} CVD windows required"));
        }
        if cvd_windows_ms
            .iter()
            .any(|w| !(1..=MAX_WINDOW_MS).contains(w))
        {
            return Err(format!("CVD windows must be 1..={MAX_WINDOW_MS} ms"));
        }
        Ok(Self {
            cvd_windows_ms,
            obi_levels,
        })
    }

    pub fn cvd_windows_ms(&self) -> &[i64] {
        &self.cvd_windows_ms
    }

    pub fn obi_levels(&self) -> &[usize] {
        &self.obi_levels
    }
}

/// Order book imbalance over the top `levels` price levels per side:
/// `(bid_qty - ask_qty) / (bid_qty + ask_qty)`, in [-1, 1]. `None` unless both sides hold
/// at least `levels` levels, so a thin or partially rebuilt book never looks balanced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Obi {
    pub levels: usize,
    pub value: Option<Decimal>,
}

/// Top-of-book prices. `microprice = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)`:
/// it leans toward the side with less resting size, where the next trade is likelier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BookFeatures {
    pub best_bid: Decimal,
    pub best_bid_qty: Decimal,
    pub best_ask: Decimal,
    pub best_ask_qty: Decimal,
    pub mid: Decimal,
    pub spread: Decimal,
    pub spread_bps: Decimal,
    pub microprice: Decimal,
    pub obi: Vec<Obi>,
}

/// `None` when either side is empty or the top of book has moved outside the range the
/// snapshot covered. Callers pass only a synced, uncrossed book.
/// Cost is O(max OBI depth), independent of how many messages arrived.
pub fn book_features(book: &OrderBook, obi_levels: &[usize]) -> Option<BookFeatures> {
    let (bid, bid_qty) = book.top_bids().next()?;
    let (ask, ask_qty) = book.top_asks().next()?;
    let mid = div(bid + ask, Decimal::TWO)?;
    let spread = ask - bid;
    let spread_bps = div(spread * Decimal::from(10_000), mid)?;
    let microprice = div(bid * ask_qty + ask * bid_qty, bid_qty + ask_qty)?;
    Some(BookFeatures {
        best_bid: bid.normalize(),
        best_bid_qty: bid_qty.normalize(),
        best_ask: ask.normalize(),
        best_ask_qty: ask_qty.normalize(),
        mid,
        spread: spread.normalize(),
        spread_bps,
        microprice,
        obi: obi(book, obi_levels),
    })
}

/// One pass per side down to the deepest requested level, summing cumulatively.
fn obi(book: &OrderBook, levels: &[usize]) -> Vec<Obi> {
    let bids = cumulative_qty(book.top_bids(), levels);
    let asks = cumulative_qty(book.top_asks(), levels);
    levels
        .iter()
        .zip(bids.into_iter().zip(asks))
        .map(|(&levels, sums)| Obi {
            levels,
            value: match sums {
                (Some(b), Some(a)) => div(b - a, b + a),
                _ => None,
            },
        })
        .collect()
}

/// Total quantity of the first `n` levels for each ascending `n`; `None` if the side has
/// fewer than `n` levels inside the range the book is known to mirror completely.
fn cumulative_qty(
    mut side: impl Iterator<Item = crate::binance::Level>,
    levels: &[usize],
) -> Vec<Option<Decimal>> {
    let (mut total, mut count) = (Decimal::ZERO, 0usize);
    levels
        .iter()
        .map(|&n| {
            for (_, qty) in side.by_ref().take(n.saturating_sub(count)) {
                total += qty;
                count += 1;
            }
            (count == n).then_some(total)
        })
        .collect()
}

/// One feature row, emitted after each depth event that leaves the book synced. Rows are a
/// pure function of the recorded envelopes, so replay reproduces them byte for byte.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeatureSnapshot {
    pub v: u16,
    pub symbol: String,
    /// Recorder sequence of the depth event behind this row (joins to the recording).
    pub seq: u64,
    pub recv_ts_ns: i64,
    pub exchange_ts_ms: Option<i64>,
    /// Feature clock the rolling windows were evaluated at.
    pub feature_ts_ms: Option<i64>,
    pub book_update_id: u64,
    /// Recorder sequence of the snapshot that last synchronized this book.
    pub synced_since_seq: u64,
    #[serde(flatten)]
    pub book: BookFeatures,
    pub cvd: Option<Decimal>,
    pub cvd_since_ms: Option<i64>,
    pub deltas: Vec<WindowDelta>,
    pub last_trade_ms: Option<i64>,
    pub trade_gaps: u64,
}

fn div(numerator: Decimal, denominator: Decimal) -> Option<Decimal> {
    numerator.checked_div(denominator).map(|v| {
        v.round_dp_with_strategy(DIV_DP, RoundingStrategy::MidpointNearestEven)
            .normalize()
    })
}

/// Signed aggressor quantity of the trades received so far with `T` in
/// `(t - window_ms, t]`, or `None` while the window reaches back before contiguous trade
/// coverage. Depth `E` can run slightly ahead of trade delivery, so a trade still in flight
/// is counted in the next row, not this one; replay reproduces this exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WindowDelta {
    pub window_ms: i64,
    pub delta: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeOutcome {
    Accepted,
    Duplicate,
    /// Trade ids skipped from `expected`; the CVD epoch restarted at this trade.
    Gap {
        expected: u64,
    },
}

/// Per-symbol trade flow: lifetime aggressor totals, an epoch CVD and rolling windows.
///
/// The CVD epoch restarts at every boundary where trades may have been missed (session
/// start, disconnect, trade-id gap, malformed trade). `cvd` is the signed sum since
/// `cvd_since_ms`, the trade time of the epoch's first trade.
///
/// Windows share one deque of `(trade_ms, signed_qty)`. Each window keeps a running sum
/// and the absolute index of its oldest included trade, so a trade or clock advance costs
/// amortized O(windows), never a scan of history. Decimal sums are exact, so the running
/// sum always equals a recomputation. Expiry assumes trade times are non-decreasing per
/// symbol, which holds for Binance aggregate trades (ids and `T` both increase).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TradeFlow {
    pub last_id: Option<u64>,
    pub trades: u64,
    pub duplicates: u64,
    pub gaps: u64,
    /// Aggressor volume since the process started, across epochs (audit only).
    pub aggressive_buy_qty: Decimal,
    pub aggressive_sell_qty: Decimal,
    pub last_price: Option<Decimal>,
    pub last_trade_ms: Option<i64>,
    pub cvd: Decimal,
    pub cvd_since_ms: Option<i64>,
    #[serde(skip)]
    windows_ms: Vec<i64>,
    /// Feature clock: latest exchange time seen on this symbol (trade `T`, depth `E`).
    #[serde(skip)]
    clock_ms: Option<i64>,
    /// Earliest time from which the deque holds every trade of the epoch.
    #[serde(skip)]
    covered_from_ms: Option<i64>,
    #[serde(skip)]
    window: VecDeque<(i64, Decimal)>,
    /// Absolute index of `window.front()`.
    #[serde(skip)]
    base: u64,
    #[serde(skip)]
    heads: Vec<u64>,
    #[serde(skip)]
    sums: Vec<Decimal>,
}

impl TradeFlow {
    pub fn new(windows_ms: &[i64]) -> Self {
        Self {
            last_id: None,
            trades: 0,
            duplicates: 0,
            gaps: 0,
            aggressive_buy_qty: Decimal::ZERO,
            aggressive_sell_qty: Decimal::ZERO,
            last_price: None,
            last_trade_ms: None,
            cvd: Decimal::ZERO,
            cvd_since_ms: None,
            windows_ms: windows_ms.to_vec(),
            clock_ms: None,
            covered_from_ms: None,
            window: VecDeque::new(),
            base: 0,
            heads: vec![0; windows_ms.len()],
            sums: vec![Decimal::ZERO; windows_ms.len()],
        }
    }

    pub fn on_trade(&mut self, trade: &AggTrade) -> TradeOutcome {
        if self.last_id.is_some_and(|last| trade.id <= last) {
            self.duplicates += 1; // Replayed or duplicated frame; never double count flow.
            return TradeOutcome::Duplicate;
        }
        let gap = self
            .last_id
            .filter(|last| trade.id != last + 1)
            .map(|last| last + 1);
        if gap.is_some() {
            self.gaps += 1;
            self.restart_epoch();
        }
        self.last_id = Some(trade.id);
        self.trades += 1;
        self.last_price = Some(trade.price);
        self.last_trade_ms = Some(trade.trade_ms);
        let signed = if trade.buyer_is_maker {
            self.aggressive_sell_qty += trade.qty;
            -trade.qty
        } else {
            self.aggressive_buy_qty += trade.qty;
            trade.qty
        };
        if self.cvd_since_ms.is_none() {
            self.cvd_since_ms = Some(trade.trade_ms);
            self.covered_from_ms = Some(trade.trade_ms);
        }
        self.cvd += signed;
        if self.window.len() >= MAX_WINDOW_TRADES {
            self.evict_front();
        }
        self.window.push_back((trade.trade_ms, signed));
        for sum in &mut self.sums {
            *sum += signed;
        }
        // After the push, so a trade older than the clock (depth `E` ran ahead) expires now.
        self.advance(trade.trade_ms);
        match gap {
            Some(expected) => TradeOutcome::Gap { expected },
            None => TradeOutcome::Accepted,
        }
    }

    /// Moves the feature clock forward (it never moves back) and expires window trades.
    pub fn advance(&mut self, ts_ms: i64) {
        let clock = self.clock_ms.map_or(ts_ms, |c| c.max(ts_ms));
        self.clock_ms = Some(clock);
        let end = self.base + self.window.len() as u64;
        for i in 0..self.windows_ms.len() {
            let cutoff = clock.saturating_sub(self.windows_ms[i]);
            while self.heads[i] < end {
                let (ts, qty) = self.window[(self.heads[i] - self.base) as usize];
                if ts > cutoff {
                    break;
                }
                self.sums[i] -= qty;
                self.heads[i] += 1;
            }
        }
        // The widest window's head is the minimum; `min` needs no sorted-config invariant.
        let keep_from = self.heads.iter().copied().min().unwrap_or(end);
        while self.base < keep_from {
            self.window.pop_front();
            self.base += 1;
        }
    }

    /// Flow of unknown size was lost (malformed trade). `last_id` is kept, so duplicates
    /// are still rejected and the missing id is counted as a gap.
    pub fn invalidate(&mut self) {
        self.restart_epoch();
    }

    /// Disconnect: trades missed while offline are a known boundary, so the next trade
    /// starts a fresh epoch without counting a gap.
    pub fn reset_session(&mut self) {
        self.restart_epoch();
        self.last_id = None;
    }

    pub fn clock_ms(&self) -> Option<i64> {
        self.clock_ms
    }

    pub fn epoch_cvd(&self) -> Option<Decimal> {
        self.cvd_since_ms.map(|_| self.cvd.normalize())
    }

    pub fn deltas(&self) -> Vec<WindowDelta> {
        self.windows_ms
            .iter()
            .zip(&self.sums)
            .map(|(&window_ms, sum)| {
                let covered = matches!(
                    (self.clock_ms, self.covered_from_ms),
                    (Some(clock), Some(from)) if clock.saturating_sub(window_ms) >= from
                );
                WindowDelta {
                    window_ms,
                    delta: covered.then(|| sum.normalize()),
                }
            })
            .collect()
    }

    pub fn window_len(&self) -> usize {
        self.window.len()
    }

    fn restart_epoch(&mut self) {
        self.cvd = Decimal::ZERO;
        self.cvd_since_ms = None;
        self.covered_from_ms = None;
        self.window.clear();
        self.base = 0;
        self.heads.iter_mut().for_each(|h| *h = 0);
        self.sums.iter_mut().for_each(|s| *s = Decimal::ZERO);
    }

    /// Memory cap reached: drop the oldest trade. Coverage moves up to its timestamp, so a
    /// window still needing it (windows are `(t - W, t]`) reports `None`, not a partial sum.
    fn evict_front(&mut self) {
        let Some((ts, qty)) = self.window.pop_front() else {
            return;
        };
        for (head, sum) in self.heads.iter_mut().zip(&mut self.sums) {
            if *head == self.base {
                *sum -= qty;
                *head += 1;
            }
        }
        self.base += 1;
        self.covered_from_ms = self.covered_from_ms.map(|from| from.max(ts));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn trade(id: u64, ms: i64, qty: &str, buyer_is_maker: bool) -> AggTrade {
        AggTrade {
            id,
            price: d("100"),
            qty: d(qty),
            trade_ms: ms,
            buyer_is_maker,
        }
    }

    fn deltas(flow: &TradeFlow) -> Vec<Option<String>> {
        flow.deltas()
            .into_iter()
            .map(|w| w.delta.map(|v| v.to_string()))
            .collect()
    }

    #[test]
    fn config_sorts_dedups_and_rejects_bad_windows() {
        let cfg = FeatureConfig::new(vec![5_000, 1_000, 5_000], vec![50, 10]).unwrap();
        assert_eq!(cfg.cvd_windows_ms, vec![1_000, 5_000]);
        assert_eq!(cfg.obi_levels, vec![10, 50]);
        let obi = vec![10];
        assert!(FeatureConfig::new(vec![], obi.clone()).is_err());
        assert!(FeatureConfig::new(vec![0], obi.clone()).is_err());
        assert!(FeatureConfig::new(vec![MAX_WINDOW_MS + 1], obi.clone()).is_err());
        assert!(FeatureConfig::new((1..=9).collect(), obi).is_err());
        assert!(FeatureConfig::new(vec![1_000], vec![]).is_err());
        assert!(FeatureConfig::new(vec![1_000], vec![0]).is_err());
        assert!(FeatureConfig::new(vec![1_000], vec![MAX_OBI_LEVELS + 1]).is_err());
    }

    #[test]
    fn aggressor_sign_follows_binance_m_flag() {
        let mut flow = TradeFlow::new(&[1_000]);
        flow.on_trade(&trade(1, 10_000, "2", false)); // Buyer aggressed.
        flow.on_trade(&trade(2, 10_000, "0.5", true)); // Seller aggressed.
        assert_eq!(flow.epoch_cvd(), Some(d("1.5")));
        assert_eq!(flow.aggressive_buy_qty, d("2"));
        assert_eq!(flow.aggressive_sell_qty, d("0.5"));
    }

    /// Golden fixture, values worked by hand. Windows 1 s and 5 s, coverage from t=10 000.
    #[test]
    fn golden_rolling_windows() {
        let mut flow = TradeFlow::new(&[1_000, 5_000]);
        flow.on_trade(&trade(1, 10_000, "1", false)); // +1
        flow.on_trade(&trade(2, 10_400, "2", true)); // -2
        flow.on_trade(&trade(3, 10_900, "0.25", false)); // +0.25
        flow.advance(11_000);
        // 1 s window (10 000, 11 000]: -2 + 0.25. 5 s window reaches before coverage.
        assert_eq!(deltas(&flow), vec![Some("-1.75".into()), None]);
        flow.on_trade(&trade(4, 14_000, "3", false)); // +3
        flow.advance(14_999);
        // One millisecond short of coverage: (9 999, 14 999] starts before the first trade.
        assert_eq!(deltas(&flow)[1], None);
        flow.advance(15_000);
        // 1 s (14 000, 15 000]: nothing. 5 s (10 000, 15 000]: trades 2-4 = 1.25.
        assert_eq!(deltas(&flow), vec![Some("0".into()), Some("1.25".into())]);
        flow.advance(15_400);
        // 5 s (10 400, 15 400]: trade 2 at exactly 10 400 leaves: 0.25 + 3.
        assert_eq!(deltas(&flow), vec![Some("0".into()), Some("3.25".into())]);
        assert_eq!(flow.epoch_cvd(), Some(d("2.25")));
        assert_eq!(flow.cvd_since_ms, Some(10_000));
        assert_eq!(flow.window_len(), 2); // Expired trades are released.
    }

    #[test]
    fn clock_never_moves_backwards() {
        let mut flow = TradeFlow::new(&[1_000]);
        flow.on_trade(&trade(1, 10_000, "1", false));
        flow.advance(12_000);
        flow.advance(9_000);
        assert_eq!(flow.clock_ms(), Some(12_000));
        assert_eq!(deltas(&flow), vec![Some("0".into())]);
    }

    #[test]
    fn trade_gap_restarts_the_epoch() {
        let mut flow = TradeFlow::new(&[1_000]);
        flow.on_trade(&trade(1, 10_000, "1", false));
        flow.advance(11_500);
        assert_eq!(deltas(&flow), vec![Some("0".into())]);
        assert_eq!(
            flow.on_trade(&trade(5, 11_600, "2", true)),
            TradeOutcome::Gap { expected: 2 }
        );
        assert_eq!(flow.epoch_cvd(), Some(d("-2")));
        assert_eq!(flow.cvd_since_ms, Some(11_600));
        assert_eq!(deltas(&flow), vec![None]); // (10 600, 11 600] predates coverage.
        flow.advance(12_600);
        // Coverage is known only after the first trade's millisecond, so a valid window
        // never claims it: (11 600, 12 600] excludes it.
        assert_eq!(deltas(&flow), vec![Some("0".into())]);
        assert_eq!(flow.gaps, 1);
    }

    #[test]
    fn duplicates_are_ignored_and_session_reset_clears_epoch() {
        let mut flow = TradeFlow::new(&[1_000]);
        flow.on_trade(&trade(7, 10_000, "1", false));
        assert_eq!(
            flow.on_trade(&trade(7, 10_000, "1", false)),
            TradeOutcome::Duplicate
        );
        assert_eq!(flow.epoch_cvd(), Some(d("1")));
        flow.reset_session();
        assert_eq!(flow.epoch_cvd(), None);
        assert_eq!(deltas(&flow), vec![None]);
        // After a reset any id is accepted without a gap: missed trades are a known boundary.
        assert_eq!(
            flow.on_trade(&trade(3, 20_000, "1", true)),
            TradeOutcome::Accepted
        );
        assert_eq!((flow.duplicates, flow.gaps), (1, 0));
    }

    #[test]
    fn memory_cap_evicts_and_marks_wide_windows_unknown() {
        let n = MAX_WINDOW_TRADES as u64 + 10;
        // The wide window would be covered (1 000 005 >= first trade at 1 000 001) if the
        // ten oldest trades had not been evicted.
        let mut flow = TradeFlow::new(&[10, n as i64]);
        for id in 1..=n {
            flow.on_trade(&trade(id, 1_000_000 + id as i64, "1", false));
        }
        assert_eq!(flow.window_len(), MAX_WINDOW_TRADES);
        flow.advance(1_000_000 + n as i64 + 5);
        assert_eq!(deltas(&flow), vec![Some("5".into()), None]);
        assert_eq!(flow.epoch_cvd(), Some(Decimal::from(n)));
        // Covered again from just after the last evicted trade (1 000 010): the sum must
        // exclude every evicted trade. (1 000 011, 1 100 021] holds trades 12..=n.
        flow.advance(1_000_011 + n as i64);
        assert_eq!(deltas(&flow)[1], Some((n - 11).to_string()));
    }

    fn book(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> OrderBook {
        let side = |l: &[(&str, &str)]| l.iter().map(|(p, q)| (d(p), d(q))).collect();
        OrderBook::from_snapshot(&crate::binance::Snapshot {
            last_update_id: 1,
            event_ms: None,
            bids: side(bids),
            asks: side(asks),
        })
    }

    /// Golden fixture; expected values computed independently (Python `decimal`, 40 digits,
    /// quantized half-even to 16 dp).
    #[test]
    fn golden_book_features() {
        let book = book(
            &[("100", "2"), ("99", "3"), ("98", "5")],
            &[("101", "1"), ("102", "4"), ("103", "10")],
        );
        let f = book_features(&book, &[1, 2, 3, 4]).unwrap();
        let s = |v: Decimal| v.to_string();
        assert_eq!(s(f.mid), "100.5");
        assert_eq!(s(f.spread), "1");
        assert_eq!(s(f.spread_bps), "99.5024875621890547");
        // (100 * 1 + 101 * 2) / 3: leans toward the thinner ask.
        assert_eq!(s(f.microprice), "100.6666666666666667");
        let obi: Vec<_> = f.obi.iter().map(|o| o.value.map(s)).collect();
        assert_eq!(
            obi,
            vec![
                Some("0.3333333333333333".into()), // (2 - 1) / 3
                Some("0".into()),                  // (5 - 5) / 10
                Some("-0.2".into()),               // (10 - 15) / 25
                None,                              // only 3 levels per side
            ]
        );
    }

    #[test]
    fn book_features_need_both_sides_and_normalize_text() {
        assert!(book_features(&book(&[("100", "1")], &[]), &[1]).is_none());
        let f = book_features(&book(&[("100.10", "1.50")], &[("100.20", "1.50")]), &[1]).unwrap();
        assert_eq!(f.best_bid.to_string(), "100.1");
        assert_eq!(f.spread.to_string(), "0.1");
        assert_eq!(f.microprice.to_string(), "100.15"); // Equal sizes: microprice = mid.
        assert_eq!(f.obi[0].value, Some(Decimal::ZERO));
    }

    /// Running sums must equal a brute-force recomputation at every step.
    #[test]
    fn running_sums_match_recomputation() {
        let windows = [7, 50, 300];
        let mut flow = TradeFlow::new(&windows);
        let mut all: Vec<(i64, Decimal)> = Vec::new();
        let mut seed = 0x2545_f491_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let (mut ts, mut clock) = (1_000i64, 1_000i64);
        let mut first = None;
        for id in 1..5_000u64 {
            ts += (next() % 5) as i64;
            let qty = Decimal::new((next() % 1_000) as i64 + 1, 3);
            let maker = next() % 2 == 0;
            flow.on_trade(&trade(id, ts, &qty.to_string(), maker));
            all.push((ts, if maker { -qty } else { qty }));
            let first = *first.get_or_insert(ts);
            clock = clock.max(ts);
            if next() % 3 == 0 {
                clock += (next() % 20) as i64;
                flow.advance(clock);
            }
            for (w, got) in windows.iter().zip(flow.deltas()) {
                let want: Decimal = all
                    .iter()
                    .filter(|(t, _)| *t > clock - w && *t <= clock)
                    .map(|(_, q)| *q)
                    .sum();
                if clock - w >= first {
                    assert_eq!(got.delta, Some(want.normalize()), "window {w} at {clock}");
                } else {
                    assert_eq!(got.delta, None);
                }
            }
        }
    }
}
