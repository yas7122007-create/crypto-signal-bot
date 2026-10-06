//! Order-flow features from the synced book and the aggregate-trade stream.
//!
//! Every value is exact decimal arithmetic over recorded envelopes, so live and replay
//! produce identical features. A value that cannot be computed from trustworthy state is
//! `None`; it is never estimated.
//!
//! Aggressor sign (Binance `aggTrade.m` = "buyer is the maker"): `m == false` means the
//! buyer lifted the ask, `+qty`; `m == true` means the seller hit the bid, `-qty`.

use std::collections::VecDeque;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::binance::AggTrade;

pub const FEATURE_SCHEMA_VERSION: u16 = 1;
/// Trades kept per symbol for rolling windows. Past this the oldest trade is evicted and
/// windows that still needed it report `None` until they are fully covered again.
pub const MAX_WINDOW_TRADES: usize = 100_000;
pub const MAX_WINDOW_MS: i64 = 3_600_000;
pub const MAX_WINDOWS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureConfig {
    /// Rolling CVD windows, ascending, unique.
    pub cvd_windows_ms: Vec<i64>,
}

impl Default for FeatureConfig {
    /// PRD V2.1 feature table: CVD over 1 s, 5 s, 15 s and 1 m.
    fn default() -> Self {
        Self {
            cvd_windows_ms: vec![1_000, 5_000, 15_000, 60_000],
        }
    }
}

impl FeatureConfig {
    pub fn new(mut cvd_windows_ms: Vec<i64>) -> Result<Self, String> {
        cvd_windows_ms.sort_unstable();
        cvd_windows_ms.dedup();
        if cvd_windows_ms.is_empty() || cvd_windows_ms.len() > MAX_WINDOWS {
            return Err(format!("1 to {MAX_WINDOWS} CVD windows required"));
        }
        if cvd_windows_ms
            .iter()
            .any(|w| !(1..=MAX_WINDOW_MS).contains(w))
        {
            return Err(format!("CVD windows must be 1..={MAX_WINDOW_MS} ms"));
        }
        Ok(Self { cvd_windows_ms })
    }
}

/// Signed aggressor quantity over `(t - window_ms, t]`, or `None` while the window reaches
/// back before the start of contiguous trade coverage.
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
        // The widest window is last (sorted config), so its head is the minimum.
        let keep_from = self.heads.last().copied().unwrap_or(end);
        while self.base < keep_from {
            self.window.pop_front();
            self.base += 1;
        }
    }

    /// Boundary where trades may have been missed but ids continue (malformed trade).
    pub fn invalidate(&mut self) {
        self.restart_epoch();
        self.last_id = None;
    }

    /// Session start or disconnect: the next trade starts a fresh epoch without a gap.
    pub fn reset_session(&mut self) {
        self.invalidate();
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

    /// Memory cap reached: drop the oldest trade. Windows still holding it lose coverage
    /// back to just after its timestamp, so they report `None` instead of a partial sum.
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
        self.covered_from_ms = self.covered_from_ms.map(|from| from.max(ts + 1));
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
        let cfg = FeatureConfig::new(vec![5_000, 1_000, 5_000]).unwrap();
        assert_eq!(cfg.cvd_windows_ms, vec![1_000, 5_000]);
        assert!(FeatureConfig::new(vec![]).is_err());
        assert!(FeatureConfig::new(vec![0]).is_err());
        assert!(FeatureConfig::new(vec![MAX_WINDOW_MS + 1]).is_err());
        assert!(FeatureConfig::new((1..=9).collect()).is_err());
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
