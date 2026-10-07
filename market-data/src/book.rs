//! Local L2 book and the Binance USD-M snapshot + diff synchronization rules:
//! buffer diffs, fetch a snapshot, drop diffs with `u < lastUpdateId`, require the first
//! applied diff to bracket `lastUpdateId` (`U <= id <= u`), then require `pu == previous u`.
//! Any violation invalidates the book; it never keeps serving data after corruption.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::binance::{DepthUpdate, Level, Snapshot, SNAPSHOT_LIMIT};

pub const MAX_LEVELS: usize = 5_000;
pub const MAX_BUFFER: usize = 4_096;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    /// Deepest snapshot price per side when the snapshot was cut at its level limit.
    /// Beyond it the book only knows levels that changed since, so it is incomplete there.
    bid_floor: Option<Decimal>,
    ask_ceiling: Option<Decimal>,
}

impl OrderBook {
    pub fn from_snapshot(snapshot: &Snapshot) -> Self {
        let mut book = Self::default();
        book.apply(&snapshot.bids, &snapshot.asks);
        let full = |side: &[Level]| side.len() >= SNAPSHOT_LIMIT as usize;
        if full(&snapshot.bids) {
            book.bid_floor = snapshot.bids.iter().map(|l| l.0).min();
        }
        if full(&snapshot.asks) {
            book.ask_ceiling = snapshot.asks.iter().map(|l| l.0).max();
        }
        book
    }

    fn apply(&mut self, bids: &[Level], asks: &[Level]) {
        for (side, levels) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
            for &(price, qty) in levels {
                if qty.is_zero() {
                    side.remove(&price);
                } else {
                    side.insert(price, qty);
                }
            }
        }
        // Updates carry absolute quantities, so trimming far levels cannot corrupt near ones.
        while self.bids.len() > MAX_LEVELS {
            self.bids.pop_first();
        }
        while self.asks.len() > MAX_LEVELS {
            self.asks.pop_last();
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids.last_key_value().map(|(p, q)| (*p, *q))
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first_key_value().map(|(p, q)| (*p, *q))
    }

    /// Bids from the best price down, stopping at the deepest price the snapshot covered.
    /// A `BTreeMap` range: the bound is found once, not compared against every level.
    pub fn top_bids(&self) -> impl Iterator<Item = Level> + '_ {
        let floor = self.bid_floor.map_or(Bound::Unbounded, Bound::Included);
        self.bids
            .range((floor, Bound::Unbounded))
            .rev()
            .map(|(p, q)| (*p, *q))
    }

    /// Asks from the best price up, stopping at the deepest price the snapshot covered.
    pub fn top_asks(&self) -> impl Iterator<Item = Level> + '_ {
        let ceiling = self.ask_ceiling.map_or(Bound::Unbounded, Bound::Included);
        self.asks
            .range((Bound::Unbounded, ceiling))
            .map(|(p, q)| (*p, *q))
    }

    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    fn crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some((b, _)), Some((a, _))) if b >= a)
    }
}

/// Audit trail entry for every state change of a book.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum SyncEvent {
    Synced {
        last_update_id: u64,
    },
    Gap {
        expected_prev: u64,
        got_prev: u64,
        got_first: u64,
        got_final: u64,
    },
    Invalidated {
        reason: String,
    },
    SnapshotIgnored {
        last_update_id: u64,
    },
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct SyncStats {
    pub applied: u64,
    pub stale_dropped: u64,
    pub buffer_dropped: u64,
    pub gaps: u64,
    pub invalidations: u64,
    pub snapshots_applied: u64,
    pub crossed: u64,
}

#[derive(Debug, Default, PartialEq)]
pub struct Outcome {
    pub events: Vec<SyncEvent>,
    pub request_snapshot: bool,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct DepthSync {
    book: OrderBook,
    synced: bool,
    /// `u` of the last applied diff; `None` right after a snapshot.
    last_final: Option<u64>,
    snapshot_id: u64,
    buffer: VecDeque<DepthUpdate>,
    snapshot_requested: bool,
    pub stats: SyncStats,
}

impl DepthSync {
    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// The book, only while it is known to be consistent.
    pub fn book(&self) -> Option<&OrderBook> {
        self.synced.then_some(&self.book)
    }

    pub fn last_update_id(&self) -> Option<u64> {
        self.synced
            .then(|| self.last_final.unwrap_or(self.snapshot_id))
    }

    pub fn on_update(&mut self, update: DepthUpdate) -> Outcome {
        let mut out = Outcome::default();
        self.handle(update, &mut out);
        out
    }

    fn handle(&mut self, update: DepthUpdate, out: &mut Outcome) {
        if !self.synced {
            if self.buffer.len() >= MAX_BUFFER {
                self.buffer.pop_front();
                self.stats.buffer_dropped += 1;
            }
            self.buffer.push_back(update);
            self.request(out);
            return;
        }
        let in_sequence = match self.last_final {
            Some(last) if update.final_update_id <= last => {
                self.stats.stale_dropped += 1;
                return;
            }
            Some(last) => update.prev_final_update_id == last,
            None if update.final_update_id < self.snapshot_id => {
                self.stats.stale_dropped += 1;
                return;
            }
            None => update.first_update_id <= self.snapshot_id,
        };
        if !in_sequence {
            self.stats.gaps += 1;
            out.events.push(SyncEvent::Gap {
                expected_prev: self.last_final.unwrap_or(self.snapshot_id),
                got_prev: update.prev_final_update_id,
                got_first: update.first_update_id,
                got_final: update.final_update_id,
            });
            self.invalidate("sequence gap", out);
            self.handle(update, out); // Buffered for the next snapshot.
            return;
        }
        self.book.apply(&update.bids, &update.asks);
        self.last_final = Some(update.final_update_id);
        self.stats.applied += 1;
        if self.book.crossed() {
            self.stats.crossed += 1;
            self.invalidate("crossed book", out);
        }
    }

    pub fn on_snapshot(&mut self, snapshot: &Snapshot) -> Outcome {
        let mut out = Outcome::default();
        // Only a snapshot requested since the last reset may build a book: one fetched
        // before a disconnect would otherwise be served as synced while offline.
        if self.synced || !self.snapshot_requested {
            out.events.push(SyncEvent::SnapshotIgnored {
                last_update_id: snapshot.last_update_id,
            });
            return out;
        }
        self.snapshot_requested = false;
        self.book = OrderBook::from_snapshot(snapshot);
        self.synced = true;
        self.last_final = None;
        self.snapshot_id = snapshot.last_update_id;
        self.stats.snapshots_applied += 1;
        out.events.push(SyncEvent::Synced {
            last_update_id: snapshot.last_update_id,
        });
        if self.book.crossed() {
            self.stats.crossed += 1;
            self.invalidate("crossed snapshot", &mut out);
        }
        for update in std::mem::take(&mut self.buffer) {
            self.handle(update, &mut out);
        }
        out
    }

    /// Connection loss: pre-disconnect diffs cannot be bridged, so drop them too.
    pub fn reset(&mut self, reason: &str) -> Outcome {
        let mut out = Outcome::default();
        self.buffer.clear();
        self.snapshot_requested = false;
        if self.synced {
            self.invalidate(reason, &mut out);
        }
        // No stream is attached right now; the first diff after reconnecting requests a snapshot.
        out.request_snapshot = false;
        self.snapshot_requested = false;
        out
    }

    /// A snapshot fetch failed; the next diff will ask again.
    pub fn snapshot_failed(&mut self) {
        self.snapshot_requested = false;
    }

    fn invalidate(&mut self, reason: &str, out: &mut Outcome) {
        self.synced = false;
        self.book = OrderBook::default();
        self.last_final = None;
        self.stats.invalidations += 1;
        out.events.push(SyncEvent::Invalidated {
            reason: reason.to_string(),
        });
        self.request(out);
    }

    fn request(&mut self, out: &mut Outcome) {
        if !self.snapshot_requested {
            self.snapshot_requested = true;
            out.request_snapshot = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(v: &str) -> Decimal {
        v.parse().unwrap()
    }

    fn update(
        first: u64,
        last: u64,
        prev: u64,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
    ) -> DepthUpdate {
        DepthUpdate {
            first_update_id: first,
            final_update_id: last,
            prev_final_update_id: prev,
            bids: bids.iter().map(|(p, q)| (d(p), d(q))).collect(),
            asks: asks.iter().map(|(p, q)| (d(p), d(q))).collect(),
        }
    }

    fn snapshot(id: u64) -> Snapshot {
        Snapshot {
            last_update_id: id,
            event_ms: None,
            bids: vec![(d("100"), d("1")), (d("99"), d("2"))],
            asks: vec![(d("101"), d("1")), (d("102"), d("3"))],
        }
    }

    #[test]
    fn first_diff_requests_snapshot_once_and_is_buffered() {
        let mut sync = DepthSync::default();
        assert!(sync.on_update(update(1, 5, 0, &[], &[])).request_snapshot);
        assert!(!sync.on_update(update(6, 8, 5, &[], &[])).request_snapshot);
        assert!(sync.book().is_none());
    }

    #[test]
    fn snapshot_bridges_buffer_and_drops_stale_diffs() {
        let mut sync = DepthSync::default();
        sync.on_update(update(90, 95, 89, &[("100", "9")], &[])); // u < lastUpdateId: dropped
        sync.on_update(update(96, 103, 95, &[("100", "5")], &[])); // brackets 100
        sync.on_update(update(104, 110, 103, &[("99", "0")], &[("101", "0")]));
        let out = sync.on_snapshot(&snapshot(100));
        assert_eq!(
            out.events,
            vec![SyncEvent::Synced {
                last_update_id: 100
            }]
        );
        assert!(!out.request_snapshot);
        let book = sync.book().unwrap();
        assert_eq!(book.best_bid(), Some((d("100"), d("5"))));
        assert_eq!(book.best_ask(), Some((d("102"), d("3"))));
        assert_eq!(book.depth(), (1, 1));
        assert_eq!(sync.last_update_id(), Some(110));
        assert_eq!(sync.stats.stale_dropped, 1);
        assert_eq!(sync.stats.applied, 2);
    }

    #[test]
    fn snapshot_older_than_buffer_is_rejected_and_rerequested() {
        let mut sync = DepthSync::default();
        sync.on_update(update(200, 205, 199, &[], &[]));
        let out = sync.on_snapshot(&snapshot(100));
        assert!(out.request_snapshot);
        assert!(matches!(
            out.events[1],
            SyncEvent::Gap { got_first: 200, .. }
        ));
        assert!(!sync.is_synced());
        let out = sync.on_snapshot(&snapshot(202)); // The buffered diff survived the failed attempt.
        assert!(sync.is_synced() && !out.request_snapshot);
        assert_eq!(sync.last_update_id(), Some(205));
    }

    #[test]
    fn pu_mismatch_invalidates_and_resyncs() {
        let mut sync = DepthSync::default();
        sync.on_update(update(99, 101, 98, &[], &[]));
        sync.on_snapshot(&snapshot(100));
        let out = sync.on_update(update(105, 107, 104, &[("100", "7")], &[])); // expected pu=101
        assert!(out.request_snapshot);
        assert_eq!(
            out.events[0],
            SyncEvent::Gap {
                expected_prev: 101,
                got_prev: 104,
                got_first: 105,
                got_final: 107
            }
        );
        assert!(sync.book().is_none() && sync.last_update_id().is_none());
        let out = sync.on_snapshot(&snapshot(106));
        assert!(sync.is_synced() && !out.request_snapshot);
        assert_eq!(sync.book().unwrap().best_bid(), Some((d("100"), d("7"))));
        assert_eq!(
            (
                sync.stats.gaps,
                sync.stats.invalidations,
                sync.stats.snapshots_applied
            ),
            (1, 1, 2)
        );
    }

    #[test]
    fn first_diff_after_snapshot_must_bracket_last_update_id() {
        let mut sync = DepthSync::default();
        sync.on_update(update(1, 1, 0, &[], &[]));
        sync.on_snapshot(&snapshot(100)); // Buffer only had stale data; now awaiting a bracketing diff.
        assert!(sync.is_synced());
        let out = sync.on_update(update(102, 104, 101, &[], &[])); // Missing 101.
        assert!(out.request_snapshot && !sync.is_synced());
    }

    #[test]
    fn duplicate_diff_is_ignored_and_zero_qty_removes_level() {
        let mut sync = DepthSync::default();
        sync.on_update(update(100, 101, 99, &[("99", "0")], &[]));
        sync.on_snapshot(&snapshot(100));
        let before = sync.clone();
        sync.on_update(update(100, 101, 99, &[("100", "50")], &[]));
        assert_eq!(sync.book(), before.book());
        assert_eq!(sync.book().unwrap().depth(), (1, 2));
    }

    #[test]
    fn crossed_book_is_never_served() {
        let mut sync = DepthSync::default();
        sync.on_update(update(100, 101, 99, &[], &[]));
        sync.on_snapshot(&snapshot(100));
        let out = sync.on_update(update(102, 102, 101, &[("101.5", "1")], &[]));
        assert!(out.request_snapshot && sync.book().is_none());
        assert_eq!(sync.stats.crossed, 1);
    }

    #[test]
    fn buffer_is_bounded_and_reset_clears_state() {
        let mut sync = DepthSync::default();
        for i in 0..(MAX_BUFFER as u64 + 10) {
            sync.on_update(update(i * 2 + 1, i * 2 + 2, i * 2, &[], &[]));
        }
        assert_eq!(sync.stats.buffer_dropped, 10);
        sync.on_snapshot(&snapshot(5)); // Diffs bridging id 5 were dropped from the buffer: gap, rerequest.
        assert!(!sync.is_synced());
        sync.reset("disconnected");
        assert!(
            sync.on_update(update(9_001, 9_002, 9_000, &[], &[]))
                .request_snapshot
        );
        let out = sync.on_snapshot(&snapshot(9_001));
        assert!(sync.is_synced() && out.events.len() == 1); // Pre-disconnect diffs were not replayed.
        assert_eq!(sync.last_update_id(), Some(9_002));
    }

    #[test]
    fn snapshot_in_flight_across_disconnect_is_ignored() {
        let mut sync = DepthSync::default();
        assert!(sync.on_update(update(10, 12, 9, &[], &[])).request_snapshot);
        sync.reset("disconnected");
        let out = sync.on_snapshot(&snapshot(11)); // Fetched before the drop, delivered after.
        assert_eq!(
            out.events,
            vec![SyncEvent::SnapshotIgnored { last_update_id: 11 }]
        );
        assert!(!sync.is_synced() && sync.book().is_none());
        let unrequested = DepthSync::default().on_snapshot(&snapshot(1));
        assert!(matches!(
            unrequested.events[0],
            SyncEvent::SnapshotIgnored { .. }
        ));
    }

    #[test]
    fn failed_snapshot_allows_new_request() {
        let mut sync = DepthSync::default();
        assert!(sync.on_update(update(1, 2, 0, &[], &[])).request_snapshot);
        sync.snapshot_failed();
        assert!(sync.on_update(update(3, 4, 2, &[], &[])).request_snapshot);
    }

    /// `top_bids`/`top_asks` used to be a full iteration cut by `take_while`; the range
    /// form must yield exactly the same levels for every book and bound, including bounds
    /// that equal a level, sit between levels, lie outside the book, or differ only in
    /// scale (`100` vs `100.0`, which `Decimal` orders as equal).
    #[test]
    fn range_iteration_matches_the_take_while_reference() {
        let reference_bids = |b: &OrderBook| -> Vec<Level> {
            let floor = b.bid_floor;
            b.bids
                .iter()
                .rev()
                .map(|(p, q)| (*p, *q))
                .take_while(|(p, _)| floor.is_none_or(|f| *p >= f))
                .collect()
        };
        let reference_asks = |b: &OrderBook| -> Vec<Level> {
            let ceiling = b.ask_ceiling;
            b.asks
                .iter()
                .map(|(p, q)| (*p, *q))
                .take_while(|(p, _)| ceiling.is_none_or(|c| *p <= c))
                .collect()
        };
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for case in 0..2_000 {
            let mut book = OrderBook::default();
            let levels = next(40);
            for _ in 0..levels {
                // Mixed scales on purpose: 1234, 123.4, 12.34 are distinct prices.
                let scale = next(3) as u32;
                let price = Decimal::new(1 + next(2_000) as i64, scale);
                let qty = Decimal::new(1 + next(500) as i64, 3);
                if next(2) == 0 {
                    book.bids.insert(price, qty);
                } else {
                    book.asks.insert(price, qty);
                }
            }
            let bound = |next: &mut dyn FnMut(u64) -> u64| match next(4) {
                0 => None,
                _ => Some(Decimal::new(next(2_200) as i64, next(3) as u32)),
            };
            book.bid_floor = bound(&mut next);
            book.ask_ceiling = bound(&mut next);
            // Bounds that are exactly an existing level, written at a different scale.
            if case % 7 == 0 {
                if let Some((p, _)) = book.bids.iter().next() {
                    let mut p = *p;
                    p.rescale(p.scale() + 1);
                    book.bid_floor = Some(p);
                }
            }
            assert_eq!(
                book.top_bids().collect::<Vec<_>>(),
                reference_bids(&book),
                "case {case}"
            );
            assert_eq!(
                book.top_asks().collect::<Vec<_>>(),
                reference_asks(&book),
                "case {case}"
            );
        }
    }

    /// A snapshot cut at its level limit says nothing about prices beyond its deepest
    /// level, so once the top levels are gone the book must not report OBI from there.
    #[test]
    fn features_stop_at_the_snapshot_range() {
        let limit = SNAPSHOT_LIMIT as i64;
        let side = |start: i64, step: i64| -> Vec<Level> {
            (0..limit)
                .map(|i| (Decimal::from(start + step * i), Decimal::ONE))
                .collect()
        };
        let mut book = OrderBook::from_snapshot(&Snapshot {
            last_update_id: 1,
            event_ms: None,
            bids: side(10_000, -1), // 10 000 down to 9 001
            asks: side(10_001, 1),  // 10 001 up to 11 000
        });
        let obi = |b: &OrderBook| crate::features::book_features(b, &[2]).map(|f| f.obi[0].value);
        assert_eq!(obi(&book), Some(Some(Decimal::ZERO)));
        // Price falls through the snapshot: all bids above 9 002 vanish, and a level appears
        // below the snapshot floor where unseen exchange levels may already rest.
        let gone: Vec<Level> = (9_002..=10_000)
            .map(|p| (Decimal::from(p), Decimal::ZERO))
            .collect();
        let mut bids = gone;
        bids.push((Decimal::from(8_000), Decimal::from(7)));
        book.apply(&bids, &[]);
        assert_eq!(book.top_bids().count(), 1); // Only 9 001 is inside the covered range.
        assert_eq!(obi(&book), Some(None));
        // A partial snapshot (fewer levels than the limit) is the whole book: no bound.
        let small = OrderBook::from_snapshot(&snapshot(1));
        assert_eq!(small.top_bids().count(), 2);
    }
}
