//! Hot-path profile of the market-data engine on a realistic, deterministic synthetic
//! stream. Developer tool only (`cargo bench --bench hotpath`); not part of the library.
//!
//! Each stage is measured two ways: a batch run without per-call timers (throughput,
//! ns/op) and a per-call run (p50/p95/p99, which include about one timer read of overhead,
//! reported as `timer`). Allocations are counted by a wrapper around the system allocator
//! that lives only in this bench binary.
//!
//! The stream: one symbol, a 1000-level snapshot per side at a 0.10 tick, then depth diffs
//! of `DIFF_LEVELS` levels each (Binance `depth@100ms`), `TRADES_PER_DIFF` aggregate trades
//! and `TICKERS_PER_DIFF` book tickers between diffs.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use market_data::bars::BarBuilder;
use market_data::binance::{envelope_from_snapshot, envelope_from_stream, parse_depth};
use market_data::book::DepthSync;
use market_data::event::{session_start, Envelope, Kind};
use market_data::features::{book_features, FeatureConfig, TradeFlow};
use market_data::pipeline::Pipeline;
use market_data::recorder::{replay, Recorder};
use market_data::store::{BarState, RowStore, StoreConfig};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: every method forwards the caller's arguments unchanged to `System`, which upholds
// the `GlobalAlloc` contract; the counters are plain atomics and never touch the memory.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Relaxed);
        // SAFETY: same layout the caller gave us; `System` has the same requirements.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System.alloc`/`realloc` with this layout (see `alloc`).
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Relaxed);
        // SAFETY: forwarded unchanged; `ptr`/`layout` satisfy `realloc`'s contract by the
        // caller's obligation.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const SYMBOL: &str = "BTCUSDT";
const DIFF_LEVELS: usize = 20;
const TRADES_PER_DIFF: u64 = 3;
const TICKERS_PER_DIFF: u64 = 5;
const SNAPSHOT_LEVELS: usize = 1000;
const MID_TICKS: i64 = 500_000; // 50 000.0 at a 0.1 tick

/// Number of depth diffs (`HOTPATH_DIFFS`, default 100 000); smaller for profilers.
fn diffs() -> u64 {
    std::env::var("HOTPATH_DIFFS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000)
}

/// xorshift64*: deterministic, dependency-free.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn price(ticks: i64) -> String {
    format!("{}.{}", ticks / 10, ticks % 10)
}

fn qty(rng: &mut Rng) -> String {
    format!("{}.{:03}", rng.below(5), 1 + rng.below(999))
}

fn side(levels: &[(String, String)]) -> String {
    let items: Vec<String> = levels
        .iter()
        .map(|(p, q)| format!(r#"["{p}","{q}"]"#))
        .collect();
    format!("[{}]", items.join(","))
}

struct Stream {
    snapshot: Envelope,
    /// Raw combined-stream frames in arrival order, with their kind.
    frames: Vec<(Kind, String, i64)>,
}

fn build_stream() -> Stream {
    let diffs = diffs();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let bids: Vec<_> = (0..SNAPSHOT_LEVELS as i64)
        .map(|i| (price(MID_TICKS - i), qty(&mut rng)))
        .collect();
    let asks: Vec<_> = (0..SNAPSHOT_LEVELS as i64)
        .map(|i| (price(MID_TICKS + 1 + i), qty(&mut rng)))
        .collect();
    let t0 = 1_700_000_000_000i64;
    let body = format!(
        r#"{{"lastUpdateId":1001,"E":{t0},"T":{t0},"bids":{},"asks":{}}}"#,
        side(&bids),
        side(&asks)
    );
    let snapshot = envelope_from_snapshot(SYMBOL, &body, t0 * 1_000_000).unwrap();
    let mut frames = Vec::new();
    let (mut update_id, mut trade_id) = (1000u64, 1u64);
    for i in 0..diffs {
        let t = t0 + 1 + i as i64 * 100;
        // Levels near the top, never crossing: bids at or below mid, asks above it.
        let level = |rng: &mut Rng, ask: bool| {
            let offset = rng.below(60) as i64;
            let ticks = if ask {
                MID_TICKS + 1 + offset
            } else {
                MID_TICKS - offset
            };
            // Keep the best levels alive so the book stays two-sided and uncrossed.
            let q = if offset == 0 || rng.below(8) != 0 {
                qty(rng)
            } else {
                "0".to_string()
            };
            (price(ticks), q)
        };
        let b: Vec<_> = (0..DIFF_LEVELS / 2)
            .map(|_| level(&mut rng, false))
            .collect();
        let a: Vec<_> = (0..DIFF_LEVELS / 2)
            .map(|_| level(&mut rng, true))
            .collect();
        let prev = update_id;
        let first = update_id + 1;
        update_id += 1 + rng.below(4);
        frames.push((
            Kind::Depth,
            format!(
                r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{t},"T":{t},"s":"{SYMBOL}","U":{first},"u":{update_id},"pu":{prev},"b":{},"a":{}}}}}"#,
                side(&b),
                side(&a)
            ),
            t,
        ));
        for k in 0..TRADES_PER_DIFF {
            let tt = t + 10 + k as i64 * 20;
            let m = rng.below(2) == 0;
            frames.push((
                Kind::AggTrade,
                format!(
                    r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":{tt},"s":"{SYMBOL}","a":{trade_id},"p":"{}","q":"{}","f":{trade_id},"l":{trade_id},"T":{tt},"m":{m}}}}}"#,
                    price(MID_TICKS),
                    qty(&mut rng)
                ),
                tt,
            ));
            trade_id += 1;
        }
        for k in 0..TICKERS_PER_DIFF {
            let tt = t + 5 + k as i64 * 15;
            frames.push((
                Kind::BookTicker,
                format!(
                    r#"{{"stream":"btcusdt@bookTicker","data":{{"e":"bookTicker","u":{update_id},"s":"{SYMBOL}","b":"{}","B":"{}","a":"{}","A":"{}","T":{tt},"E":{tt}}}}}"#,
                    price(MID_TICKS),
                    qty(&mut rng),
                    price(MID_TICKS + 1),
                    qty(&mut rng)
                ),
                tt,
            ));
        }
    }
    Stream { snapshot, frames }
}

fn envelopes(stream: &Stream) -> Vec<Envelope> {
    let mut out = vec![session_start("bench", &FeatureConfig::default(), 0)];
    // The first diff is buffered, then the snapshot syncs the book, as live.
    let mut snapshot_done = false;
    for (kind, frame, t) in &stream.frames {
        let env = envelope_from_stream(frame, t * 1_000_000).unwrap();
        out.push(env);
        if *kind == Kind::Depth && !snapshot_done {
            out.push(stream.snapshot.clone());
            snapshot_done = true;
        }
    }
    for (seq, env) in out.iter_mut().enumerate() {
        env.seq = seq as u64;
    }
    out
}

struct Stat {
    name: &'static str,
    n: u64,
    batch: Duration,
    p50: u64,
    p95: u64,
    p99: u64,
    allocs: f64,
    bytes: Option<f64>,
}

fn percentiles(mut samples: Vec<u64>) -> (u64, u64, u64) {
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    (at(0.50), at(0.95), at(0.99))
}

/// Times `op` over every input twice: once in a batch, once per call.
fn measure<T>(
    name: &'static str,
    inputs: &[T],
    bytes: Option<f64>,
    mut op: impl FnMut(&T),
) -> Stat {
    let a0 = ALLOCS.load(Relaxed);
    let start = Instant::now();
    for x in inputs {
        op(x);
    }
    let batch = start.elapsed();
    let allocs = (ALLOCS.load(Relaxed) - a0) as f64 / inputs.len() as f64;
    let samples: Vec<u64> = inputs
        .iter()
        .map(|x| {
            let s = Instant::now();
            op(x);
            s.elapsed().as_nanos() as u64
        })
        .collect();
    let (p50, p95, p99) = percentiles(samples);
    Stat {
        name,
        n: inputs.len() as u64,
        batch,
        p50,
        p95,
        p99,
        allocs,
        bytes,
    }
}

fn print(stats: &[Stat]) {
    println!(
        "| stage | n | ns/op (batch) | ops/s | p50 ns | p95 ns | p99 ns | allocs/op | bytes/op |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for s in stats {
        let per = s.batch.as_nanos() as f64 / s.n as f64;
        println!(
            "| {} | {} | {:.0} | {:.0} | {} | {} | {} | {:.1} | {} |",
            s.name,
            s.n,
            per,
            1e9 / per,
            s.p50,
            s.p95,
            s.p99,
            s.allocs,
            s.bytes.map_or("-".into(), |b| format!("{b:.0}"))
        );
    }
}

fn proc_status(field: &str) -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(field))
                .map(|l| l[field.len()..].trim().to_string())
        })
        .unwrap_or_else(|| "n/a".into())
}

/// User + system CPU seconds of this process (Linux `/proc/self/stat`, 100 Hz ticks).
fn cpu_seconds() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) / 100.0)
}

fn main() {
    let wall = Instant::now();
    let stream = build_stream();
    let timer = {
        let samples: Vec<u64> = (0..100_000)
            .map(|_| {
                let s = Instant::now();
                s.elapsed().as_nanos() as u64
            })
            .collect();
        percentiles(samples).0
    };
    let by_kind = |k: Kind| -> Vec<&(Kind, String, i64)> {
        stream.frames.iter().filter(|f| f.0 == k).collect()
    };
    let (depth, trades, tickers) = (
        by_kind(Kind::Depth),
        by_kind(Kind::AggTrade),
        by_kind(Kind::BookTicker),
    );
    let mut stats = Vec::new();

    // 1. Wire JSON -> envelope (combined frame + header parse, payload copied out).
    for (name, frames) in [
        ("parse frame: depth", &depth),
        ("parse frame: aggTrade", &trades),
        ("parse frame: bookTicker", &tickers),
    ] {
        let avg = frames.iter().map(|f| f.1.len()).sum::<usize>() as f64 / frames.len() as f64;
        stats.push(measure(name, frames, Some(avg), |f| {
            black_box(envelope_from_stream(black_box(&f.1), 0).unwrap());
        }));
    }

    let envs = envelopes(&stream);
    let depth_envs: Vec<&Envelope> = envs.iter().filter(|e| e.kind == Kind::Depth).collect();

    // 2. Depth payload -> Decimal levels (the pipeline parses the payload a second time).
    stats.push(measure("parse depth payload", &depth_envs, None, |e| {
        black_box(parse_depth(black_box(e.payload.get())).unwrap());
    }));

    // 3. L2 book update alone, on pre-parsed diffs.
    let updates: Vec<_> = depth_envs
        .iter()
        .map(|e| parse_depth(e.payload.get()).unwrap())
        .collect();
    let snapshot = market_data::binance::parse_snapshot(stream.snapshot.payload.get()).unwrap();
    let fresh_book = || {
        let mut sync = DepthSync::default();
        sync.on_update(updates[0].clone());
        sync.on_snapshot(&snapshot);
        sync
    };
    {
        let mut sync = fresh_book();
        let rest = &updates[1..];
        let a0 = ALLOCS.load(Relaxed);
        let start = Instant::now();
        for u in rest {
            black_box(sync.on_update(u.clone()));
        }
        let batch_with_clone = start.elapsed();
        let allocs = (ALLOCS.load(Relaxed) - a0) as f64 / rest.len() as f64;
        assert!(
            sync.is_synced(),
            "synthetic stream must keep the book synced"
        );
        // Clone cost on its own, so it can be subtracted.
        let start = Instant::now();
        for u in rest {
            black_box(u.clone());
        }
        let clone = start.elapsed();
        let mut sync = fresh_book();
        let samples: Vec<u64> = rest
            .iter()
            .map(|u| {
                let u = u.clone();
                let s = Instant::now();
                black_box(sync.on_update(u));
                s.elapsed().as_nanos() as u64
            })
            .collect();
        let (p50, p95, p99) = percentiles(samples);
        stats.push(Stat {
            name: "L2 book update (20 levels)",
            n: rest.len() as u64,
            batch: batch_with_clone.saturating_sub(clone),
            p50,
            p95,
            p99,
            allocs,
            bytes: None,
        });
    }

    // 4. Book features (mid, spread, microprice, OBI top 10 and 50) on the live book.
    {
        let cfg = FeatureConfig::default();
        let sync = fresh_book();
        let book = sync.book().unwrap();
        let reps: Vec<u32> = (0..200_000).collect();
        stats.push(measure("book features + OBI", &reps, None, |_| {
            black_box(book_features(black_box(book), cfg.obi_levels()).unwrap());
        }));
    }

    // 5. Trade flow: CVD + 4 rolling windows.
    {
        let parsed: Vec<_> = envs
            .iter()
            .filter(|e| e.kind == Kind::AggTrade)
            .map(|e| market_data::binance::parse_agg_trade(e.payload.get()).unwrap())
            .collect();
        let cfg = FeatureConfig::default();
        let mut flow = TradeFlow::new(cfg.cvd_windows_ms());
        let mut flow2 = TradeFlow::new(cfg.cvd_windows_ms());
        let mut first = true;
        stats.push(measure(
            "trade flow update (CVD+windows)",
            &parsed,
            None,
            |t| {
                // The per-call pass replays the same trades into a second flow.
                let f = if first { &mut flow } else { &mut flow2 };
                black_box(f.on_trade(t));
                if std::ptr::eq(t, parsed.last().unwrap()) {
                    first = false;
                }
            },
        ));
    }

    // 6. End-to-end pipeline per envelope (parse + book + flow + features + bars).
    let (rows, bars) = {
        let mut p = Pipeline::new(FeatureConfig::default());
        let mut rows = Vec::new();
        let mut bars = Vec::new();
        let a0 = ALLOCS.load(Relaxed);
        let start = Instant::now();
        for e in &envs {
            let step = p.handle(e);
            if let Some(r) = step.feature {
                rows.push(r);
            }
            if let Some(b) = step.bar {
                bars.push(b);
            }
        }
        let batch = start.elapsed();
        let allocs = (ALLOCS.load(Relaxed) - a0) as f64 / envs.len() as f64;
        assert_eq!(
            rows.len() as u64,
            diffs(),
            "every diff must produce a feature row"
        );
        let mut by_kind: std::collections::BTreeMap<&'static str, Vec<u64>> = Default::default();
        let mut p = Pipeline::new(FeatureConfig::default());
        for e in &envs {
            let s = Instant::now();
            black_box(p.handle(e));
            let ns = s.elapsed().as_nanos() as u64;
            let k = match e.kind {
                Kind::Depth => "pipeline: depth event",
                Kind::AggTrade => "pipeline: aggTrade event",
                Kind::BookTicker => "pipeline: bookTicker event",
                _ => continue,
            };
            by_kind.entry(k).or_default().push(ns);
        }
        let all_n = envs.len() as u64;
        let (p50, p95, p99) = percentiles(by_kind.values().flatten().copied().collect());
        stats.push(Stat {
            name: "pipeline: all events",
            n: all_n,
            batch,
            p50,
            p95,
            p99,
            allocs,
            bytes: None,
        });
        for (k, v) in by_kind {
            let n = v.len() as u64;
            let mean = v.iter().sum::<u64>() / n;
            let (p50, p95, p99) = percentiles(v);
            stats.push(Stat {
                name: k,
                n,
                batch: Duration::from_nanos(mean * n),
                p50,
                p95,
                p99,
                allocs: f64::NAN,
                bytes: None,
            });
        }
        (rows, bars)
    };

    // 7. Serialization of what is written per event.
    let row_bytes = rows
        .iter()
        .map(|r| serde_json::to_vec(r).unwrap().len())
        .sum::<usize>() as f64
        / rows.len() as f64;
    stats.push(measure(
        "serialize feature row",
        &rows,
        Some(row_bytes),
        |r| {
            black_box(serde_json::to_vec(black_box(r)).unwrap());
        },
    ));
    let env_bytes = envs
        .iter()
        .map(|e| serde_json::to_vec(e).unwrap().len())
        .sum::<usize>() as f64
        / envs.len() as f64;
    stats.push(measure("serialize envelope", &envs, Some(env_bytes), |e| {
        black_box(serde_json::to_vec(black_box(e)).unwrap());
    }));

    // 8. Recorder (serialize + gzip fast + buffered write), feature store, replay.
    let dir = tempfile::tempdir().unwrap();
    // HOTPATH_KEEP=1 keeps the recording and outputs (for replay-equivalence checks).
    let dir_path = dir.path().to_path_buf();
    // `dir` must stay alive (or be kept) for the whole run, or it is deleted under us.
    let kept = std::env::var_os("HOTPATH_KEEP").is_some();
    let _dir = if kept {
        let _ = dir.keep();
        None
    } else {
        Some(dir)
    };
    let rec_dir = dir_path.as_path().join("rec");
    {
        // The per-call pass writes to a second recorder, so `rec_dir` holds one clean copy.
        let mut recs = [
            Recorder::create(&rec_dir, Duration::from_secs(3600), 1 << 40).unwrap(),
            Recorder::create(
                &dir_path.as_path().join("rec2"),
                Duration::from_secs(3600),
                1 << 40,
            )
            .unwrap(),
        ];
        let mut calls = 0usize;
        stats.push(measure("recorder write (gzip)", &envs, None, |e| {
            let which = usize::from(calls >= envs.len());
            calls += 1;
            black_box(recs[which].record(e.clone()).unwrap());
        }));
        for rec in recs {
            rec.finish().unwrap();
        }
    }
    let gz: u64 = std::fs::read_dir(&rec_dir)
        .unwrap()
        .map(|f| f.unwrap().metadata().unwrap().len())
        .sum();
    {
        let mut store = RowStore::create(
            &dir_path.as_path().join("features"),
            "features",
            StoreConfig::default(),
        )
        .unwrap();
        stats.push(measure("feature store write (gzip)", &rows, None, |r| {
            store.write(r).unwrap();
        }));
        store.finish().unwrap();
    }
    let replay_stat = {
        let a0 = ALLOCS.load(Relaxed);
        let start = Instant::now();
        let mut n = 0u64;
        replay(&rec_dir, |e| {
            n += 1;
            black_box(e);
        })
        .unwrap();
        let batch = start.elapsed();
        Stat {
            name: "replay read (gunzip + parse)",
            n,
            batch,
            p50: 0,
            p95: 0,
            p99: 0,
            allocs: (ALLOCS.load(Relaxed) - a0) as f64 / n as f64,
            bytes: Some(gz as f64 / n as f64),
        }
    };
    stats.push(replay_stat);
    let full_replay = {
        let start = Instant::now();
        let mut p = Pipeline::new(FeatureConfig::default());
        let mut n = 0u64;
        replay(&rec_dir, |e| {
            n += black_box(p.handle(&e)).feature.is_some() as u64;
        })
        .unwrap();
        assert_eq!(n, diffs());
        start.elapsed()
    };
    stats.push(Stat {
        name: "replay + pipeline (all events)",
        n: envs.len() as u64,
        batch: full_replay,
        p50: 0,
        p95: 0,
        p99: 0,
        allocs: f64::NAN,
        bytes: None,
    });

    // 9. Bars and the Python boundary file (atomic write + fsync, once a minute per symbol).
    {
        let mut builder = BarBuilder::default();
        stats.push(measure("bar builder on row", &rows, None, |r| {
            black_box(builder.on_row(r));
        }));
    }
    let state_dir = dir_path.as_path().join("state");
    let mut state = BarState::create(&state_dir, 256).unwrap();
    // Warm to a full 256-bar window by repeating the bars with shifted times.
    let mut window_bars = Vec::new();
    for i in 0..300i64 {
        let mut b = bars[(i as usize) % bars.len()].clone();
        b.start_ms = 1_700_000_040_000 + i * 60_000; // minute-aligned, as real bars are
        b.end_ms = b.start_ms + 60_000;
        window_bars.push(b);
    }
    for b in &window_bars[..256] {
        state.update(b).unwrap();
    }
    let state_bytes = std::fs::metadata(state_dir.join("BTCUSDT.json"))
        .unwrap()
        .len();
    stats.push(measure(
        "state file write (256 bars, fsync)",
        &window_bars[256..],
        Some(state_bytes as f64),
        |b| state.update(b).unwrap(),
    ));
    // Breakdown of the state write: building the JSON vs putting bytes on disk.
    {
        let window: std::collections::VecDeque<_> = window_bars[44..300].iter().cloned().collect();
        let reps: Vec<u32> = (0..200).collect();
        stats.push(measure("state: json! + to_vec (current)", &reps, None, |_| {
            let body = serde_json::json!({"v": 1, "kind": "bar_window", "symbol": SYMBOL, "bars": window});
            black_box(serde_json::to_vec(&body).unwrap());
        }));
        let bytes = serde_json::to_vec(
            &serde_json::json!({"v": 1, "kind": "bar_window", "symbol": SYMBOL, "bars": window}),
        )
        .unwrap();
        let reps: Vec<u32> = (0..40).collect();
        let path = state_dir.join("probe.json");
        let tmp = state_dir.join(".probe.json.tmp");
        stats.push(measure(
            "state: write + rename, no fsync",
            &reps,
            None,
            |_| {
                std::fs::write(&tmp, &bytes).unwrap();
                std::fs::rename(&tmp, &path).unwrap();
            },
        ));
        stats.push(measure(
            "state: write + fsync + rename",
            &reps,
            None,
            |_| {
                use std::io::Write;
                let mut f = std::fs::File::create(&tmp).unwrap();
                f.write_all(&bytes).unwrap();
                f.sync_all().unwrap();
                std::fs::rename(&tmp, &path).unwrap();
            },
        ));
    }
    // HOTPATH_STATE_OUT=PATH copies the 256-bar state file out for scripts/bench_boundary.py.
    if let Some(out) = std::env::var_os("HOTPATH_STATE_OUT") {
        std::fs::copy(state_dir.join("BTCUSDT.json"), out).unwrap();
    }

    println!(
        "\nmarket-data hot path ({} diffs, {} events)",
        diffs(),
        envs.len()
    );
    println!("timer overhead p50: {timer} ns (included in per-call percentiles)\n");
    print(&stats);
    println!(
        "\nrecording: {gz} bytes gzip for {} events ({:.1} B/event); state file {state_bytes} bytes",
        envs.len(),
        gz as f64 / envs.len() as f64
    );
    println!(
        "allocated in total: {} MB over {} allocations",
        ALLOC_BYTES.load(Relaxed) / 1_000_000,
        ALLOCS.load(Relaxed)
    );
    println!("outputs: {}", dir_path.display());
    println!(
        "peak RSS (VmHWM): {}; CPU {:.2} s over {:.2} s wall",
        proc_status("VmHWM:"),
        cpu_seconds().unwrap_or(f64::NAN),
        wall.elapsed().as_secs_f64()
    );
}
