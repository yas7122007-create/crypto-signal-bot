//! Bounded, compressed storage for derived rows (feature snapshots, bars).
//!
//! Rows are gzip NDJSON in `<prefix>-<run ms>-<pid>-<n>-<index>.ndjson.gz`. Every file of a
//! run keeps a `.partial` suffix until the whole run finishes cleanly, so a crashed or failed
//! run never leaves a file that looks complete; sync flushes keep the open file readable up
//! to the last flush.
//! Files rotate at `rotate_bytes` of uncompressed rows, and after `max_files` files the
//! store stops writing and counts dropped rows instead of growing the disk or deleting
//! older data. Nothing is ever overwritten.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreConfig {
    pub rotate_bytes: u64,
    pub max_files: u32,
}

impl Default for StoreConfig {
    /// 256 MB of rows per file (roughly 15-25 MB gzip) and at most 64 files per run.
    fn default() -> Self {
        Self {
            rotate_bytes: 256 * 1024 * 1024,
            max_files: 64,
        }
    }
}

/// Distinguishes stores opened by one process in the same millisecond.
static RUN_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

pub struct RowStore {
    dir: PathBuf,
    prefix: &'static str,
    run: String,
    config: StoreConfig,
    open: Option<GzEncoder<BufWriter<File>>>,
    /// Every file of this run, renamed only by `finish`.
    partials: Vec<PathBuf>,
    files: u32,
    written: u64,
    pub rows: u64,
    pub dropped_rows: u64,
}

impl RowStore {
    pub fn create(dir: &Path, prefix: &'static str, config: StoreConfig) -> io::Result<Self> {
        if config.rotate_bytes == 0 || config.max_files == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rotate_bytes and max_files must be positive",
            ));
        }
        fs::create_dir_all(dir)?;
        let run_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let n = RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Self {
            dir: dir.to_path_buf(),
            prefix,
            run: format!("{run_ms:013}-{}-{n}", std::process::id()),
            config,
            open: None,
            partials: Vec::new(),
            files: 0,
            written: 0,
            rows: 0,
            dropped_rows: 0,
        })
    }

    pub fn write(&mut self, row: &impl Serialize) -> io::Result<()> {
        let mut line = serde_json::to_vec(row).map_err(io::Error::other)?;
        line.push(b'\n');
        if self.open.is_some() && self.written >= self.config.rotate_bytes {
            self.close_open()?;
        }
        if self.open.is_none() {
            if self.files >= self.config.max_files {
                self.dropped_rows += 1; // Bounded: stop, never delete older data.
                return Ok(());
            }
            self.open_next()?;
        }
        if let Some(file) = self.open.as_mut() {
            file.write_all(&line)?;
        }
        self.written += line.len() as u64;
        self.rows += 1;
        Ok(())
    }

    /// Gzip sync flush: the `.partial` file is readable up to here after a crash.
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(file) = self.open.as_mut() {
            file.flush()?;
            file.get_mut().flush()?;
        }
        Ok(())
    }

    /// Clean end of run: every file of the run gets its final name. Returns
    /// `(rows written, rows dropped by the file cap)`.
    pub fn finish(mut self) -> io::Result<(u64, u64)> {
        self.close_open()?;
        for partial in std::mem::take(&mut self.partials) {
            let done = partial.with_extension(""); // Strips ".partial".
                                                   // hard_link fails if `done` exists: no check-then-rename race, no overwrite.
            fs::hard_link(&partial, &done)?;
            fs::remove_file(&partial)?;
        }
        Ok((self.rows, self.dropped_rows))
    }

    fn open_next(&mut self) -> io::Result<()> {
        let name = format!("{}-{}-{:06}.ndjson.gz", self.prefix, self.run, self.files);
        let partial = self.dir.join(format!("{name}.partial"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)?;
        self.open = Some(GzEncoder::new(BufWriter::new(file), Compression::fast()));
        self.partials.push(partial);
        self.files += 1;
        self.written = 0;
        Ok(())
    }

    fn close_open(&mut self) -> io::Result<()> {
        let Some(file) = self.open.take() else {
            return Ok(());
        };
        let mut inner = file.finish()?;
        inner.flush()?;
        inner.get_ref().sync_all()
    }
}

/// Latest bars per symbol as one small JSON file each (`<dir>/<SYMBOL>.json`), replaced
/// atomically (write to a temporary file, then rename), so a reader never sees a torn
/// file. This is the live contract the Python bridge reads.
pub struct BarState {
    dir: PathBuf,
    keep: usize,
    bars: std::collections::BTreeMap<String, std::collections::VecDeque<crate::bars::Bar>>,
}

pub const STATE_SCHEMA_VERSION: u16 = 1;

impl BarState {
    pub fn create(dir: &Path, keep: usize) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            keep: keep.max(1),
            bars: Default::default(),
        })
    }

    pub fn update(&mut self, bar: &crate::bars::Bar) -> io::Result<()> {
        // Symbols come from exchange data: only plain names may become file names.
        let valid = (1..=30).contains(&bar.symbol.len())
            && bar
                .symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit());
        if !valid {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad symbol"));
        }
        let window = self.bars.entry(bar.symbol.clone()).or_default();
        if window
            .back()
            .is_some_and(|last| last.session_id != bar.session_id)
        {
            window.clear(); // Never mix sessions in one window.
        }
        if window.len() >= self.keep {
            window.pop_front();
        }
        window.push_back(bar.clone());
        let body = serde_json::json!({
            "v": STATE_SCHEMA_VERSION,
            "kind": "bar_window",
            "symbol": bar.symbol,
            "bars": window,
        });
        let path = self.dir.join(format!("{}.json", bar.symbol));
        let tmp = self.dir.join(format!(".{}.json.tmp", bar.symbol));
        let mut file = File::create(&tmp)?;
        serde_json::to_writer(&mut file, &body).map_err(io::Error::other)?;
        file.sync_all()?;
        fs::rename(&tmp, &path)
    }
}

/// Completed files of one prefix in write order (`.partial` files are excluded).
pub fn row_files(dir: &Path, prefix: &str) -> io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{prefix}-")) && n.ends_with(".ndjson.gz"))
        })
        .collect();
    files.sort();
    Ok(files)
}

/// All rows of one prefix as lines, in write order.
pub fn read_rows(dir: &Path, prefix: &str) -> io::Result<Vec<String>> {
    let mut lines = Vec::new();
    for path in row_files(dir, prefix)? {
        let reader = BufReader::new(MultiGzDecoder::new(File::open(path)?));
        for line in reader.lines() {
            lines.push(line?);
        }
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotates_caps_and_never_shows_partial_as_complete() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = RowStore::create(
            dir.path(),
            "rows",
            StoreConfig {
                rotate_bytes: 20,
                max_files: 3,
            },
        )
        .unwrap();
        for i in 0..10 {
            store
                .write(&serde_json::json!({ "i": i, "pad": "xxxxxxxxxx" }))
                .unwrap();
        }
        store.flush().unwrap();
        // One row per file (each row exceeds 20 bytes): 3 files, then rows are dropped.
        assert_eq!((store.rows, store.dropped_rows), (3, 7));
        // Until the run finishes, nothing looks complete, even rotated files.
        assert!(row_files(dir.path(), "rows").unwrap().is_empty());
        assert_eq!(store.finish().unwrap(), (3, 7));
        assert_eq!(row_files(dir.path(), "rows").unwrap().len(), 3);
        let mut open = RowStore::create(dir.path(), "open", StoreConfig::default()).unwrap();
        open.write(&1).unwrap();
        open.flush().unwrap();
        assert!(row_files(dir.path(), "open").unwrap().is_empty()); // Still `.partial`.
                                                                    // A crash now: the flushed `.partial` decodes up to the flush.
        let partial = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.to_string_lossy().contains("open-"))
            .unwrap();
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(
            &mut MultiGzDecoder::new(File::open(&partial).unwrap()),
            &mut text,
        );
        assert_eq!(text, "1\n");
        open.finish().unwrap();
        assert_eq!(read_rows(dir.path(), "open").unwrap(), ["1"]);
        let rows = read_rows(dir.path(), "rows").unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows[2].contains("\"i\":2"));
        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(names.iter().all(|n| !n.ends_with(".partial")));
    }

    #[test]
    fn concurrent_stores_never_collide_or_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = RowStore::create(dir.path(), "rows", StoreConfig::default()).unwrap();
        let mut b = RowStore::create(dir.path(), "rows", StoreConfig::default()).unwrap();
        a.write(&"a").unwrap();
        b.write(&"b").unwrap();
        a.finish().unwrap();
        b.finish().unwrap();
        assert_eq!(read_rows(dir.path(), "rows").unwrap().len(), 2);
    }

    fn bar(start_ms: i64, session_id: i64) -> crate::bars::Bar {
        use rust_decimal::Decimal;
        crate::bars::Bar {
            v: crate::bars::BAR_SCHEMA_VERSION,
            feature_v: crate::features::FEATURE_SCHEMA_VERSION,
            symbol: "BTCUSDT".into(),
            session_id: Some(session_id),
            start_ms,
            end_ms: start_ms + crate::bars::BAR_MS,
            rows: 600,
            complete: start_ms % 120_000 == 0,
            incomplete_reason: (start_ms % 120_000 != 0).then_some("row_gap"),
            synced_since_seq: 3,
            close_seq: 99,
            open_mid: Decimal::new(500_001, 1),
            high_mid: Decimal::new(500_005, 1),
            low_mid: Decimal::new(499_995, 1),
            close_mid: Decimal::new(500_002, 1),
            close_microprice: Decimal::new(5_000_023_456, 5),
            mean_spread_bps: Decimal::new(2, 2),
            close_spread_bps: Decimal::new(2, 2),
            close_obi: vec![crate::features::Obi {
                levels: 10,
                value: Some(Decimal::new(-125, 3)),
            }],
            close_trade_state: crate::features::TradeState::Active,
            buy_qty: Some(Decimal::new(12_345, 3)),
            sell_qty: None,
        }
    }

    /// The state file is the live contract the Python bridge reads: its exact bytes are
    /// pinned to the `json!` rendering (sorted keys, Decimals as strings), so a change to
    /// how it is written cannot silently change what the bridge parses.
    #[test]
    fn state_file_bytes_are_pinned_and_windows_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = BarState::create(dir.path(), 2).unwrap();
        let bars = [
            bar(1_700_000_040_000, 1),
            bar(1_700_000_100_000, 1),
            bar(1_700_000_160_000, 1),
        ];
        for b in &bars {
            state.update(b).unwrap();
        }
        let written = fs::read(dir.path().join("BTCUSDT.json")).unwrap();
        let expected = serde_json::to_vec(&serde_json::json!({
            "v": STATE_SCHEMA_VERSION,
            "kind": "bar_window",
            "symbol": "BTCUSDT",
            "bars": [&bars[1], &bars[2]],
        }))
        .unwrap();
        assert_eq!(
            String::from_utf8(written).unwrap(),
            String::from_utf8(expected).unwrap()
        );
        // A new session never shares a window with the old one.
        state.update(&bar(1_700_000_220_000, 2)).unwrap();
        let v: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("BTCUSDT.json")).unwrap()).unwrap();
        assert_eq!(v["bars"].as_array().unwrap().len(), 1);
        assert_eq!(v["bars"][0]["session_id"], 2);
        // No temporary file is left behind after an atomic replace.
        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, ["BTCUSDT.json"]);
    }

    #[test]
    fn rejects_zero_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let zero = StoreConfig {
            rotate_bytes: 0,
            max_files: 1,
        };
        assert!(RowStore::create(dir.path(), "rows", zero).is_err());
    }
}
