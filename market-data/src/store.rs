//! Bounded, compressed storage for derived rows (feature snapshots, bars).
//!
//! Rows are gzip NDJSON in `<prefix>-<run ms>-<index>.ndjson.gz`. The open file carries a
//! `.partial` suffix until it is rotated or the run finishes cleanly, so a crashed run never
//! leaves a file that looks complete; sync flushes keep it readable up to the last flush.
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

pub struct RowStore {
    dir: PathBuf,
    prefix: &'static str,
    run_ms: u128,
    config: StoreConfig,
    open: Option<(GzEncoder<BufWriter<File>>, PathBuf)>,
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
        Ok(Self {
            dir: dir.to_path_buf(),
            prefix,
            run_ms,
            config,
            open: None,
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
            self.close()?;
        }
        if self.open.is_none() {
            if self.files >= self.config.max_files {
                self.dropped_rows += 1; // Bounded: stop, never delete older data.
                return Ok(());
            }
            self.open_next()?;
        }
        if let Some((file, _)) = self.open.as_mut() {
            file.write_all(&line)?;
        }
        self.written += line.len() as u64;
        self.rows += 1;
        Ok(())
    }

    /// Gzip sync flush: the `.partial` file is readable up to here after a crash.
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some((file, _)) = self.open.as_mut() {
            file.flush()?;
            file.get_mut().flush()?;
        }
        Ok(())
    }

    /// Clean end of run: the open file gets its final name.
    pub fn finish(mut self) -> io::Result<()> {
        self.close()
    }

    fn open_next(&mut self) -> io::Result<()> {
        let name = format!(
            "{}-{:013}-{:06}.ndjson.gz",
            self.prefix, self.run_ms, self.files
        );
        let partial = self.dir.join(format!("{name}.partial"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)?;
        self.open = Some((
            GzEncoder::new(BufWriter::new(file), Compression::fast()),
            partial,
        ));
        self.files += 1;
        self.written = 0;
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        let Some((file, partial)) = self.open.take() else {
            return Ok(());
        };
        let mut inner = file.finish()?;
        inner.flush()?;
        inner.get_ref().sync_all()?;
        let done = partial.with_extension(""); // Strips ".partial".
        if done.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{} exists", done.display()),
            ));
        }
        fs::rename(&partial, &done)
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
        // The cap closed the third file when the fourth row arrived.
        assert_eq!(row_files(dir.path(), "rows").unwrap().len(), 3);
        store.finish().unwrap();
        let mut open = RowStore::create(dir.path(), "open", StoreConfig::default()).unwrap();
        open.write(&1).unwrap();
        open.flush().unwrap();
        assert!(row_files(dir.path(), "open").unwrap().is_empty()); // Still `.partial`.
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
    fn rejects_zero_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let zero = StoreConfig {
            rotate_bytes: 0,
            max_files: 1,
        };
        assert!(RowStore::create(dir.path(), "rows", zero).is_err());
    }
}
