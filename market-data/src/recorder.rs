//! Append-only recorder: gzip-compressed newline-delimited JSON envelopes, rotated by time
//! and size. Files are named `events-<open unix ms>-<first seq>.ndjson.gz`, so sorting names
//! gives replay order. Periodic gzip sync-flushes keep a crashed file readable up to the
//! last flush; the reader treats a torn tail as truncation, not as corruption.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::event::Envelope;

const FLUSH_EVERY_EVENTS: u32 = 1_000;
const FLUSH_EVERY: Duration = Duration::from_secs(1);

pub struct Recorder {
    dir: PathBuf,
    file: Option<GzEncoder<BufWriter<File>>>,
    opened: Instant,
    written: u64,
    next_seq: u64,
    unflushed: u32,
    last_flush: Instant,
    rotate_after: Duration,
    rotate_bytes: u64,
}

impl Recorder {
    pub fn create(dir: &Path, rotate_after: Duration, rotate_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            file: None,
            opened: Instant::now(),
            written: 0,
            next_seq: 0,
            unflushed: 0,
            last_flush: Instant::now(),
            rotate_after,
            rotate_bytes,
        })
    }

    /// Assigns the next sequence number, appends the envelope and returns it.
    pub fn record(&mut self, mut env: Envelope) -> io::Result<Envelope> {
        env.seq = self.next_seq;
        let mut line = serde_json::to_vec(&env).map_err(io::Error::other)?;
        line.push(b'\n');
        if self.file.is_none()
            || self.opened.elapsed() >= self.rotate_after
            || self.written >= self.rotate_bytes
        {
            self.rotate()?;
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("recorder file not open"))?;
        file.write_all(&line)?;
        self.written += line.len() as u64;
        self.next_seq += 1;
        self.unflushed += 1;
        if self.unflushed >= FLUSH_EVERY_EVENTS || self.last_flush.elapsed() >= FLUSH_EVERY {
            self.flush()?;
        }
        Ok(env)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.flush()?; // gzip sync flush, then BufWriter to the OS.
            file.get_mut().flush()?;
        }
        self.unflushed = 0;
        self.last_flush = Instant::now();
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        self.close()
    }

    fn close(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            let mut inner = file.finish()?;
            inner.flush()?;
            inner.get_ref().sync_all()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.close()?;
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let path = self
            .dir
            .join(format!("events-{ms:013}-{:012}.ndjson.gz", self.next_seq));
        let file = OpenOptions::new().write(true).create_new(true).open(path)?; // Never overwrite.
        self.file = Some(GzEncoder::new(BufWriter::new(file), Compression::fast()));
        self.opened = Instant::now();
        self.written = 0;
        Ok(())
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ReplayStats {
    pub files: usize,
    pub events: u64,
    pub truncated_files: usize,
}

/// Recording files in replay order.
pub fn recording_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("events-") && n.ends_with(".ndjson.gz"))
        })
        .collect();
    files.sort();
    Ok(files)
}

/// Streams every recorded envelope in order into `sink`. A complete line that fails to
/// parse in the middle of a file is corruption and stops the replay with an error.
pub fn replay(dir: &Path, mut sink: impl FnMut(Envelope)) -> io::Result<ReplayStats> {
    let mut stats = ReplayStats::default();
    for path in recording_files(dir)? {
        stats.files += 1;
        let mut reader = BufReader::new(MultiGzDecoder::new(File::open(&path)?));
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) if !line.ends_with('\n') => {
                    stats.truncated_files += 1; // Torn final line from a crash.
                    break;
                }
                Ok(_) => match serde_json::from_str::<Envelope>(&line) {
                    Ok(env) => {
                        stats.events += 1;
                        sink(env);
                    }
                    Err(e) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "{}: corrupt event after {} events: {e}",
                                path.display(),
                                stats.events
                            ),
                        ))
                    }
                },
                // Gzip stream cut short: everything before the last sync flush was delivered.
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidInput
                    ) =>
                {
                    stats.truncated_files += 1;
                    break;
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(stats)
}
