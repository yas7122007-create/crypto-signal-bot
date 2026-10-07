//! The `market-data replay` binary: recorded feature config enforcement, failed replays
//! leaving nothing that looks complete, and per-session row identity.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use market_data::binance::{envelope_from_snapshot, envelope_from_stream};
use market_data::event::{connection, session_start, Envelope};
use market_data::features::FeatureConfig;
use market_data::recorder::Recorder;

const T0: i64 = 1_700_000_040_000;

fn replay(input: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_market-data"))
        .arg("replay")
        .arg("--input")
        .arg(input)
        .args(extra)
        .output()
        .unwrap()
}

fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn depth(id: u64, e: i64) -> Envelope {
    let frame = format!(
        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{e},"T":{e},"s":"BTCUSDT","U":{id},"u":{id},"pu":{},"b":[["100","{}"]],"a":[]}}}}"#,
        id - 1,
        1 + id % 3
    );
    envelope_from_stream(&frame, e * 1_000_000).unwrap()
}

fn snapshot(id: u64, e: i64) -> Envelope {
    let body = format!(
        r#"{{"lastUpdateId":{id},"E":{e},"T":{e},"bids":[["100","1"]],"asks":[["110","1"]]}}"#
    );
    envelope_from_snapshot("BTCUSDT", &body, e * 1_000_000).unwrap()
}

/// One session: its start at `recv_ns`, a synced book and `diffs` depth rows.
fn session(recorder: &mut Recorder, config: &FeatureConfig, recv_ns: i64, diffs: u64) {
    recorder
        .record(session_start("test", config, recv_ns))
        .unwrap();
    recorder
        .record(connection("connected", "", recv_ns + 1))
        .unwrap();
    let t = recv_ns / 1_000_000;
    recorder.record(depth(10, t)).unwrap();
    recorder.record(snapshot(10, t)).unwrap();
    for id in 11..11 + diffs {
        recorder.record(depth(id, t + (id as i64) * 100)).unwrap();
    }
}

fn recording(dir: &Path, sessions: &[(&FeatureConfig, i64, u64)]) {
    let mut recorder = Recorder::create(dir, Duration::from_secs(3600), u64::MAX).unwrap();
    for (config, recv_ns, diffs) in sessions {
        session(&mut recorder, config, *recv_ns, *diffs);
    }
    recorder.finish().unwrap();
}

fn files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|d| {
            d.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

#[test]
fn replay_enforces_the_recorded_feature_config() {
    let dir = tempfile::tempdir().unwrap();
    let rec = dir.path().join("rec");
    let recorded = FeatureConfig::new(vec![1_000], vec![5]).unwrap();
    recording(&rec, &[(&recorded, T0 * 1_000_000, 0)]);

    let out = dir.path().join("features");
    let default = replay(&rec, &["--features-dir", out.to_str().unwrap()]);
    assert!(!default.status.success());
    let message = stderr(&default);
    assert!(
        message.contains("recording was made with feature config"),
        "{message}"
    );
    assert!(message.contains("\"cvd_windows_ms\":[1000]"), "{message}");
    assert!(market_data::store::row_files(&out, "features")
        .unwrap()
        .is_empty());

    let same = replay(&rec, &["--cvd-windows-ms", "1000", "--obi-levels", "5"]);
    assert!(same.status.success(), "{}", stderr(&same));
    let forced = replay(&rec, &["--allow-config-mismatch"]);
    assert!(forced.status.success());
    let report: serde_json::Value = serde_json::from_slice(&forced.stdout).unwrap();
    assert_eq!(report["state"]["events"]["config_mismatch"], 1);
}

#[test]
fn silence_thresholds_alone_are_a_config_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let rec = dir.path().join("rec");
    let recorded = FeatureConfig::default()
        .with_trade_silence(5_000, 60_000)
        .unwrap();
    recording(&rec, &[(&recorded, T0 * 1_000_000, 0)]);
    assert!(!replay(&rec, &[]).status.success());
    let quiet_only = replay(&rec, &["--trade-quiet-ms", "5000"]);
    assert!(!quiet_only.status.success());
    let both = replay(
        &rec,
        &["--trade-quiet-ms", "5000", "--trade-stale-ms", "60000"],
    );
    assert!(both.status.success(), "{}", stderr(&both));
}

#[test]
fn a_later_mismatching_session_leaves_no_complete_output() {
    let dir = tempfile::tempdir().unwrap();
    let rec = dir.path().join("rec");
    let other = FeatureConfig::new(vec![1_000], vec![5]).unwrap();
    let t1 = (T0 + 3_600_000) * 1_000_000;
    recording(
        &rec,
        &[
            (&FeatureConfig::default(), T0 * 1_000_000, 20),
            (&other, t1, 20),
        ],
    );
    let out = dir.path().join("features");
    let run = replay(&rec, &["--features-dir", out.to_str().unwrap()]);
    assert!(!run.status.success());
    // Rows of the first session were written, but only as `.partial` files.
    let names = files(&out);
    assert!(!names.is_empty());
    assert!(names.iter().all(|n| n.ends_with(".partial")), "{names:?}");
    assert!(market_data::store::row_files(&out, "features")
        .unwrap()
        .is_empty());
}

#[test]
fn each_session_rows_carry_their_own_session_id() {
    let dir = tempfile::tempdir().unwrap();
    let rec = dir.path().join("rec");
    let config = FeatureConfig::default();
    let (t0, t1) = (T0 * 1_000_000, (T0 + 3_600_000) * 1_000_000);
    recording(&rec, &[(&config, t0, 5), (&config, t1, 7)]);
    let out = dir.path().join("features");
    let run = replay(&rec, &["--features-dir", out.to_str().unwrap()]);
    assert!(run.status.success(), "{}", stderr(&run));
    let ids: Vec<i64> = market_data::store::read_rows(&out, "features")
        .unwrap()
        .iter()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["session_id"]
                .as_i64()
                .unwrap()
        })
        .collect();
    // Snapshot plus diffs per session; ids are the sessions' own receive times.
    assert_eq!(ids.iter().filter(|&&id| id == t0).count(), 6);
    assert_eq!(ids.iter().filter(|&&id| id == t1).count(), 8);
    assert_eq!(ids.len(), 14);
}

#[test]
fn a_recording_without_session_start_is_reported_unverified() {
    let dir = tempfile::tempdir().unwrap();
    let rec = dir.path().join("rec");
    let mut recorder = Recorder::create(&rec, Duration::from_secs(3600), u64::MAX).unwrap();
    recorder.record(connection("connected", "", 1)).unwrap();
    recorder.finish().unwrap();
    let run = replay(&rec, &[]);
    assert!(run.status.success());
    assert!(stderr(&run).contains("without a recorded feature config"));
    let report: serde_json::Value = serde_json::from_slice(&run.stdout).unwrap();
    assert_eq!(report["state"]["events"]["config_unverified"], 1);
}
