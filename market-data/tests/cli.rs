//! The `market-data replay` binary refuses a recording made with another feature config.

use std::process::Command;
use std::time::Duration;

use market_data::event::{connection, session_start};
use market_data::features::FeatureConfig;
use market_data::recorder::Recorder;

fn replay(input: &std::path::Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_market-data"))
        .arg("replay")
        .arg("--input")
        .arg(input)
        .args(extra)
        .output()
        .unwrap()
}

#[test]
fn replay_enforces_the_recorded_feature_config() {
    let dir = tempfile::tempdir().unwrap();
    let rec = dir.path().join("rec");
    let recorded = FeatureConfig::new(vec![1_000], vec![5]).unwrap();
    let mut recorder = Recorder::create(&rec, Duration::from_secs(3600), u64::MAX).unwrap();
    recorder
        .record(session_start("test", &recorded, 1))
        .unwrap();
    recorder.record(connection("connected", "", 2)).unwrap();
    recorder.finish().unwrap();

    let out = dir.path().join("features");
    let default = replay(&rec, &["--features-dir", out.to_str().unwrap()]);
    assert!(!default.status.success());
    assert!(String::from_utf8_lossy(&default.stderr).contains("different feature config"));
    // Nothing that looks complete is left behind.
    assert!(market_data::store::row_files(&out, "features")
        .unwrap()
        .is_empty());

    let same = replay(&rec, &["--cvd-windows-ms", "1000", "--obi-levels", "5"]);
    assert!(
        same.status.success(),
        "{}",
        String::from_utf8_lossy(&same.stderr)
    );
    let forced = replay(&rec, &["--allow-config-mismatch"]);
    assert!(forced.status.success());
    let report: serde_json::Value = serde_json::from_slice(&forced.stdout).unwrap();
    assert_eq!(report["state"]["events"]["config_mismatch"], 1);
}
