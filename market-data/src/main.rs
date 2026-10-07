//! market-data record --symbols BTCUSDT,ETHUSDT --out recordings [--ws-url URL] [--rest-url URL]
//! market-data replay --input recordings [--audit]
//! Both take [--features-dir DIR] [--features-rotate-mb 256] [--features-max-files 64]
//! [--cvd-windows-ms 1000,5000,15000,60000] [--obi-levels 10,50]
//! [--trade-quiet-ms 10000] [--trade-stale-ms 120000].
//! record also takes [--bars-dir DIR] [--state-dir DIR]; replay takes [--bars-dir DIR].
//! replay refuses a recording made with a different feature config unless
//! --allow-config-mismatch is given.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use market_data::binance::{normalize_symbol, MAX_SYMBOLS, REST_URL, WS_URL};
use market_data::features::FeatureConfig;
use market_data::live::{run, Config, Outputs};
use market_data::pipeline::{AuditEvent, Pipeline};
use market_data::recorder::replay;
use market_data::store::{RowStore, StoreConfig};

fn usage() -> ExitCode {
    eprintln!("usage:\n  market-data record --symbols BTCUSDT,ETHUSDT --out DIR [--ws-url wss://...] [--rest-url https://...]\n  market-data replay --input DIR [--audit]\n  both: [--features-dir DIR] [--features-rotate-mb 256] [--features-max-files 64] [--cvd-windows-ms 1000,5000,15000,60000] [--obi-levels 10,50] [--trade-quiet-ms 10000] [--trade-stale-ms 120000]\n  record: [--bars-dir DIR] [--state-dir DIR]\n  replay: [--bars-dir DIR] [--allow-config-mismatch]");
    ExitCode::from(2)
}

fn feature_config(args: &[String]) -> Result<FeatureConfig, String> {
    let defaults = FeatureConfig::default();
    let list = |name: &str| -> Result<Option<Vec<i64>>, String> {
        option(args, name)
            .map(|raw| {
                raw.split(',')
                    .map(|v| v.trim().parse::<i64>().map_err(|e| format!("{name}: {e}")))
                    .collect()
            })
            .transpose()
    };
    let windows = list("--cvd-windows-ms")?.unwrap_or_else(|| defaults.cvd_windows_ms().to_vec());
    let levels = match list("--obi-levels")? {
        Some(v) => v
            .into_iter()
            .map(|n| usize::try_from(n).map_err(|e| format!("--obi-levels: {e}")))
            .collect::<Result<_, _>>()?,
        None => defaults.obi_levels().to_vec(),
    };
    let (quiet, stale) = defaults.trade_silence_ms();
    let number = |name: &str, default: i64| -> Result<i64, String> {
        option(args, name).map_or(Ok(default), |v| {
            v.trim().parse::<i64>().map_err(|e| format!("{name}: {e}"))
        })
    };
    FeatureConfig::new(windows, levels)?.with_trade_silence(
        number("--trade-quiet-ms", quiet)?,
        number("--trade-stale-ms", stale)?,
    )
}

fn store_config(args: &[String]) -> Result<StoreConfig, String> {
    let defaults = StoreConfig::default();
    let parse = |name: &str| -> Result<Option<u64>, String> {
        option(args, name)
            .map(|v| v.trim().parse::<u64>().map_err(|e| format!("{name}: {e}")))
            .transpose()
    };
    let rotate_bytes = match parse("--features-rotate-mb")? {
        Some(mb) if (1..=4096).contains(&mb) => mb * 1024 * 1024,
        Some(_) => return Err("--features-rotate-mb must be 1..=4096".into()),
        None => defaults.rotate_bytes,
    };
    let max_files = match parse("--features-max-files")? {
        Some(n) if (1..=10_000).contains(&n) => n as u32,
        Some(_) => return Err("--features-max-files must be 1..=10000".into()),
        None => defaults.max_files,
    };
    Ok(StoreConfig {
        rotate_bytes,
        max_files,
    })
}

fn option(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn secure(url: &str, scheme: &str) -> bool {
    url.starts_with(&format!("{scheme}://"))
        || url.starts_with("ws://127.0.0.1")
        || url.starts_with("http://127.0.0.1")
}

const SHARED: &[&str] = &[
    "--features-dir",
    "--features-rotate-mb",
    "--features-max-files",
    "--cvd-windows-ms",
    "--obi-levels",
    "--trade-quiet-ms",
    "--trade-stale-ms",
    "--bars-dir",
];

/// Unknown flags and missing values are errors: a typo must not silently disable output.
fn check_args(args: &[String], values: &[&str], switches: &[&str]) -> Result<(), String> {
    let mut i = 1;
    while i < args.len() {
        let flag = args[i].as_str();
        if switches.contains(&flag) {
            i += 1;
        } else if values.contains(&flag) || SHARED.contains(&flag) {
            match args.get(i + 1) {
                Some(v) if !v.starts_with("--") => i += 2,
                _ => return Err(format!("{flag} needs a value")),
            }
        } else {
            return Err(format!("unknown argument {flag}"));
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (values, switches): (&[&str], &[&str]) = match args.first().map(String::as_str) {
        Some("record") => (
            &[
                "--symbols",
                "--out",
                "--ws-url",
                "--rest-url",
                "--state-dir",
            ],
            &[],
        ),
        Some("replay") => (&["--input"], &["--audit", "--allow-config-mismatch"]),
        _ => return usage(),
    };
    if let Err(e) = check_args(&args, values, switches) {
        eprintln!("{e}");
        return usage();
    }
    match args.first().map(String::as_str) {
        Some("record") => record(&args),
        _ => replay_command(&args),
    }
}

fn record(args: &[String]) -> ExitCode {
    let (Some(symbols), Some(out)) = (option(args, "--symbols"), option(args, "--out")) else {
        return usage();
    };
    let mut unique = BTreeSet::new();
    for raw in symbols.split(',') {
        match normalize_symbol(raw) {
            Ok(symbol) => {
                unique.insert(symbol);
            }
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
        }
    }
    if unique.is_empty() || unique.len() > MAX_SYMBOLS {
        eprintln!("1 to {MAX_SYMBOLS} symbols required");
        return ExitCode::from(2);
    }
    let ws_base = option(args, "--ws-url").unwrap_or_else(|| WS_URL.to_string());
    let rest_base = option(args, "--rest-url").unwrap_or_else(|| REST_URL.to_string());
    if !secure(&ws_base, "wss") || !secure(&rest_base, "https") {
        eprintln!("--ws-url must be wss:// and --rest-url https:// (plain only for 127.0.0.1)");
        return ExitCode::from(2);
    }
    let (features, features_store) =
        match feature_config(args).and_then(|f| Ok((f, store_config(args)?))) {
            Ok(both) => both,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
        };
    let cfg = Config {
        symbols: unique.into_iter().collect(),
        ws_base,
        rest_base,
        out: PathBuf::from(out),
        stale_after: Duration::from_secs(30),
        rotate_after: Duration::from_secs(3600),
        rotate_bytes: 512 * 1024 * 1024,
        channel_capacity: 10_000,
        features,
        features_dir: option(args, "--features-dir").map(PathBuf::from),
        features_store,
        bars_dir: option(args, "--bars-dir").map(PathBuf::from),
        state_dir: option(args, "--state-dir").map(PathBuf::from),
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Recorder failure stops the service loudly; a supervisor restart starts a new session.
            eprintln!("recorder error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn replay_command(args: &[String]) -> ExitCode {
    let Some(input) = option(args, "--input") else {
        return usage();
    };
    let (features, store) = match feature_config(args).and_then(|f| Ok((f, store_config(args)?))) {
        Ok(both) => both,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let allow_mismatch = args.iter().any(|a| a == "--allow-config-mismatch");
    let create = |flag: &str, prefix: &'static str, config: StoreConfig| {
        option(args, flag)
            .map(|dir| RowStore::create(&PathBuf::from(dir), prefix, config))
            .transpose()
            .map_err(|e| format!("{flag}: {e}"))
    };
    let outputs = create("--features-dir", "features", store).and_then(|features| {
        Ok(Outputs {
            features,
            bars: create("--bars-dir", "bars", StoreConfig::default())?,
            state: None,
        })
    });
    let mut outputs = match outputs {
        Ok(outputs) => outputs,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let mut pipeline = Pipeline::new(features);
    let mut failure: Option<std::io::Error> = None;
    let read = replay(&PathBuf::from(input), |env| {
        if failure.is_some() {
            return; // Already failed: the rest is read but not processed.
        }
        // Snapshot requests are ignored: recorded snapshots replay in order.
        let step = pipeline.handle(&env);
        if pipeline.stats.config_mismatch > 0 && !allow_mismatch {
            let recorded = pipeline
                .audit()
                .filter_map(|e| match &e.event {
                    AuditEvent::ConfigMismatch { recorded } => Some(recorded.to_string()),
                    _ => None,
                })
                .last();
            failure = Some(std::io::Error::other(format!(
                "recording was made with feature config {}; pass the same flags or --allow-config-mismatch",
                recorded.unwrap_or_default()
            )));
            return;
        }
        failure = outputs.write(&step).err();
    });
    // The first failure wins over a later read error.
    let result = match failure.take() {
        Some(e) => Err(e),
        None => read,
    };
    // Failed replays leave every output file `.partial`, never looking complete.
    let (result, written) = match outputs.close(result.is_ok()) {
        Ok(written) => (result, written),
        Err(e) => (result.and(Err(e)), serde_json::Value::Null),
    };
    let dropped: u64 = written
        .as_object()
        .map(|o| o.values().filter_map(|v| v["dropped_rows"].as_u64()).sum())
        .unwrap_or(0);
    let result = match result {
        Ok(_) if dropped > 0 => Err(std::io::Error::other(format!(
            "{dropped} rows dropped by --features-max-files; output is incomplete"
        ))),
        other => other,
    };
    if pipeline.stats.config_unverified > 0 {
        eprintln!(
            "warning: {} session(s) without a recorded feature config; not verifiable",
            pipeline.stats.config_unverified
        );
    }
    match result {
        Ok(stats) => {
            let mut report = serde_json::json!({
                "files": stats.files, "events": stats.events, "truncated_files": stats.truncated_files,
                "state": pipeline.summary(), "outputs": written,
            });
            if args.iter().any(|a| a == "--audit") {
                report["audit"] =
                    serde_json::to_value(pipeline.audit().collect::<Vec<_>>()).unwrap_or_default();
            }
            println!("{report:#}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            // A partial feature file stays `.partial`: never mistaken for a complete one.
            eprintln!("replay failed: {e}");
            ExitCode::FAILURE
        }
    }
}
