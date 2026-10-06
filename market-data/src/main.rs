//! market-data record --symbols BTCUSDT,ETHUSDT --out recordings [--ws-url URL] [--rest-url URL]
//! market-data replay --input recordings [--audit]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use market_data::binance::{normalize_symbol, MAX_SYMBOLS, REST_URL, WS_URL};
use market_data::live::{run, Config};
use market_data::pipeline::Pipeline;
use market_data::recorder::replay;

fn usage() -> ExitCode {
    eprintln!("usage:\n  market-data record --symbols BTCUSDT,ETHUSDT --out DIR [--ws-url wss://...] [--rest-url https://...]\n  market-data replay --input DIR [--audit]");
    ExitCode::from(2)
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

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("record") => record(&args),
        Some("replay") => replay_command(&args),
        _ => usage(),
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
    let cfg = Config {
        symbols: unique.into_iter().collect(),
        ws_base,
        rest_base,
        out: PathBuf::from(out),
        stale_after: Duration::from_secs(30),
        rotate_after: Duration::from_secs(3600),
        rotate_bytes: 512 * 1024 * 1024,
        channel_capacity: 10_000,
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
    let mut pipeline = Pipeline::default();
    match replay(&PathBuf::from(input), |env| {
        pipeline.handle(&env); // Snapshot requests are ignored: recorded snapshots replay in order.
    }) {
        Ok(stats) => {
            let mut report = serde_json::json!({
                "files": stats.files, "events": stats.events, "truncated_files": stats.truncated_files,
                "state": pipeline.summary(),
            });
            if args.iter().any(|a| a == "--audit") {
                report["audit"] =
                    serde_json::to_value(pipeline.audit().collect::<Vec<_>>()).unwrap_or_default();
            }
            println!("{report:#}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("replay failed: {e}");
            ExitCode::FAILURE
        }
    }
}
