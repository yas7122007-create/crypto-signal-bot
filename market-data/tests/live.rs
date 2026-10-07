//! The real live loop (`live::run_until`) against in-process fake Binance servers on
//! 127.0.0.1: a WebSocket that injects a sequence gap and then drops the connection, and an
//! HTTP depth endpoint. This exercises reconnect, resync, recording and feature output over
//! real sockets. It is NOT a test against Binance itself.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use market_data::features::FeatureConfig;
use market_data::live::{run_until, Config};
use market_data::pipeline::{AuditEvent, Pipeline};
use market_data::recorder::replay;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

/// Shared state of the fake exchange. The script advances on snapshots served, not on
/// wall-clock time, so a slow runner cannot reorder gap, resync and reconnect.
#[derive(Default)]
struct Fake {
    last_u: AtomicU64,
    served: AtomicU64,
    done: Notify,
}

/// Diffs sent after a milestone before the next step, 10 ms apart.
const TAIL: u32 = 30;

fn diff(id: u64, prev: u64) -> String {
    let qty = id % 5 + 1;
    format!(
        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{e},"T":{e},"s":"BTCUSDT","U":{id},"u":{id},"pu":{prev},"b":[["100","{qty}"]],"a":[["101","{}"]]}}}}"#,
        6 - qty,
        e = 1_700_000_000_000 + id * 1_000, // 1 s apart: bars close.
    )
}

fn trade(id: u64) -> String {
    format!(
        r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":{t},"s":"BTCUSDT","a":{id},"p":"100.5","q":"0.{id}","f":1,"l":1,"T":{t},"m":{}}}}}"#,
        id.is_multiple_of(3),
        t = 1_700_000_000_000 + id * 1_000,
    )
}

/// Connection 0: once the first snapshot is served, skip one update id (a `pu` gap); once
/// the resync snapshot is served, close. Connection 1: ids restart at 1000; once its
/// snapshot is served, fire `done` and keep streaming until the client goes away.
async fn fake_ws(listener: TcpListener, fake: Arc<Fake>) {
    for connection in 0u64.. {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
            continue;
        };
        let target = if connection == 0 { 2 } else { 3 };
        let (mut id, mut gap_in, mut tail) = (1_000 * connection, Some(10u32), TAIL);
        'stream: loop {
            id += 1;
            let served = fake.served.load(Ordering::SeqCst);
            if connection == 0 && served >= 1 {
                if let Some(n) = gap_in.as_mut() {
                    *n -= 1;
                    if *n == 0 {
                        id += 1; // The client sees pu = id - 1, never received: a gap.
                        gap_in = None;
                    }
                }
            }
            fake.last_u.store(id, Ordering::SeqCst);
            for frame in [diff(id, id - 1), trade(id)] {
                if ws.send(Message::text(frame)).await.is_err() {
                    break 'stream; // Client gone.
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            if served >= target {
                tail = tail.saturating_sub(1);
                if tail == 0 {
                    if connection == 0 {
                        let _ = ws.close(None).await;
                        break;
                    }
                    fake.done.notify_one();
                    tail = u32::MAX;
                }
            }
        }
    }
}

/// Depth snapshot at the latest id the WebSocket has sent.
async fn fake_rest(listener: TcpListener, fake: Arc<Fake>) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => request.extend_from_slice(&buf[..n]),
            }
        }
        let id = fake.last_u.load(Ordering::SeqCst);
        let body = format!(
            r#"{{"lastUpdateId":{id},"E":{e},"T":{e},"bids":[["100","1"],["99","1"]],"asks":[["101","1"],["102","1"]]}}"#,
            e = 1_700_000_000_000 + id * 1_000
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        if stream.write_all(response.as_bytes()).await.is_ok() {
            fake.served.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_loop_resyncs_records_and_replays_identically() {
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ws_port, rest_port) = (
        ws.local_addr().unwrap().port(),
        rest.local_addr().unwrap().port(),
    );
    let fake = Arc::new(Fake::default());
    tokio::spawn(fake_ws(ws, fake.clone()));
    tokio::spawn(fake_rest(rest, fake.clone()));

    let dir = tempfile::tempdir().unwrap();
    let features_dir = dir.path().join("live-features");
    let config = FeatureConfig::new(vec![100, 1_000], vec![1, 2]).unwrap();
    let cfg = Config {
        symbols: vec!["BTCUSDT".into()],
        ws_base: format!("ws://127.0.0.1:{ws_port}/stream"),
        rest_base: format!("http://127.0.0.1:{rest_port}"),
        out: dir.path().join("rec"),
        stale_after: Duration::from_secs(30),
        rotate_after: Duration::from_secs(3600),
        rotate_bytes: 4_096, // Force rotation during the run.
        channel_capacity: 1_000,
        features: config.clone(),
        features_dir: Some(features_dir.clone()),
        features_store: market_data::store::StoreConfig {
            rotate_bytes: 8_192, // Several feature files too.
            max_files: 1_000,
        },
        bars_dir: Some(dir.path().join("live-bars")),
        state_dir: Some(dir.path().join("state")),
    };
    let stop = async move {
        fake.done.notified().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    tokio::time::timeout(Duration::from_secs(30), run_until(cfg, stop))
        .await
        .expect("live loop did not finish")
        .unwrap();

    let mut pipeline = Pipeline::new(config);
    let mut replayed = String::new();
    let mut replayed_bars = Vec::new();
    let stats = replay(&dir.path().join("rec"), |env| {
        let step = pipeline.handle(&env);
        if let Some(row) = step.feature {
            replayed.push_str(&serde_json::to_string(&row).unwrap());
            replayed.push('\n');
        }
        replayed_bars.extend(step.bar.map(|b| serde_json::to_string(&b).unwrap()));
    })
    .unwrap();
    let live_bars = market_data::store::read_rows(&dir.path().join("live-bars"), "bars").unwrap();
    assert!(!live_bars.is_empty(), "bars expected");
    assert_eq!(replayed_bars, live_bars);
    // The state file holds the latest bars for the Python bridge, as valid JSON.
    let state: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("state").join("BTCUSDT.json")).unwrap(),
    )
    .unwrap();
    let window = state["bars"].as_array().unwrap();
    let last: serde_json::Value = serde_json::from_str(live_bars.last().unwrap()).unwrap();
    assert_eq!(window.last().unwrap(), &last);
    let feature_files = market_data::store::row_files(&features_dir, "features").unwrap();
    assert!(feature_files.len() > 1, "feature rotation expected");
    let live = market_data::store::read_rows(&features_dir, "features")
        .unwrap()
        .join("\n")
        + "\n";

    assert!(stats.files > 1, "rotation expected");
    assert_eq!(stats.truncated_files, 0);
    assert!(!live.is_empty());
    assert_eq!(replayed, live, "replayed features differ from live output");

    let count = |f: &dyn Fn(&AuditEvent) -> bool| pipeline.audit().filter(|e| f(&e.event)).count();
    let synced = count(&|e| {
        matches!(
            e,
            AuditEvent::Book(market_data::book::SyncEvent::Synced { .. })
        )
    });
    let gaps = count(&|e| {
        matches!(
            e,
            AuditEvent::Book(market_data::book::SyncEvent::Gap { .. })
        )
    });
    let drops =
        count(&|e| matches!(e, AuditEvent::Connection { event } if event == "disconnected"));
    assert!(
        synced >= 3,
        "initial sync, resync after gap, resync after reconnect: {synced}"
    );
    assert_eq!((gaps, drops), (1, 1));

    // Feature rows come from three distinct sync epochs and never from an unsynced book.
    let epochs: std::collections::BTreeSet<u64> = live
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["synced_since_seq"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert!(epochs.len() >= 3, "sync epochs with features: {epochs:?}");
    // Trades flow too: some rows carry a CVD and a covered window.
    let rows: Vec<serde_json::Value> = live
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(rows.iter().any(|r| r["cvd"].is_string()));
    assert!(rows.iter().any(|r| r["deltas"][0]["delta"].is_string()));
    assert!(rows.iter().all(|r| r["obi"][0]["value"].is_string()));
    let book = pipeline.book("BTCUSDT").unwrap();
    assert!(book.is_synced());
    assert!(book.last_update_id().unwrap() >= 1_000);
}
