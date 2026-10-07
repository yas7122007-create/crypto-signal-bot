//! Live ingestion: one combined WebSocket, a sequential snapshot fetcher, and a single
//! processing loop that records every envelope before the pipeline sees it.
//! Channels are bounded: a slow disk applies backpressure to the socket instead of
//! growing memory; if Binance then drops us, the reconnect path resynchronizes the books.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout, Instant};
use tokio_tungstenite::tungstenite::Message;

use crate::binance::{
    envelope_from_snapshot, envelope_from_stream, snapshot_url, stream_url, unparsed,
};
use crate::event::{connection, now_ns, session_start, Envelope};
use crate::features::FeatureConfig;
use crate::pipeline::{Action, Pipeline};
use crate::recorder::Recorder;
use crate::store::{RowStore, StoreConfig};

const MAX_SNAPSHOT_BYTES: usize = 5 * 1024 * 1024;
/// Depth limit=1000 costs 20 weight; one per second stays at half of Binance's 2400/min,
/// leaving room for the Python bot's REST calls from the same IP.
const SNAPSHOT_SPACING: Duration = Duration::from_secs(1);
const HEALTHY_AFTER: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct Config {
    pub symbols: Vec<String>,
    pub ws_base: String,
    pub rest_base: String,
    pub out: PathBuf,
    pub stale_after: Duration,
    pub rotate_after: Duration,
    pub rotate_bytes: u64,
    pub channel_capacity: usize,
    pub features: FeatureConfig,
    /// Optional directory of feature rows; replay of the recording reproduces them exactly.
    pub features_dir: Option<PathBuf>,
    pub features_store: StoreConfig,
}

/// Reconnect delay: 1 s, 2 s, 4 s ... capped at 60 s.
pub fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.min(6)).min(Duration::from_secs(60))
}

pub async fn run(cfg: Config) -> io::Result<()> {
    run_until(cfg, shutdown_signal()).await
}

/// The live loop, stopping cleanly when `shutdown` completes (tests pass their own).
pub async fn run_until(cfg: Config, shutdown: impl Future<Output = ()>) -> io::Result<()> {
    let (tx, mut events) = mpsc::channel::<Envelope>(cfg.channel_capacity);
    // At most one request per symbol per reset; reconnects are >= 1 s apart, so even a
    // 10 s fetch timeout cannot queue more than a few hundred.
    let (snap_tx, snap_rx) = mpsc::channel::<String>(1024);
    let mut recorder = Recorder::create(&cfg.out, cfg.rotate_after, cfg.rotate_bytes)?;
    let mut features = cfg
        .features_dir
        .as_deref()
        .map(|dir| RowStore::create(dir, "features", cfg.features_store.clone()))
        .transpose()?;
    let mut pipeline = Pipeline::new(cfg.features.clone());
    // The feature config travels with the recording, so replay can verify it.
    pipeline.handle(&recorder.record(session_start(
        "market-data record",
        &cfg.features,
        now_ns(),
    ))?);

    let tasks = [
        tokio::spawn(websocket(cfg.clone(), tx.clone())),
        tokio::spawn(snapshots(cfg.rest_base.clone(), snap_rx, tx)),
    ];
    let mut result = async {
        let mut status = interval(Duration::from_secs(10));
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                Some(env) = events.recv() => {
                    for action in ingest(env, &mut recorder, &mut pipeline, &mut features)? {
                        let Action::RequestSnapshot(symbol) = action;
                        // Requests are deduplicated per symbol, so the queue only fills if the
                        // fetcher died. Stop loudly instead of silently never syncing.
                        if snap_tx.try_send(symbol).is_err() {
                            return Err(io::Error::other("snapshot fetcher stopped"));
                        }
                    }
                }
                _ = status.tick() => {
                    recorder.flush()?;
                    if let Some(out) = features.as_mut() {
                        out.flush()?;
                    }
                    let dropped = features.as_ref().map_or(0, |f| f.dropped_rows);
                    eprintln!("{}", serde_json::json!({"status": pipeline.summary(), "feature_rows_dropped": dropped}));
                }
                _ = &mut shutdown => return Ok(()),
            }
        }
    }
    .await;
    for task in tasks {
        task.abort();
    }
    // Keep what was already received: drain the channel through the same path.
    while result.is_ok() {
        let Ok(env) = events.try_recv() else { break };
        result = ingest(env, &mut recorder, &mut pipeline, &mut features).map(drop);
    }
    // Every step runs even after a failure; the first error is the one reported. A failed
    // run leaves its features as `.partial`, never as a file that looks complete.
    let features = match (features, &result) {
        (Some(out), Ok(())) => out.finish(),
        (Some(mut out), Err(_)) => out.flush(),
        (None, _) => Ok(()),
    };
    let finished = recorder.finish();
    result.and(features).and(finished)
}

/// Record first, so replay sees exactly what the pipeline saw; then features, then actions.
fn ingest(
    env: Envelope,
    recorder: &mut Recorder,
    pipeline: &mut Pipeline,
    features: &mut Option<RowStore>,
) -> io::Result<Vec<Action>> {
    let env = recorder.record(env)?;
    let step = pipeline.handle(&env);
    if let (Some(out), Some(row)) = (features.as_mut(), &step.feature) {
        out.write(row)?;
    }
    Ok(step.actions)
}

/// Ctrl-C everywhere, plus SIGTERM on Unix so supervisors stop the recorder cleanly.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn websocket(cfg: Config, tx: mpsc::Sender<Envelope>) {
    let url = stream_url(&cfg.ws_base, &cfg.symbols);
    let mut attempt = 0u32;
    loop {
        let (reason, healthy) = match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok((mut ws, _)) => {
                let connected_at = Instant::now();
                if tx
                    .send(connection("connected", "", now_ns()))
                    .await
                    .is_err()
                {
                    return;
                }
                let mut reason = String::from("closed by server");
                loop {
                    match timeout(cfg.stale_after, ws.next()).await {
                        Err(_) => {
                            reason =
                                format!("stale: no message for {} s", cfg.stale_after.as_secs());
                            break;
                        }
                        Ok(None) => break,
                        Ok(Some(Err(e))) => {
                            reason = e.to_string();
                            break;
                        }
                        Ok(Some(Ok(Message::Text(text)))) => {
                            let recv = now_ns();
                            let env = envelope_from_stream(text.as_str(), recv)
                                .unwrap_or_else(|_| unparsed(text.as_str(), recv));
                            if tx.send(env).await.is_err() {
                                return;
                            }
                        }
                        Ok(Some(Ok(Message::Ping(payload)))) => {
                            if let Err(e) = ws.send(Message::Pong(payload)).await {
                                reason = e.to_string();
                                break;
                            }
                        }
                        Ok(Some(Ok(Message::Close(_)))) => break,
                        Ok(Some(Ok(_))) => {}
                    }
                }
                (reason, connected_at.elapsed() >= HEALTHY_AFTER)
            }
            Err(e) => (e.to_string(), false),
        };
        let reason: String = reason.chars().take(200).collect();
        eprintln!(
            "{}",
            serde_json::json!({"ws": "disconnected", "reason": reason, "attempt": attempt})
        );
        if tx
            .send(connection("disconnected", &reason, now_ns()))
            .await
            .is_err()
        {
            return;
        }
        // Reset only after a connection that stayed up, so an accept-then-drop loop keeps
        // backing off instead of reconnecting every second toward an IP ban.
        if healthy {
            attempt = 0;
        }
        sleep(backoff(attempt)).await;
        attempt = attempt.saturating_add(1);
    }
}

/// Owns snapshot retries: failed symbols are rescheduled with backoff here, so one
/// failing symbol never blocks the others and the pipeline is not asked again every diff.
async fn snapshots(
    rest_base: String,
    mut requests: mpsc::Receiver<String>,
    tx: mpsc::Sender<Envelope>,
) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!(
                "{}",
                serde_json::json!({"snapshot": "client error", "error": e.to_string()})
            );
            return;
        }
    };
    // symbol -> (consecutive failures, earliest next attempt)
    let mut pending: HashMap<String, (u32, Instant)> = HashMap::new();
    let mut next_slot = Instant::now();
    loop {
        let due = pending
            .iter()
            .min_by_key(|(_, (_, at))| *at)
            .map(|(s, (_, at))| (s.clone(), *at));
        tokio::select! {
            request = requests.recv() => match request {
                Some(symbol) => { pending.entry(symbol).or_insert((0, Instant::now())); }
                None => return,
            },
            _ = tokio::time::sleep_until(due.as_ref().map(|(_, at)| (*at).max(next_slot)).unwrap_or(next_slot)),
                if due.is_some() => {
                let Some((symbol, _)) = due else { continue };
                next_slot = Instant::now() + SNAPSHOT_SPACING;
                match fetch(&client, &rest_base, &symbol).await {
                    Ok(env) => {
                        pending.remove(&symbol);
                        if tx.send(env).await.is_err() {
                            return;
                        }
                    }
                    Err((error, retry_after)) => {
                        let failures = pending.get(&symbol).map(|(n, _)| *n).unwrap_or(0) + 1;
                        let wait = retry_after.unwrap_or_else(|| backoff(failures));
                        pending.insert(symbol.clone(), (failures, Instant::now() + wait));
                        if retry_after.is_some() {
                            next_slot = next_slot.max(Instant::now() + wait); // Rate limited: pause all symbols.
                        }
                        eprintln!("{}", serde_json::json!({"snapshot": symbol, "error": error, "retry_in_s": wait.as_secs()}));
                    }
                }
            }
        }
    }
}

async fn fetch(
    client: &reqwest::Client,
    base: &str,
    symbol: &str,
) -> Result<Envelope, (String, Option<Duration>)> {
    let mut response = client
        .get(snapshot_url(base, symbol))
        .send()
        .await
        .map_err(|e| (e.to_string(), None))?;
    let status = response.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        let wait = response
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60)
            .clamp(1, 3600);
        return Err((format!("HTTP {status}"), Some(Duration::from_secs(wait))));
    }
    if !status.is_success() {
        return Err((format!("HTTP {status}"), None));
    }
    if response
        .content_length()
        .is_some_and(|n| n > MAX_SNAPSHOT_BYTES as u64)
    {
        return Err(("snapshot too large".into(), None));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| (e.to_string(), None))? {
        if body.len() + chunk.len() > MAX_SNAPSHOT_BYTES {
            return Err(("snapshot too large".into(), None)); // Chunked bodies have no length header.
        }
        body.extend_from_slice(&chunk);
    }
    let text = std::str::from_utf8(&body).map_err(|e| (e.to_string(), None))?;
    envelope_from_snapshot(symbol, text, now_ns()).map_err(|e| (e.to_string(), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let secs: Vec<u64> = (0..10).map(|a| backoff(a).as_secs()).collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 16, 32, 60, 60, 60, 60]);
        assert_eq!(backoff(u32::MAX).as_secs(), 60);
    }
}
