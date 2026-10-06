//! Live ingestion: one combined WebSocket, a sequential snapshot fetcher, and a single
//! processing loop that records every envelope before the pipeline sees it.
//! Channels are bounded: a slow disk applies backpressure to the socket instead of
//! growing memory; if Binance then drops us, the reconnect path resynchronizes the books.

use std::collections::HashMap;
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
use crate::event::{connection, now_ns, Envelope};
use crate::pipeline::{Action, Pipeline};
use crate::recorder::Recorder;

const MAX_SNAPSHOT_BYTES: usize = 5 * 1024 * 1024;
/// Depth limit=1000 costs 20 weight; one per second stays at half of Binance's 2400/min,
/// leaving room for the Python bot's REST calls from the same IP.
const SNAPSHOT_SPACING: Duration = Duration::from_secs(1);

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
}

/// Reconnect delay: 1 s, 2 s, 4 s ... capped at 60 s.
pub fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.min(6)).min(Duration::from_secs(60))
}

pub async fn run(cfg: Config) -> io::Result<()> {
    let (tx, mut events) = mpsc::channel::<Envelope>(cfg.channel_capacity);
    let (snap_tx, snap_rx) = mpsc::channel::<String>(cfg.symbols.len().max(1) * 2);
    let mut recorder = Recorder::create(&cfg.out, cfg.rotate_after, cfg.rotate_bytes)?;
    let mut pipeline = Pipeline::default();
    pipeline.handle(&recorder.record(connection(
        "session_start",
        "market-data record",
        now_ns(),
    ))?);

    tokio::spawn(websocket(cfg.clone(), tx.clone()));
    tokio::spawn(snapshots(cfg.rest_base.clone(), snap_rx, tx));

    let mut status = interval(Duration::from_secs(10));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            Some(env) = events.recv() => {
                // Record first: replay must see exactly what the pipeline saw.
                let env = recorder.record(env)?;
                for action in pipeline.handle(&env) {
                    let Action::RequestSnapshot(symbol) = action;
                    if snap_tx.try_send(symbol.clone()).is_err() {
                        pipeline.snapshot_failed(&symbol);
                    }
                }
            }
            _ = status.tick() => {
                recorder.flush()?;
                eprintln!("{}", serde_json::json!({"status": pipeline.summary()}));
            }
            _ = &mut shutdown => break,
        }
    }
    recorder.finish()
}

async fn websocket(cfg: Config, tx: mpsc::Sender<Envelope>) {
    let url = stream_url(&cfg.ws_base, &cfg.symbols);
    let mut attempt = 0u32;
    loop {
        let reason = match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok((mut ws, _)) => {
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
                            attempt = 0; // Healthy data resets the backoff.
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
                reason
            }
            Err(e) => e.to_string(),
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
    let response = client
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
    let body = response.bytes().await.map_err(|e| (e.to_string(), None))?;
    if body.len() > MAX_SNAPSHOT_BYTES {
        return Err(("snapshot too large".into(), None));
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
