//! # Linexus Logger — The Immutable Audit Trail
//!
//! The operational log service behind Nexus: agents' journal lines and task
//! audit events are shipped here (via Nexus — nothing talks to the Logger
//! directly except Nexus), persisted to append-only JSONL segments, and served
//! back for the log viewers in Daedalus IT.
//!
//! Environment:
//! * `LOGGER_BIND`             — listen address (default `0.0.0.0:5151`,
//!                               matching Nexus's `LINEXUS_LOGGER_URL` default)
//! * `LOGGER_DATA_DIR`         — segment directory (default `./logger-data`)
//! * `LOGGER_SEGMENT_ENTRIES`  — entries per segment before rotation (10000)
//! * `LOGGER_INDEX_ENTRIES`    — in-memory query window (50000)
//! * `LOGGER_RETENTION_DAYS`   — drop whole segments older than this (30)
//! * `LINEXUS_SERVICE_TOKEN`   — shared bearer token; unset runs open (dev)

// The Demiurge ledger modules predate the operational log service and are
// exercised by their own tests; the binary itself now fronts the oplog.
#[allow(dead_code)]
mod decay;
mod http;
mod oplog;
#[allow(dead_code)]
mod segmented_log;

use std::sync::{Arc, Mutex};

use chrono::{Duration, Utc};

fn env_usize(key: &str, fallback: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(true).init();
    tracing::info!("=== LINEXUS LOGGER — Immutable Audit Trail ===");

    let bind = std::env::var("LOGGER_BIND").unwrap_or_else(|_| "0.0.0.0:5151".to_string());
    let data_dir =
        std::env::var("LOGGER_DATA_DIR").unwrap_or_else(|_| "./logger-data".to_string());
    let segment_entries = env_usize("LOGGER_SEGMENT_ENTRIES", 10_000);
    let index_entries = env_usize("LOGGER_INDEX_ENTRIES", 50_000);
    let retention_days = env_usize("LOGGER_RETENTION_DAYS", 30);
    let token = std::env::var("LINEXUS_SERVICE_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());

    if token.is_none() {
        tracing::warn!(
            "LINEXUS_SERVICE_TOKEN is unset — the logs API is running OPEN. \
             Fine for development; set the token anywhere that matters, because \
             an open write endpoint on an audit trail lets anyone manufacture history."
        );
    }

    let log = oplog::OpLog::open(&data_dir, segment_entries, index_entries)?;
    let stats = log.stats();
    tracing::info!(
        dir = %data_dir,
        indexed = stats.indexed_entries,
        active_segment = stats.active_segment,
        "segmented log replayed"
    );

    let log = Arc::new(Mutex::new(log));

    // Retention sweep: hourly, dropping whole expired segments. Immutability
    // means files are deleted, never edited.
    {
        let log = Arc::clone(&log);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                tick.tick().await;
                let cutoff = Utc::now() - Duration::days(retention_days as i64);
                match log.lock().unwrap().sweep_expired(cutoff) {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(segments = n, "retention sweep dropped segments"),
                    Err(e) => tracing::warn!(error = %e, "retention sweep failed"),
                }
            }
        });
    }

    let app = http::router(http::AppState { log, token });
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind = %bind, "logger HTTP listening");
    axum::serve(listener, app).await?;
    Ok(())
}
