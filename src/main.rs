//! # Linexus Logger — The Immutable Audit Trail
//!
//! Two trails live here:
//!   * the **Demiurge economic ledger** (`segmented_log`, `decay`) — append-only
//!     value transactions for the Vicinagora economy; and
//!   * the **operational audit log** (`audit`, `api`) — infrastructure events
//!     (agents applying changes, task steps, drift) that Nexus ingests and
//!     serves back to Daedalus IT.
//!
//! The binary runs the operational logger's HTTP service; the economic ledger
//! modules remain available as a library for the economy line.

#![allow(dead_code)]

mod api;
mod audit;
mod decay;
mod segmented_log;

use audit::AuditStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(true).init();

    let db_url = std::env::var("LOGGER_DATABASE_URL")
        .unwrap_or_else(|_| "sqlite://linexus-logger.sqlite".to_string());
    let bind = std::env::var("LOGGER_BIND").unwrap_or_else(|_| "0.0.0.0:5151".to_string());
    let token = std::env::var("LOGGER_SERVICE_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());

    tracing::info!("=== LINEXUS LOGGER — Immutable Audit Trail ===");
    tracing::info!("Operational Audit Log: STARTING");
    if token.is_none() {
        tracing::warn!("LOGGER_SERVICE_TOKEN unset — HTTP auth disabled (development mode)");
    }

    let store = AuditStore::connect(&db_url).await?;
    tracing::info!(db = %db_url, "audit store ready");

    let app = api::router(api::AppState { store, token });
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind = %bind, "logger HTTP listening");
    axum::serve(listener, app).await?;

    Ok(())
}
