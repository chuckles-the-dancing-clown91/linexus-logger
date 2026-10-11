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
//!
//! `LINEXUS_ENV=production` marks a real deployment: the service then refuses
//! to start without `LOGGER_SERVICE_TOKEN`, where development only warns.
//! Events older than `LOGGER_RETENTION_DAYS` (default 90; `0` keeps
//! everything) are pruned at start and every hour.

#![allow(dead_code)]

mod api;
mod audit;
mod decay;
mod metrics;
mod segmented_log;

use audit::AuditStore;

/// Environment variable naming the deployment environment.
const ENV_LINEXUS_ENV: &str = "LINEXUS_ENV";
/// Environment variable holding the retention, in days.
const ENV_RETENTION_DAYS: &str = "LOGGER_RETENTION_DAYS";
/// Retention when [`ENV_RETENTION_DAYS`] is unset.
const DEFAULT_RETENTION_DAYS: u32 = 90;
/// How often retention runs after the prune at start.
const PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Whether this is a production deployment (`LINEXUS_ENV=production`).
fn is_production(var: &dyn Fn(&str) -> Option<String>) -> bool {
    var(ENV_LINEXUS_ENV).is_some_and(|v| v.trim().eq_ignore_ascii_case("production"))
}

/// The configured retention in days: the default when unset, `None` when
/// `0` (keep everything), an error for anything unparseable.
fn retention_days(var: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Option<u32>> {
    let Some(raw) = var(ENV_RETENTION_DAYS).filter(|s| !s.trim().is_empty()) else {
        return Ok(Some(DEFAULT_RETENTION_DAYS));
    };
    match raw.trim().parse::<u32>() {
        Ok(0) => Ok(None),
        Ok(days) => Ok(Some(days)),
        Err(_) => anyhow::bail!("{ENV_RETENTION_DAYS}={raw:?} is not a whole number of days"),
    }
}

/// Prune now, then every [`PRUNE_INTERVAL`]. A failed prune is logged and
/// retried next round; it never takes the service down.
fn spawn_retention(store: AuditStore, days: u32) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PRUNE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await; // the first tick fires at once
            match store.prune_older_than(days).await {
                Ok(0) => {}
                Ok(n) => {
                    metrics::pruned(n);
                    tracing::info!(removed = n, days, "retention prune");
                }
                Err(e) => tracing::warn!(error = %e, "retention prune failed"),
            }
        }
    });
}

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
        if is_production(&|k| std::env::var(k).ok()) {
            anyhow::bail!(
                "refusing to start: {ENV_LINEXUS_ENV}=production but LOGGER_SERVICE_TOKEN is unset — the audit trail would accept writes from any caller"
            );
        }
        tracing::warn!("LOGGER_SERVICE_TOKEN unset — HTTP auth disabled (development mode)");
    }
    let retention = retention_days(&|k| std::env::var(k).ok())?;

    let store = AuditStore::connect(&db_url).await?;
    tracing::info!(db = %db_url, "audit store ready");
    match retention {
        Some(days) => spawn_retention(store.clone(), days),
        None => tracing::warn!("{ENV_RETENTION_DAYS}=0: audit events are kept forever"),
    }

    let app = api::router(api::AppState { store, token });
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind = %bind, "logger HTTP listening");
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_is_only_the_explicit_value() {
        assert!(is_production(&|_| Some("production".into())));
        assert!(is_production(&|_| Some(" Production ".into())));
        assert!(!is_production(&|_| Some("development".into())));
        assert!(!is_production(&|_| None));
    }

    #[test]
    fn retention_defaults_disables_on_zero_and_rejects_garbage() {
        assert_eq!(
            retention_days(&|_| None).unwrap(),
            Some(DEFAULT_RETENTION_DAYS)
        );
        assert_eq!(
            retention_days(&|_| Some("".into())).unwrap(),
            Some(DEFAULT_RETENTION_DAYS)
        );
        assert_eq!(retention_days(&|_| Some("30".into())).unwrap(), Some(30));
        assert_eq!(retention_days(&|_| Some("0".into())).unwrap(), None);
        assert!(retention_days(&|_| Some("forever".into())).is_err());
    }
}
