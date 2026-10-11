//! A few process counters, served on `GET /metrics` in Prometheus text format.
//!
//! No metrics crate: the counters are static atomics and the exporter is a
//! handful of lines, which is all a service with two routes needs.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::{extract::Request, middleware::Next, response::Response};

static HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);
static LOGS_INGESTED: AtomicU64 = AtomicU64::new(0);
static LOGS_PRUNED: AtomicU64 = AtomicU64::new(0);

/// Count `n` ingested events.
pub fn ingested(n: u64) {
    LOGS_INGESTED.fetch_add(n, Ordering::Relaxed);
}

/// Count `n` events removed by retention.
pub fn pruned(n: u64) {
    LOGS_PRUNED.fetch_add(n, Ordering::Relaxed);
}

/// Middleware counting every request (including `/metrics` itself).
pub async fn count_request(req: Request, next: Next) -> Response {
    HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
    next.run(req).await
}

/// The exposition, `text/plain; version=0.0.4`.
pub fn render() -> String {
    let mut out = String::new();
    for (name, help, value) in [
        (
            "logger_http_requests_total",
            "HTTP requests received.",
            HTTP_REQUESTS.load(Ordering::Relaxed),
        ),
        (
            "logger_logs_ingested_total",
            "Audit events stored.",
            LOGS_INGESTED.load(Ordering::Relaxed),
        ),
        (
            "logger_logs_pruned_total",
            "Audit events removed by retention.",
            LOGS_PRUNED.load(Ordering::Relaxed),
        ),
    ] {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_text() {
        ingested(3);
        let text = render();
        assert!(text.contains("# TYPE logger_logs_ingested_total counter\n"));
        let n: u64 = text
            .lines()
            .find_map(|l| l.strip_prefix("logger_logs_ingested_total "))
            .unwrap()
            .parse()
            .unwrap();
        assert!(n >= 3);
    }
}
