//! HTTP surface for the operational audit log.
//!
//! Nexus is the only expected caller: it ingests events on behalf of agents and
//! queries them back when Daedalus IT tails a machine's journal. Auth is a
//! shared bearer service token (`LOGGER_SERVICE_TOKEN`); when unset the service
//! runs open for local development.
//!
//! * `GET  /healthz`         — liveness.
//! * `GET  /metrics`         — Prometheus counters (open, like `/healthz`).
//! * `POST /logs`            — ingest one entry or a batch (JSON object or
//!   array); a batch is stored atomically.
//! * `GET  /logs?agent_id=&task_id=&level=&source=&since=&until=&before=&limit=`
//!   — query, most-recent-first. `since`/`until` are RFC 3339 (inclusive;
//!   anything else is `400`); `before` (alias `cursor`) is the id of the
//!   oldest record of the previous page and pages further back.

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;

use crate::audit::{AuditStore, IngestEntry, LogQuery};
use crate::metrics;

#[derive(Clone)]
pub struct AppState {
    pub store: AuditStore,
    /// Expected bearer token. `None`/empty disables auth (dev only).
    pub token: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_text))
        .route("/logs", post(ingest).get(query))
        .layer(axum::middleware::from_fn(metrics::count_request))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn metrics_text() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics::render(),
    )
}

/// Ingest accepts either a single entry object or an array of them.
#[derive(Deserialize)]
#[serde(untagged)]
enum IngestBody {
    One(IngestEntry),
    Many(Vec<IngestEntry>),
}

async fn ingest(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<IngestBody>,
) -> Response {
    if !authorized(&headers, &st.token) {
        return unauthorized();
    }
    let entries = match body {
        IngestBody::One(e) => vec![e],
        IngestBody::Many(v) => v,
    };
    match st.store.ingest_many(entries).await {
        Ok(recs) => {
            metrics::ingested(recs.len() as u64);
            (
                StatusCode::CREATED,
                Json(json!({ "ingested": recs.len(), "records": recs })),
            )
                .into_response()
        }
        Err(e) => internal(&e),
    }
}

#[derive(Deserialize)]
struct QueryParams {
    agent_id: Option<String>,
    task_id: Option<String>,
    level: Option<String>,
    source: Option<String>,
    since: Option<String>,
    until: Option<String>,
    #[serde(alias = "cursor")]
    before: Option<String>,
    limit: Option<i64>,
}

/// An RFC 3339 bound, or why it is not one.
fn rfc3339(name: &str, v: Option<String>) -> Result<Option<String>, String> {
    match v {
        None => Ok(None),
        Some(s) => chrono::DateTime::parse_from_rfc3339(s.trim())
            .map(|t| Some(t.to_rfc3339()))
            .map_err(|_| format!("{name} must be an RFC 3339 timestamp")),
    }
}

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))).into_response()
}

async fn query(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<QueryParams>,
) -> Response {
    if !authorized(&headers, &st.token) {
        return unauthorized();
    }
    let (since, until) = match (rfc3339("since", p.since), rfc3339("until", p.until)) {
        (Ok(s), Ok(u)) => (s, u),
        (Err(e), _) | (_, Err(e)) => return bad_request(&e),
    };
    let q = LogQuery {
        agent_id: p.agent_id,
        task_id: p.task_id,
        level: p.level,
        source: p.source,
        since,
        until,
        before: p.before,
        limit: p.limit.unwrap_or(100),
    };
    match st.store.query(&q).await {
        Ok(recs) => Json(recs).into_response(),
        Err(e) => internal(&e),
    }
}

fn authorized(headers: &HeaderMap, token: &Option<String>) -> bool {
    let expected = match token {
        None => return true,
        Some(t) if t.is_empty() => return true,
        Some(t) => t,
    };
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| constant_time_eq(t.trim(), expected))
        .unwrap_or(false)
}

/// Timing-safe comparison so the token can't be recovered byte-by-byte.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "missing or invalid bearer token" })),
    )
        .into_response()
}

fn internal(e: &anyhow::Error) -> Response {
    tracing::error!(error = %e, "logger request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": e.to_string() })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_bounds_must_be_rfc3339() {
        assert_eq!(rfc3339("since", None), Ok(None));
        assert_eq!(
            rfc3339("since", Some(" 2026-06-01T12:00:00+02:00 ".into())),
            Ok(Some("2026-06-01T12:00:00+02:00".into()))
        );
        assert_eq!(
            rfc3339("until", Some("yesterday".into())),
            Err("until must be an RFC 3339 timestamp".into())
        );
    }
}
