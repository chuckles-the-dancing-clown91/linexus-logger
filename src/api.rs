//! HTTP surface for the operational audit log.
//!
//! Nexus is the only expected caller: it ingests events on behalf of agents and
//! queries them back when Daedalus IT tails a machine's journal. Auth is a
//! shared bearer service token (`LOGGER_SERVICE_TOKEN`); when unset the service
//! runs open for local development.
//!
//! * `GET  /healthz`         — liveness.
//! * `POST /logs`            — ingest one entry or a batch (JSON object or array).
//! * `GET  /logs?agent_id=&task_id=&limit=` — query, most-recent-first.

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::audit::{AuditStore, IngestEntry, LogQuery};

#[derive(Clone)]
pub struct AppState {
    pub store: AuditStore,
    /// Expected bearer token. `None`/empty disables auth (dev only).
    pub token: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/logs", post(ingest).get(query))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
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
        Ok(recs) => (
            StatusCode::CREATED,
            Json(json!({ "ingested": recs.len(), "records": recs })),
        )
            .into_response(),
        Err(e) => internal(&e),
    }
}

#[derive(Deserialize)]
struct QueryParams {
    agent_id: Option<String>,
    task_id: Option<String>,
    limit: Option<i64>,
}

async fn query(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<QueryParams>,
) -> Response {
    if !authorized(&headers, &st.token) {
        return unauthorized();
    }
    let q = LogQuery {
        agent_id: p.agent_id,
        task_id: p.task_id,
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
