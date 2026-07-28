//! HTTP surface for the Logger — the contract Nexus's `gateway_client` calls.
//!
//! * `GET  /healthz`          — liveness + counters.
//! * `POST /logs`             — ingest one entry or a batch.
//! * `GET  /logs?agent_id=&task_id=&level=&limit=` — most-recent-first query.
//!
//! Auth is the shared bearer service token (`LINEXUS_SERVICE_TOKEN`), the same
//! credential Nexus presents to the Orchestrator. Unset runs the service open
//! for local development; set, every logs route requires it. The Logger is the
//! audit trail — an open write endpoint in production would let anyone
//! manufacture history, which is worse than having none.

use std::sync::{Arc, Mutex};

use axum::{
    extract::{Query as AxumQuery, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::oplog::{OpEntry, OpLog, Query};

#[derive(Clone)]
pub struct AppState {
    pub log: Arc<Mutex<OpLog>>,
    /// Expected bearer token. `None`/empty disables auth (dev only).
    pub token: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/logs", get(query_logs).post(ingest))
        .with_state(state)
}

async fn healthz(State(st): State<AppState>) -> impl IntoResponse {
    let stats = st.log.lock().unwrap().stats();
    (StatusCode::OK, Json(json!({ "status": "ok", "log": stats })))
}

/// Constant-time comparison — the token is the only thing between an open
/// network and a writable audit trail.
fn token_eq(a: &str, b: &str) -> bool {
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

pub fn authorized(headers: &HeaderMap, expected: &Option<String>) -> bool {
    let Some(expected) = expected.as_deref().filter(|t| !t.is_empty()) else {
        return true; // dev mode: no token configured
    };
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim)
        .is_some_and(|got| token_eq(got, expected))
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "invalid or missing service token" })),
    )
        .into_response()
}

/// The wire shape Nexus ships. Everything except `message` is optional, and
/// an omitted timestamp is stamped at ingest — the shipper's clock wins when
/// it has one, because the agent observed the event, not us.
#[derive(Debug, Deserialize)]
pub struct IngestEntry {
    pub message: String,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum IngestBody {
    One(IngestEntry),
    Many(Vec<IngestEntry>),
}

impl IngestEntry {
    fn into_op(self) -> OpEntry {
        OpEntry {
            timestamp: self.timestamp.unwrap_or_else(Utc::now),
            level: self
                .level
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "info".into()),
            source: self
                .source
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "unknown".into()),
            message: self.message,
            agent_id: self.agent_id.filter(|s| !s.is_empty()),
            task_id: self.task_id.filter(|s| !s.is_empty()),
            metadata: self.metadata,
        }
    }
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
    let mut log = st.log.lock().unwrap();
    let mut ingested = 0usize;
    for e in entries {
        match log.append(e.into_op()) {
            Ok(()) => ingested += 1,
            Err(err) => {
                // Disk refusing writes is the one failure the Logger cannot
                // paper over: report it loudly so the caller knows history is
                // no longer being kept.
                tracing::error!(%err, "append failed — audit trail is not persisting");
                return (
                    StatusCode::INSUFFICIENT_STORAGE,
                    Json(json!({ "error": "append failed", "ingested": ingested })),
                )
                    .into_response();
            }
        }
    }
    (StatusCode::ACCEPTED, Json(json!({ "ingested": ingested }))).into_response()
}

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

async fn query_logs(
    State(st): State<AppState>,
    headers: HeaderMap,
    AxumQuery(q): AxumQuery<LogsQuery>,
) -> Response {
    if !authorized(&headers, &st.token) {
        return unauthorized();
    }
    let out = st.log.lock().unwrap().query(&Query {
        agent_id: q.agent_id.filter(|s| !s.is_empty()),
        task_id: q.task_id.filter(|s| !s.is_empty()),
        level: q.level.filter(|s| !s.is_empty()),
        limit: q.limit.unwrap_or(100).clamp(1, 1000),
    });
    (StatusCode::OK, Json(out)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(token: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(t) = token {
            h.insert(header::AUTHORIZATION, format!("Bearer {t}").parse().unwrap());
        }
        h
    }

    #[test]
    fn open_when_no_token_configured() {
        assert!(authorized(&headers_with(None), &None));
        assert!(authorized(&headers_with(None), &Some(String::new())));
    }

    #[test]
    fn closed_when_token_configured() {
        let expected = Some("s3cret".to_string());
        assert!(!authorized(&headers_with(None), &expected));
        assert!(!authorized(&headers_with(Some("wrong")), &expected));
        assert!(authorized(&headers_with(Some("s3cret")), &expected));
    }

    #[test]
    fn ingest_defaults_are_filled() {
        let e = IngestEntry {
            message: "hello".into(),
            level: None,
            source: None,
            agent_id: Some(String::new()),
            task_id: None,
            timestamp: None,
            metadata: None,
        };
        let op = e.into_op();
        assert_eq!(op.level, "info");
        assert_eq!(op.source, "unknown");
        assert!(op.agent_id.is_none(), "empty string normalizes to absent");
    }
}
