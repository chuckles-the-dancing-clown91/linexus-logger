//! Operational audit log — the RMM/infrastructure event trail.
//!
//! This is distinct from the Demiurge economic ledger (`segmented_log`,
//! `decay`): those record value transactions for the Vicinagora economy, while
//! this records *operational* events — an agent applying a change, a task step
//! succeeding or failing, a service reporting drift. Nexus ingests these on
//! behalf of agents and queries them back when Daedalus IT tails a machine's
//! journal.
//!
//! Records are shaped so that projecting to Daedalus IT's `LogLine`
//! (`{timestamp, level, source, message}`) is a straight field selection.

use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::str::FromStr;

/// One stored operational event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    /// UUIDv7 — time-ordered id assigned by the logger on ingest.
    pub id: String,
    /// RFC 3339 timestamp of the event.
    pub timestamp: String,
    /// Machine UUID the event pertains to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// Correlating task UUID, if the event was part of a task execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// `info` | `warn` | `error`.
    pub level: String,
    /// Origin of the event (e.g. `agent`, `nexus`, a component name).
    pub source: String,
    /// Human-readable message.
    pub message: String,
    /// Optional structured context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// An event as submitted for ingest. `id` and `timestamp` are optional — the
/// logger assigns a UUIDv7 and stamps the receive time when they're omitted.
#[derive(Debug, Clone, Deserialize)]
pub struct IngestEntry {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default = "default_level")]
    pub level: String,
    #[serde(default = "default_source")]
    pub source: String,
    pub message: String,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

fn default_level() -> String {
    "info".to_string()
}
fn default_source() -> String {
    "agent".to_string()
}

impl IngestEntry {
    /// Materialize into a stored record, assigning id/timestamp if absent.
    fn into_record(self) -> AuditRecord {
        AuditRecord {
            id: self
                .id
                .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
            timestamp: self
                .timestamp
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
            agent_id: self.agent_id,
            task_id: self.task_id,
            level: self.level,
            source: self.source,
            message: self.message,
            metadata: self.metadata,
        }
    }
}

/// Filters for a log query. `None` fields are unconstrained.
#[derive(Debug, Default, Clone)]
pub struct LogQuery {
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub limit: i64,
}

/// SQLite-backed persistence for operational events. Append-only in practice —
/// there is no update or delete path.
#[derive(Clone)]
pub struct AuditStore {
    pool: SqlitePool,
}

impl AuditStore {
    /// Open (creating if needed) the audit database at `url`, e.g.
    /// `sqlite://linexus-logger.sqlite`.
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let opts = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            // WAL keeps readers (log tails) from blocking writers (ingest).
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS audit_logs (
                seq       INTEGER PRIMARY KEY AUTOINCREMENT,
                id        TEXT NOT NULL UNIQUE,
                ts        TEXT NOT NULL,
                agent_id  TEXT,
                task_id   TEXT,
                level     TEXT NOT NULL,
                source    TEXT NOT NULL,
                message   TEXT NOT NULL,
                metadata  TEXT
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_audit_agent ON audit_logs(agent_id, seq)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_audit_task ON audit_logs(task_id, seq)")
            .execute(&pool)
            .await?;

        Ok(Self { pool })
    }

    /// Persist one submitted entry, returning the materialized record.
    pub async fn ingest(&self, entry: IngestEntry) -> anyhow::Result<AuditRecord> {
        let rec = entry.into_record();
        let metadata = rec
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m))
            .transpose()?;

        sqlx::query(
            "INSERT INTO audit_logs (id, ts, agent_id, task_id, level, source, message, metadata)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&rec.id)
        .bind(&rec.timestamp)
        .bind(&rec.agent_id)
        .bind(&rec.task_id)
        .bind(&rec.level)
        .bind(&rec.source)
        .bind(&rec.message)
        .bind(&metadata)
        .execute(&self.pool)
        .await?;

        Ok(rec)
    }

    /// Persist a batch, returning the materialized records in input order.
    pub async fn ingest_many(&self, entries: Vec<IngestEntry>) -> anyhow::Result<Vec<AuditRecord>> {
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            out.push(self.ingest(entry).await?);
        }
        Ok(out)
    }

    /// Query events most-recent-first, optionally filtered by agent and/or task.
    pub async fn query(&self, q: &LogQuery) -> anyhow::Result<Vec<AuditRecord>> {
        let mut sql = String::from(
            "SELECT id, ts, agent_id, task_id, level, source, message, metadata FROM audit_logs",
        );
        let mut conds: Vec<&str> = Vec::new();
        if q.agent_id.is_some() {
            conds.push("agent_id = ?");
        }
        if q.task_id.is_some() {
            conds.push("task_id = ?");
        }
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        sql.push_str(" ORDER BY seq DESC LIMIT ?");

        let mut query = sqlx::query(&sql);
        if let Some(a) = &q.agent_id {
            query = query.bind(a);
        }
        if let Some(t) = &q.task_id {
            query = query.bind(t);
        }
        let limit = q.limit.clamp(1, 1000);
        query = query.bind(limit);

        let rows = query.fetch_all(&self.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let metadata: Option<String> = row.try_get("metadata")?;
            let metadata = metadata
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .unwrap_or(None);
            out.push(AuditRecord {
                id: row.try_get("id")?,
                timestamp: row.try_get("ts")?,
                agent_id: row.try_get("agent_id")?,
                task_id: row.try_get("task_id")?,
                level: row.try_get("level")?,
                source: row.try_get("source")?,
                message: row.try_get("message")?,
                metadata,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(msg: &str, agent: Option<&str>) -> IngestEntry {
        IngestEntry {
            id: None,
            timestamp: None,
            agent_id: agent.map(String::from),
            task_id: None,
            level: "info".into(),
            source: "test".into(),
            message: msg.into(),
            metadata: None,
        }
    }

    #[tokio::test]
    async fn ingest_and_query_most_recent_first() {
        let store = AuditStore::connect("sqlite::memory:").await.unwrap();
        store.ingest(entry("first", Some("a1"))).await.unwrap();
        store.ingest(entry("second", Some("a1"))).await.unwrap();
        store.ingest(entry("other", Some("a2"))).await.unwrap();

        let a1 = store
            .query(&LogQuery {
                agent_id: Some("a1".into()),
                task_id: None,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(a1.len(), 2);
        assert_eq!(a1[0].message, "second"); // most recent first
        assert_eq!(a1[1].message, "first");

        let all = store
            .query(&LogQuery {
                agent_id: None,
                task_id: None,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn assigns_id_and_timestamp_when_absent() {
        let store = AuditStore::connect("sqlite::memory:").await.unwrap();
        let rec = store.ingest(entry("hi", None)).await.unwrap();
        assert!(!rec.id.is_empty());
        assert!(!rec.timestamp.is_empty());
    }

    #[tokio::test]
    async fn limit_is_respected() {
        let store = AuditStore::connect("sqlite::memory:").await.unwrap();
        for i in 0..5 {
            store
                .ingest(entry(&format!("m{i}"), Some("a1")))
                .await
                .unwrap();
        }
        let got = store
            .query(&LogQuery {
                agent_id: Some("a1".into()),
                task_id: None,
                limit: 3,
            })
            .await
            .unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].message, "m4");
    }
}
