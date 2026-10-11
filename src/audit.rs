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
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
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
            id: self.id.unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
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
    /// Exact `level` (`info` | `warn` | `error`).
    pub level: Option<String>,
    /// Exact `source`.
    pub source: Option<String>,
    /// RFC 3339 lower bound on the event timestamp (inclusive).
    pub since: Option<String>,
    /// RFC 3339 upper bound on the event timestamp (inclusive).
    pub until: Option<String>,
    /// Paging cursor: the id of the oldest record of the previous page; only
    /// records ingested before it are returned. An unknown id yields nothing.
    pub before: Option<String>,
    pub limit: i64,
}

/// SQLite-backed persistence for operational events. Append-only, except for
/// retention: [`AuditStore::prune_older_than`] drops events past their age.
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
            .map(serde_json::to_string)
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

    /// Persist a batch atomically — all of it or none of it, so a rejected
    /// entry (a duplicate id, say) never leaves half a task's trail behind.
    /// Returns the materialized records in input order.
    pub async fn ingest_many(&self, entries: Vec<IngestEntry>) -> anyhow::Result<Vec<AuditRecord>> {
        let mut tx = self.pool.begin().await?;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let rec = entry.into_record();
            let metadata = rec
                .metadata
                .as_ref()
                .map(serde_json::to_string)
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
            .execute(&mut *tx)
            .await?;
            out.push(rec);
        }
        tx.commit().await?;
        Ok(out)
    }

    /// Delete events older than `days` days, returning how many went. The
    /// timestamp is compared as a date (`datetime(ts)`), so entries stamped
    /// with any UTC offset age correctly.
    pub async fn prune_older_than(&self, days: u32) -> anyhow::Result<u64> {
        let cutoff = (chrono::Utc::now() - chrono::Duration::days(i64::from(days))).to_rfc3339();
        let res = sqlx::query("DELETE FROM audit_logs WHERE datetime(ts) < datetime(?)")
            .bind(&cutoff)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }

    /// Query events most-recent-first, filtered by whatever `q` constrains.
    pub async fn query(&self, q: &LogQuery) -> anyhow::Result<Vec<AuditRecord>> {
        let mut sql = String::from(
            "SELECT id, ts, agent_id, task_id, level, source, message, metadata FROM audit_logs",
        );
        // Conditions and their bind values, in order.
        let mut conds: Vec<&str> = Vec::new();
        let mut binds: Vec<&String> = Vec::new();
        for (cond, value) in [
            ("agent_id = ?", &q.agent_id),
            ("task_id = ?", &q.task_id),
            ("level = ?", &q.level),
            ("source = ?", &q.source),
            ("datetime(ts) >= datetime(?)", &q.since),
            ("datetime(ts) <= datetime(?)", &q.until),
            ("seq < (SELECT seq FROM audit_logs WHERE id = ?)", &q.before),
        ] {
            if let Some(v) = value {
                conds.push(cond);
                binds.push(v);
            }
        }
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        sql.push_str(" ORDER BY seq DESC LIMIT ?");

        let mut query = sqlx::query(&sql);
        for v in binds {
            query = query.bind(v);
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
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(a1.len(), 2);
        assert_eq!(a1[0].message, "second"); // most recent first
        assert_eq!(a1[1].message, "first");

        let all = store
            .query(&LogQuery {
                limit: 10,
                ..Default::default()
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
                limit: 3,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].message, "m4");
    }

    fn stamped(msg: &str, ts: &str, level: &str, source: &str) -> IngestEntry {
        IngestEntry {
            timestamp: Some(ts.into()),
            level: level.into(),
            source: source.into(),
            ..entry(msg, Some("a1"))
        }
    }

    #[tokio::test]
    async fn filters_by_level_source_and_time_window() {
        let store = AuditStore::connect("sqlite::memory:").await.unwrap();
        store
            .ingest_many(vec![
                stamped("old", "2026-01-01T00:00:00Z", "info", "agent"),
                stamped("mid", "2026-06-01T12:00:00+02:00", "warn", "nexus"),
                stamped(
                    "new",
                    "2026-10-01T00:00:00.123456789+00:00",
                    "error",
                    "agent",
                ),
            ])
            .await
            .unwrap();

        let q = |f: &dyn Fn(&mut LogQuery)| {
            let mut q = LogQuery {
                limit: 10,
                ..Default::default()
            };
            f(&mut q);
            q
        };
        let msgs = |recs: Vec<AuditRecord>| recs.into_iter().map(|r| r.message).collect::<Vec<_>>();

        let got = store
            .query(&q(&|q| q.level = Some("warn".into())))
            .await
            .unwrap();
        assert_eq!(msgs(got), ["mid"]);
        let got = store
            .query(&q(&|q| q.source = Some("agent".into())))
            .await
            .unwrap();
        assert_eq!(msgs(got), ["new", "old"]);
        // `since` is inclusive and offset-aware: 10:00Z is the same instant
        // as the "mid" entry's 12:00+02:00.
        let got = store
            .query(&q(&|q| q.since = Some("2026-06-01T10:00:00Z".into())))
            .await
            .unwrap();
        assert_eq!(msgs(got), ["new", "mid"]);
        let got = store
            .query(&q(&|q| {
                q.since = Some("2026-02-01T00:00:00Z".into());
                q.until = Some("2026-09-01T00:00:00Z".into());
            }))
            .await
            .unwrap();
        assert_eq!(msgs(got), ["mid"]);
        // Nanosecond timestamps (what chrono stamps) compare too.
        let got = store
            .query(&q(&|q| q.since = Some("2026-10-01T00:00:00Z".into())))
            .await
            .unwrap();
        assert_eq!(msgs(got), ["new"]);
    }

    #[tokio::test]
    async fn pages_with_a_before_cursor() {
        let store = AuditStore::connect("sqlite::memory:").await.unwrap();
        for i in 0..5 {
            store
                .ingest(entry(&format!("m{i}"), Some("a1")))
                .await
                .unwrap();
        }
        let page1 = store
            .query(&LogQuery {
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page1.iter().map(|r| r.message.as_str()).collect::<Vec<_>>(),
            ["m4", "m3"]
        );
        let page2 = store
            .query(&LogQuery {
                limit: 2,
                before: Some(page1[1].id.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page2.iter().map(|r| r.message.as_str()).collect::<Vec<_>>(),
            ["m2", "m1"]
        );
        let page3 = store
            .query(&LogQuery {
                limit: 2,
                before: Some(page2[1].id.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page3.len(), 1);
        assert_eq!(page3[0].message, "m0");
        // An unknown cursor returns nothing rather than everything.
        let none = store
            .query(&LogQuery {
                limit: 2,
                before: Some("nope".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn ingest_many_is_all_or_nothing() {
        let store = AuditStore::connect("sqlite::memory:").await.unwrap();
        let dup = IngestEntry {
            id: Some("same-id".into()),
            ..entry("first", None)
        };
        let again = IngestEntry {
            id: Some("same-id".into()),
            ..entry("second", None)
        };
        assert!(
            store
                .ingest_many(vec![entry("ok", None), dup, again])
                .await
                .is_err()
        );
        let all = store
            .query(&LogQuery {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            all.is_empty(),
            "a failed batch must leave nothing behind: {all:?}"
        );
    }

    #[tokio::test]
    async fn prune_drops_only_old_events() {
        let store = AuditStore::connect("sqlite::memory:").await.unwrap();
        let old = (chrono::Utc::now() - chrono::Duration::days(100)).to_rfc3339();
        let recent = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
        store
            .ingest_many(vec![
                stamped("old", &old, "info", "agent"),
                stamped("recent", &recent, "info", "agent"),
                entry("now", None),
            ])
            .await
            .unwrap();
        assert_eq!(store.prune_older_than(90).await.unwrap(), 1);
        let left = store
            .query(&LogQuery {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(left.len(), 2);
        assert!(left.iter().all(|r| r.message != "old"));
        assert_eq!(store.prune_older_than(90).await.unwrap(), 0);
    }
}
