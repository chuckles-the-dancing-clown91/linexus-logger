//! Operational log — the append-only audit trail behind the Logger's HTTP
//! surface.
//!
//! Entries land in JSONL segment files on disk (`segment-000042.jsonl`), one
//! JSON object per line, rotated by entry count. A segment is never rewritten:
//! append is the only mutation, and retention removes whole expired segments
//! rather than editing them — an audit trail you can edit is a notebook, not
//! an audit trail.
//!
//! Queries are served from a bounded in-memory index rebuilt from the segment
//! files at boot, so a restart loses nothing and the query path never touches
//! disk. The index holds the newest `max_index_entries`; older history stays
//! on disk for offline forensics.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One operational log entry. `agent_id`/`task_id` are the correlation keys:
/// the same task id appears in the Orchestrator's plan, the agent's journal,
/// and Daedalus IT's TaskRun, so one grep follows an action end to end.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpEntry {
    /// Stamped by the Logger at ingest unless the shipper supplied one —
    /// the agent's clock is authoritative for events it observed.
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub source: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Filters for a query. Empty filters match everything.
#[derive(Debug, Default, Clone)]
pub struct Query {
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub level: Option<String>,
    pub limit: usize,
}

/// The store. Wrap it in a `Mutex` for shared use — appends are short and the
/// call rate here is log-shipping, not request serving.
pub struct OpLog {
    dir: PathBuf,
    entries_per_segment: usize,
    max_index_entries: usize,
    /// Newest-last ring of recent entries (the query working set).
    index: VecDeque<OpEntry>,
    /// Open handle for the active segment.
    active: File,
    active_id: u64,
    active_len: usize,
    total_appended: u64,
}

impl OpLog {
    /// Open (or create) the log in `dir`, replaying existing segments into the
    /// in-memory index so a restart picks up exactly where it stopped.
    pub fn open(
        dir: impl AsRef<Path>,
        entries_per_segment: usize,
        max_index_entries: usize,
    ) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut segment_ids = list_segments(&dir)?;
        segment_ids.sort_unstable();

        // Replay newest segments first, but only as many entries as the index
        // holds — a years-old log must not make boot O(everything).
        let mut index: VecDeque<OpEntry> = VecDeque::new();
        for id in segment_ids.iter().rev() {
            if index.len() >= max_index_entries {
                break;
            }
            let mut seg = read_segment(&segment_path(&dir, *id))?;
            seg.extend(index.drain(..));
            index = seg.into();
            while index.len() > max_index_entries {
                index.pop_front();
            }
        }

        let (active_id, active_len) = match segment_ids.last() {
            Some(&last) => {
                let len = count_lines(&segment_path(&dir, last))?;
                if len >= entries_per_segment {
                    (last + 1, 0)
                } else {
                    (last, len)
                }
            }
            None => (0, 0),
        };

        let active = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&dir, active_id))?;

        Ok(Self {
            dir,
            entries_per_segment,
            max_index_entries,
            index,
            active,
            active_id,
            active_len,
            total_appended: 0,
        })
    }

    /// Append one entry: write-through to the active segment, then index it.
    /// Disk first — an entry that is queryable but would vanish on restart is
    /// a lie about durability.
    pub fn append(&mut self, entry: OpEntry) -> std::io::Result<()> {
        if self.active_len >= self.entries_per_segment {
            self.rotate()?;
        }
        let mut line = serde_json::to_string(&entry).map_err(std::io::Error::other)?;
        line.push('\n');
        self.active.write_all(line.as_bytes())?;
        self.active.flush()?;
        self.active_len += 1;
        self.total_appended += 1;

        self.index.push_back(entry);
        while self.index.len() > self.max_index_entries {
            self.index.pop_front();
        }
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.active_id += 1;
        self.active_len = 0;
        self.active = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&self.dir, self.active_id))?;
        Ok(())
    }

    /// Most-recent-first entries matching the query.
    pub fn query(&self, q: &Query) -> Vec<OpEntry> {
        let limit = if q.limit == 0 { 100 } else { q.limit };
        self.index
            .iter()
            .rev()
            .filter(|e| {
                q.agent_id
                    .as_deref()
                    .is_none_or(|want| e.agent_id.as_deref() == Some(want))
                    && q.task_id
                        .as_deref()
                        .is_none_or(|want| e.task_id.as_deref() == Some(want))
                    && q.level
                        .as_deref()
                        .is_none_or(|want| e.level.eq_ignore_ascii_case(want))
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// Drop whole segments whose entries are all older than `cutoff`. The
    /// active segment is never touched; immutability means retention deletes
    /// files, it does not edit them. Returns how many segments were removed.
    pub fn sweep_expired(&mut self, cutoff: DateTime<Utc>) -> std::io::Result<usize> {
        let mut ids = list_segments(&self.dir)?;
        ids.sort_unstable();
        let mut removed = 0;
        for id in ids {
            if id == self.active_id {
                continue;
            }
            let path = segment_path(&self.dir, id);
            let entries = read_segment(&path)?;
            let all_expired = !entries.is_empty() && entries.iter().all(|e| e.timestamp < cutoff);
            if all_expired || entries.is_empty() {
                fs::remove_file(&path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Counters for the health endpoint.
    pub fn stats(&self) -> Stats {
        Stats {
            indexed_entries: self.index.len(),
            appended_since_boot: self.total_appended,
            active_segment: self.active_id,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Stats {
    pub indexed_entries: usize,
    pub appended_since_boot: u64,
    pub active_segment: u64,
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("segment-{id:06}.jsonl"))
}

fn list_segments(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".jsonl"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            out.push(id);
        }
    }
    Ok(out)
}

/// Read a segment, skipping unparseable lines rather than refusing the whole
/// file — one torn write during a crash must not take the audit trail's
/// remaining history offline.
fn read_segment(path: &Path) -> std::io::Result<Vec<OpEntry>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<OpEntry>(&line) {
            Ok(e) => out.push(e),
            Err(err) => tracing::warn!(%err, "skipping unparseable log line"),
        }
    }
    Ok(out)
}

fn count_lines(path: &Path) -> std::io::Result<usize> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    Ok(BufReader::new(file).lines().count())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(msg: &str, agent: Option<&str>, task: Option<&str>) -> OpEntry {
        OpEntry {
            timestamp: Utc::now(),
            level: "info".into(),
            source: "test".into(),
            message: msg.into(),
            agent_id: agent.map(String::from),
            task_id: task.map(String::from),
            metadata: None,
        }
    }

    fn tmpdir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oplog-test-{name}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_and_query_most_recent_first() {
        let dir = tmpdir("basic");
        let mut log = OpLog::open(&dir, 100, 1000).unwrap();
        log.append(entry("first", Some("a1"), None)).unwrap();
        log.append(entry("second", Some("a1"), None)).unwrap();
        log.append(entry("other agent", Some("a2"), None)).unwrap();

        let got = log.query(&Query {
            agent_id: Some("a1".into()),
            limit: 10,
            ..Default::default()
        });
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].message, "second", "newest must come first");
        assert_eq!(got[1].message, "first");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn rotation_by_entry_count() {
        let dir = tmpdir("rotate");
        let mut log = OpLog::open(&dir, 2, 1000).unwrap();
        for i in 0..5 {
            log.append(entry(&format!("m{i}"), None, None)).unwrap();
        }
        let mut ids = list_segments(&dir).unwrap();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1, 2], "5 entries at 2/segment = 3 segments");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn restart_replays_from_disk() {
        let dir = tmpdir("replay");
        {
            let mut log = OpLog::open(&dir, 2, 1000).unwrap();
            for i in 0..5 {
                log.append(entry(&format!("m{i}"), Some("a1"), None)).unwrap();
            }
        } // dropped: simulates a process exit

        let log = OpLog::open(&dir, 2, 1000).unwrap();
        let got = log.query(&Query {
            limit: 100,
            ..Default::default()
        });
        assert_eq!(got.len(), 5, "everything must survive a restart");
        assert_eq!(got[0].message, "m4", "order must survive the replay");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn restart_resumes_the_partial_segment() {
        let dir = tmpdir("resume");
        {
            let mut log = OpLog::open(&dir, 10, 1000).unwrap();
            log.append(entry("only", None, None)).unwrap();
        }
        {
            let mut log = OpLog::open(&dir, 10, 1000).unwrap();
            log.append(entry("second", None, None)).unwrap();
        }
        let mut ids = list_segments(&dir).unwrap();
        ids.sort_unstable();
        assert_eq!(ids, vec![0], "a partial segment is resumed, not abandoned");
        assert_eq!(count_lines(&segment_path(&dir, 0)).unwrap(), 2);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn index_is_bounded_but_disk_is_not() {
        let dir = tmpdir("bounded");
        let mut log = OpLog::open(&dir, 10, 3).unwrap();
        for i in 0..8 {
            log.append(entry(&format!("m{i}"), None, None)).unwrap();
        }
        let got = log.query(&Query {
            limit: 100,
            ..Default::default()
        });
        assert_eq!(got.len(), 3, "index holds only the newest N");
        assert_eq!(got[0].message, "m7");
        // ...but every entry is still on disk.
        let on_disk: usize = list_segments(&dir)
            .unwrap()
            .iter()
            .map(|id| count_lines(&segment_path(&dir, *id)).unwrap())
            .sum();
        assert_eq!(on_disk, 8);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn task_filter_and_level_filter() {
        let dir = tmpdir("filters");
        let mut log = OpLog::open(&dir, 100, 1000).unwrap();
        log.append(entry("planned", None, Some("t1"))).unwrap();
        let mut failed = entry("failed", None, Some("t1"));
        failed.level = "error".into();
        log.append(failed).unwrap();
        log.append(entry("unrelated", None, Some("t2"))).unwrap();

        let by_task = log.query(&Query {
            task_id: Some("t1".into()),
            limit: 10,
            ..Default::default()
        });
        assert_eq!(by_task.len(), 2);

        let errors = log.query(&Query {
            task_id: Some("t1".into()),
            level: Some("ERROR".into()),
            limit: 10,
            ..Default::default()
        });
        assert_eq!(errors.len(), 1, "level filter is case-insensitive");
        assert_eq!(errors[0].message, "failed");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn retention_drops_whole_expired_segments_only() {
        let dir = tmpdir("retention");
        let mut log = OpLog::open(&dir, 2, 1000).unwrap();
        let old = Utc::now() - chrono::Duration::days(90);
        for i in 0..4 {
            let mut e = entry(&format!("old{i}"), None, None);
            e.timestamp = old;
            log.append(e).unwrap();
        }
        log.append(entry("fresh", None, None)).unwrap();

        let removed = log
            .sweep_expired(Utc::now() - chrono::Duration::days(30))
            .unwrap();
        assert_eq!(removed, 2, "the two fully-old segments go");
        let survivors = list_segments(&dir).unwrap();
        assert!(
            survivors.contains(&log.active_id),
            "the active segment is never touched"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn torn_line_does_not_poison_replay() {
        let dir = tmpdir("torn");
        {
            let mut log = OpLog::open(&dir, 100, 1000).unwrap();
            log.append(entry("good", None, None)).unwrap();
        }
        // Simulate a crash mid-write: garbage on the end of the segment.
        let path = segment_path(&dir, 0);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"{\"timestamp\": \"not json").unwrap();
        f.write_all(b"\n").unwrap();
        drop(f);

        let log = OpLog::open(&dir, 100, 1000).unwrap();
        let got = log.query(&Query {
            limit: 10,
            ..Default::default()
        });
        assert_eq!(got.len(), 1, "the good entry survives the torn one");
        fs::remove_dir_all(dir).ok();
    }
}
