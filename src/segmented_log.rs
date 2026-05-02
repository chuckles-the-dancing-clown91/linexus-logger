//! Segmented Log — Append-only immutable log for Demiurge transactions.
use linexus_core::demiurge::LedgerEntry;
use std::sync::Mutex;

pub struct LogSegment { pub segment_id: u64, pub entries: Vec<LedgerEntry>, pub max_entries: usize }

impl LogSegment {
    pub fn new(segment_id: u64, max_entries: usize) -> Self { Self { segment_id, entries: Vec::with_capacity(max_entries), max_entries } }
    pub fn is_full(&self) -> bool { self.entries.len() >= self.max_entries }
    pub fn append(&mut self, entry: LedgerEntry) -> bool { if self.is_full() { return false; } self.entries.push(entry); true }
}

pub struct SegmentedLog { segments: Mutex<Vec<LogSegment>>, entries_per_segment: usize, next_segment_id: Mutex<u64> }

impl SegmentedLog {
    pub fn new(entries_per_segment: usize) -> Self {
        Self { segments: Mutex::new(vec![LogSegment::new(0, entries_per_segment)]), entries_per_segment, next_segment_id: Mutex::new(1) }
    }

    pub fn append(&self, entry: LedgerEntry) {
        let mut segments = self.segments.lock().unwrap();
        if let Some(current) = segments.last_mut() {
            if current.append(entry.clone()) { return; }
        }
        let mut next_id = self.next_segment_id.lock().unwrap();
        let mut new_segment = LogSegment::new(*next_id, self.entries_per_segment);
        new_segment.append(entry);
        segments.push(new_segment);
        *next_id += 1;
    }

    pub fn total_entries(&self) -> usize { self.segments.lock().unwrap().iter().map(|s| s.entries.len()).sum() }
    pub fn segment_count(&self) -> usize { self.segments.lock().unwrap().len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linexus_core::demiurge::ValueClass;
    use uuid::Uuid;

    fn make_entry() -> LedgerEntry {
        LedgerEntry::mint(Uuid::new_v4(), None, None, ValueClass::Pranjurity(10), 1_700_000_000, "test".into())
    }

    #[test] fn log_appends_and_counts() {
        let log = SegmentedLog::new(100);
        log.append(make_entry()); log.append(make_entry());
        assert_eq!(log.total_entries(), 2);
        assert_eq!(log.segment_count(), 1);
    }

    #[test] fn log_creates_new_segment_when_full() {
        let log = SegmentedLog::new(2);
        log.append(make_entry()); log.append(make_entry());
        assert_eq!(log.segment_count(), 1);
        log.append(make_entry());
        assert_eq!(log.segment_count(), 2);
        assert_eq!(log.total_entries(), 3);
    }
}
