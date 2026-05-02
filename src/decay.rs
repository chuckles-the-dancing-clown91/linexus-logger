//! Decay Sweeper — returns expired Demiurge to the Vicinagora Commons.
use linexus_core::demiurge::LedgerEntry;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DecaySweepResult { pub decayed_count: u64, pub total_swept: u64, pub sweep_timestamp: u64 }

pub fn sweep_decayed_entries(entries: &[LedgerEntry]) -> DecaySweepResult {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut decayed_count = 0u64;
    let mut total_swept = 0u64;
    for entry in entries {
        if entry.is_decayed(now) { decayed_count += 1; total_swept += entry.value.amount(); }
    }
    DecaySweepResult { decayed_count, total_swept, sweep_timestamp: now }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linexus_core::demiurge::ValueClass;
    use uuid::Uuid;

    #[test] fn fresh_entries_not_swept() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let entry = LedgerEntry::mint(Uuid::new_v4(), None, None, ValueClass::Pranjurity(100), now, "t".into());
        assert_eq!(sweep_decayed_entries(&[entry]).decayed_count, 0);
    }

    #[test] fn old_entries_swept() {
        let entry = LedgerEntry::mint(Uuid::new_v4(), None, None, ValueClass::Supranjus(50), 1_000_000_000, "t".into());
        let result = sweep_decayed_entries(&[entry]);
        assert_eq!(result.decayed_count, 1);
        assert_eq!(result.total_swept, 50);
    }
}
