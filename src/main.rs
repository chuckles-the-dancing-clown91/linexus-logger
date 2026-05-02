//! # Linexus Logger — The Immutable Audit Trail

mod segmented_log;
mod decay;

use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_target(true).init();
    tracing::info!("=== LINEXUS LOGGER — Immutable Audit Trail ===");
    tracing::info!("Segmented Log: ACTIVE");
    tracing::info!("Demiurge Decay Sweeper: ARMED");

    use linexus_core::demiurge::{LedgerEntry, ValueClass, EPOCH_DECAY_SECS};
    let entry = LedgerEntry::mint(uuid::Uuid::new_v4(), None, None, ValueClass::Pranjurity(100), 1_700_000_000, "system_init".into());
    tracing::info!("Minted test entry, expires in {} secs", entry.remaining_ttl(1_700_000_000));
    assert_eq!(entry.remaining_ttl(1_700_000_000), EPOCH_DECAY_SECS);
}
