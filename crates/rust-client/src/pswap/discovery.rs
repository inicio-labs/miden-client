//! Post-sync correlator that joins consumed-nullifier events with the
//! PSWAP-attachment notes collected by [`super::observer::PswapChainObserver`]
//! and emits [`super::lineage::PswapLineageRoundUpdate`] entries describing
//! each round transition.
//!
//! See module-level docs on [`crate::pswap`] for the overall design and
//! the `pswap_creator_reconstructs_lineage_from_attachments` test in the
//! protocol repo (`crates/miden-testing/tests/scripts/pswap.rs`) for the
//! executable contract this correlator implements at runtime.
//!
//! Skeleton only in this commit — the body of [`discover_pswap_rounds`]
//! is added by the follow-on commit that also lands the
//! `current_window_nullifier_blocks` field on `StateSyncUpdate`.

use alloc::vec::Vec;

use crate::ClientError;
use crate::sync::StateSyncUpdate;

use super::lineage::PswapLineageRoundUpdate;
use super::observer::PswapChainNoteUpdate;

/// Joins the post-sync state update with the per-sync PSWAP chain-note
/// collector and returns one [`PswapLineageRoundUpdate`] per advanced
/// round.
///
/// Walks each active lineage; for each whose `current_tip_nullifier`
/// appears in the sync's nullifier window, looks up the round's `(payback,
/// remainder)` candidate pair in the collector (indexed by
/// `(order_id, depth)`), classifies them via reconstruction
/// (`PswapNote::payback_note` / `remainder_note`), and builds a round
/// update. Loops on the new tip to catch same-block multi-fill.
///
/// The store applies each returned round update inside its own SQL
/// transaction via [`crate::store::Store::apply_pswap_round`].
///
/// Empty `chain_note_updates` is the steady-state happy path (no PSWAP
/// activity this sync) and returns `Ok(vec![])` after one cheap store
/// query.
pub async fn discover_pswap_rounds(
    _state_sync_update: &StateSyncUpdate,
    _chain_note_updates: &[PswapChainNoteUpdate],
) -> Result<Vec<PswapLineageRoundUpdate>, ClientError> {
    // Filled in by the follow-on commit that wires StateSync's nullifier
    // window field. Returning an empty vec keeps the call site type-clean
    // while the correlator body lands.
    Ok(Vec::new())
}
