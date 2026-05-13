//! Per-note observer that collects PSWAP-attachment notes for active
//! lineages during sync.
//!
//! See module-level docs on [`crate::pswap`] for the overall design.
//!
//! Skeleton only in this commit — the [`PswapChainObserver::observe`] body
//! is filled in by the follow-on commit that adds the
//! [`crate::sync::NoteObserver`] trait and the attachment-parsing helpers.
//! The type signatures and the per-sync collector contract are defined
//! here so dependent steps (the Store trait, lineage creation hook,
//! `Client::sync_state` wiring) can land independently and link.

use alloc::sync::Arc;
use alloc::vec::Vec;

use miden_protocol::Felt;
use miden_protocol::account::AccountId;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::NoteId;

use crate::store::Store;
use crate::utils::RwLock;

// PSWAP CHAIN NOTE UPDATE
// ================================================================================================

/// Sync-time observation of a note that may belong to a tracked PSWAP chain.
///
/// The observer pushes one of these per incoming `CommittedNote` whose
/// metadata carries a PSWAP attachment AND whose `order_id` matches an
/// active lineage in our store. By the time the post-sync correlator runs,
/// the collector contains only updates relevant to *our* lineages — at
/// most `2 * active_lineages` per round (one payback + one remainder
/// per chain) regardless of how many other PSWAP orders flew by on the
/// network.
///
/// Naming follows the codebase convention for sync-time observations
/// (`NoteUpdateTracker`, `NoteUpdateType`, `NoteUpdateAction`).
#[derive(Debug, Clone)]
pub struct PswapChainNoteUpdate {
    /// Note ID as observed in the sync response.
    pub note_id: NoteId,
    /// `attachment_word[1]` — stable across the whole chain. Matches
    /// `PswapLineageRecord::order_id()`.
    pub order_id: Felt,
    /// `attachment_word[2]` — the round counter stamped by the PSWAP
    /// script on every output note it emits.
    pub depth: u64,
    /// `attachment_word[0]` — `fill_amount` on a payback, `payout_amount`
    /// on a remainder. The correlator decides which role this note is
    /// playing via reconstruction (see `classify_by_reconstruction`).
    pub amount: u64,
    /// `metadata.sender()` — the account that consumed the previous tip
    /// and emitted this note as part of the fill (or reclaim) transaction.
    pub sender: AccountId,
    /// Block number the note was committed in.
    pub block: BlockNumber,
}

// PSWAP CHAIN OBSERVER
// ================================================================================================

/// Per-sync collector of PSWAP-attachment notes relevant to active
/// lineages on this client.
///
/// One instance is attached to `StateSync` per `sync_state` call via
/// `with_note_observer`. The collector vector is shared with the
/// post-sync correlator (`discover_pswap_rounds`), which drains it and
/// joins it with the consumed-nullifier signal from the same sync window.
///
/// Implementation lives in a follow-on commit on this branch.
#[allow(dead_code)] // Fields wired up by the follow-on commit that implements `observe`.
pub struct PswapChainObserver {
    /// Store handle used to look up active lineages by `order_id`. The
    /// observer filters at the source so the collector only ever holds
    /// updates we will actually correlate.
    pub(crate) store: Arc<dyn Store>,
    /// Per-sync shared collector. The observer write-locks to push;
    /// the correlator write-locks to drain. `RwLock` matches the
    /// codebase's existing sync abstraction (re-exported from `miden-tx`)
    /// and works no-std. Contention is non-existent in practice — the
    /// observer and correlator run sequentially on the same async task.
    pub(crate) chain_note_updates: Arc<RwLock<Vec<PswapChainNoteUpdate>>>,
}

impl PswapChainObserver {
    /// Builds an observer wired to the given store and collector. The
    /// caller (`Client::sync_state`) owns the collector so it can drain
    /// it after `StateSync::sync_state` returns.
    pub fn new(
        store: Arc<dyn Store>,
        chain_note_updates: Arc<RwLock<Vec<PswapChainNoteUpdate>>>,
    ) -> Self {
        Self { store, chain_note_updates }
    }
}
