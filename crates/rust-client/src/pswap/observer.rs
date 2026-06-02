//! Per-note observer that collects PSWAP-attachment notes for active
//! lineages during sync.
//!
//! See module-level docs on [`crate::pswap`] for the overall design.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;

use async_trait::async_trait;
use miden_protocol::Felt;
use miden_protocol::account::AccountId;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{NoteId, NoteInclusionProof};
use miden_standards::note::PswapNote;

use crate::ClientError;
use crate::pswap::PswapLineageState;
use crate::rpc::domain::note::CommittedNote;
use crate::store::Store;
use crate::sync::NoteObserver;
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
    /// Inclusion proof for the note in the block. Captured here so the
    /// post-sync correlator can hand it to the store when inserting
    /// the reconstructed payback into `input_notes` — without it, the
    /// payback would land in `Expected` state with no way to advance
    /// to `Committed` (the default `NoteScreener` Discards private
    /// notes it does not already track, so the inclusion proof would
    /// never reach the screener's state-promotion path).
    pub inclusion_proof: NoteInclusionProof,
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
pub struct PswapChainObserver {
    /// Store handle used to look up active lineages by `order_id`. The
    /// observer filters at the source so the collector only ever holds
    /// updates we will actually correlate.
    store: Arc<dyn Store>,
    /// Per-sync shared collector. The observer write-locks to push;
    /// the correlator write-locks to drain. The access pattern is
    /// write-only on both sides — `RwLock` here is the no-std-friendly
    /// lock re-exported from `miden_tx::utils::sync` (the only one
    /// available), used effectively as a `Mutex<Vec<_>>`. Contention is
    /// non-existent in practice: the observer runs inside
    /// `StateSync::sync_state` and the correlator runs in
    /// `Client::sync_state` after `StateSync::sync_state` returns, on
    /// the same async task.
    chain_note_updates: Arc<RwLock<Vec<PswapChainNoteUpdate>>>,
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

#[async_trait(?Send)]
impl NoteObserver for PswapChainObserver {
    fn name(&self) -> &'static str {
        "PswapChainObserver"
    }

    async fn observe(&self, committed_note: &CommittedNote) -> Result<(), ClientError> {
        // 1. Read the PSWAP attachment word, if present. Notes without
        //    a PSWAP attachment are the vast majority of sync arrivals;
        //    fast-rejecting them here keeps the observer cheap.
        let Some((order_id, depth, amount)) = pswap_attachment_fields(committed_note) else {
            return Ok(());
        };

        // 2. Does this `order_id` match an active lineage on this
        //    client? One indexed store lookup. Foreign creators' orders,
        //    stale finished orders, and asset-pair noise are all
        //    filtered out here so the collector stays small.
        let lineage = match self.store.get_pswap_lineage(order_id).await? {
            Some(lineage) => lineage,
            None => return Ok(()),
        };
        if lineage.state != PswapLineageState::Active {
            return Ok(());
        }

        // 3. Forward-only depth filter. A round update for a depth we
        //    have already processed is either a duplicate delivery or a
        //    re-org; either way the correlator's existing fail-loud
        //    depth check will catch it, but skipping the collect here
        //    avoids polluting the per-sync collector with noise.
        if depth < lineage.current_depth + 1 {
            return Ok(());
        }

        // 4. Record. The correlator drains this after
        //    `StateSync::sync_state` returns and decides per-note role
        //    (payback vs remainder) via reconstruction.
        let inclusion_proof = committed_note.inclusion_proof().clone();
        let update = PswapChainNoteUpdate {
            note_id: *committed_note.note_id(),
            order_id,
            depth,
            amount,
            sender: committed_note.sender(),
            block: inclusion_proof.location().block_num(),
            inclusion_proof,
        };

        self.chain_note_updates.write().push(update);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------

/// Reads the PSWAP attachment word from a [`CommittedNote`] and returns
/// `Some((order_id, depth, amount))` if found. Returns `None` for any of:
///
/// - the note has not had its metadata upgraded yet (still in `Header`
///   state),
/// - the note has no PSWAP-scheme attachment, or
/// - the attachment exists but has no content words (which would be a
///   protocol-invariant violation — treated as "skip" rather than error
///   since the observer is fail-open).
fn pswap_attachment_fields(committed_note: &CommittedNote) -> Option<(Felt, u64, u64)> {
    // TEMP-PROTOCOL-ADAPTER: returns `None` for every PSWAP note. The
    // wire format already carries attachment content for private notes,
    // but the in-tree `CommittedNote` doesn't expose it — so we can
    // detect PSWAP-scheme attachments via metadata headers but can't
    // read the content word `[amount, order_id, depth, 0]`. Private-
    // note PSWAP chain tracking is a no-op until upstream extends
    // `CommittedNote`/`FetchedNote::Private` with a `NoteAttachments`
    // field (~15 LOC in `rpc/domain/note.rs`).
    let headers = committed_note.metadata().attachment_headers();
    let has_pswap = headers
        .iter()
        .any(|h| h.scheme() == Some(PswapNote::PSWAP_ATTACHMENT_SCHEME));
    if !has_pswap {
        return None;
    }

    // We *know* this is a PSWAP note (header scheme matches), but we
    // can't read the content word from sync data alone. Skip silently;
    // the post-sync correlator will not see a `PswapChainNoteUpdate`
    // for this note and the lineage will stall at the previous tip.
    None
}
