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
use miden_protocol::asset::AssetAmount;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{NoteId, NoteInclusionProof, NoteTag};
use miden_standards::note::PswapNote;
use tracing::warn;

use crate::ClientError;
use crate::pswap::PswapLineageState;
use crate::rpc::NodeRpcClient;
use crate::rpc::domain::note::{CommittedNote, FetchedNote};
use crate::store::Store;
use crate::sync::NoteObserver;
use crate::utils::RwLock;

// PSWAP CHAIN NOTE UPDATE
// ================================================================================================

/// Sync-time observation of a note carrying a PSWAP attachment, scoped to
/// an active lineage on this client.
///
/// Produced by the observer's post-sync `apply()` hook (not directly by
/// `observe()`), once the per-sync `GetNotesById` fetch has resolved the
/// attachment content for each candidate.
#[derive(Debug, Clone)]
pub struct PswapChainNoteUpdate {
    pub note_id: NoteId,
    /// `attachment_word[1]` — stable across the chain, matches
    /// `PswapLineageRecord::order_id()`.
    pub order_id: Felt,
    /// `attachment_word[2]` — round counter stamped by the PSWAP script.
    pub depth: u32,
    /// `attachment_word[0]` — `fill_amount` on a payback, `payout_amount`
    /// on a remainder. Role is distinguished by [`Self::tag`].
    pub amount: AssetAmount,
    /// `metadata.sender()` — the account that consumed the previous tip.
    pub sender: AccountId,
    /// `metadata.tag()` — distinguishes payback (P2ID-style tag) from
    /// remainder (asset-pair Subscription tag) without reconstruction
    /// guesswork. Verified against the lineage's asset-pair tag in the
    /// correlator.
    pub tag: NoteTag,
    /// Block in which the note was committed.
    pub block_num: BlockNumber,
    /// Inclusion proof, captured here so `apply_pswap_round` can insert
    /// the reconstructed note in `Unverified` state (carries the proof,
    /// promoted to `Committed` by the next sync's state-promotion path).
    pub inclusion_proof: NoteInclusionProof,
}

// PENDING PSWAP NOTE
// ================================================================================================

/// Per-sync entry written by `observe()` and consumed by `apply()`.
///
/// Carries the metadata-derivable fields we learn during sync (note_id,
/// sender, tag, inclusion_proof) but NOT the attachment content word
/// `[amount, order_id, depth, 0]`. The latter requires a follow-up
/// `GetNotesById` fetch — done in `apply()`. See the module-level TEMP
/// note on the `PswapChainObserver::apply` doc for the upstream PR that
/// will let us read attachments inline.
#[derive(Debug, Clone)]
struct PendingPswapNote {
    note_id: NoteId,
    sender: AccountId,
    tag: NoteTag,
    block_num: BlockNumber,
    inclusion_proof: NoteInclusionProof,
}

// PSWAP CHAIN OBSERVER
// ================================================================================================

/// Per-sync collector of PSWAP-attachment notes relevant to active
/// lineages on this client.
///
/// One instance is attached to `StateSync` per `sync_state` call via
/// `with_note_observer`. Two phases:
/// - **`observe()`** — runs per-note during sync. Cheap filter: detects
///   PSWAP-scheme attachment via metadata headers and queues a pending
///   record. NO store query, NO RPC call, NO DB write.
/// - **`apply()`** — runs once post-sync. Batches one `GetNotesById`
///   call to resolve attachment content for all pending records, builds
///   [`PswapChainNoteUpdate`]s, filters to our active lineages, runs the
///   correlator, and applies each emitted round update.
pub struct PswapChainObserver {
    /// Store handle for active-lineage lookups + round-update writes.
    store: Arc<dyn Store>,
    /// RPC handle used in `apply()` to fetch attachment content via
    /// `GetNotesById`. **TEMP**: once
    /// [`#2214`](https://github.com/0xMiden/miden-client/pull/2214) lands
    /// upstream, sync itself will populate attachments for private notes
    /// (carried alongside the `StateSyncUpdate`); the observer can drop
    /// this field and read attachments inline from `observe()`'s
    /// `CommittedNote` instead.
    rpc: Arc<dyn NodeRpcClient>,
    /// Per-sync pending-fetch queue. Populated by `observe()`, drained
    /// by `apply()`. `RwLock` here is the no-std-friendly lock from
    /// `crate::utils`; used effectively as a `Mutex<Vec<_>>` because
    /// `observe()` and `apply()` never overlap.
    pending_pswap_notes: Arc<RwLock<Vec<PendingPswapNote>>>,
}

impl PswapChainObserver {
    /// Builds an observer wired to the given store + RPC handle. Both
    /// per-sync collectors are private to the observer.
    pub fn new(store: Arc<dyn Store>, rpc: Arc<dyn NodeRpcClient>) -> Self {
        Self {
            store,
            rpc,
            pending_pswap_notes: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait(?Send)]
impl NoteObserver for PswapChainObserver {
    fn name(&self) -> &'static str {
        "PswapChainObserver"
    }

    async fn observe(&self, committed_note: &CommittedNote) -> Result<(), ClientError> {
        // Cheap filter: detect a PSWAP-scheme attachment header via metadata.
        // The actual attachment content word `[amount, order_id, depth, 0]`
        // is NOT in the sync stream — fetched in `apply()` via GetNotesById.
        let has_pswap_attachment = committed_note
            .metadata()
            .attachment_headers()
            .iter()
            .any(|h| h.scheme() == Some(PswapNote::PSWAP_ATTACHMENT_SCHEME));
        if !has_pswap_attachment {
            return Ok(());
        }

        let inclusion_proof = committed_note.inclusion_proof().clone();
        self.pending_pswap_notes.write().push(PendingPswapNote {
            note_id: *committed_note.note_id(),
            sender: committed_note.sender(),
            tag: committed_note.metadata().tag(),
            block_num: inclusion_proof.location().block_num(),
            inclusion_proof,
        });
        Ok(())
    }

    /// Resolves attachment content for queued pending notes via one batched
    /// `GetNotesById` call, builds [`PswapChainNoteUpdate`]s, filters to
    /// active lineages on this client, runs `discover_pswap_rounds`, and
    /// applies each emitted round update.
    ///
    /// Per-round `apply_pswap_round` failures are logged but do not abort
    /// remaining rounds — one corrupted lineage cannot stall the rest of
    /// the wallet's PSWAP progress.
    async fn apply(
        &self,
        sync_update: &crate::sync::StateSyncUpdate,
    ) -> Result<(), ClientError> {
        let pending = core::mem::take(&mut *self.pending_pswap_notes.write());

        // Fast path: no PSWAP activity AND no consumed nullifiers — nothing
        // for the correlator to do.
        if pending.is_empty() && sync_update.current_window_nullifier_blocks.is_empty() {
            return Ok(());
        }

        let chain_note_updates = if pending.is_empty() {
            Vec::new()
        } else {
            self.build_chain_note_updates(pending).await?
        };

        let round_updates = crate::pswap::discovery::discover_pswap_rounds(
            self.store.clone(),
            sync_update,
            &chain_note_updates,
        )
        .await?;

        for round_update in round_updates {
            if let Err(err) = self.store.apply_pswap_round(&round_update).await {
                warn!(
                    order_id = round_update.order_id.as_canonical_u64(),
                    round_depth = round_update.round_depth,
                    error = ?err,
                    "apply_pswap_round failed; lineage left at previous tip",
                );
            }
        }
        Ok(())
    }
}

impl PswapChainObserver {
    /// Fetches attachment content for `pending` via one batched `GetNotesById`
    /// call, builds [`PswapChainNoteUpdate`]s, and filters to active lineages.
    async fn build_chain_note_updates(
        &self,
        pending: Vec<PendingPswapNote>,
    ) -> Result<Vec<PswapChainNoteUpdate>, ClientError> {
        let note_ids: Vec<NoteId> = pending.iter().map(|p| p.note_id).collect();
        let fetched = self.rpc.get_notes_by_id(&note_ids).await?;

        let mut updates = Vec::with_capacity(fetched.len());
        for fetched_note in fetched {
            let Some(pending_rec) = pending.iter().find(|p| p.note_id == fetched_note.id()) else {
                // Node returned a note we didn't ask about — defensive skip.
                continue;
            };

            let Some((order_id, depth, amount)) = extract_pswap_attachment(&fetched_note) else {
                // Either the attachment is missing (shouldn't happen — we
                // filtered by metadata header in observe()) or content is
                // malformed. Soft-skip and continue.
                continue;
            };

            // Filter by active lineage. Foreign creators' orders, terminated
            // lineages, and asset-pair noise are all rejected here.
            let lineage = match self.store.get_pswap_lineage(order_id).await? {
                Some(l) if l.state == PswapLineageState::Active => l,
                _ => continue,
            };

            // Forward-only depth: skip already-processed rounds.
            if depth < lineage.current_depth + 1 {
                continue;
            }

            updates.push(PswapChainNoteUpdate {
                note_id: pending_rec.note_id,
                order_id,
                depth,
                amount,
                sender: pending_rec.sender,
                tag: pending_rec.tag,
                block_num: pending_rec.block_num,
                inclusion_proof: pending_rec.inclusion_proof.clone(),
            });
        }
        Ok(updates)
    }
}

// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------

/// Extracts the PSWAP attachment word `[amount, order_id, depth, 0]` from a
/// [`FetchedNote`] (post-`GetNotesById`). Returns `Some((order_id, depth,
/// amount))` if the note carries a PSWAP-scheme attachment with at least one
/// content word; `None` for any of: no PSWAP attachment, empty content,
/// or amount/depth that don't fit their typed bounds.
///
/// **TEMP-PROTOCOL-ADAPTER**: until [`#2214`](https://github.com/0xMiden/miden-client/pull/2214)
/// lands, this is the only path that can read attachment content for
/// private notes (sync stream only carries metadata; content arrives via
/// `GetNotesById`). After #2214 lands, attachments will be available
/// alongside the `StateSyncUpdate` and this helper can read them inline
/// without the per-sync RPC round trip.
fn extract_pswap_attachment(fetched_note: &FetchedNote) -> Option<(Felt, u32, AssetAmount)> {
    let attachments = match fetched_note {
        FetchedNote::Private(_, _, _, attachments) => attachments,
        FetchedNote::Public(note, _) => note.attachments(),
    };

    let pswap_attach = attachments.find(PswapNote::PSWAP_ATTACHMENT_SCHEME)?;
    let word = pswap_attach.content().as_words().first()?;

    let amount = AssetAmount::new(word[0].as_canonical_u64()).ok()?;
    let order_id = word[1];
    let depth = u32::try_from(word[2].as_canonical_u64()).ok()?;
    Some((order_id, depth, amount))
}
