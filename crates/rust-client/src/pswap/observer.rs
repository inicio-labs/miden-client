//! Per-note observer that collects PSWAP-attachment notes for active
//! lineages during sync.

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

/// Observed PSWAP-attachment note resolved against an active lineage.
/// Built in `apply()` after `GetNotesById` resolves the attachment word.
#[derive(Debug, Clone)]
pub struct PswapChainNoteUpdate {
    pub note_id: NoteId,
    /// Attachment slot `[1]` — stable across the chain.
    pub order_id: Felt,
    /// Attachment slot `[2]` — round counter.
    pub depth: u32,
    /// Attachment slot `[0]` — `fill_amount` (payback) or `payout_amount`
    /// (remainder). Role distinguished by [`Self::tag`].
    pub amount: AssetAmount,
    pub sender: AccountId,
    /// Payback uses the P2ID-style tag; remainder uses the asset-pair tag.
    pub tag: NoteTag,
    pub block_num: BlockNumber,
    pub inclusion_proof: NoteInclusionProof,
}

// PENDING PSWAP NOTE
// ================================================================================================

/// Per-sync queue entry — metadata-derivable fields only. The attachment
/// content word is fetched in `apply()` via `GetNotesById` (TEMP — see
/// upstream PR #2214).
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

/// Per-sync collector of PSWAP-attachment notes for this client's lineages.
///
/// - `observe()` runs per-note during sync: cheap PSWAP-scheme filter,
///   queues a pending record (no store query, no RPC, no DB write).
/// - `apply()` runs once post-sync: batches one `GetNotesById`, builds
///   [`PswapChainNoteUpdate`]s, runs the correlator, applies round updates.
pub struct PswapChainObserver {
    store: Arc<dyn Store>,
    /// **TEMP**: drop once upstream PR #2214 ships attachments alongside
    /// `StateSyncUpdate` — observer can then read them inline from
    /// `observe()`'s `CommittedNote` without a per-sync RPC round trip.
    rpc: Arc<dyn NodeRpcClient>,
    /// Used as a `Mutex<Vec<_>>` — `observe()` writes, `apply()` drains,
    /// never concurrently.
    pending_pswap_notes: Arc<RwLock<Vec<PendingPswapNote>>>,
}

impl PswapChainObserver {
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

    /// Fetches attachments via one batched `GetNotesById`, runs the
    /// correlator, applies each round update. Per-round failures are
    /// logged; one corrupted lineage does not stall the rest.
    async fn apply(
        &self,
        sync_update: &crate::sync::StateSyncUpdate,
    ) -> Result<(), ClientError> {
        let pending = core::mem::take(&mut *self.pending_pswap_notes.write());

        // Fast path: nothing PSWAP-related happened this sync.
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
    /// Batched `GetNotesById` → extract attachments → filter to our active
    /// lineages → build chain note updates.
    async fn build_chain_note_updates(
        &self,
        pending: Vec<PendingPswapNote>,
    ) -> Result<Vec<PswapChainNoteUpdate>, ClientError> {
        let note_ids: Vec<NoteId> = pending.iter().map(|p| p.note_id).collect();
        let fetched = self.rpc.get_notes_by_id(&note_ids).await?;

        let mut updates = Vec::with_capacity(fetched.len());
        for fetched_note in fetched {
            // Skip notes the node returned but we didn't ask about.
            let Some(pending_rec) = pending.iter().find(|p| p.note_id == fetched_note.id()) else {
                continue;
            };

            // Soft-skip if attachment is missing or malformed.
            let Some((order_id, depth, amount)) = extract_pswap_attachment(&fetched_note) else {
                continue;
            };

            // Reject foreign / terminated lineages.
            let lineage = match self.store.get_pswap_lineage(order_id).await? {
                Some(l) if l.state == PswapLineageState::Active => l,
                _ => continue,
            };

            // Forward-only depth.
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

/// Extracts `(order_id, depth, amount)` from a PSWAP attachment word
/// `[amount, order_id, depth, 0]`. Returns `None` if no PSWAP attachment,
/// empty content, or amount/depth outside typed bounds.
///
/// **TEMP-PROTOCOL-ADAPTER**: only path that reads attachment content for
/// private notes today. Replaced once upstream PR #2214 puts attachments
/// on the `StateSyncUpdate`.
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
