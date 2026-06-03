//! PSWAP chain tracking — follows partial-swap orders across fills so the
//! creator can always see the current tip and reclaim the unfilled balance.
//!
//! Flow:
//! 1. Create → [`PswapLineageRecord`] row + asset-pair tag subscription.
//! 2. Sync → [`PswapChainObserver`] collects PSWAP-attachment notes;
//!    [`discover_pswap_rounds`] correlates them with consumed-nullifier
//!    events and emits one [`PswapLineageRoundUpdate`] per round.
//! 3. Reclaim → [`Client::build_pswap_cancel_by_order`].
//!
//! Protocol invariants (≤1 payback + ≤1 remainder per round, attachment
//! word layout, deterministic reconstruction) live on
//! `miden_standards::note::PswapNote`.

pub mod discovery;
pub mod errors;
pub mod lineage;
pub mod observer;
mod types;

pub use errors::PswapLineageError;
pub use lineage::{PswapLineageFilter, PswapLineageRecord, PswapLineageRoundUpdate, PswapLineageState};
pub use observer::{PswapChainNoteUpdate, PswapChainObserver};
// `PswapTransactionObserver` is defined inline below in this file.

use alloc::boxed::Box;
use alloc::collections::BTreeSet;
use alloc::sync::Arc;

use async_trait::async_trait;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;
use miden_tx::auth::TransactionAuthenticator;

use crate::ClientError;
use crate::store::Store;
use crate::sync::{NoteTagRecord, NoteTagSource};
use crate::transaction::{TransactionObserver, TransactionResult, notes_from_output};
use crate::Client;

// PSWAP TRANSACTION OBSERVER
// ================================================================================================

/// [`TransactionObserver`] that registers a [`PswapLineageRecord`] +
/// asset-pair tag subscription for every PSWAP this wallet just created
/// (any output where `PswapNote::try_from` succeeds AND `parent_depth == 0`).
///
/// Tracks regardless of the PSWAP's `creator_account_id` — service-style
/// wallets that submit PSWAPs on behalf of remote clients get chain
/// visibility. Reclaim surfaces `CreatorNotLocal` later if applicable.
/// Idempotent on the store side (upsert by `order_id` + `(tag, source)`
/// insert).
pub struct PswapTransactionObserver {
    store: Arc<dyn Store>,
}

impl PswapTransactionObserver {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

#[async_trait(?Send)]
impl TransactionObserver for PswapTransactionObserver {
    fn name(&self) -> &'static str {
        "PswapTransactionObserver"
    }

    async fn observe(
        &self,
        tx_result: &TransactionResult,
        submission_height: BlockNumber,
    ) -> Result<(), ClientError> {
        let output_notes = tx_result.executed_transaction().output_notes();

        for note in notes_from_output(output_notes) {
            let Ok(pswap) = PswapNote::try_from(note) else {
                continue;
            };

            // Skip remainders we emitted while filling someone else's PSWAP —
            // those belong to that chain's creator, not us.
            if pswap.parent_depth() != 0 {
                continue;
            }

            let record = build_initial_lineage_record(note, &pswap, submission_height);
            let asset_pair_tag = record.asset_pair_tag();
            let original_note_id = record.current_tip_note_id;

            self.store.upsert_pswap_lineage(&record).await?;
            self.store
                .add_note_tag(NoteTagRecord {
                    tag: asset_pair_tag,
                    source: NoteTagSource::Subscription(original_note_id),
                })
                .await?;
        }

        Ok(())
    }
}

fn build_initial_lineage_record(
    note: &Note,
    pswap: &PswapNote,
    submission_height: BlockNumber,
) -> PswapLineageRecord {
    // At depth 0, remaining_* == initial offered/requested.
    PswapLineageRecord {
        original_pswap: pswap.clone(),
        current_tip_note_id: note.id(),
        current_tip_nullifier: note.nullifier(),
        current_depth: 0,
        remaining_offered: pswap.offered_asset().clone(),
        remaining_requested: pswap.storage().requested_asset().clone(),
        state: PswapLineageState::Active,
        created_at_block: submission_height,
        updated_at_block: submission_height,
    }
}

// =============================================================================
// PUBLIC API
// =============================================================================

use alloc::vec::Vec;

use miden_protocol::Felt;
use miden_protocol::account::AccountId;

use crate::store::NoteFilter;
use crate::transaction::{TransactionRequest, TransactionRequestBuilder};

impl<AUTH: TransactionAuthenticator + Sync + 'static> Client<AUTH> {
    /// Returns every PSWAP lineage tracked by this client.
    pub async fn pswap_lineages(&self) -> Result<Vec<PswapLineageRecord>, ClientError> {
        self.store
            .list_pswap_lineages(PswapLineageFilter::All)
            .await
            .map_err(Into::into)
    }

    /// Returns lineages created by a specific local account.
    pub async fn pswap_lineages_for(
        &self,
        creator: AccountId,
    ) -> Result<Vec<PswapLineageRecord>, ClientError> {
        self.store
            .list_pswap_lineages(PswapLineageFilter::ByCreator(creator))
            .await
            .map_err(Into::into)
    }

    /// Returns the lineage for one order, or `None` if not tracked.
    pub async fn pswap_lineage(
        &self,
        order_id: Felt,
    ) -> Result<Option<PswapLineageRecord>, ClientError> {
        self.store.get_pswap_lineage(order_id).await.map_err(Into::into)
    }

    /// Builds a tx that reclaims the unfilled offered asset on the current
    /// tip of an Active lineage.
    ///
    /// Errors: [`PswapLineageError::NotFound`], [`NotActive`],
    /// [`CreatorNotLocal`] (reclaim needs creator's signing authority),
    /// or [`TipMissing`] (tip note isn't in `output_notes`/`input_notes`
    /// — sync regression).
    pub async fn build_pswap_cancel_by_order(
        &self,
        order_id: Felt,
    ) -> Result<TransactionRequest, ClientError> {
        let lineage = self
            .store
            .get_pswap_lineage(order_id)
            .await?
            .ok_or(PswapLineageError::NotFound(order_id))?;

        if lineage.state != PswapLineageState::Active {
            return Err(PswapLineageError::NotActive(lineage.state).into());
        }

        // Reclaim requires the creator's signing authority. Fail loud here
        // rather than deferring to an opaque signing failure.
        let creator = lineage.creator_account_id();
        let local_accounts: BTreeSet<_> =
            self.store.get_account_ids().await?.into_iter().collect();
        if !local_accounts.contains(&creator) {
            return Err(PswapLineageError::CreatorNotLocal(creator).into());
        }

        // Depth 0 tip lives in `output_notes` (we minted it); depth > 0 in
        // `input_notes` (inserted by `apply_pswap_round`).
        let tip_note: Note = if lineage.current_depth == 0 {
            let record = self
                .store
                .get_output_notes(NoteFilter::Unique(lineage.current_tip_note_id))
                .await?
                .into_iter()
                .next()
                .ok_or(PswapLineageError::TipMissing)?;
            record.try_into().map_err(ClientError::NoteRecordConversionError)?
        } else {
            let record = self
                .store
                .get_input_notes(NoteFilter::Unique(lineage.current_tip_note_id))
                .await?
                .into_iter()
                .next()
                .ok_or(PswapLineageError::TipMissing)?;
            record.try_into().map_err(ClientError::NoteRecordConversionError)?
        };

        TransactionRequestBuilder::new()
            .build_pswap_cancel(tip_note, lineage.creator_account_id())
            .map_err(ClientError::TransactionRequestError)
    }
}
