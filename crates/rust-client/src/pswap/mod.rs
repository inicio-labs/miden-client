//! PSWAP chain tracking — follows partial-swap orders across fills by
//! foreign accounts so the creator can always see the current tip and
//! reclaim the unfilled balance.
//!
//! ### Flow
//!
//! 1. `build_pswap_create` submission → [`PswapLineageRecord`] row + asset-pair
//!    tag subscription so sync delivers future remainders.
//! 2. `Client::sync_state` → [`PswapChainObserver`] collects PSWAP-attachment
//!    notes; [`discover_pswap_rounds`] joins them with consumed-nullifier
//!    events from the same sync and emits one [`PswapLineageRoundUpdate`] per
//!    advanced round, each applied atomically by the store.
//! 3. Reclaim → [`Client::build_pswap_cancel_by_order`] reconstructs the
//!    current tip via `PswapNote::remainder_note` and delegates to
//!    `build_pswap_cancel`.
//!
//! Submodules: [`lineage`] (types), [`observer`] (per-note collector),
//! [`discovery`] (post-sync correlator), [`errors`].
//!
//! Protocol-side invariants (≤1 payback + ≤1 remainder per round, attachment
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

use alloc::collections::BTreeSet;

use miden_protocol::block::BlockNumber;
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;
use miden_tx::auth::TransactionAuthenticator;

use crate::ClientError;
use crate::sync::{NoteTagRecord, NoteTagSource};
use crate::transaction::TransactionResult;
use crate::{Client, transaction::notes_from_output};

impl<AUTH: TransactionAuthenticator + Sync + 'static> Client<AUTH> {
    /// For each PSWAP this wallet just submitted (any output where
    /// `PswapNote::try_from(note)` succeeds AND `parent_depth == 0`),
    /// inserts a [`PswapLineageRecord`] and subscribes the asset-pair
    /// tag so sync delivers future remainders. Idempotent (upsert on
    /// `order_id` + `(tag, source)` insert).
    ///
    /// Tracks regardless of the PSWAP's `creator_account_id` — service-
    /// style wallets that submit PSWAPs for remote clients get chain
    /// visibility. Reclaim ([`Self::build_pswap_cancel_by_order`])
    /// surfaces `CreatorNotLocal` when reclaim isn't possible from this
    /// wallet.
    pub(crate) async fn record_created_pswap_lineages(
        &self,
        tx_result: &TransactionResult,
        submission_height: BlockNumber,
    ) -> Result<(), ClientError> {
        let output_notes = tx_result.executed_transaction().output_notes();

        for note in notes_from_output(output_notes) {
            // Cheap PSWAP-shape filter — fails fast for non-PSWAP
            // outputs (the dominant case for any transaction).
            let Ok(pswap) = PswapNote::try_from(note) else {
                continue;
            };

            // Skip remainders we emitted while filling someone else's
            // PSWAP. Those belong to the OTHER chain (the one being
            // filled), which is tracked by THAT chain's creator's
            // wallet — not us.
            if pswap.parent_depth() != 0 {
                continue;
            }

            // Private PSWAPs are tracked the same way as public ones.
            // The `NoteAttachment` is the protocol's *explicit* public
            // sidecar for private notes (see
            // `miden_protocol::note::NoteAttachment` doc, "An
            // attachment is a _public_ extension to a note"), and the
            // adapter commit `d2cddf8f` threads the deserialised
            // `NoteAttachments` through `CommittedNote` so the
            // observer reads `attachment_word[0]` regardless of
            // note_type. No special-case warning is needed.

            let record = build_initial_lineage_record(note, &pswap, submission_height);
            let asset_pair_tag = record.asset_pair_tag();
            let original_note_id = record.current_tip_note_id;

            self.store.upsert_pswap_lineage(&record).await?;
            // Subscription keyed by the original PSWAP's NoteId — generic
            // enough for any future observer with a subscription lifecycle.
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
    PswapLineageRecord {
        original_pswap: pswap.clone(),
        current_tip_note_id: note.id(),
        current_tip_nullifier: note.nullifier(),
        current_depth: 0,
        remaining_offered: pswap.offered_asset().amount(),
        // `requested_asset_amount()` returns u64 (legacy shape on
        // `PswapNoteStorage`); the value originated from a validated
        // `FungibleAsset` so it's always ≤ `AssetAmount::MAX`.
        remaining_requested: miden_protocol::asset::AssetAmount::new(
            pswap.storage().requested_asset_amount(),
        )
        .expect("PSWAP storage's requested_asset_amount is bounded by FungibleAsset's invariant"),
        last_consumer_account_id: None,
        last_payout_amount: None,
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

    /// Builds a transaction request that reclaims the unfilled offered
    /// asset on the *current tip* of an Active lineage. Works whether
    /// the tip is the original PSWAP (`current_depth == 0`) or a
    /// remainder this client never originated (`current_depth > 0`) —
    /// in the latter case the tip is reconstructed byte-identically
    /// via `PswapNote::remainder_note`.
    ///
    /// Errors:
    /// - [`PswapLineageError::NotFound`] if no lineage exists for `order_id`.
    /// - [`PswapLineageError::NotActive`] if the lineage already terminated
    ///   (`FullyFilled` or `Reclaimed`).
    /// - [`PswapLineageError::Reconstruction`] if the protocol's
    ///   `remainder_note` helper rejects the stored inputs (indicates
    ///   row corruption or protocol/client version skew).
    /// - [`PswapLineageError::TipMissing`] if `current_depth == 0` but
    ///   the original output note is not in the local `output_notes`
    ///   table — implies a sync regression.
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

        // Reclaim requires the creator's signing authority. We may be
        // tracking lineages whose creator is NOT a local account (e.g.
        // service-style wallets that submitted a PSWAP on behalf of a
        // remote client — see `record_created_pswap_lineages`); reclaim
        // is unavailable for those. Failing here is loud and
        // actionable; deferring the check would manifest as an opaque
        // signing failure inside transaction execution.
        let creator = lineage.creator_account_id();
        let local_accounts: BTreeSet<_> =
            self.store.get_account_ids().await?.into_iter().collect();
        if !local_accounts.contains(&creator) {
            return Err(PswapLineageError::CreatorNotLocal(creator).into());
        }

        let tip_note: Note = if lineage.current_depth == 0 {
            // The original PSWAP is in the local store as an output
            // note we minted ourselves. The exact recipient is needed
            // for `build_pswap_cancel` to recompute the script root;
            // fetching it from the store is canonical.
            let record = self
                .store
                .get_output_notes(NoteFilter::Unique(lineage.current_tip_note_id))
                .await?
                .into_iter()
                .next()
                .ok_or(PswapLineageError::TipMissing)?;
            record.try_into().map_err(ClientError::NoteRecordConversionError)?
        } else {
            // The current tip is a remainder this client never
            // originated. Reconstruct it byte-identically from the
            // stored `last_consumer` / `last_payout` / `remaining_*`.
            // `lineage::build_record_from_columns` validates these
            // fields' consistency at deserialization time
            // (last_consumer + last_payout must both be present iff
            // current_depth > 0), so the unwraps below are infallible
            // by row invariant — but we propagate
            // `InconsistentRow` defensively.
            let last_consumer = lineage.last_consumer_account_id.ok_or(
                PswapLineageError::InconsistentRow(alloc::string::String::from(
                    "current_depth > 0 but last_consumer_account_id is NULL",
                )),
            )?;
            let last_payout = lineage.last_payout_amount.ok_or(
                PswapLineageError::InconsistentRow(alloc::string::String::from(
                    "current_depth > 0 but last_payout_amount is NULL",
                )),
            )?;

            // The protocol's `remainder_note` builder takes a typed
            // `PswapNoteAttachment { amount, order_id, depth }` rather
            // than loose `(depth, payout)` args; construct it from the
            // lineage's persisted round-N state. `last_payout` /
            // `remaining_*` are already `AssetAmount` after the
            // store-side refactor, so no per-call conversion needed.
            let attachment = miden_standards::note::PswapNoteAttachment::new(
                last_payout,
                lineage.order_id(),
                lineage.current_depth,
            );
            lineage
                .original_pswap
                .remainder_note(
                    last_consumer,
                    &attachment,
                    lineage.remaining_offered,
                    lineage.remaining_requested,
                )
                .map_err(PswapLineageError::Reconstruction)?
        };

        TransactionRequestBuilder::new()
            .build_pswap_cancel(tip_note, lineage.creator_account_id())
            .map_err(ClientError::TransactionRequestError)
    }

}
