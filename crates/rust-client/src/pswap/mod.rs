//! PSWAP chain tracking for partial swap orders this client originated.
//!
//! ## What this module does
//!
//! A PSWAP (partial swap) note lets a creator post an offer that anyone can fill
//! incrementally. Each fill consumes the current "tip" PSWAP note and emits, in
//! the same transaction, a P2ID payback to the creator and a remainder PSWAP for
//! the still-unfilled portion. The remainder becomes the new tip; the chain
//! continues until fully filled or reclaimed by the creator.
//!
//! Today the client tracks only the transaction it submits — once a foreign
//! account fills the order, the creator loses sight of the chain. This module
//! adds a persistent lineage table plus a sync-time chain walker that follows
//! the order across arbitrarily many fills by other accounts, exposing each
//! round's state (current tip, remaining amounts, depth) and surfacing each
//! payback as a consumable input note in the local store.
//!
//! ## How it fits into the existing flow
//!
//! - On `build_pswap_create` submission, a [`PswapLineageRecord`] row is
//!   inserted into the new `pswap_lineages` table. The asset-pair tag is
//!   registered so sync can pick up future remainder notes.
//! - During [`crate::Client::sync_state`], a [`PswapChainObserver`] (an
//!   implementation of [`crate::sync::NoteObserver`]) inspects every incoming
//!   note for a PSWAP attachment. Notes whose `order_id` matches a tracked
//!   active lineage are pushed into a per-sync collector.
//! - After the network sync returns, [`discover_pswap_rounds`] joins the
//!   collected notes with the consumed-nullifier signal from the same sync to
//!   advance each lineage by one or more rounds. Each round produces a
//!   [`PswapLineageRoundUpdate`] that's applied atomically by the store.
//! - For reclaim, [`Client::build_pswap_cancel_by_order`] reconstructs the
//!   current tip via `PswapNote::remainder_note(...)` and delegates to the
//!   existing `build_pswap_cancel` builder.
//!
//! ## Module layout
//!
//! - [`lineage`] — types describing the persistent lineage record and a
//!   round transition.
//! - [`observer`] — the per-note observer that filters PSWAP-attachment notes
//!   for active lineages.
//! - [`discovery`] — the post-sync correlator that builds round updates.
//! - [`errors`] — error types specific to PSWAP chain tracking.
//!
//! See `/Users/vaibhavjindal/.claude/plans/plan-with-me-and-cheeky-corbato.md`
//! for the full design rationale and protocol-side contract.

pub mod discovery;
pub mod errors;
pub mod lineage;
pub mod observer;

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
    /// Records any new PSWAP orders this transaction created as
    /// [`PswapLineageRecord`] rows in the store, and registers the
    /// matching asset-pair tag so future remainders are delivered via
    /// sync.
    ///
    /// Called by `apply_transaction_update` immediately after the
    /// transaction has been persisted to the store. Idempotent: an
    /// upsert on `order_id` plus a tag-source insert that's also
    /// idempotent on its `(tag, source)` row. Safe to retry; safe to
    /// no-op (the common case — the transaction is not a
    /// `build_pswap_create`).
    ///
    /// ### Filter criteria
    ///
    /// An output note becomes a tracked lineage iff all of:
    ///   * `PswapNote::try_from(&note)` succeeds — the note is a PSWAP
    ///     (this is cheap and fails fast for non-PSWAP outputs),
    ///   * `pswap.parent_depth() == 0` — it's the originating order,
    ///     not a remainder this client happened to emit while filling
    ///     someone else's PSWAP,
    ///   * `pswap.storage().creator_account_id()` is in
    ///     `store.get_account_ids()` — the creator is a local
    ///     account; we don't track orders for foreign creators.
    ///
    /// Multiple PSWAP creates in a single transaction (unusual but not
    /// prohibited by protocol) produce one row each.
    pub(crate) async fn record_created_pswap_lineages(
        &self,
        tx_result: &TransactionResult,
        submission_height: BlockNumber,
    ) -> Result<(), ClientError> {
        let output_notes = tx_result.executed_transaction().output_notes();
        let tracked_account_ids: BTreeSet<_> =
            self.store.get_account_ids().await?.into_iter().collect();

        for note in notes_from_output(output_notes) {
            let Ok(pswap) = PswapNote::try_from(note) else {
                continue;
            };

            if pswap.parent_depth() != 0 {
                continue;
            }

            let creator = pswap.storage().creator_account_id();
            if !tracked_account_ids.contains(&creator) {
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
            let order_id = record.order_id();

            self.store.upsert_pswap_lineage(&record).await?;
            // TEMP-PROTOCOL-ADAPTER: `Client::insert_note_tag` was renamed
            // to `add_note_tag` and only accepts `(NoteTag)` with an
            // implicit `NoteTagSource::User` source. We need to register
            // with `PswapAssetPair(order_id)` source so the tag is
            // reference-counted per-lineage and dropped on terminal
            // state. Bypass the client wrapper and call the store
            // directly.
            // REVERT-WHEN: upstream exposes a client-level wrapper that
            // accepts a full `NoteTagRecord` (or PSWAP becomes a
            // first-class concern of `Client::add_note_tag`).
            self.store
                .add_note_tag(NoteTagRecord {
                    tag: asset_pair_tag,
                    source: NoteTagSource::PswapAssetPair(order_id),
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
        // TEMP-PROTOCOL-ADAPTER: protocol 0.15 returns `AssetAmount`
        // from `FungibleAsset::amount()`. Convert to u64 for storage.
        remaining_offered: pswap.offered_asset().amount().into(),
        remaining_requested: pswap.storage().requested_asset_amount(),
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
            // The senior-engineer review of commit c86bcd8b validated
            // these fields' consistency at deserialization time, so
            // the unwraps below are infallible by invariant.
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

            // TEMP-PROTOCOL-ADAPTER: protocol 0.15 takes `&PswapNoteAttachment`
            // (was `(depth, payout)`) and `AssetAmount` for remainders.
            let attachment = miden_standards::note::PswapNoteAttachment::new(
                miden_protocol::asset::AssetAmount::new(last_payout)
                    .map_err(ClientError::AssetError)?,
                lineage.order_id(),
                u32::try_from(lineage.current_depth).map_err(|_| {
                    PswapLineageError::InconsistentRow(alloc::string::String::from(
                        "current_depth does not fit in u32",
                    ))
                })?,
            );
            let new_remaining_offered =
                miden_protocol::asset::AssetAmount::new(lineage.remaining_offered)
                    .map_err(ClientError::AssetError)?;
            let new_remaining_requested =
                miden_protocol::asset::AssetAmount::new(lineage.remaining_requested)
                    .map_err(ClientError::AssetError)?;
            lineage
                .original_pswap
                .remainder_note(
                    last_consumer,
                    &attachment,
                    new_remaining_offered,
                    new_remaining_requested,
                )
                .map_err(PswapLineageError::Reconstruction)?
        };

        TransactionRequestBuilder::new()
            .build_pswap_cancel(tip_note, lineage.creator_account_id())
            .map_err(ClientError::TransactionRequestError)
    }

}
