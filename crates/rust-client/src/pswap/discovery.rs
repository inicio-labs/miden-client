//! Post-sync correlator that joins consumed-nullifier events with the
//! PSWAP-attachment notes collected by [`super::observer::PswapChainObserver`]
//! and emits [`super::lineage::PswapLineageRoundUpdate`] entries describing
//! each round transition.
//!
//! See module-level docs on [`crate::pswap`] for the overall design and
//! the `pswap_creator_reconstructs_lineage_from_attachments` test in the
//! protocol repo (`crates/miden-testing/tests/scripts/pswap.rs`) for the
//! executable contract this correlator implements at runtime.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::sync::Arc;
use alloc::vec::Vec;

use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::block::BlockNumber;
use miden_protocol::note::NoteId;
use tracing::error;

use super::errors::PswapLineageError;
use super::lineage::{
    PswapLineageFilter,
    PswapLineageRecord,
    PswapLineageRoundUpdate,
    PswapLineageState,
};
use super::observer::PswapChainNoteUpdate;
use super::types::OrderIdKey;
use crate::ClientError;
use crate::store::Store;
use crate::sync::StateSyncUpdate;

// -----------------------------------------------------------------------------
// PUBLIC ENTRY POINT
// -----------------------------------------------------------------------------

/// Returns one [`PswapLineageRoundUpdate`] per round advanced this sync.
/// Inner loop catches same-block multi-fill via in-memory advancement.
pub async fn discover_pswap_rounds(
    store: Arc<dyn Store>,
    state_sync_update: &StateSyncUpdate,
    chain_note_updates: &[PswapChainNoteUpdate],
) -> Result<Vec<PswapLineageRoundUpdate>, ClientError> {
    let consumed_note_ids: BTreeSet<NoteId> =
        state_sync_update.note_updates.consumed_note_ids().collect();

    if consumed_note_ids.is_empty() && chain_note_updates.is_empty() {
        return Ok(Vec::new());
    }

    // Load only lineages whose tip is in this sync window.
    let active_lineages = store
        .list_pswap_lineages(PswapLineageFilter::ActiveByTipNoteIds(
            consumed_note_ids.iter().copied().collect(),
        ))
        .await?;
    if active_lineages.is_empty() {
        return Ok(Vec::new());
    }

    // Group notes by (order_id, depth) for O(1) per-round lookup.
    let mut notes_by_order_depth: BTreeMap<(OrderIdKey, u32), Vec<&PswapChainNoteUpdate>> =
        BTreeMap::new();
    for note in chain_note_updates {
        notes_by_order_depth
            .entry((OrderIdKey::from(note.attachment.order_id()), note.attachment.depth()))
            .or_insert_with(Vec::new)
            .push(note);
    }

    // All rounds discovered this sync share the sync's terminal block.
    let sync_block = state_sync_update.block_num;
    let mut round_updates: Vec<PswapLineageRoundUpdate> = Vec::new();

    for lineage_record in active_lineages {
        let mut lineage = lineage_record;

        // Same-block multi-fill: re-check the new tip after each in-memory advance.
        while consumed_note_ids.contains(&lineage.current_tip_note_id) {
            let round_depth = lineage.current_depth + 1;
            let notes = notes_by_order_depth
                .get(&(lineage.order_id_key(), round_depth))
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            let update = match build_round_update(&lineage, round_depth, sync_block, notes) {
                Ok(Some(u)) => u,
                Ok(None) => break,
                Err(err) => {
                    error!(
                        order_id = ?lineage.order_id(),
                        round_depth,
                        error = ?err,
                        "discover_pswap_rounds: round build failed; skipping lineage",
                    );
                    break;
                },
            };

            lineage = lineage.apply_round_in_memory(&update);
            round_updates.push(update);
        }
    }

    Ok(round_updates)
}

// -----------------------------------------------------------------------------
// PER-ROUND CLASSIFICATION
// -----------------------------------------------------------------------------

/// Builds one round's [`PswapLineageRoundUpdate`].
fn build_round_update(
    lineage: &PswapLineageRecord,
    round_depth: u32,
    at_block_num: BlockNumber,
    notes: &[&PswapChainNoteUpdate],
) -> Result<Option<PswapLineageRoundUpdate>, ClientError> {
    let original = &lineage.original_pswap;
    let offered_faucet = original.offered_asset().faucet_id();
    let requested_faucet = original.storage().requested_asset().faucet_id();
    let zero_offered = FungibleAsset::new(offered_faucet, 0).expect("FA(_, 0) is always valid");
    let zero_requested = FungibleAsset::new(requested_faucet, 0).expect("FA(_, 0) is always valid");
    // payback → requested faucet (fill); remainder → offered faucet (payout).
    let to_fill = |amount: AssetAmount| FungibleAsset::new(requested_faucet, u64::from(amount));
    let to_payout = |amount: AssetAmount| FungibleAsset::new(offered_faucet, u64::from(amount));

    match notes.len() {
        0 => {
            // Reclaim — cancel branch emits no outputs; only the creator can hit it.
            Ok(Some(PswapLineageRoundUpdate {
                order_id: lineage.order_id(),
                round_depth,
                consumer_account_id: lineage.creator_account_id(),
                fill_amount: zero_requested,
                payout_amount: lineage.remaining_offered,
                remaining_offered: zero_offered,
                remaining_requested: zero_requested,
                state: PswapLineageState::Reclaimed,
                tip_note_id: None,
                at_block: at_block_num,
                payback: None,
                payback_inclusion_proof: None,
                remainder: None,
                remainder_inclusion_proof: None,
            }))
        },
        1 => {
            // Full fill — only payback emitted; remaining_requested → 0.
            let payback_note_update = notes[0];
            let payback = original
                .payback_note(payback_note_update.sender, &payback_note_update.attachment)
                .map_err(PswapLineageError::Reconstruction)?;
            let fill_amount = to_fill(payback_note_update.attachment.amount())
                .map_err(ClientError::AssetError)?;

            Ok(Some(PswapLineageRoundUpdate {
                order_id: lineage.order_id(),
                round_depth,
                consumer_account_id: payback_note_update.sender,
                fill_amount,
                payout_amount: lineage.remaining_offered,
                remaining_offered: zero_offered,
                remaining_requested: zero_requested,
                state: PswapLineageState::FullyFilled,
                tip_note_id: None,
                at_block: at_block_num,
                payback: Some(payback),
                payback_inclusion_proof: Some(payback_note_update.inclusion_proof.clone()),
                remainder: None,
                remainder_inclusion_proof: None,
            }))
        },
        2 => {
            // Partial fill — payback + remainder. Distinguish by tag.
            let payback_tag = original.storage().payback_note_tag();
            let (payback_note_update, remainder_note_update) = if notes[0].tag == payback_tag {
                (notes[0], notes[1])
            } else {
                (notes[1], notes[0])
            };

            let payback_note = original
                .payback_note(payback_note_update.sender, &payback_note_update.attachment)
                .map_err(PswapLineageError::Reconstruction)?;

            let fill_amount = to_fill(payback_note_update.attachment.amount())
                .map_err(ClientError::AssetError)?;
            let payout_amount = to_payout(remainder_note_update.attachment.amount())
                .map_err(ClientError::AssetError)?;

            // Saturating sub — clamp to zero on over-fill.
            let remaining_requested = lineage.remaining_requested.sub(fill_amount)
                .unwrap_or(zero_requested);
            let remaining_offered = lineage.remaining_offered.sub(payout_amount)
                .unwrap_or(zero_offered);

            let remainder_note = original
                .remainder_note(
                    remainder_note_update.sender,
                    &remainder_note_update.attachment,
                    remaining_offered.amount(),
                    remaining_requested.amount(),
                )
                .map_err(PswapLineageError::Reconstruction)?;
            Ok(Some(PswapLineageRoundUpdate {
                order_id: lineage.order_id(),
                round_depth,
                consumer_account_id: payback_note_update.sender,
                fill_amount,
                payout_amount,
                remaining_offered,
                remaining_requested,
                state: PswapLineageState::Active,
                tip_note_id: Some(remainder_note_update.note_id),
                at_block: at_block_num,
                payback: Some(payback_note),
                payback_inclusion_proof: Some(payback_note_update.inclusion_proof.clone()),
                remainder: Some(remainder_note),
                remainder_inclusion_proof: Some(remainder_note_update.inclusion_proof.clone()),
            }))
        },
        _ => unreachable!("PSWAP emits ≤ 2 notes per (order_id, depth)"),
    }
}

// -----------------------------------------------------------------------------
// IN-MEMORY LINEAGE ADVANCE
// -----------------------------------------------------------------------------

impl PswapLineageRecord {
    /// Returns the post-round version. Drives the same-block multi-fill loop.
    pub(crate) fn apply_round_in_memory(
        mut self,
        update: &PswapLineageRoundUpdate,
    ) -> PswapLineageRecord {
        self.current_depth = update.round_depth;
        self.remaining_offered = update.remaining_offered;
        self.remaining_requested = update.remaining_requested;
        self.state = update.state;
        self.updated_at_block = update.at_block;
        if let Some(note_id) = update.tip_note_id {
            self.current_tip_note_id = note_id;
        }
        self
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    //! Correlator tests — exercise `build_round_update` + multi-fill advance.
    use alloc::vec;
    use alloc::vec::Vec;

    use miden_protocol::account::AccountId;
    use miden_protocol::asset::AssetAmount;
    use miden_protocol::crypto::merkle::SparseMerklePath;
    use miden_protocol::note::{Note, NoteInclusionProof};
    use miden_standards::note::{PswapNote, PswapNoteAttachment};

    use super::super::lineage::test_helpers::{build_test_pswap, fixed_account_ids};
    use super::*;

    /// `PswapNoteAttachment` from raw u64s, keyed off the PSWAP's order_id.
    fn pswap_attachment(pswap: &PswapNote, depth: u32, amount: u64) -> PswapNoteAttachment {
        PswapNoteAttachment::new(
            AssetAmount::new(amount).expect("amount fits in AssetAmount"),
            pswap.order_id(),
            depth,
        )
    }
    fn asset_amount(v: u64) -> AssetAmount {
        AssetAmount::new(v).expect("amount fits in AssetAmount")
    }

    /// Minimum-valid inclusion proof — correlator never inspects the path.
    fn dummy_inclusion_proof(block: u32) -> NoteInclusionProof {
        let path = SparseMerklePath::from_parts(0, Vec::new())
            .expect("empty SparseMerklePath is valid");
        NoteInclusionProof::new(BlockNumber::from(block), 0, path)
            .expect("zero index is well below the per-block notes ceiling")
    }

    /// Active lineage at depth 0 built from a fresh test PSWAP.
    fn initial_record(pswap: PswapNote, offered: u64, requested: u64) -> PswapLineageRecord {
        let note = Note::from(pswap.clone());
        let offered_faucet = pswap.offered_asset().faucet_id();
        let requested_faucet = pswap.storage().requested_asset().faucet_id();
        PswapLineageRecord {
            original_pswap: pswap,
            current_tip_note_id: note.id(),
            current_depth: 0,
            remaining_offered: FungibleAsset::new(offered_faucet, offered)
                .expect("test value fits in FungibleAsset"),
            remaining_requested: FungibleAsset::new(requested_faucet, requested)
                .expect("test value fits in FungibleAsset"),
            state: PswapLineageState::Active,
            created_at_block: BlockNumber::from(0),
            updated_at_block: BlockNumber::from(0),
        }
    }

    /// `PswapChainNoteUpdate` mirroring `note` (id + tag) so the
    /// correlator's tag-based payback/remainder split works.
    fn chain_update_from(
        note: &Note,
        attachment: PswapNoteAttachment,
        sender: AccountId,
        block: u32,
    ) -> PswapChainNoteUpdate {
        PswapChainNoteUpdate {
            note_id: note.id(),
            attachment,
            sender,
            tag: note.metadata().tag(),
            block_num: BlockNumber::from(block),
            inclusion_proof: dummy_inclusion_proof(block),
        }
    }

    /// 2-candidate partial fill → `Active`, both `remaining_*` reduced.
    #[test]
    fn build_round_update_partial_fill_advances_active() {
        let (_sender, _creator, offered_faucet, requested_faucet) = fixed_account_ids();
        // Distinct consumer asserts round-trip through `consumer_account_id`.
        let consumer = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        )
        .unwrap();
        let creator = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
        )
        .unwrap();

        let pswap = build_test_pswap(consumer, creator, offered_faucet, 100, requested_faucet, 50);
        let record = initial_record(pswap.clone(), 100, 50);

        let fill_amount = 20;
        let payout_amount = 40;
        let new_off = 100 - payout_amount;
        let new_req = 50 - fill_amount;

        let payback_att = pswap_attachment(&pswap, 1, fill_amount);
        let remainder_att = pswap_attachment(&pswap, 1, payout_amount);
        let payback = pswap.payback_note(consumer, &payback_att).unwrap();
        let remainder = pswap
            .remainder_note(consumer, &remainder_att, asset_amount(new_off), asset_amount(new_req))
            .unwrap();

        let cand_payback = chain_update_from(&payback, payback_att, consumer, 7);
        let cand_remainder = chain_update_from(&remainder, remainder_att, consumer, 7);

        let update = build_round_update(
            &record,
            1,
            BlockNumber::from(7),
            &[&cand_payback, &cand_remainder],
        )
        .unwrap()
        .expect("partial fill must produce a round update");

        assert_eq!(update.round_depth, 1);
        assert_eq!(update.consumer_account_id, consumer);
        assert_eq!(update.fill_amount.amount(), asset_amount(fill_amount));
        assert_eq!(update.payout_amount.amount(), asset_amount(payout_amount));
        assert_eq!(update.remaining_offered.amount(), asset_amount(new_off));
        assert_eq!(update.remaining_requested.amount(), asset_amount(new_req));
        assert_eq!(update.state, PswapLineageState::Active);
        assert_eq!(update.tip_note_id, Some(remainder.id()));
        assert!(update.payback.is_some());
        assert!(update.remainder.is_some());
        assert!(update.payback_inclusion_proof.is_some());
    }

    /// 1-candidate full fill → `FullyFilled`, no remainder, both zeros.
    #[test]
    fn build_round_update_full_fill_marks_fully_filled() {
        let (_sender, _creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let consumer = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        )
        .unwrap();
        let creator = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
        )
        .unwrap();

        // Smaller initial sizes so the single fill exhausts both sides.
        let pswap = build_test_pswap(consumer, creator, offered_faucet, 30, requested_faucet, 50);
        let record = initial_record(pswap.clone(), 30, 50);

        let fill_amount = 50; // exhausts requested side
        let payback_att = pswap_attachment(&pswap, 1, fill_amount);
        let payback = pswap.payback_note(consumer, &payback_att).unwrap();
        let cand = chain_update_from(&payback, payback_att, consumer, 9);

        let update = build_round_update(&record, 1, BlockNumber::from(9), &[&cand])
            .unwrap()
            .expect("full fill must produce a round update");

        assert_eq!(update.state, PswapLineageState::FullyFilled);
        assert_eq!(update.fill_amount.amount(), asset_amount(fill_amount));
        assert_eq!(update.payout_amount.amount(), asset_amount(30)); // entire remaining_offered
        assert_eq!(update.remaining_offered.amount(), AssetAmount::ZERO);
        assert_eq!(update.remaining_requested.amount(), AssetAmount::ZERO);
        assert_eq!(update.tip_note_id, None);
        assert!(update.remainder.is_none());
    }

    /// 0-candidate consumption → `Reclaimed`, consumer == creator, both
    /// `remaining_*` zeroed. Regression guard.
    #[test]
    fn build_round_update_zero_outputs_marks_reclaimed_with_remaining_zero() {
        let (_sender, _creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let consumer = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        )
        .unwrap();
        let creator = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
        )
        .unwrap();

        let pswap = build_test_pswap(consumer, creator, offered_faucet, 80, requested_faucet, 40);
        let record = initial_record(pswap, 80, 40);

        let update = build_round_update(&record, 1, BlockNumber::from(5), &[])
            .unwrap()
            .expect("zero-output consumption must produce a round update");

        assert_eq!(update.state, PswapLineageState::Reclaimed);
        assert_eq!(update.consumer_account_id, creator);
        assert_eq!(update.fill_amount.amount(), AssetAmount::ZERO);
        assert_eq!(update.payout_amount.amount(), asset_amount(80));
        assert_eq!(update.remaining_offered.amount(), AssetAmount::ZERO);
        // Regression: reclaim used to leak the pre-reclaim
        // `remaining_requested` into the terminal row.
        assert_eq!(update.remaining_requested.amount(), AssetAmount::ZERO);
        assert!(update.payback.is_none());
        assert!(update.payback_inclusion_proof.is_none());
    }

    /// Same-block multi-fill: round 2 must build against round 1's
    /// in-memory-advanced lineage, not the original.
    #[test]
    fn apply_round_in_memory_chains_correctly_for_multi_fill() {
        let (_sender, _creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let consumer = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        )
        .unwrap();
        let creator = AccountId::try_from(
            miden_protocol::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
        )
        .unwrap();

        let pswap = build_test_pswap(consumer, creator, offered_faucet, 100, requested_faucet, 50);
        let record0 = initial_record(pswap.clone(), 100, 50);

        // ── Round 1: partial fill, 20 requested for 40 offered.
        let fill1 = 20;
        let payout1 = 40;
        let new_off1 = 100 - payout1;
        let new_req1 = 50 - fill1;
        let payback_att1 = pswap_attachment(&pswap, 1, fill1);
        let remainder_att1 = pswap_attachment(&pswap, 1, payout1);
        let payback1 = pswap.payback_note(consumer, &payback_att1).unwrap();
        let remainder1 = pswap
            .remainder_note(consumer, &remainder_att1, asset_amount(new_off1), asset_amount(new_req1))
            .unwrap();
        let cand_p1 = chain_update_from(&payback1, payback_att1, consumer, 11);
        let cand_r1 = chain_update_from(&remainder1, remainder_att1, consumer, 11);

        let update1 =
            build_round_update(&record0, 1, BlockNumber::from(11), &[&cand_p1, &cand_r1])
                .unwrap()
                .unwrap();

        // Mirrors `discover_pswap_rounds`'s inner loop.
        let record1 = record0.apply_round_in_memory(&update1);
        assert_eq!(record1.current_depth, 1);
        assert_eq!(record1.remaining_offered.amount(), asset_amount(new_off1));
        assert_eq!(record1.remaining_requested.amount(), asset_amount(new_req1));
        assert_eq!(record1.current_tip_note_id, remainder1.id());
        assert_eq!(record1.state, PswapLineageState::Active);

        // ── Round 2: full fill of the remainder, exhausts requested side.
        let fill2 = new_req1; // = 30
        let payback_att2 = pswap_attachment(&pswap, 2, fill2);
        let payback2 = pswap.payback_note(consumer, &payback_att2).unwrap();
        let cand_p2 = chain_update_from(&payback2, payback_att2, consumer, 11);

        let update2 = build_round_update(&record1, 2, BlockNumber::from(11), &[&cand_p2])
            .unwrap()
            .unwrap();

        assert_eq!(update2.round_depth, 2);
        assert_eq!(update2.state, PswapLineageState::FullyFilled);
        assert_eq!(update2.fill_amount.amount(), asset_amount(fill2));
        assert_eq!(update2.payout_amount.amount(), asset_amount(new_off1)); // remaining_offered exhausted
        assert_eq!(update2.remaining_offered.amount(), AssetAmount::ZERO);
        assert_eq!(update2.remaining_requested.amount(), AssetAmount::ZERO);

        let record2 = record1.apply_round_in_memory(&update2);
        assert_eq!(record2.state, PswapLineageState::FullyFilled);
        let emitted = vec![update1, update2];
        assert_eq!(emitted.len(), 2);
    }
}
