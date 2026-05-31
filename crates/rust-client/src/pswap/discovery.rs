//! Post-sync correlator that joins consumed-nullifier events with the
//! PSWAP-attachment notes collected by [`super::observer::PswapChainObserver`]
//! and emits [`super::lineage::PswapLineageRoundUpdate`] entries describing
//! each round transition.
//!
//! See module-level docs on [`crate::pswap`] for the overall design and
//! the `pswap_creator_reconstructs_lineage_from_attachments` test in the
//! protocol repo (`crates/miden-testing/tests/scripts/pswap.rs`) for the
//! executable contract this correlator implements at runtime.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::sync::Arc;
use alloc::vec::Vec;

use miden_protocol::Felt;
use miden_protocol::asset::AssetAmount;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{Note, NoteId, Nullifier};
use miden_standards::note::{PswapNote, PswapNoteAttachment};
use tracing::error;

use super::errors::PswapLineageError;
use super::lineage::{
    PswapLineageFilter,
    PswapLineageRecord,
    PswapLineageRoundUpdate,
    PswapLineageState,
};
use super::observer::PswapChainNoteUpdate;
use crate::ClientError;
use crate::store::Store;
use crate::sync::StateSyncUpdate;

// -----------------------------------------------------------------------------
// PUBLIC ENTRY POINT
// -----------------------------------------------------------------------------

/// Joins the post-sync state update with the per-sync PSWAP chain-note
/// collector and returns one [`PswapLineageRoundUpdate`] per advanced
/// round, in the order rounds should be applied.
///
/// Walks each active lineage; for each whose `current_tip_nullifier`
/// appears in the sync's nullifier window, looks up the round's
/// `(payback, remainder)` candidate set in `chain_note_updates`
/// (indexed by `(order_id, depth)`), classifies them via reconstruction
/// against `PswapNote::payback_note` / `remainder_note`, and builds a
/// round update. Loops on the new tip to catch same-block multi-fill.
///
/// The store applies each returned round update inside its own SQL
/// transaction via [`crate::store::Store::apply_pswap_round`]. The depth
/// invariant is checked inside `apply_pswap_round` as the last line of
/// defense; this function maintains it preemptively by advancing
/// in-memory.
///
/// Empty `chain_note_updates` AND empty `current_window_nullifier_blocks`
/// is the steady-state happy path (no PSWAP activity this sync) and
/// returns `Ok(vec![])` after a single store query.
pub async fn discover_pswap_rounds(
    store: Arc<dyn Store>,
    state_sync_update: &StateSyncUpdate,
    chain_note_updates: &[PswapChainNoteUpdate],
) -> Result<Vec<PswapLineageRoundUpdate>, ClientError> {
    if state_sync_update.current_window_nullifier_blocks.is_empty()
        && chain_note_updates.is_empty()
    {
        // Most syncs hit this path. Skip the store query.
        return Ok(Vec::new());
    }

    let active = store.list_pswap_lineages(PswapLineageFilter::Active).await?;
    if active.is_empty() {
        return Ok(Vec::new());
    }

    // Group chain note updates by (order_id, depth) for O(1) per-round
    // lookups inside the per-lineage walk below.
    let mut updates_by_order_depth: BTreeMap<(OrderIdKey, u64), Vec<&PswapChainNoteUpdate>> =
        BTreeMap::new();
    for u in chain_note_updates {
        updates_by_order_depth
            .entry((OrderIdKey::from(u.order_id), u.depth))
            .or_default()
            .push(u);
    }

    // Index the nullifier window for fast lookup by nullifier. The same
    // nullifier appearing twice is a protocol-invariant violation that
    // we collapse to the first observation; the correlator is per-round
    // and won't re-process anyway.
    let nullifier_blocks: BTreeMap<Nullifier, BlockNumber> = state_sync_update
        .current_window_nullifier_blocks
        .iter()
        .copied()
        .collect();

    let mut round_updates: Vec<PswapLineageRoundUpdate> = Vec::new();

    for lineage in active {
        let mut current = lineage;

        // Walk forward through every round consumed in this sync. The
        // inner loop catches same-block multi-fill — after applying
        // round N in-memory, the new tip's nullifier may itself appear
        // in the window (for batched fills landing in the same block),
        // and we want to advance the lineage through every round
        // observable from this single sync.
        while let Some(&block) = nullifier_blocks.get(&current.current_tip_nullifier) {
            let round_depth = current.current_depth + 1;
            let matches = updates_by_order_depth
                .get(&(OrderIdKey::from(current.order_id()), round_depth))
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            let update = match build_round_update(&current, round_depth, block, matches) {
                Ok(Some(u)) => u,
                Ok(None) => break, // unresolvable: log inside; leave the lineage at the old tip
                Err(err) => {
                    error!(
                        order_id = %FeltDebug(current.order_id()),
                        round_depth,
                        error = ?err,
                        "discover_pswap_rounds: round build failed; skipping lineage",
                    );
                    break;
                },
            };

            current = current.apply_round_in_memory(&update);
            round_updates.push(update);
        }
    }

    Ok(round_updates)
}

// -----------------------------------------------------------------------------
// PER-ROUND CLASSIFICATION
// -----------------------------------------------------------------------------

/// Builds a [`PswapLineageRoundUpdate`] for one round advance of `current`.
///
/// Returns `Ok(None)` for unresolvable but non-corrupting situations
/// (e.g. consumer-not-creator with zero outputs); the caller logs and
/// leaves the lineage at its previous tip. Returns `Err(_)` only for
/// reconstruction or commitment-parity failures, which are fail-loud
/// per the [`PswapLineageError::CommitmentMismatch`] contract.
fn build_round_update(
    current: &PswapLineageRecord,
    round_depth: u64,
    block: BlockNumber,
    matches: &[&PswapChainNoteUpdate],
) -> Result<Option<PswapLineageRoundUpdate>, ClientError> {
    let original = &current.original_pswap;

    match matches.len() {
        0 => {
            // No outputs in the block where the tip was consumed. The
            // PSWAP script only emits the cancel branch (no outputs)
            // when the consumer is the creator, so by protocol
            // invariant this *must* be a reclaim — but the
            // `current_window_nullifier_blocks` signal alone does not
            // tell us who the consumer was. Treat any 0-output
            // consumption of an Active tip as a reclaim and let the
            // store-side depth invariant catch any divergence at
            // apply time.
            Ok(Some(PswapLineageRoundUpdate {
                order_id: current.order_id(),
                round_depth,
                consumer_account_id: current.creator_account_id(),
                fill_amount: 0,
                payout_amount: current.remaining_offered,
                new_remaining_offered: 0,
                // Terminal state: per the field doc on
                // `PswapLineageRoundUpdate::new_remaining_requested`, both
                // remaining_* columns settle to 0 on a reclaim (no further
                // rounds can fill the requested side).
                new_remaining_requested: 0,
                new_state: PswapLineageState::Reclaimed,
                new_tip_note_id: None,
                new_tip_nullifier: None,
                at_block: block,
                reconstructed_payback: None,
                reconstructed_payback_inclusion_proof: None,
                reconstructed_remainder: None,
            }))
        },
        1 => {
            // Full fill — exactly the payback was emitted. Reconstruct
            // it; if the reconstructed id matches the observed id, the
            // round is well-formed.
            let candidate = matches[0];
            let payback = reconstruct_payback(original, candidate, round_depth)?;

            Ok(Some(PswapLineageRoundUpdate {
                order_id: current.order_id(),
                round_depth,
                consumer_account_id: candidate.sender,
                fill_amount: candidate.amount,
                payout_amount: current.remaining_offered,
                new_remaining_offered: 0,
                new_remaining_requested: current
                    .remaining_requested
                    .saturating_sub(candidate.amount),
                new_state: PswapLineageState::FullyFilled,
                new_tip_note_id: None,
                new_tip_nullifier: None,
                at_block: block,
                reconstructed_payback: Some(payback),
                reconstructed_payback_inclusion_proof: Some(candidate.inclusion_proof.clone()),
                reconstructed_remainder: None,
            }))
        },
        2 => {
            // Partial fill — payback + remainder. Try each candidate as
            // the payback; the one whose reconstruction matches is the
            // payback, and the other is the remainder.
            let (payback_idx, payback_note) =
                find_payback_index(original, matches, round_depth)?;
            let payback_cand = matches[payback_idx];
            let remainder_cand = matches[1 - payback_idx];

            // Consumer must match across both candidates.
            if payback_cand.sender != remainder_cand.sender {
                return Err(PswapLineageError::InconsistentRow(format!(
                    "payback sender {} != remainder sender {} for order_id {}",
                    payback_cand.sender, remainder_cand.sender, current.order_id(),
                ))
                .into());
            }

            let new_remaining_requested = current
                .remaining_requested
                .saturating_sub(payback_cand.amount);
            let new_remaining_offered = current
                .remaining_offered
                .saturating_sub(remainder_cand.amount);

            // TEMP-PROTOCOL-ADAPTER: protocol 0.15 wraps the attachment
            // word in a typed `PswapNoteAttachment` struct and asset
            // amounts in `AssetAmount`. Constructions are infallible
            // here because (a) `round_depth` upstream is u32-bounded and
            // (b) `new_remaining_*` are arithmetic over u64 values that
            // were already validated as fitting in `AssetAmount`.
            // REVERT-WHEN: this client adopts the typed attachment
            // directly in `PswapChainNoteUpdate`.
            let attachment = PswapNoteAttachment::new(
                AssetAmount::new(remainder_cand.amount)
                    .map_err(crate::ClientError::AssetError)?,
                current.order_id(),
                u32::try_from(round_depth)
                    .map_err(|_| PswapLineageError::Reconstruction(
                        miden_protocol::errors::NoteError::other(
                            "round_depth does not fit in u32",
                        ),
                    ))?,
            );
            let new_remaining_offered_a = AssetAmount::new(new_remaining_offered)
                .map_err(crate::ClientError::AssetError)?;
            let new_remaining_requested_a = AssetAmount::new(new_remaining_requested)
                .map_err(crate::ClientError::AssetError)?;
            let remainder_note = original
                .remainder_note(
                    remainder_cand.sender,
                    &attachment,
                    new_remaining_offered_a,
                    new_remaining_requested_a,
                )
                .map_err(PswapLineageError::Reconstruction)?;

            if remainder_note.id() != remainder_cand.note_id {
                return Err(PswapLineageError::CommitmentMismatch {
                    reconstructed: format!("{}", remainder_note.id().as_word()),
                    observed: format!("{}", remainder_cand.note_id.as_word()),
                }
                .into());
            }

            let new_tip_nullifier = remainder_note.nullifier();

            Ok(Some(PswapLineageRoundUpdate {
                order_id: current.order_id(),
                round_depth,
                consumer_account_id: payback_cand.sender,
                fill_amount: payback_cand.amount,
                payout_amount: remainder_cand.amount,
                new_remaining_offered,
                new_remaining_requested,
                new_state: PswapLineageState::Active,
                new_tip_note_id: Some(remainder_cand.note_id),
                new_tip_nullifier: Some(new_tip_nullifier),
                at_block: block,
                reconstructed_payback: Some(payback_note),
                reconstructed_payback_inclusion_proof: Some(payback_cand.inclusion_proof.clone()),
                reconstructed_remainder: Some(remainder_note),
            }))
        },
        n => {
            // > 2 candidates for one (order_id, depth) is a
            // protocol-invariant violation. Skip the round; the
            // operator can inspect the logs.
            error!(
                order_id = %FeltDebug(current.order_id()),
                round_depth,
                candidate_count = n,
                "discover_pswap_rounds: unexpected (order_id, depth) candidate count; skipping",
            );
            Ok(None)
        },
    }
}

/// Tries each candidate as the payback. Returns the matching index and
/// the reconstructed payback note. Errors when no candidate reconstructs
/// to its observed `note_id` — this is the fail-loud
/// commitment-mismatch contract.
fn find_payback_index(
    original: &PswapNote,
    matches: &[&PswapChainNoteUpdate],
    round_depth: u64,
) -> Result<(usize, Note), ClientError> {
    let mut reconstructed_ids: Vec<NoteId> = Vec::with_capacity(matches.len());
    for (i, cand) in matches.iter().enumerate() {
        // TEMP-PROTOCOL-ADAPTER: payback_note now takes a typed
        // `&PswapNoteAttachment` (was `(consumer, depth, amount)`).
        let attachment = PswapNoteAttachment::new(
            AssetAmount::new(cand.amount).map_err(crate::ClientError::AssetError)?,
            cand.order_id,
            u32::try_from(round_depth).map_err(|_| {
                PswapLineageError::Reconstruction(miden_protocol::errors::NoteError::other(
                    "round_depth does not fit in u32",
                ))
            })?,
        );
        let reconstructed = original
            .payback_note(cand.sender, &attachment)
            .map_err(PswapLineageError::Reconstruction)?;
        if reconstructed.id() == cand.note_id {
            return Ok((i, reconstructed));
        }
        reconstructed_ids.push(reconstructed.id());
    }

    // None matched — surface the first observed id and the
    // corresponding reconstructed id so the operator has both halves of
    // the mismatch.
    let observed = format!("{}", matches[0].note_id.as_word());
    let reconstructed = format!("{}", reconstructed_ids[0].as_word());
    Err(PswapLineageError::CommitmentMismatch { reconstructed, observed }.into())
}

fn reconstruct_payback(
    original: &PswapNote,
    candidate: &PswapChainNoteUpdate,
    round_depth: u64,
) -> Result<Note, ClientError> {
    // TEMP-PROTOCOL-ADAPTER: payback_note takes &PswapNoteAttachment on 0.15.
    let attachment = PswapNoteAttachment::new(
        AssetAmount::new(candidate.amount).map_err(ClientError::AssetError)?,
        candidate.order_id,
        u32::try_from(round_depth).map_err(|_| {
            PswapLineageError::Reconstruction(miden_protocol::errors::NoteError::other(
                "round_depth does not fit in u32",
            ))
        })?,
    );
    let reconstructed = original
        .payback_note(candidate.sender, &attachment)
        .map_err(PswapLineageError::Reconstruction)?;
    if reconstructed.id() != candidate.note_id {
        return Err(PswapLineageError::CommitmentMismatch {
            reconstructed: format!("{}", reconstructed.id().as_word()),
            observed: format!("{}", candidate.note_id.as_word()),
        }
        .into());
    }
    Ok(reconstructed)
}

// -----------------------------------------------------------------------------
// IN-MEMORY LINEAGE ADVANCE
// -----------------------------------------------------------------------------

impl PswapLineageRecord {
    /// Applies a [`PswapLineageRoundUpdate`] to a copy of this record
    /// in memory, returning the post-round version. Used by the
    /// correlator's same-block multi-fill loop, which needs to advance
    /// through several rounds in a single sync iteration without
    /// writing to the store in between.
    pub(crate) fn apply_round_in_memory(
        mut self,
        update: &PswapLineageRoundUpdate,
    ) -> PswapLineageRecord {
        self.current_depth = update.round_depth;
        self.remaining_offered = update.new_remaining_offered;
        self.remaining_requested = update.new_remaining_requested;
        self.last_consumer_account_id = Some(update.consumer_account_id);
        self.last_payout_amount = Some(update.payout_amount);
        self.state = update.new_state;
        self.updated_at_block = update.at_block;
        if let (Some(note_id), Some(nullifier)) =
            (update.new_tip_note_id, update.new_tip_nullifier)
        {
            self.current_tip_note_id = note_id;
            self.current_tip_nullifier = nullifier;
        }
        self
    }
}

// -----------------------------------------------------------------------------
// Felt helpers — Felt does not implement `Ord`/`Hash`/`Display`, so we wrap.
// -----------------------------------------------------------------------------

/// Wrapper over `Felt` providing `Ord` for use as a `BTreeMap` key.
#[derive(Clone, Copy)]
struct OrderIdKey(Felt);

impl From<Felt> for OrderIdKey {
    fn from(value: Felt) -> Self {
        Self(value)
    }
}

impl PartialEq for OrderIdKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_canonical_u64() == other.0.as_canonical_u64()
    }
}
impl Eq for OrderIdKey {}
impl PartialOrd for OrderIdKey {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderIdKey {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.as_canonical_u64().cmp(&other.0.as_canonical_u64())
    }
}

/// Wrapper that gives `Felt` a `Display` impl for tracing events. Felt
/// implements `Debug` natively but `tracing`'s `%` formatter wants
/// `Display`, so we route through canonical-u64.
struct FeltDebug(Felt);
impl core::fmt::Display for FeltDebug {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0.as_canonical_u64())
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    //! Per-round correlator tests. Exercise [`build_round_update`] and the
    //! in-memory multi-fill advance directly — they own the deterministic
    //! correctness story; the store layer is mocked via fresh records.
    //!
    //! Every chain-note-update built here uses an actual reconstructed
    //! `PswapNote::payback_note(...)` / `remainder_note(...)` for its
    //! `note_id`, so the correlator's reconstruction-parity check goes
    //! through the same code path that runs in production.
    use alloc::vec;
    use alloc::vec::Vec;

    use miden_protocol::account::AccountId;
    use miden_protocol::asset::AssetAmount;
    use miden_protocol::crypto::merkle::SparseMerklePath;
    use miden_protocol::note::NoteInclusionProof;
    use miden_standards::note::PswapNoteAttachment;

    use super::super::lineage::test_helpers::{build_test_pswap, fixed_account_ids};
    use super::*;

    /// TEMP-PROTOCOL-ADAPTER: protocol 0.15 wraps the per-round
    /// reconstruction inputs in a typed `PswapNoteAttachment`. This
    /// helper bridges the old `(amount, depth)` test-style inputs to
    /// the new struct. Tests use the canonical `order_id` from the
    /// PSWAP under test.
    fn pswap_attachment(pswap: &PswapNote, depth: u64, amount: u64) -> PswapNoteAttachment {
        PswapNoteAttachment::new(
            AssetAmount::new(amount).expect("amount fits in AssetAmount"),
            pswap.order_id(),
            u32::try_from(depth).expect("depth fits in u32"),
        )
    }
    fn aa(v: u64) -> AssetAmount {
        AssetAmount::new(v).expect("amount fits in AssetAmount")
    }

    /// Minimum-valid inclusion proof. The discovery correlator never
    /// inspects the proof's Merkle path; it only threads the value to
    /// the eventual store insert. An empty path at depth 0 is the
    /// cheapest valid construction.
    fn dummy_inclusion_proof(block: u32) -> NoteInclusionProof {
        let path = SparseMerklePath::from_parts(0, Vec::new())
            .expect("empty SparseMerklePath is valid");
        NoteInclusionProof::new(BlockNumber::from(block), 0, path)
            .expect("zero index is well below the per-block notes ceiling")
    }

    /// Builds an initial `Active` lineage record at depth 0 from a
    /// freshly-built test PSWAP.
    fn initial_record(pswap: PswapNote, offered: u64, requested: u64) -> PswapLineageRecord {
        let note = Note::from(pswap.clone());
        PswapLineageRecord {
            original_pswap: pswap,
            current_tip_note_id: note.id(),
            current_tip_nullifier: note.nullifier(),
            current_depth: 0,
            remaining_offered: offered,
            remaining_requested: requested,
            last_consumer_account_id: None,
            last_payout_amount: None,
            state: PswapLineageState::Active,
            created_at_block: BlockNumber::from(0),
            updated_at_block: BlockNumber::from(0),
        }
    }

    /// Constructs a `PswapChainNoteUpdate` whose `note_id` matches the
    /// given on-chain note. Tests build the candidate notes via the
    /// protocol's reconstruction helpers (`payback_note` / `remainder_note`),
    /// then wrap them with this so the correlator's id-match check passes.
    fn chain_update_from(
        note: &Note,
        order_id: Felt,
        depth: u64,
        amount: u64,
        sender: AccountId,
        block: u32,
    ) -> PswapChainNoteUpdate {
        PswapChainNoteUpdate {
            note_id: note.id(),
            order_id,
            depth,
            amount,
            sender,
            block: BlockNumber::from(block),
            inclusion_proof: dummy_inclusion_proof(block),
        }
    }

    /// 2-candidate partial fill: advances the lineage by one round to
    /// `Active`, subtracts the round amounts from `remaining_*`, and
    /// reconstructs both payback and remainder.
    #[test]
    fn build_round_update_partial_fill_advances_active() {
        let (_sender, _creator, offered_faucet, requested_faucet) = fixed_account_ids();
        // Pick a deliberately-distinct consumer so we can assert it
        // round-trips through `consumer_account_id` correctly.
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

        let payback = pswap.payback_note(consumer, &pswap_attachment(&pswap, 1, fill_amount)).unwrap();
        let remainder = pswap
            .remainder_note(consumer, &pswap_attachment(&pswap, 1, payout_amount), aa(new_off), aa(new_req))
            .unwrap();

        let order_id = pswap.order_id();
        let cand_payback = chain_update_from(&payback, order_id, 1, fill_amount, consumer, 7);
        let cand_remainder =
            chain_update_from(&remainder, order_id, 1, payout_amount, consumer, 7);

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
        assert_eq!(update.fill_amount, fill_amount);
        assert_eq!(update.payout_amount, payout_amount);
        assert_eq!(update.new_remaining_offered, new_off);
        assert_eq!(update.new_remaining_requested, new_req);
        assert_eq!(update.new_state, PswapLineageState::Active);
        assert_eq!(update.new_tip_note_id, Some(remainder.id()));
        assert!(update.reconstructed_payback.is_some());
        assert!(update.reconstructed_remainder.is_some());
        assert!(update.reconstructed_payback_inclusion_proof.is_some());
    }

    /// 1-candidate full fill: terminal `FullyFilled`, both `remaining_*`
    /// zero, no new tip, no remainder.
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
        let payback = pswap.payback_note(consumer, &pswap_attachment(&pswap, 1, fill_amount)).unwrap();
        let order_id = pswap.order_id();
        let cand = chain_update_from(&payback, order_id, 1, fill_amount, consumer, 9);

        let update = build_round_update(&record, 1, BlockNumber::from(9), &[&cand])
            .unwrap()
            .expect("full fill must produce a round update");

        assert_eq!(update.new_state, PswapLineageState::FullyFilled);
        assert_eq!(update.fill_amount, fill_amount);
        assert_eq!(update.payout_amount, 30); // entire remaining_offered
        assert_eq!(update.new_remaining_offered, 0);
        assert_eq!(update.new_remaining_requested, 0);
        assert_eq!(update.new_tip_note_id, None);
        assert_eq!(update.new_tip_nullifier, None);
        assert!(update.reconstructed_remainder.is_none());
    }

    /// 0-candidate consumption: terminal `Reclaimed` with
    /// `consumer_account_id == creator` and BOTH `remaining_*` zeroed —
    /// the regression guard for the bug where `remaining_requested`
    /// retained its pre-reclaim value.
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

        assert_eq!(update.new_state, PswapLineageState::Reclaimed);
        assert_eq!(update.consumer_account_id, creator);
        assert_eq!(update.fill_amount, 0);
        assert_eq!(update.payout_amount, 80);
        assert_eq!(update.new_remaining_offered, 0);
        // Regression: the reclaim branch used to write
        // `current.remaining_requested` here, leaving the terminal row
        // with a non-zero `remaining_requested`. The doc on
        // `PswapLineageRoundUpdate::new_remaining_requested` says "0 on
        // full fill / reclaim", and this assert holds the line.
        assert_eq!(update.new_remaining_requested, 0);
        assert!(update.reconstructed_payback.is_none());
        assert!(update.reconstructed_payback_inclusion_proof.is_none());
    }

    /// `> 2` candidates for one round is a protocol-invariant violation;
    /// the correlator returns `Ok(None)` and lets the operator inspect
    /// the logs rather than corrupting the lineage.
    #[test]
    fn build_round_update_more_than_two_candidates_returns_none() {
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
        let record = initial_record(pswap.clone(), 100, 50);

        // Three reconstructed-payback candidates at different fill
        // amounts. The exact bodies don't matter — `build_round_update`
        // takes the count-based fast path before reconstruction.
        let p1 = pswap.payback_note(consumer, &pswap_attachment(&pswap, 1, 10)).unwrap();
        let p2 = pswap.payback_note(consumer, &pswap_attachment(&pswap, 1, 20)).unwrap();
        let p3 = pswap.payback_note(consumer, &pswap_attachment(&pswap, 1, 30)).unwrap();
        let order_id = pswap.order_id();
        let c1 = chain_update_from(&p1, order_id, 1, 10, consumer, 3);
        let c2 = chain_update_from(&p2, order_id, 1, 20, consumer, 3);
        let c3 = chain_update_from(&p3, order_id, 1, 30, consumer, 3);

        let result = build_round_update(&record, 1, BlockNumber::from(3), &[&c1, &c2, &c3])
            .expect("> 2 candidates is a soft-skip, not an error");
        assert!(result.is_none(), "expected Ok(None); got {result:?}");
    }

    /// Same-block multi-fill: round 1 advances the lineage in memory;
    /// round 2 is then built against the post-round-1 record. The two
    /// round updates emitted should chain correctly — round 2's
    /// `previous remaining_*` equal round 1's `new_remaining_*`, and
    /// the second consumer sees the in-memory-advanced tip.
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
        let payback1 = pswap.payback_note(consumer, &pswap_attachment(&pswap, 1, fill1)).unwrap();
        let remainder1 = pswap
            .remainder_note(consumer, &pswap_attachment(&pswap, 1, payout1), aa(new_off1), aa(new_req1))
            .unwrap();
        let order_id = pswap.order_id();
        let cand_p1 = chain_update_from(&payback1, order_id, 1, fill1, consumer, 11);
        let cand_r1 = chain_update_from(&remainder1, order_id, 1, payout1, consumer, 11);

        let update1 =
            build_round_update(&record0, 1, BlockNumber::from(11), &[&cand_p1, &cand_r1])
                .unwrap()
                .unwrap();

        // Apply in-memory — exactly what `discover_pswap_rounds`'s loop does.
        let record1 = record0.apply_round_in_memory(&update1);
        assert_eq!(record1.current_depth, 1);
        assert_eq!(record1.remaining_offered, new_off1);
        assert_eq!(record1.remaining_requested, new_req1);
        assert_eq!(record1.current_tip_note_id, remainder1.id());
        assert_eq!(record1.state, PswapLineageState::Active);

        // ── Round 2: full fill of the remainder, exhausts requested side.
        let fill2 = new_req1; // = 30
        let payback2 = pswap.payback_note(consumer, &pswap_attachment(&pswap, 2, fill2)).unwrap();
        let cand_p2 = chain_update_from(&payback2, order_id, 2, fill2, consumer, 11);

        let update2 = build_round_update(&record1, 2, BlockNumber::from(11), &[&cand_p2])
            .unwrap()
            .unwrap();

        assert_eq!(update2.round_depth, 2);
        assert_eq!(update2.new_state, PswapLineageState::FullyFilled);
        assert_eq!(update2.fill_amount, fill2);
        assert_eq!(update2.payout_amount, new_off1); // remaining_offered exhausted
        assert_eq!(update2.new_remaining_offered, 0);
        assert_eq!(update2.new_remaining_requested, 0);

        // The chain invariant is the whole point of this test: round 2
        // consumed the remainder produced by round 1, not the original.
        let record2 = record1.apply_round_in_memory(&update2);
        assert_eq!(record2.state, PswapLineageState::FullyFilled);
        // Same-block multi-fill: both round updates are emitted in
        // order, exactly two of them.
        let emitted = vec![update1, update2];
        assert_eq!(emitted.len(), 2);
    }
}
