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
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{Note, NoteId, Nullifier};
use miden_standards::note::PswapNote;
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
                new_remaining_requested: current.remaining_requested,
                new_state: PswapLineageState::Reclaimed,
                new_tip_note_id: None,
                new_tip_nullifier: None,
                at_block: block,
                reconstructed_payback: None,
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

            let remainder_note = original
                .remainder_note(
                    remainder_cand.sender,
                    round_depth,
                    remainder_cand.amount,
                    new_remaining_offered,
                    new_remaining_requested,
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
        let reconstructed = original
            .payback_note(cand.sender, round_depth, cand.amount)
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
    let reconstructed = original
        .payback_note(candidate.sender, round_depth, candidate.amount)
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
