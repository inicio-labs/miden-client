//! Persistent record and per-round transition types for one PSWAP order.
//!
//! See module-level docs on [`crate::pswap`].

use alloc::string::String;

use miden_protocol::Felt;
use miden_protocol::Word;
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{Note, NoteId, NoteTag, NoteType, Nullifier};
use miden_standards::note::PswapNote;

use super::errors::PswapLineageError;

// PSWAP LINEAGE STATE
// ================================================================================================

/// Terminal lifecycle states of a PSWAP order.
///
/// Stored as the `state` column on the `pswap_lineages` table. The numeric
/// values are part of the on-disk encoding and must remain stable across
/// schema versions; consult `crates/sqlite-store/src/store.sql` before
/// renumbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PswapLineageState {
    /// The order is still active — `current_tip_*` columns describe a live
    /// PSWAP note that can be filled further or reclaimed.
    Active = 0,
    /// Every requested unit was filled. No more rounds will arrive; the row
    /// is kept for historical querying.
    FullyFilled = 1,
    /// The creator reclaimed the remaining offered amount via
    /// `build_pswap_cancel`. No more rounds will arrive.
    Reclaimed = 2,
}

impl PswapLineageState {
    /// Returns the byte representation used in the SQL `state` column.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parses a state byte from the SQL `state` column. Errors on unknown
    /// discriminants — defensive against forward-incompatible schema versions.
    pub fn try_from_u8(value: u8) -> Result<Self, PswapLineageError> {
        match value {
            0 => Ok(Self::Active),
            1 => Ok(Self::FullyFilled),
            2 => Ok(Self::Reclaimed),
            other => Err(PswapLineageError::UnknownState(other)),
        }
    }
}

// PSWAP LINEAGE RECORD
// ================================================================================================

/// Persistent record of one PSWAP order's chain state.
///
/// The `original_pswap` field is the source of truth for every immutable
/// "initial" detail: creator/sender, offered and requested assets, serial
/// number, note types. Access them via the convenience accessors below
/// ([`Self::order_id`], [`Self::creator_account_id`], [`Self::offered_asset`],
/// etc.) which delegate to [`PswapNote`]'s getters — no duplicate fields on
/// this record.
///
/// The mutable fields describe the live tip and per-round bookkeeping needed
/// to reconstruct it.
#[derive(Debug, Clone)]
pub struct PswapLineageRecord {
    /// The originating [`PswapNote`] at depth 0. The source of truth for
    /// every immutable "initial" field. Serialised into the
    /// `pswap_lineages.original_pswap` column.
    pub original_pswap: PswapNote,

    /// Note ID of the current live tip. Equal to `original_pswap.id()` when
    /// `current_depth == 0`; otherwise the ID of a remainder this client did
    /// not originate (reconstructible via
    /// [`PswapNote::remainder_note`] with the `last_*` fields below).
    pub current_tip_note_id: NoteId,
    /// Nullifier of the current tip. Indexed in SQLite so the per-note
    /// observer can match incoming consumed-nullifier events against active
    /// lineages with a single query.
    pub current_tip_nullifier: Nullifier,
    /// 0 for the original tip; increments by 1 each round.
    pub current_depth: u64,
    /// Offered-asset units still unfilled at this point in the chain. Starts
    /// at `original_pswap.offered_asset().amount()` and decreases by the
    /// per-round `payout_amount` until reaching zero.
    pub remaining_offered: u64,
    /// Requested-asset units still unfilled. Starts at
    /// `original_pswap.storage().requested_asset_amount()` and decreases by
    /// the per-round `fill_amount`.
    pub remaining_requested: u64,

    /// Account that consumed the previous tip and emitted the current one.
    /// `None` iff `current_depth == 0` (the original tip was not consumed
    /// by anyone yet). Required input to [`PswapNote::remainder_note`] when
    /// the creator wants to reclaim the current tip.
    pub last_consumer_account_id: Option<AccountId>,
    /// Offered-asset units paid out in the round that produced the current
    /// tip. `None` iff `current_depth == 0`. Required input to
    /// [`PswapNote::remainder_note`].
    pub last_payout_amount: Option<u64>,

    /// Current lifecycle state — see [`PswapLineageState`].
    pub state: PswapLineageState,
    /// Block number at which the original PSWAP was submitted; useful for
    /// debugging and UI.
    pub created_at_block: BlockNumber,
    /// Block number of the most recent state-mutating round. Equals
    /// `created_at_block` immediately after creation.
    pub updated_at_block: BlockNumber,
}

impl PswapLineageRecord {
    /// `order_id == original_pswap.serial[1]` — the stable identifier shared
    /// by every note in the chain, surfaced in attachment word slot `[1]`.
    pub fn order_id(&self) -> Felt {
        self.original_pswap.order_id()
    }

    /// ID of the original (depth-0) PSWAP note.
    pub fn initial_note_id(&self) -> NoteId {
        Note::from(self.original_pswap.clone()).id()
    }

    /// Account that created the order — recipient of every payback in the
    /// chain.
    pub fn creator_account_id(&self) -> AccountId {
        self.original_pswap.storage().creator_account_id()
    }

    /// Account that submitted the create transaction; equals
    /// [`Self::creator_account_id`] in the v1 flow but the protocol does not
    /// require it.
    pub fn sender_account_id(&self) -> AccountId {
        self.original_pswap.sender()
    }

    /// Asset offered by the creator (and progressively paid out to fillers
    /// as remainder amounts).
    pub fn offered_asset(&self) -> &FungibleAsset {
        self.original_pswap.offered_asset()
    }

    /// Asset the creator wants in exchange (paid back to the creator across
    /// rounds).
    pub fn requested_asset(&self) -> &FungibleAsset {
        self.original_pswap.storage().requested_asset()
    }

    /// `NoteType` of the original PSWAP — also the type of every remainder
    /// emitted along the chain.
    pub fn note_type(&self) -> NoteType {
        self.original_pswap.note_type()
    }

    /// `NoteType` configured for the per-round P2ID payback notes.
    pub fn payback_note_type(&self) -> NoteType {
        self.original_pswap.storage().payback_note_type()
    }

    /// Word containing the original PSWAP's serial number — needed to call
    /// [`PswapNote::payback_note`] / [`PswapNote::remainder_note`] for
    /// arbitrary depths.
    pub fn initial_serial_number(&self) -> Word {
        self.original_pswap.serial_number()
    }

    /// Cached asset-pair tag — registered at lineage creation so sync
    /// returns every remainder in this chain.
    ///
    /// Computed via [`PswapNote::create_tag`] from the immutable note type
    /// and asset pair; deterministic and cheap.
    pub fn asset_pair_tag(&self) -> NoteTag {
        PswapNote::create_tag(self.note_type(), self.offered_asset(), self.requested_asset())
    }
}

// PSWAP LINEAGE ROUND UPDATE
// ================================================================================================

/// One round's transition, produced by the post-sync correlator
/// (`discover_pswap_rounds`) and applied atomically by
/// `Store::apply_pswap_round`.
///
/// Every PSWAP lineage advances in rounds: a fill consumes the current tip
/// and emits at most one payback + one remainder. A reclaim consumes the
/// tip with no outputs. This struct captures one such transition end to
/// end, including the reconstructed notes the correlator built and verified
/// against the on-chain note IDs.
#[derive(Debug, Clone)]
pub struct PswapLineageRoundUpdate {
    /// Identifies which lineage this update targets.
    pub order_id: Felt,
    /// `previous_depth + 1`. The protocol's PSWAP script stamps this in the
    /// attachment word of every output note emitted in this round.
    pub round_depth: u64,
    /// Account that consumed the previous tip and emitted the new outputs.
    /// For a reclaim, equals the creator.
    pub consumer_account_id: AccountId,
    /// Requested-asset units the consumer filled this round. Read from
    /// the payback's attachment word slot `[0]`; falls back to
    /// `previous_remaining_requested` for a terminal full-fill that emits
    /// no remainder.
    pub fill_amount: u64,
    /// Offered-asset units paid out to the consumer this round. Read from
    /// the remainder's attachment word slot `[0]`; equals
    /// `previous_remaining_offered` for a terminal full-fill or a reclaim.
    pub payout_amount: u64,
    /// `previous_remaining_offered - payout_amount` (0 on full fill /
    /// reclaim).
    pub new_remaining_offered: u64,
    /// `previous_remaining_requested - fill_amount` (0 on full fill /
    /// reclaim).
    pub new_remaining_requested: u64,
    /// Terminal state after this round — `Active` if a new remainder was
    /// produced, `FullyFilled` if the requested side was exhausted, or
    /// `Reclaimed` if the consumer is the creator and no outputs were
    /// emitted.
    pub new_state: PswapLineageState,
    /// Identity of the new tip (the remainder). `None` for terminal states.
    pub new_tip_note_id: Option<NoteId>,
    /// Nullifier of the new tip. `None` for terminal states.
    pub new_tip_nullifier: Option<Nullifier>,
    /// Block number in which the previous tip was consumed.
    pub at_block: BlockNumber,
    /// Reconstructed payback note built via [`PswapNote::payback_note`]. The
    /// correlator verified `reconstructed.id() == observed.note_id` before
    /// emitting this update. The store inserts this into `input_notes`
    /// (idempotent on `note_id` PK) so the creator's normal consume flow
    /// finds it. `None` only on a reclaim, where no payback is emitted.
    pub reconstructed_payback: Option<Note>,
    /// Reconstructed remainder note built via [`PswapNote::remainder_note`].
    /// The correlator verified its `note_id` matched. `None` on terminal
    /// states. Retained for diagnostics; not persisted directly.
    pub reconstructed_remainder: Option<Note>,
}

// PSWAP LINEAGE FILTER
// ================================================================================================

/// Filter for [`crate::store::Store::list_pswap_lineages`].
#[derive(Debug, Clone)]
pub enum PswapLineageFilter {
    /// Return every row in the table.
    All,
    /// Return only rows whose `state == PswapLineageState::Active`.
    Active,
    /// Return rows whose `creator_account_id` matches.
    ByCreator(AccountId),
    /// Return at most one row whose `order_id` matches.
    ByOrderId(Felt),
}

// SERDE HELPERS
// ================================================================================================

/// Builds a [`PswapLineageRecord`] from the column-level data the SQLite
/// backend reads back, validating the discriminants.
///
/// Kept in the rust-client crate (rather than the SQLite store crate) so
/// alternative backends can reuse the parsing logic.
pub fn build_record_from_columns(
    original_pswap: PswapNote,
    current_tip_note_id: NoteId,
    current_tip_nullifier: Nullifier,
    current_depth: u64,
    remaining_offered: u64,
    remaining_requested: u64,
    last_consumer_account_id: Option<AccountId>,
    last_payout_amount: Option<u64>,
    state_byte: u8,
    created_at_block: BlockNumber,
    updated_at_block: BlockNumber,
) -> Result<PswapLineageRecord, PswapLineageError> {
    // Sanity: `last_*` columns must be present iff depth > 0. Persisting
    // them inconsistently would silently break reclaim reconstruction.
    let depth_is_zero = current_depth == 0;
    let last_consumer_present = last_consumer_account_id.is_some();
    let last_payout_present = last_payout_amount.is_some();
    if depth_is_zero != !last_consumer_present || depth_is_zero != !last_payout_present {
        return Err(PswapLineageError::InconsistentRow(String::from(
            "last_consumer_account_id and last_payout_amount must both be set iff current_depth > 0",
        )));
    }

    Ok(PswapLineageRecord {
        original_pswap,
        current_tip_note_id,
        current_tip_nullifier,
        current_depth,
        remaining_offered,
        remaining_requested,
        last_consumer_account_id,
        last_payout_amount,
        state: PswapLineageState::try_from_u8(state_byte)?,
        created_at_block,
        updated_at_block,
    })
}

