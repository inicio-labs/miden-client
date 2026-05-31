//! Persistent record and per-round transition types for one PSWAP order.
//!
//! See module-level docs on [`crate::pswap`].

use alloc::string::String;

use miden_protocol::Felt;
use miden_protocol::Word;
use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{Note, NoteId, NoteInclusionProof, NoteTag, NoteType, Nullifier};
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
    /// Inclusion proof of the payback note in the block where it was
    /// emitted. Threaded all the way from
    /// `PswapChainObserver::observe` (which captures it from the
    /// `CommittedNote` it sees during sync) so the store can insert
    /// the reconstructed payback in `Unverified` state — ready for
    /// the normal sync state-promotion path. Without it the payback
    /// would land in `Expected` state forever, since the default
    /// `NoteScreener` Discards private notes it does not already
    /// track and so never sees the payback again on a subsequent sync.
    /// `None` exactly when `reconstructed_payback.is_none()` —
    /// i.e. for reclaim rounds.
    pub reconstructed_payback_inclusion_proof: Option<NoteInclusionProof>,
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
#[cfg_attr(any(test, feature = "testing"), allow(clippy::too_many_arguments))]
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

#[cfg(test)]
pub(crate) mod test_helpers {
    //! Small synthetic-PSWAP factory shared by the lineage / observer /
    //! discovery / store tests. Kept in `pub(crate)` so each module can
    //! import without re-deriving the boilerplate.

    use miden_protocol::Word;
    use miden_protocol::account::AccountId;
    use miden_protocol::asset::FungibleAsset;
    use miden_protocol::note::NoteType;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET,
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    /// Returns `(sender, creator, offered_faucet, requested_faucet)` —
    /// four distinct testing AccountIds chosen to satisfy PSWAP's
    /// faucet-distinctness invariant.
    pub fn fixed_account_ids() -> (AccountId, AccountId, AccountId, AccountId) {
        (
            AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE).unwrap(),
            AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2).unwrap(),
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap(),
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap(),
        )
    }

    /// Builds a fully-formed [`PswapNote`] for use in tests. Defaults:
    /// public note type, 100-unit offered, 50-unit requested, serial
    /// number `[1, 2, 3, 4]`. Override via the params.
    pub fn build_test_pswap(
        sender: AccountId,
        creator: AccountId,
        offered_faucet: AccountId,
        offered_amount: u64,
        requested_faucet: AccountId,
        requested_amount: u64,
    ) -> PswapNote {
        let offered = FungibleAsset::new(offered_faucet, offered_amount).unwrap();
        let requested = FungibleAsset::new(requested_faucet, requested_amount).unwrap();
        let storage = PswapNoteStorage::builder()
            .requested_asset(requested)
            .creator_account_id(creator)
            .build();
        PswapNote::builder()
            .sender(sender)
            .storage(storage)
            .serial_number(Word::from([
                miden_protocol::Felt::new(1).unwrap(),
                miden_protocol::Felt::new(2).unwrap(),
                miden_protocol::Felt::new(3).unwrap(),
                miden_protocol::Felt::new(4).unwrap(),
            ]))
            .note_type(NoteType::Public)
            .offered_asset(offered)
            .build()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use miden_protocol::Word;

    use super::test_helpers::{build_test_pswap, fixed_account_ids};
    use super::*;

    /// Stable byte encoding of `PswapLineageState`. The values are
    /// persisted in the `pswap_lineages.state` SQL column; reordering
    /// would silently corrupt existing databases.
    #[test]
    fn state_byte_encoding_is_stable() {
        assert_eq!(PswapLineageState::Active.as_u8(), 0);
        assert_eq!(PswapLineageState::FullyFilled.as_u8(), 1);
        assert_eq!(PswapLineageState::Reclaimed.as_u8(), 2);
    }

    /// Round-trip every state via `try_from_u8`. Belt-and-suspenders
    /// against a future renumbering breaking the on-disk format.
    #[test]
    fn state_try_from_u8_round_trips_known_variants() {
        for state in
            [PswapLineageState::Active, PswapLineageState::FullyFilled, PswapLineageState::Reclaimed]
        {
            assert_eq!(PswapLineageState::try_from_u8(state.as_u8()).unwrap(), state);
        }
    }

    /// Unknown discriminants must error — defends against a future
    /// store reading a forward-incompatible byte.
    #[test]
    fn state_try_from_u8_rejects_unknown() {
        match PswapLineageState::try_from_u8(99) {
            Err(PswapLineageError::UnknownState(99)) => {},
            other => panic!("expected UnknownState(99), got {other:?}"),
        }
    }

    /// Happy path for `build_record_from_columns` at depth 0 — both
    /// `last_*` columns are `None`, every field carries through.
    #[test]
    fn build_record_from_columns_accepts_valid_depth_zero_row() {
        let (sender, creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let pswap =
            build_test_pswap(sender, creator, offered_faucet, 100, requested_faucet, 50);
        let initial_note_id = miden_protocol::note::Note::from(pswap.clone()).id();
        let nullifier = miden_protocol::note::Note::from(pswap.clone()).nullifier();

        let record = build_record_from_columns(
            pswap,
            initial_note_id,
            nullifier,
            0,
            100,
            50,
            None,
            None,
            PswapLineageState::Active.as_u8(),
            BlockNumber::from(7),
            BlockNumber::from(7),
        )
        .unwrap();

        assert_eq!(record.current_depth, 0);
        assert_eq!(record.remaining_offered, 100);
        assert_eq!(record.remaining_requested, 50);
        assert!(record.last_consumer_account_id.is_none());
        assert!(record.last_payout_amount.is_none());
        assert_eq!(record.state, PswapLineageState::Active);
    }

    /// Happy path at `current_depth > 0` — both `last_*` columns MUST
    /// be populated.
    #[test]
    fn build_record_from_columns_accepts_valid_advanced_row() {
        let (sender, creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let consumer = sender; // any non-creator account; reuse for brevity
        let pswap =
            build_test_pswap(sender, creator, offered_faucet, 100, requested_faucet, 50);
        let note = miden_protocol::note::Note::from(pswap.clone());
        let record = build_record_from_columns(
            pswap,
            note.id(),
            note.nullifier(),
            3,
            70,
            35,
            Some(consumer),
            Some(20),
            PswapLineageState::Active.as_u8(),
            BlockNumber::from(7),
            BlockNumber::from(12),
        )
        .unwrap();

        assert_eq!(record.last_consumer_account_id, Some(consumer));
        assert_eq!(record.last_payout_amount, Some(20));
    }

    /// `current_depth == 0` with a populated `last_consumer` is the
    /// classic inconsistency that breaks `remainder_note` reconstruction
    /// (it implies a foreign account consumed the original PSWAP but we
    /// somehow have round-0 state). Must surface as `InconsistentRow`.
    #[test]
    fn build_record_from_columns_rejects_depth_zero_with_last_consumer() {
        let (sender, creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let pswap =
            build_test_pswap(sender, creator, offered_faucet, 100, requested_faucet, 50);
        let note = miden_protocol::note::Note::from(pswap.clone());
        match build_record_from_columns(
            pswap,
            note.id(),
            note.nullifier(),
            0,
            100,
            50,
            Some(sender), // ← inconsistent: depth 0 but last_consumer set
            None,
            PswapLineageState::Active.as_u8(),
            BlockNumber::from(0),
            BlockNumber::from(0),
        ) {
            Err(PswapLineageError::InconsistentRow(_)) => {},
            other => panic!("expected InconsistentRow, got {other:?}"),
        }
    }

    /// `current_depth > 0` with `last_payout_amount` NULL breaks the
    /// remainder reconstruction path used by reclaim. Must surface as
    /// `InconsistentRow`.
    #[test]
    fn build_record_from_columns_rejects_advanced_depth_without_last_payout() {
        let (sender, creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let pswap =
            build_test_pswap(sender, creator, offered_faucet, 100, requested_faucet, 50);
        let note = miden_protocol::note::Note::from(pswap.clone());
        match build_record_from_columns(
            pswap,
            note.id(),
            note.nullifier(),
            1,
            50,
            25,
            Some(sender),
            None, // ← inconsistent: depth > 0 but last_payout NULL
            PswapLineageState::Active.as_u8(),
            BlockNumber::from(0),
            BlockNumber::from(0),
        ) {
            Err(PswapLineageError::InconsistentRow(_)) => {},
            other => panic!("expected InconsistentRow, got {other:?}"),
        }
    }

    /// Unknown state discriminant in the row bubbles up as
    /// `UnknownState`. Reused: the same validation also covers schema-
    /// drift defense for the `state` column.
    #[test]
    fn build_record_from_columns_rejects_unknown_state() {
        let (sender, creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let pswap =
            build_test_pswap(sender, creator, offered_faucet, 100, requested_faucet, 50);
        let note = miden_protocol::note::Note::from(pswap.clone());
        match build_record_from_columns(
            pswap,
            note.id(),
            note.nullifier(),
            0,
            100,
            50,
            None,
            None,
            42,
            BlockNumber::from(0),
            BlockNumber::from(0),
        ) {
            Err(PswapLineageError::UnknownState(42)) => {},
            other => panic!("expected UnknownState(42), got {other:?}"),
        }
    }

    /// `asset_pair_tag()` and `order_id()` accessors delegate to the
    /// stored `PswapNote` rather than persisting the values
    /// separately. Verifies the delegation is consistent (no column
    /// duplication that could drift from the blob).
    #[test]
    fn accessors_delegate_to_stored_pswap_note() {
        let (sender, creator, offered_faucet, requested_faucet) = fixed_account_ids();
        let pswap =
            build_test_pswap(sender, creator, offered_faucet, 100, requested_faucet, 50);

        let expected_order_id = pswap.order_id();
        let expected_tag =
            miden_standards::note::PswapNote::create_tag(
                pswap.note_type(),
                pswap.offered_asset(),
                pswap.storage().requested_asset(),
            );

        let note = miden_protocol::note::Note::from(pswap.clone());
        let record = PswapLineageRecord {
            original_pswap: pswap,
            current_tip_note_id: note.id(),
            current_tip_nullifier: note.nullifier(),
            current_depth: 0,
            remaining_offered: 100,
            remaining_requested: 50,
            last_consumer_account_id: None,
            last_payout_amount: None,
            state: PswapLineageState::Active,
            created_at_block: BlockNumber::from(0),
            updated_at_block: BlockNumber::from(0),
        };

        assert_eq!(record.order_id(), expected_order_id);
        assert_eq!(record.asset_pair_tag(), expected_tag);
        assert_eq!(record.creator_account_id(), creator);

        // Silence Word-unused warning from the test_helpers import.
        let _ = Word::default();
    }
}

