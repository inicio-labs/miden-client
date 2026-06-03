//! Persistent record and per-round transition types for one PSWAP order.
//!
//! See module-level docs on [`crate::pswap`].

use alloc::format;
use alloc::vec::Vec;

use miden_protocol::Felt;
use miden_protocol::Word;
use miden_protocol::account::AccountId;
use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::block::BlockNumber;
use miden_protocol::note::{Note, NoteId, NoteInclusionProof, NoteTag, NoteType, Nullifier};
use miden_standards::note::PswapNote;

use super::errors::PswapLineageError;

// PSWAP LINEAGE STATE
// ================================================================================================

/// Lifecycle state of a PSWAP order. Numeric values are part of the
/// on-disk encoding — do not renumber (see `sqlite-store/src/store.sql`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PswapLineageState {
    /// Still fillable / reclaimable.
    Active = 0,
    /// Fully filled. Terminal.
    FullyFilled = 1,
    /// Reclaimed by the creator. Terminal.
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
    /// 0 for the original tip; increments by 1 each round. Matches the
    /// protocol's `PswapNoteAttachment::depth()` (u32).
    pub current_depth: u32,
    /// Offered-asset units still unfilled at this point in the chain. Starts
    /// at `original_pswap.offered_asset().amount()` and decreases by the
    /// per-round `payout_amount` until reaching zero. Typed as
    /// [`AssetAmount`] so the `<= AssetAmount::MAX` invariant is enforced
    /// at every construction site; SQLite still stores as INTEGER bytes
    /// — the conversion happens at the row decoder
    /// ([`build_record_from_columns`]).
    pub remaining_offered: AssetAmount,
    /// Requested-asset units still unfilled. Starts at
    /// `original_pswap.storage().requested_asset_amount()` and decreases by
    /// the per-round `fill_amount`. See [`Self::remaining_offered`] for
    /// notes on the `AssetAmount` boundary.
    pub remaining_requested: AssetAmount,

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

    /// `order_id()` wrapped in the `Ord`/`Eq`-compatible
    /// [`crate::pswap::types::OrderIdKey`] for use as a `BTreeMap` key.
    pub(crate) fn order_id_key(&self) -> super::types::OrderIdKey {
        super::types::OrderIdKey::from(self.order_id())
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
    pub round_depth: u32,
    /// Account that consumed the previous tip and emitted the new outputs.
    /// For a reclaim, equals the creator.
    pub consumer_account_id: AccountId,
    /// Requested-asset units the consumer filled this round. Read from
    /// the payback's attachment word slot `[0]`; falls back to
    /// `previous_remaining_requested` for a terminal full-fill that emits
    /// no remainder.
    pub fill_amount: AssetAmount,
    /// Offered-asset units paid out to the consumer this round. Read from
    /// the remainder's attachment word slot `[0]`; equals
    /// `previous_remaining_offered` for a terminal full-fill or a reclaim.
    pub payout_amount: AssetAmount,
    /// Remaining offered-asset units AFTER this round (0 on full fill / reclaim).
    pub remaining_offered: AssetAmount,
    /// Remaining requested-asset units AFTER this round (0 on full fill / reclaim).
    pub remaining_requested: AssetAmount,
    /// Lineage state AFTER this round: `Active` if a remainder was emitted,
    /// `FullyFilled` if requested side exhausted, `Reclaimed` if consumer == creator
    /// with no outputs.
    pub state: PswapLineageState,
    /// Identity of the new tip (the remainder). `None` for terminal rounds.
    pub tip_note_id: Option<NoteId>,
    /// Nullifier of the new tip. `None` for terminal rounds.
    pub tip_nullifier: Option<Nullifier>,
    /// Block in which the previous tip was consumed.
    pub at_block: BlockNumber,
    /// Reconstructed payback note (verified against the observed note id).
    /// Inserted into `input_notes` so the creator's normal consume flow finds it.
    /// `None` only on a reclaim round.
    pub payback: Option<Note>,
    /// Inclusion proof for `payback`. Threaded so the store can insert the
    /// payback in `Unverified` state (skips the Expected-state limbo that
    /// would otherwise strand a private payback). `None` iff `payback.is_none()`.
    pub payback_inclusion_proof: Option<NoteInclusionProof>,
    /// Reconstructed remainder note (verified against the observed note id).
    /// Inserted into `input_notes` by `apply_pswap_round` so the remainder's
    /// nullifier is tracked by the standard nullifier-sync mechanism — needed
    /// for round N+1 detection, especially for private PSWAPs where the
    /// default `NoteScreener` doesn't pick the remainder up via the asset-pair
    /// tag. `None` for terminal rounds.
    pub remainder: Option<Note>,
    /// Inclusion proof for `remainder`. Threaded so the store can insert the
    /// remainder in `Unverified` state (same rationale as
    /// `payback_inclusion_proof`). `None` iff `remainder.is_none()`.
    pub remainder_inclusion_proof: Option<NoteInclusionProof>,
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
    /// Return Active rows whose `current_tip_nullifier` is in the given set.
    /// Empty input returns no rows. Used by the sync correlator to load only
    /// the lineages whose tip was consumed in this sync window — avoids the
    /// "load every active lineage" scan when activity is sparse. See
    /// [`crate::pswap::discovery::discover_pswap_rounds`].
    ActiveByTipNullifiers(Vec<Nullifier>),
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
    current_depth: u32,
    remaining_offered: u64,
    remaining_requested: u64,
    state_byte: u8,
    created_at_block: BlockNumber,
    updated_at_block: BlockNumber,
) -> Result<PswapLineageRecord, PswapLineageError> {
    // Validate the `<= AssetAmount::MAX` invariant at this single boundary
    // — a row exceeding MAX is corruption (or legacy from before typing).
    let to_amount = |raw: u64, field: &'static str| -> Result<AssetAmount, PswapLineageError> {
        AssetAmount::new(raw).map_err(|err| {
            PswapLineageError::InconsistentRow(format!(
                "{field} = {raw} exceeds AssetAmount::MAX: {err}"
            ))
        })
    };
    let remaining_offered = to_amount(remaining_offered, "remaining_offered")?;
    let remaining_requested = to_amount(remaining_requested, "remaining_requested")?;

    Ok(PswapLineageRecord {
        original_pswap,
        current_tip_note_id,
        current_tip_nullifier,
        current_depth,
        remaining_offered,
        remaining_requested,
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

    /// Happy path for `build_record_from_columns` at depth 0.
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
            PswapLineageState::Active.as_u8(),
            BlockNumber::from(7),
            BlockNumber::from(7),
        )
        .unwrap();

        assert_eq!(record.current_depth, 0);
        assert_eq!(record.remaining_offered, AssetAmount::new(100).unwrap());
        assert_eq!(record.remaining_requested, AssetAmount::new(50).unwrap());
        assert_eq!(record.state, PswapLineageState::Active);
    }

    /// Happy path at `current_depth > 0`.
    #[test]
    fn build_record_from_columns_accepts_valid_advanced_row() {
        let (sender, creator, offered_faucet, requested_faucet) = fixed_account_ids();
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
            PswapLineageState::Active.as_u8(),
            BlockNumber::from(7),
            BlockNumber::from(12),
        )
        .unwrap();

        assert_eq!(record.current_depth, 3);
        assert_eq!(record.remaining_offered, AssetAmount::new(70).unwrap());
    }

    /// Unknown state discriminant in the row bubbles up as `UnknownState`.
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
            remaining_offered: AssetAmount::new(100).unwrap(),
            remaining_requested: AssetAmount::new(50).unwrap(),
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

