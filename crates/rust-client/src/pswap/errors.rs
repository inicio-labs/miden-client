//! Errors specific to PSWAP chain tracking.

use alloc::string::String;

use miden_protocol::Felt;
use miden_protocol::account::AccountId;
use miden_protocol::errors::NoteError;

use super::lineage::PswapLineageState;

/// Failures raised by the PSWAP chain-tracking subsystem.
#[derive(Debug, thiserror::Error)]
pub enum PswapLineageError {
    /// No `pswap_lineages` row with the given `order_id`.
    #[error("no PSWAP lineage tracked for order_id {0}")]
    NotFound(Felt),

    /// The lineage exists but is already in a terminal state.
    #[error("PSWAP lineage is not active (state = {0:?}); no further rounds expected")]
    NotActive(PswapLineageState),

    /// The lineage's creator is not a local account — reclaim requires
    /// the creator's signing authority.
    #[error(
        "PSWAP creator account {0} is not local; reclaim requires the creator's signing authority"
    )]
    CreatorNotLocal(AccountId),

    /// The current tip is missing from the store — `pswap_lineages` is
    /// out of sync with `output_notes`/`input_notes`.
    #[error("current tip note is missing from the local store; pswap_lineages is out of sync")]
    TipMissing,

    /// `PswapNote::payback_note` / `remainder_note` reconstruction failed.
    #[error("PSWAP note reconstruction failed: {0}")]
    Reconstruction(#[source] NoteError),

    /// `SQLite` read a `state` byte with no matching [`PswapLineageState`] variant.
    #[error("unknown PSWAP lineage state byte: {0}")]
    UnknownState(u8),

    /// A stored row's columns are mutually inconsistent.
    #[error("PSWAP lineage row is internally inconsistent: {0}")]
    InconsistentRow(String),
}
