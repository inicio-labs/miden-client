//! Errors specific to PSWAP chain tracking.
//!
//! See module-level docs on [`crate::pswap`].

use alloc::string::String;

use miden_protocol::Felt;
use miden_protocol::errors::NoteError;

use crate::ClientError;
use crate::store::StoreError;

/// Failures raised by the PSWAP chain-tracking subsystem.
///
/// Variants split into three groups:
///
/// 1. Lookup / state-transition failures the caller may reasonably handle
///    (`NotFound`, `NotActive`).
/// 2. Reconstruction / persistence failures from the protocol or store
///    layers (`Reconstruction`, `Store`).
/// 3. Defensive integrity violations that indicate a protocol bug or
///    corrupted local state (`UnknownState`, `InconsistentRow`,
///    `CommitmentMismatch`). These should be loud — see the variant docs
///    for the corruption-vs-protocol-bug distinction.
///
/// Conversion to [`ClientError`] is `From`-based so call sites can use `?`
/// without manual wrapping.
#[derive(Debug, thiserror::Error)]
pub enum PswapLineageError {
    /// No `pswap_lineages` row with the given `order_id`. Caller likely
    /// passed an order this client did not originate, or a typo.
    #[error("no PSWAP lineage tracked for order_id {0}")]
    NotFound(Felt),

    /// The lineage exists but is no longer `Active` — i.e. it was already
    /// `FullyFilled` or `Reclaimed`. The terminal state's byte is included
    /// for diagnostics.
    #[error("PSWAP lineage is not active (state = {0}); no further rounds expected")]
    NotActive(u8),

    /// The current tip stored on the lineage row is missing from the
    /// expected store table. Implies a desync between `pswap_lineages` and
    /// `output_notes` (for depth 0) or a programming error in
    /// `apply_pswap_round`.
    #[error("current tip note is missing from the local store; pswap_lineages is out of sync")]
    TipMissing,

    /// The reconstruction call to [`miden_standards::note::PswapNote::payback_note`]
    /// or [`miden_standards::note::PswapNote::remainder_note`] failed. The
    /// inner `NoteError` carries the protocol-layer reason. Indicates either
    /// a protocol/client version mismatch or corrupted lineage inputs.
    #[error("PSWAP note reconstruction failed: {0}")]
    Reconstruction(#[source] NoteError),

    /// A reconstructed note's commitment / ID did not match the on-chain
    /// note we observed. This is a *fail-loud* condition — the protocol
    /// guarantees byte-identical reconstruction (see
    /// `pswap_creator_reconstructs_lineage_from_attachments` in the
    /// protocol's test suite). A mismatch means we are running against a
    /// protocol version whose helpers disagree with the on-chain script,
    /// or the lineage row is corrupted; either way, silently advancing the
    /// lineage would corrupt future state.
    #[error(
        "reconstructed PSWAP note id {reconstructed} does not match observed id {observed}; \
         lineage round skipped to avoid corruption (protocol/client version skew or row corruption)"
    )]
    CommitmentMismatch { reconstructed: String, observed: String },

    /// The SQLite backend read a `state` byte that does not correspond to
    /// any defined [`super::lineage::PswapLineageState`] variant. Implies a
    /// forward-incompatible schema version.
    #[error("unknown PSWAP lineage state byte: {0}")]
    UnknownState(u8),

    /// Defensive: a stored row's columns are mutually inconsistent — e.g.
    /// `last_consumer_account_id` is `NULL` while `current_depth > 0`.
    /// Indicates corruption or a bug in `apply_pswap_round`.
    #[error("PSWAP lineage row is internally inconsistent: {0}")]
    InconsistentRow(String),

    /// Reserved for the v2 cold-start `import_pswap_lineage` API. The v1
    /// implementation returns this from the stub so callers learn the API
    /// exists but is not yet functional.
    #[error("PSWAP chain tracking does not yet support importing a lineage from on-chain data")]
    NotImplemented,

    /// Propagated from the store layer. Kept as a distinct variant rather
    /// than collapsing into `ClientError` so callers can match specifically
    /// on PSWAP store failures.
    #[error("PSWAP store operation failed")]
    Store(#[from] StoreError),
}

impl From<PswapLineageError> for ClientError {
    fn from(value: PswapLineageError) -> Self {
        ClientError::PswapLineageError(value)
    }
}
