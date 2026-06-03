//! Side-effect-only observer trait for committed transactions.
//!
//! Analogous to [`crate::sync::NoteObserver`] but scoped to
//! `Client::apply_transaction`. Lets feature subsystems (e.g. PSWAP
//! chain tracking) hook into the post-apply pipeline without
//! `apply_transaction` knowing about them by name.

use alloc::boxed::Box;

use async_trait::async_trait;
use miden_protocol::block::BlockNumber;

use crate::ClientError;
use crate::transaction::TransactionResult;

/// Side-effect-only observer of committed transactions.
///
/// Attached to [`crate::Client`] via `Client::with_transaction_observer(...)`.
/// `observe()` runs once per transaction, AFTER the transaction's standard
/// updates have been persisted. Errors are logged (tagged with
/// [`Self::name`]) and never abort `apply_transaction`.
#[async_trait(?Send)]
pub trait TransactionObserver: Send + Sync {
    /// Short identifier for `tracing::warn!` events on `observe()` errors.
    fn name(&self) -> &'static str;

    /// Called once per `apply_transaction`, after the transaction's effects
    /// have been written to the store. Return `Ok(())` for "not interested";
    /// reserve `Err(_)` for genuine internal failures.
    async fn observe(
        &self,
        tx_result: &TransactionResult,
        submission_height: BlockNumber,
    ) -> Result<(), ClientError>;
}
