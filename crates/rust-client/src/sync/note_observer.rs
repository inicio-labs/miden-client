//! Side-effect-only observer trait for per-note arrivals during sync.
//!
//! Distinct from [`OnNoteReceived`] (the per-note *decision* hook —
//! one active screener per sync). Observers cannot influence what
//! `StateSync` does with a note; multiple can be attached and run
//! independently. Errors are logged, never aborting sync.

use alloc::boxed::Box;

use async_trait::async_trait;

use crate::ClientError;
use crate::rpc::domain::note::CommittedNote;
use crate::sync::StateSyncUpdate;

/// Side-effect-only observer of note arrivals during sync.
///
/// Attached to [`crate::sync::StateSync`] via
/// `StateSync::with_note_observer(...)`. `observe()` runs after the
/// screener verdict, with the same `CommittedNote`. Errors are logged
/// (tagged with [`Self::name`]) and never abort sync.
///
/// Takes `&self` so observers can be shared via `Arc` with code that
/// drains their collected state. Use `Mutex` / `RwLock` from
/// `crate::utils` if interior mutability is needed.
#[async_trait(?Send)]
pub trait NoteObserver {
    /// Short identifier used as the `observer` field on `tracing::warn!`
    /// events for `observe()` errors. Use a `&'static str` like
    /// `"PswapChainObserver"`.
    fn name(&self) -> &'static str;

    /// Called once per `CommittedNote` arriving in the sync window,
    /// after the screener verdict. Return `Ok(())` for the "not
    /// interested" case; reserve `Err(_)` for genuine internal failures.
    async fn observe(&self, committed_note: &CommittedNote) -> Result<(), ClientError>;

    /// Post-sync hook. Invoked by [`crate::sync::StateSync::run_apply_hooks`]
    /// once per sync, with the completed [`StateSyncUpdate`] by reference.
    /// Observers drain any per-sync collector populated by
    /// [`Self::observe`] and apply their feature-specific post-sync work
    /// (e.g. running a correlator, writing feature-specific store rows).
    ///
    /// Default impl is a no-op for simple observers that only need
    /// per-note hooks. Errors are logged via the dispatcher and never
    /// abort the rest of the apply pass.
    async fn apply(&self, _sync_update: &StateSyncUpdate) -> Result<(), ClientError> {
        Ok(())
    }
}
