//! Generic observer trait for side-effect-only watching of per-note
//! arrivals during sync.
//!
//! ## Why this exists
//!
//! [`OnNoteReceived`] (sibling trait, defined in `state_sync.rs`) is the
//! per-note *decision* hook: every implementation returns exactly one
//! [`NoteUpdateAction`] (Commit / Insert / Discard) and shapes how that
//! note flows through the rest of the sync pipeline. There is one
//! screener active per `sync_state` call, by design.
//!
//! `NoteObserver` is the per-note *recording* hook: implementations
//! cannot influence what `StateSync` does with a note; they only get to
//! see each note as it arrives and update feature-specific state on the
//! side. Several observers can be attached to the same sync round (PSWAP
//! chain tracking, future lineage features, dApp event mirrors, etc.)
//! and they run independently.
//!
//! Keeping the two concerns in separate traits is deliberate:
//!
//! - The screener's contract (single Commit/Insert/Discard verdict per
//!   note) is unaffected by however many observers are running. A
//!   reviewer of `NoteScreener` does not have to learn about observers
//!   to understand it, and a downstream substituting `NoteScreener` does
//!   not silently lose observability.
//! - Observers fail open: errors are logged and sync continues. Mixing
//!   that policy into the screener's `?`-propagating signature would
//!   either widen the screener's contract or force per-observer error
//!   handling inside the screener, both regressions.
//!
//! See `crate::pswap::observer::PswapChainObserver` for the first
//! production implementation, and `crate::sync::state_sync::StateSync`
//! for the dispatch site (`note_state_sync` fans out to all attached
//! observers after invoking the screener).

use alloc::boxed::Box;

use async_trait::async_trait;

use crate::ClientError;
use crate::rpc::domain::note::CommittedNote;

/// Side-effect-only observer of note arrivals during sync.
///
/// One instance can be attached to a [`crate::sync::StateSync`] via
/// `StateSync::with_note_observer(...)`. The orchestrator calls
/// [`Self::observe`] after the per-note screener decision, with the
/// same `CommittedNote` the screener received.
///
/// ## Contract
///
/// - **Side-effect only.** Implementations must not depend on observing
///   notes in any particular order beyond what `StateSync::sync_state`
///   produces. They must not assume that consuming `&self` implies
///   exclusive access; the same observer may be invoked by multiple
///   sync rounds concurrently if a caller spawns several
///   `StateSync::sync_state` futures from one shared observer.
/// - **Failure mode.** Errors returned from [`Self::observe`] are
///   logged by the orchestrator (`StateSync::note_state_sync` uses the
///   value returned by [`Self::name`] as the tracing field) and
///   **never** abort sync. If an observer needs hard-failure semantics,
///   it must surface them through some other path (e.g. by marking
///   internal state and failing the next call). This is the policy
///   that lets observers be attached freely without widening sync's
///   blast radius.
/// - **Cardinality.** Several observers can be attached to one
///   `StateSync`. They run in attachment order; ordering between
///   observers is not guaranteed beyond that and implementations must
///   not depend on it.
///
/// ## Implementation note
///
/// `&self` is intentional. Observers are commonly cloneable via `Arc`
/// and shared with the code that drained their collected state (e.g.
/// the per-sync collector held by `PswapChainObserver` is also held by
/// the post-sync correlator). If you need interior mutability, use
/// `Mutex` / `RwLock` from `crate::utils` so the observer remains
/// `Send + Sync` and no-std compatible. Avoid `&mut self`: it would
/// force every caller into exclusive ownership of the observer for the
/// lifetime of one `sync_state` call, defeating the per-sync drain
/// pattern.
#[async_trait(?Send)]
pub trait NoteObserver {
    /// Short, stable identifier used as the `observer` field on
    /// `tracing::warn!` events when [`Self::observe`] returns `Err`.
    /// Implementations should return a `&'static str` (e.g.
    /// `"PswapChainObserver"`) so logs are greppable.
    fn name(&self) -> &'static str;

    /// Called once per `CommittedNote` arriving in the sync window,
    /// after the screener verdict.
    ///
    /// Errors are logged by the orchestrator and never abort sync (see
    /// the trait-level "Failure mode" docs). Returning `Ok(())` for the
    /// "not interested in this note" case is the expected idiom;
    /// `Err(_)` should be reserved for genuine internal failures worth
    /// surfacing in operational logs.
    async fn observe(&self, committed_note: &CommittedNote) -> Result<(), ClientError>;
}
