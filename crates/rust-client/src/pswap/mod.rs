//! PSWAP chain tracking for partial swap orders this client originated.
//!
//! ## What this module does
//!
//! A PSWAP (partial swap) note lets a creator post an offer that anyone can fill
//! incrementally. Each fill consumes the current "tip" PSWAP note and emits, in
//! the same transaction, a P2ID payback to the creator and a remainder PSWAP for
//! the still-unfilled portion. The remainder becomes the new tip; the chain
//! continues until fully filled or reclaimed by the creator.
//!
//! Today the client tracks only the transaction it submits — once a foreign
//! account fills the order, the creator loses sight of the chain. This module
//! adds a persistent lineage table plus a sync-time chain walker that follows
//! the order across arbitrarily many fills by other accounts, exposing each
//! round's state (current tip, remaining amounts, depth) and surfacing each
//! payback as a consumable input note in the local store.
//!
//! ## How it fits into the existing flow
//!
//! - On `build_pswap_create` submission, a [`PswapLineageRecord`] row is
//!   inserted into the new `pswap_lineages` table. The asset-pair tag is
//!   registered so sync can pick up future remainder notes.
//! - During [`crate::Client::sync_state`], a [`PswapChainObserver`] (an
//!   implementation of [`crate::sync::NoteObserver`]) inspects every incoming
//!   note for a PSWAP attachment. Notes whose `order_id` matches a tracked
//!   active lineage are pushed into a per-sync collector.
//! - After the network sync returns, [`discover_pswap_rounds`] joins the
//!   collected notes with the consumed-nullifier signal from the same sync to
//!   advance each lineage by one or more rounds. Each round produces a
//!   [`PswapLineageRoundUpdate`] that's applied atomically by the store.
//! - For reclaim, [`Client::build_pswap_cancel_by_order`] reconstructs the
//!   current tip via `PswapNote::remainder_note(...)` and delegates to the
//!   existing `build_pswap_cancel` builder.
//!
//! ## Module layout
//!
//! - [`lineage`] — types describing the persistent lineage record and a
//!   round transition.
//! - [`observer`] — the per-note observer that filters PSWAP-attachment notes
//!   for active lineages.
//! - [`discovery`] — the post-sync correlator that builds round updates.
//! - [`errors`] — error types specific to PSWAP chain tracking.
//!
//! See `/Users/vaibhavjindal/.claude/plans/plan-with-me-and-cheeky-corbato.md`
//! for the full design rationale and protocol-side contract.

pub mod discovery;
pub mod errors;
pub mod lineage;
pub mod observer;

pub use errors::PswapLineageError;
pub use lineage::{PswapLineageFilter, PswapLineageRecord, PswapLineageRoundUpdate, PswapLineageState};
pub use observer::{PswapChainNoteUpdate, PswapChainObserver};
