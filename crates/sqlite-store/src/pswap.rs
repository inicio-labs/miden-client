//! SQLite-backed implementation of the PSWAP lineage methods on the
//! [`miden_client::store::Store`] trait.
//!
//! See `crates/rust-client/src/pswap/` for the design and types.

use std::string::String;
use std::vec::Vec;

use miden_client::account::AccountId;
use miden_client::note::{BlockNumber, Note, NoteId, Nullifier, PswapNote};
use miden_client::pswap::{
    PswapLineageError,
    PswapLineageFilter,
    PswapLineageRecord,
    PswapLineageRoundUpdate,
    PswapLineageState,
    lineage::build_record_from_columns,
};
use miden_client::store::StoreError;
use miden_client::utils::{Deserializable, DeserializationError, Serializable};
use miden_protocol::Felt;
use rusqlite::{Connection, Row, Transaction, params};

use super::SqliteStore;
use crate::note::upsert_input_note_tx;
use crate::sql_error::SqlResultExt;

impl SqliteStore {
    // ---------------------------------------------------------------------------------------
    // PSWAP LINEAGE — public entry points
    //
    // The `Store` trait impl in `lib.rs` delegates here via
    // `interact_with_connection`.
    // ---------------------------------------------------------------------------------------

    pub(crate) fn upsert_pswap_lineage(
        conn: &mut Connection,
        record: PswapLineageRecord,
    ) -> Result<(), StoreError> {
        let tx = conn.transaction().into_store_error()?;
        upsert_pswap_lineage_tx(&tx, &record)?;
        tx.commit().into_store_error()
    }

    pub(crate) fn get_pswap_lineage(
        conn: &mut Connection,
        order_id: Felt,
    ) -> Result<Option<PswapLineageRecord>, StoreError> {
        let mut stmt = conn
            .prepare_cached(&std::format!("{SELECT_LINEAGE_COLUMNS_PREFIX} WHERE order_id = ?"))
            .into_store_error()?;

        let mut rows = stmt.query(params![order_id.to_bytes()]).into_store_error()?;
        match rows.next().into_store_error()? {
            Some(row) => Ok(Some(record_from_row(row)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn list_pswap_lineages(
        conn: &mut Connection,
        filter: PswapLineageFilter,
    ) -> Result<Vec<PswapLineageRecord>, StoreError> {
        // Pull every row that matches the SQL-expressible part of the
        // filter; the `ByCreator` variant is applied in Rust because the
        // creator is embedded in the serialised `original_pswap` blob.
        let sql_filter = sql_filter_part(&filter);
        let full_sql = std::format!("{SELECT_LINEAGE_COLUMNS_PREFIX}{sql_filter}");
        let mut stmt = conn.prepare_cached(&full_sql).into_store_error()?;

        let rows: Vec<PswapLineageRecord> = match &filter {
            PswapLineageFilter::All | PswapLineageFilter::ByCreator(_) => {
                collect_rows(stmt.query([]).into_store_error()?)?
            },
            PswapLineageFilter::Active => collect_rows(
                stmt.query(params![PswapLineageState::Active.as_u8()])
                    .into_store_error()?,
            )?,
            PswapLineageFilter::ByOrderId(order_id) => {
                collect_rows(stmt.query(params![order_id.to_bytes()]).into_store_error()?)?
            },
        };

        Ok(rows)
    }

    pub(crate) fn apply_pswap_round(
        conn: &mut Connection,
        update: PswapLineageRoundUpdate,
    ) -> Result<(), StoreError> {
        let tx = conn.transaction().into_store_error()?;

        // 1. Mutate the lineage row in place.
        update_lineage_tip_tx(&tx, &update)?;

        // 2. Insert the reconstructed payback (if any). The note table
        //    upsert routes through INSERT OR REPLACE keyed by `note_id` so
        //    the default `NoteScreener`'s prior insertion for a public
        //    payback is not duplicated; for a private payback this is the
        //    only insertion site.
        if let Some(payback_note) = &update.reconstructed_payback {
            insert_reconstructed_payback_tx(&tx, payback_note, update.at_block)?;
        }

        tx.commit().into_store_error()
    }
}

// -------------------------------------------------------------------------------------------
// QUERY HELPERS
// -------------------------------------------------------------------------------------------

const SELECT_LINEAGE_COLUMNS_PREFIX: &str = "\
SELECT order_id, original_pswap, current_tip_note_id, current_tip_nullifier, \
       current_depth, remaining_offered, remaining_requested, \
       last_consumer_account_id, last_payout_amount, state, \
       created_at_block, updated_at_block \
FROM pswap_lineages";

fn sql_filter_part(filter: &PswapLineageFilter) -> &'static str {
    match filter {
        PswapLineageFilter::All | PswapLineageFilter::ByCreator(_) => "",
        PswapLineageFilter::Active => " WHERE state = ?",
        PswapLineageFilter::ByOrderId(_) => " WHERE order_id = ?",
    }
}

fn collect_rows(mut rows: rusqlite::Rows<'_>) -> Result<Vec<PswapLineageRecord>, StoreError> {
    let mut out = Vec::new();
    while let Some(row) = rows.next().into_store_error()? {
        out.push(record_from_row(row)?);
    }
    Ok(out)
}

fn record_from_row(row: &Row<'_>) -> Result<PswapLineageRecord, StoreError> {
    let original_pswap_bytes: Vec<u8> = row.get(1).into_store_error()?;
    let current_tip_text: String = row.get(2).into_store_error()?;
    let nullifier_text: String = row.get(3).into_store_error()?;
    let current_depth: u64 = row.get(4).into_store_error()?;
    let remaining_offered: u64 = row.get(5).into_store_error()?;
    let remaining_requested: u64 = row.get(6).into_store_error()?;
    let last_consumer_bytes: Option<Vec<u8>> = row.get(7).into_store_error()?;
    let last_payout_amount: Option<u64> = row.get(8).into_store_error()?;
    let state_byte: u8 = row.get(9).into_store_error()?;
    let created_at_block: u32 = row.get(10).into_store_error()?;
    let updated_at_block: u32 = row.get(11).into_store_error()?;

    // `PswapNote` does not impl Serializable directly; persist as `Note`
    // and round-trip via the existing conversion.
    let note = Note::read_from_bytes(&original_pswap_bytes)
        .map_err(StoreError::DataDeserializationError)?;
    let original_pswap = PswapNote::try_from(&note)
        .map_err(|err| StoreError::DataDeserializationError(deser_err(err.to_string())))?;

    let current_tip_note_id = NoteId::try_from_hex(&current_tip_text)
        .map_err(|err| StoreError::DataDeserializationError(deser_err(err.to_string())))?;
    let current_tip_nullifier = Nullifier::from_hex(&nullifier_text)
        .map_err(|err| StoreError::DataDeserializationError(deser_err(err.to_string())))?;

    let last_consumer_account_id = last_consumer_bytes
        .map(|bytes| AccountId::read_from_bytes(&bytes))
        .transpose()
        .map_err(StoreError::DataDeserializationError)?;

    build_record_from_columns(
        original_pswap,
        current_tip_note_id,
        current_tip_nullifier,
        current_depth,
        remaining_offered,
        remaining_requested,
        last_consumer_account_id,
        last_payout_amount,
        state_byte,
        BlockNumber::from(created_at_block),
        BlockNumber::from(updated_at_block),
    )
    .map_err(map_pswap_err)
}

// -------------------------------------------------------------------------------------------
// WRITE HELPERS
// -------------------------------------------------------------------------------------------

fn upsert_pswap_lineage_tx(
    tx: &Transaction<'_>,
    record: &PswapLineageRecord,
) -> Result<(), StoreError> {
    const SQL: &str = "\
INSERT OR REPLACE INTO pswap_lineages \
(order_id, original_pswap, current_tip_note_id, current_tip_nullifier, \
 current_depth, remaining_offered, remaining_requested, \
 last_consumer_account_id, last_payout_amount, state, \
 created_at_block, updated_at_block) \
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    let original_pswap_bytes = Note::from(record.original_pswap.clone()).to_bytes();
    let last_consumer_bytes = record.last_consumer_account_id.map(|id| id.to_bytes());

    tx.prepare_cached(SQL)
        .into_store_error()?
        .execute(params![
            record.order_id().to_bytes(),
            original_pswap_bytes,
            record.current_tip_note_id.as_word().to_string(),
            record.current_tip_nullifier.to_hex(),
            record.current_depth,
            record.remaining_offered,
            record.remaining_requested,
            last_consumer_bytes,
            record.last_payout_amount,
            record.state.as_u8(),
            record.created_at_block.as_u32(),
            record.updated_at_block.as_u32(),
        ])
        .into_store_error()?;

    Ok(())
}

fn update_lineage_tip_tx(
    tx: &Transaction<'_>,
    update: &PswapLineageRoundUpdate,
) -> Result<(), StoreError> {
    let order_id_bytes = update.order_id.to_bytes();

    // Confirm the lineage exists; otherwise `apply_pswap_round` is being
    // called on a foreign order — fail loudly so the caller can fix the
    // upstream observer/correlator logic that produced the update.
    const CHECK_SQL: &str = "SELECT 1 FROM pswap_lineages WHERE order_id = ?";
    let exists: bool = tx
        .prepare_cached(CHECK_SQL)
        .into_store_error()?
        .exists(params![order_id_bytes])
        .into_store_error()?;
    if !exists {
        return Err(StoreError::DatabaseError(std::format!(
            "apply_pswap_round: no lineage row for order_id {}",
            update.order_id
        )));
    }

    let updated_block = update.at_block.as_u32();
    let last_consumer_bytes = update.consumer_account_id.to_bytes();

    let rows_changed = match (update.new_tip_note_id, update.new_tip_nullifier) {
        (Some(note_id), Some(nullifier)) => {
            // Active continuation — new tip overwrites the previous one.
            const SQL: &str = "\
UPDATE pswap_lineages SET \
 current_tip_note_id = ?, current_tip_nullifier = ?, \
 current_depth = ?, remaining_offered = ?, remaining_requested = ?, \
 last_consumer_account_id = ?, last_payout_amount = ?, \
 state = ?, updated_at_block = ? \
WHERE order_id = ?";
            tx.prepare_cached(SQL)
                .into_store_error()?
                .execute(params![
                    note_id.as_word().to_string(),
                    nullifier.to_hex(),
                    update.round_depth,
                    update.new_remaining_offered,
                    update.new_remaining_requested,
                    last_consumer_bytes,
                    update.payout_amount,
                    update.new_state.as_u8(),
                    updated_block,
                    order_id_bytes,
                ])
                .into_store_error()?
        },
        _ => {
            // Terminal — keep the existing tip columns for diagnostics.
            const SQL: &str = "\
UPDATE pswap_lineages SET \
 remaining_offered = ?, remaining_requested = ?, \
 last_consumer_account_id = ?, last_payout_amount = ?, \
 state = ?, updated_at_block = ? \
WHERE order_id = ?";
            tx.prepare_cached(SQL)
                .into_store_error()?
                .execute(params![
                    update.new_remaining_offered,
                    update.new_remaining_requested,
                    last_consumer_bytes,
                    update.payout_amount,
                    update.new_state.as_u8(),
                    updated_block,
                    order_id_bytes,
                ])
                .into_store_error()?
        },
    };

    if rows_changed == 0 {
        return Err(StoreError::DatabaseError(std::format!(
            "apply_pswap_round: zero rows updated for order_id {}",
            update.order_id
        )));
    }

    Ok(())
}

fn insert_reconstructed_payback_tx(
    tx: &Transaction<'_>,
    payback_note: &Note,
    at_block: BlockNumber,
) -> Result<(), StoreError> {
    use miden_client::store::InputNoteRecord;
    use miden_client::store::input_note_states::ExpectedNoteState;

    // Build an Expected InputNoteRecord. The default `NoteScreener` may
    // have already inserted the same `note_id` row for a public payback;
    // the underlying upsert is INSERT OR REPLACE so the row converges to
    // one consistent entry either way.
    let metadata = payback_note.metadata().clone();
    let details = miden_client::note::NoteDetails::from(payback_note.clone());

    let state = ExpectedNoteState {
        metadata: Some(metadata.clone()),
        after_block_num: at_block,
        tag: Some(metadata.tag()),
    };

    let record = InputNoteRecord::new(details, None, state.into());
    upsert_input_note_tx(tx, &record)
}

// -------------------------------------------------------------------------------------------
// ERROR MAPPING
// -------------------------------------------------------------------------------------------

fn map_pswap_err(err: PswapLineageError) -> StoreError {
    StoreError::DatabaseError(std::format!("pswap_lineage: {err}"))
}

fn deser_err(msg: String) -> DeserializationError {
    DeserializationError::InvalidValue(msg)
}
