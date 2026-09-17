use crate::bat_bench::tpcc_schema::TpccRow;
use crate::bat_bench::tpcc_schema::{
    District, Item, Table, TpccDatabase, Warehouse, k_district, k_item, k_warehouse,
};
use crate::bat_bench::tpcc_txn::TpccTxn;
use crate::bat_db::IsolationLevel;
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_root::index_root::RootIndexType;

fn sample_warehouse() -> TpccRow {
    TpccRow::Warehouse(Box::new(Warehouse {
        w_name: "W1".into(),
        w_street_1: "s1".into(),
        w_street_2: "s2".into(),
        w_city: "city".into(),
        w_state: "CA".into(),
        w_zip: "123451111".into(),
        w_tax: 0.05,
        w_ytd: 300_000.0,
    }))
}

fn sample_district() -> TpccRow {
    TpccRow::District(Box::new(District {
        d_name: "D1".into(),
        d_street_1: "s1".into(),
        d_street_2: "s2".into(),
        d_city: "city".into(),
        d_state: "CA".into(),
        d_zip: "123451111".into(),
        d_tax: 0.05,
        d_ytd: 30_000.0,
        d_next_o_id: 1,
    }))
}

fn sample_item(price: f64) -> TpccRow {
    TpccRow::Item(Box::new(Item {
        i_im_id: 1,
        i_name: "item".into(),
        i_price: price,
        i_data: "data".into(),
    }))
}

#[test]
fn tpcc_read_committed_refreshes_across_statements() {
    let db = TpccDatabase::new(RootIndexType::default());
    let key = k_warehouse(1);
    let mut setup = TpccTxn::begin(&db);
    assert!(matches!(setup.insert(Table::Warehouse, key, sample_warehouse()), CRUDOperationResult::Inserted(_)));
    setup.commit();

    let mut reader = TpccTxn::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    reader.begin_statement();
    let first = reader.read_ts();
    assert!(matches!(reader.point(Table::Warehouse, key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));

    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut writer = TpccTxn::begin(&db);
            assert!(matches!(writer.update(Table::Warehouse, key, sample_warehouse()), CRUDOperationResult::Updated(_)));
            writer.commit();
        }).join().unwrap();
    });

    assert_eq!(reader.read_ts(), first);
    reader.begin_statement();
    assert!(reader.read_ts() > first);
    assert!(matches!(reader.update(Table::Warehouse, key, sample_warehouse()), CRUDOperationResult::Updated(_)));
    reader.commit();
}

#[test]
fn tpcc_read_committed_statement_is_consistent_across_tree_classes() {
    let db = TpccDatabase::new(RootIndexType::default());
    let warehouse = k_warehouse(1);
    let item = k_item(1);
    let mut setup = TpccTxn::begin(&db);
    assert!(matches!(setup.insert(Table::Warehouse, warehouse, sample_warehouse()), CRUDOperationResult::Inserted(_)));
    assert!(matches!(setup.insert(Table::Item, item, sample_item(1.0)), CRUDOperationResult::Inserted(_)));
    setup.commit();

    let mut reader = TpccTxn::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    reader.begin_statement();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut writer = TpccTxn::begin(&db);
            let mut new_warehouse = sample_warehouse();
            if let TpccRow::Warehouse(row) = &mut new_warehouse { row.w_ytd = 400_000.0; }
            assert!(matches!(writer.update(Table::Warehouse, warehouse, new_warehouse), CRUDOperationResult::Updated(_)));
            assert!(matches!(writer.update(Table::Item, item, sample_item(2.0)), CRUDOperationResult::Updated(_)));
            writer.commit();
        }).join().unwrap();
    });

    assert!(matches!(reader.point(Table::Warehouse, warehouse), CRUDOperationResult::MatchedRecords(r) if r[0].payload.as_warehouse().w_ytd == 300_000.0));
    assert!(matches!(reader.point(Table::Item, item), CRUDOperationResult::MatchedRecords(r) if r[0].payload.as_item().i_price == 1.0));
    reader.begin_statement();
    assert!(matches!(reader.point(Table::Warehouse, warehouse), CRUDOperationResult::MatchedRecords(r) if r[0].payload.as_warehouse().w_ytd == 400_000.0));
    assert!(matches!(reader.point(Table::Item, item), CRUDOperationResult::MatchedRecords(r) if r[0].payload.as_item().i_price == 2.0));
    reader.commit();
}

/// The cross-table analogue of `bat_test::query_transaction_tests::
/// multi_op_transaction_sees_own_writes_and_isolates_others`: one
/// `TpccTxn` writes to *two different tables* (Warehouse, District) —
/// exactly the shared-snapshot-registration fix the multi-table refactor
/// exists for (see `TpccTxn::begin`'s doc) — and both writes must become
/// visible to other transactions atomically, as one unit, not one table
/// at a time.
#[test]
fn cross_table_transaction_is_atomic_across_tables() {
    let db = TpccDatabase::new(RootIndexType::default());
    let w_key = k_warehouse(1);
    let d_key = k_district(1, 1);

    let mut tx1 = TpccTxn::begin(&db);
    assert!(matches!(
        tx1.insert(Table::Warehouse, w_key, sample_warehouse()),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx1.insert(Table::District, d_key, sample_district()),
        CRUDOperationResult::Inserted(_)
    ));

    // Own writes, across both tables, are visible within the same
    // still-open transaction.
    assert!(
        matches!(tx1.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1)
    );
    assert!(
        matches!(tx1.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1)
    );

    let db_ref = &db;

    // A transaction on a different worker, snapshotting before tx1
    // commits, must see NEITHER table's write — if the shared snapshot
    // registration were broken (e.g. only registered against one
    // table), this could observe a partially-committed transaction.
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut tx2 = TpccTxn::begin(db_ref);
            assert!(matches!(tx2.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
            assert!(matches!(tx2.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()));
            tx2.commit();
        }).join().unwrap();
    });

    tx1.commit();

    // A transaction on yet another worker, snapshotting after tx1's
    // commit, must now see both writes.
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let mut tx3 = TpccTxn::begin(db_ref);
            assert!(matches!(tx3.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
            assert!(matches!(tx3.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1));
            tx3.commit();
        }).join().unwrap();
    });
}

/// First-writer-wins must still hold per-table under the shared `ctx`:
/// a concurrent transaction's commit on `Table::District`, after tx1's
/// snapshot was drawn, must make tx1 lose the race on that same table.
#[test]
fn first_writer_wins_conflict_holds_per_table_under_shared_ctx() {
    let db = TpccDatabase::new(RootIndexType::default());
    let d_key = k_district(1, 1);
    assert!(matches!(
        crate::bat_bench::tpcc_schema::dispatch_crud_big(
            &db,
            Table::District,
            CRUDOperation::Insert(d_key, sample_district())
        ),
        CRUDOperationResult::Inserted(_)
    ));

    let mut tx1 = TpccTxn::begin(&db);

    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = TpccTxn::begin(db_ref);
                assert!(matches!(
                    tx2.update(Table::District, d_key, sample_district()),
                    CRUDOperationResult::Updated(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    assert!(matches!(
        tx1.update(Table::District, d_key, sample_district()),
        CRUDOperationResult::Conflict
    ));
}

/// The cross-table analogue of `bat_test::query_transaction_tests::
/// dropped_transaction_reverts_its_earlier_writes_on_conflict`: one
/// `TpccTxn` writes to *two different tables*, then loses a
/// first-writer-wins race on a later op and drops without `commit()` —
/// both of its earlier writes, across both tables, must be reverted, not
/// left stuck as if committed (see `Drop`'s doc).
#[test]
fn dropped_tpcc_txn_reverts_writes_across_tables_on_conflict() {
    let db = TpccDatabase::new(RootIndexType::default());
    let w_key = k_warehouse(1);
    let d_key = k_district(1, 1);

    let mut tx1 = TpccTxn::begin(&db);
    assert!(matches!(
        tx1.insert(Table::Warehouse, w_key, sample_warehouse()),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        tx1.insert(Table::District, d_key, sample_district()),
        CRUDOperationResult::Inserted(_)
    ));

    // A concurrent transaction on another worker inserts and commits a
    // second district key *after* tx1's snapshot was already taken.
    let d_key2 = k_district(1, 2);
    let db_ref = &db;
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut tx2 = TpccTxn::begin(db_ref);
                assert!(matches!(
                    tx2.insert(Table::District, d_key2, sample_district()),
                    CRUDOperationResult::Inserted(_)
                ));
                tx2.commit();
            })
            .join()
            .unwrap();
    });

    // tx1's snapshot predates tx2's insert, so tx1's own attempt to
    // write the same key must lose the race.
    assert!(matches!(
        tx1.insert(Table::District, d_key2, sample_district()),
        CRUDOperationResult::Conflict
    ));

    // tx1 is dropped here without commit — both of its earlier writes
    // (Warehouse and District tables) must be reverted.
    drop(tx1);

    let mut tx3 = TpccTxn::begin(&db);
    assert!(
        matches!(tx3.point(Table::Warehouse, w_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "warehouse write by since-aborted tx1 must not be visible"
    );
    assert!(
        matches!(tx3.point(Table::District, d_key), CRUDOperationResult::MatchedRecords(r) if r.is_empty()),
        "district write by since-aborted tx1 must not be visible"
    );
    tx3.commit();
}
