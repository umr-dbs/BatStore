use crate::bat_bench::tpcc_schema::TpccRow;
use crate::bat_bench::tpcc_schema::{
    District, Item, Table, TpccDatabase, Warehouse, k_district, k_item, k_warehouse,
};
use crate::bat_bench::tpcc_txn::TpccTxn;
use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::IsolationLevel;
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
fn tpcc_read_committed_refreshes_automatically() {
    let db = TpccDatabase::new(RootIndexType::default());
    let key = k_warehouse(1);
    let mut setup = TpccTxn::begin(&db);
    assert!(matches!(
        setup.insert(Table::Warehouse, key, sample_warehouse()),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut reader = TpccTxn::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    assert!(
        matches!(reader.point(Table::Warehouse, key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1)
    );
    let first = reader.read_ts();

    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut writer = TpccTxn::begin(&db);
                assert!(matches!(
                    writer.update(Table::Warehouse, key, sample_warehouse()),
                    CRUDOperationResult::Updated(_)
                ));
                writer.commit();
            })
            .join()
            .unwrap();
    });

    assert!(matches!(
        reader.update(Table::Warehouse, key, sample_warehouse()),
        CRUDOperationResult::Updated(_)
    ));
    assert!(reader.read_ts() > first);
    reader.commit();
}

#[test]
fn tpcc_read_committed_refreshes_automatically_across_tree_classes() {
    let db = TpccDatabase::new(RootIndexType::default());
    let warehouse = k_warehouse(1);
    let item = k_item(1);
    let mut setup = TpccTxn::begin(&db);
    assert!(matches!(
        setup.insert(Table::Warehouse, warehouse, sample_warehouse()),
        CRUDOperationResult::Inserted(_)
    ));
    assert!(matches!(
        setup.insert(Table::Item, item, sample_item(1.0)),
        CRUDOperationResult::Inserted(_)
    ));
    setup.commit();

    let mut reader = TpccTxn::begin_with_isolation(&db, IsolationLevel::ReadCommitted);
    assert!(
        matches!(reader.point(Table::Warehouse, warehouse), CRUDOperationResult::MatchedRecords(r) if r[0].payload.as_warehouse().w_ytd == 300_000.0)
    );
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut writer = TpccTxn::begin(&db);
                let mut new_warehouse = sample_warehouse();
                if let TpccRow::Warehouse(row) = &mut new_warehouse {
                    row.w_ytd = 400_000.0;
                }
                assert!(matches!(
                    writer.update(Table::Warehouse, warehouse, new_warehouse),
                    CRUDOperationResult::Updated(_)
                ));
                assert!(matches!(
                    writer.update(Table::Item, item, sample_item(2.0)),
                    CRUDOperationResult::Updated(_)
                ));
                writer.commit();
            })
            .join()
            .unwrap();
    });

    assert!(
        matches!(reader.point(Table::Warehouse, warehouse), CRUDOperationResult::MatchedRecords(r) if r[0].payload.as_warehouse().w_ytd == 400_000.0)
    );
    assert!(
        matches!(reader.point(Table::Item, item), CRUDOperationResult::MatchedRecords(r) if r[0].payload.as_item().i_price == 2.0)
    );
    reader.commit();
}

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

/// A `TpccTxn` sees its own writes into leaves that a structure
/// modification created after it began, on both tree classes (see
/// `bat_db::own_writes`): `Item` uses standard leaves, `District` the big
/// leaves of `TreeClass::Big`.
#[test]
fn tpcc_txn_sees_own_writes_after_concurrent_splits() {
    use crate::bat_query::interval::Interval;
    let db = TpccDatabase::new(RootIndexType::default());
    let mut setup = TpccTxn::begin(&db);
    for i in 1..=3u32 {
        setup.insert(Table::Item, k_item(i), sample_item(i as f64));
        setup.insert(Table::District, k_district(1, i as u8), sample_district());
    }
    setup.commit();

    let mut tx = TpccTxn::begin(&db);
    // Another worker splits the leaves of both tables after tx began.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut other = TpccTxn::begin(&db);
                for i in 1000..6000u32 {
                    other.insert(Table::Item, k_item(i), sample_item(1.0));
                }
                for w in 2..800u32 {
                    for d in 1..=10u8 {
                        other.insert(Table::District, k_district(w, d), sample_district());
                    }
                }
                other.commit();
            })
            .join()
            .unwrap();
    });

    let item = k_item(500);
    let district = k_district(1, 9);
    assert!(matches!(tx.insert(Table::Item, item, sample_item(5.0)), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tx.insert(Table::District, district, sample_district()), CRUDOperationResult::Inserted(_)));
    assert!(matches!(tx.update(Table::Item, k_item(2), sample_item(20.0)), CRUDOperationResult::Updated(_)));

    for (table, key) in [(Table::Item, item), (Table::District, district)] {
        assert!(
            matches!(tx.point(table, key), CRUDOperationResult::MatchedRecords(r) if r.len() == 1),
            "{table:?}: own insert must be visible"
        );
    }
    let price = |r: &TpccRow| match r {
        TpccRow::Item(i) => i.i_price,
        other => panic!("not an item: {other:?}"),
    };
    assert!(matches!(
        tx.point(Table::Item, k_item(2)),
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && price(&r[0].payload) == 20.0
    ));
    let items = Interval::new(k_item(1), k_item(999));
    assert_eq!(tx.range_count(Table::Item, items), 4);
    assert_eq!(tx.range_fold(Table::Item, items, 0.0, |s, _, r| s + price(r)), 1.0 + 20.0 + 3.0 + 5.0);
    assert!(matches!(tx.range(Table::Item, items, false), CRUDOperationResult::MatchedRecords(r) if r.len() == 4));
    assert_eq!(tx.range_min(Table::Item, Interval::new(k_item(4), k_item(999))).map(|r| r.key), Some(item));
    let districts = Interval::new(k_district(1, 0), k_district(1, 15));
    assert_eq!(tx.range_count(Table::District, districts), 4);
    tx.commit();
}
