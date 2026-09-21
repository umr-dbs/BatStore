use crate::bat_bench::tpcc_schema::*;
use crate::bat_wal::record::WalPayload;

/// Test-only `Stock::s_dist` entry: `"dist{i}"` left-padded to the field's
/// fixed 24-byte width with `x`.
fn dist_bytes(i: usize) -> [u8; 24] {
    let s = format!("dist{i}");
    let mut b = [b'x'; 24];
    b[..s.len()].copy_from_slice(s.as_bytes());
    b
}

fn round_trip(row: TpccRow) {
    let mut bytes = Vec::new();
    row.wal_encode(&mut bytes);
    let decoded = TpccRow::wal_decode(&bytes).expect("decode should succeed");
    assert_eq!(
        format!("{row}"),
        format!("{decoded}"),
        "Display mismatch after round-trip"
    );
    let mut re_encoded = Vec::new();
    decoded.wal_encode(&mut re_encoded);
    assert_eq!(bytes, re_encoded, "byte mismatch after round-trip");
}

#[test]
fn every_variant_round_trips() {
    round_trip(TpccRow::Empty);
    round_trip(TpccRow::Warehouse(Box::new(Warehouse {
        w_name: "W1".into(),
        w_street_1: "s1".into(),
        w_street_2: "s2".into(),
        w_city: "city".into(),
        w_state: "CA".into(),
        w_zip: "123451111".into(),
        w_tax: 0.05,
        w_ytd: 300_000.0,
    })));
    round_trip(TpccRow::District(Box::new(District {
        d_name: "D1".into(),
        d_street_1: "s1".into(),
        d_street_2: "s2".into(),
        d_city: "city".into(),
        d_state: "CA".into(),
        d_zip: "123451111".into(),
        d_tax: 0.05,
        d_ytd: 30_000.0,
        d_next_o_id: 3001,
    })));
    round_trip(TpccRow::Customer(Box::new(Customer {
        c_first: "Amir".into(),
        c_middle: "OE".into(),
        c_last: "BARBAR".into(),
        c_street_1: "s1".into(),
        c_street_2: "s2".into(),
        c_city: "city".into(),
        c_state: "CA".into(),
        c_zip: "123451111".into(),
        c_phone: "1234567890123456".into(),
        c_since: 1234567890,
        c_credit_bad: true,
        c_credit_lim: 50_000.0,
        c_discount: 0.15,
        c_balance: -10.0,
        c_ytd_payment: 10.0,
        c_payment_cnt: 1,
        c_delivery_cnt: 0,
        c_data: "x".repeat(400),
    })));
    round_trip(TpccRow::CustomerNameIdx);
    round_trip(TpccRow::History(Box::new(History {
        h_c_id: 1,
        h_c_d_id: 2,
        h_c_w_id: 3,
        h_d_id: 2,
        h_w_id: 3,
        h_date: 42,
        h_amount: 10.0,
        h_data: "note".into(),
    })));
    round_trip(TpccRow::NewOrder(NewOrderMarker { no_o_id: 3001 }));
    round_trip(TpccRow::Order(Box::new(Order {
        o_c_id: 7,
        o_entry_d: 42,
        o_carrier_id: None,
        o_ol_cnt: 10,
        o_all_local: true,
    })));
    round_trip(TpccRow::Order(Box::new(Order {
        o_c_id: 7,
        o_entry_d: 42,
        o_carrier_id: Some(3),
        o_ol_cnt: 10,
        o_all_local: false,
    })));
    round_trip(TpccRow::OrderLine(Box::new(OrderLine {
        ol_i_id: 99,
        ol_supply_w_id: 1,
        ol_delivery_d: None,
        ol_quantity: 5,
        ol_amount: 12.34,
        ol_dist_info: [b'd'; 24],
    })));
    round_trip(TpccRow::OrderLine(Box::new(OrderLine {
        ol_i_id: 99,
        ol_supply_w_id: 1,
        ol_delivery_d: Some(99),
        ol_quantity: 5,
        ol_amount: 12.34,
        ol_dist_info: [b'd'; 24],
    })));
    round_trip(TpccRow::Item(Box::new(Item {
        i_im_id: 5,
        i_name: "widget".into(),
        i_price: 9.99,
        i_data: "ORIGINALxyz".into(),
    })));
    round_trip(TpccRow::Stock(Box::new(Stock {
        s_quantity: -5,
        s_dist: std::array::from_fn(dist_bytes),
        s_ytd: 1.0,
        s_order_cnt: 2,
        s_remote_cnt: 3,
        s_data: "data".into(),
        s_su_suppkey: 4321,
    })));
    round_trip(TpccRow::CustLastOrder(42));
    round_trip(TpccRow::Supplier(Box::new(Supplier {
        s_name: "Supplier#1".into(),
        s_address: "addr".into(),
        s_nationkey: 7,
        s_phone: "1234567890123456".into(),
        s_acctbal: 1234.56,
        s_comment: "comment".into(),
    })));
    round_trip(TpccRow::Nation(Box::new(Nation {
        n_name: "GERMANY".into(),
        n_regionkey: 3,
        n_comment: "comment".into(),
    })));
    round_trip(TpccRow::Region(Box::new(Region {
        r_name: "EUROPE".into(),
        r_comment: "comment".into(),
    })));
}

#[test]
fn decode_rejects_truncated_bytes() {
    let row = TpccRow::Customer(Box::new(Customer {
        c_first: "Amir".into(),
        c_middle: "OE".into(),
        c_last: "BARBAR".into(),
        c_street_1: "s1".into(),
        c_street_2: "s2".into(),
        c_city: "city".into(),
        c_state: "CA".into(),
        c_zip: "123451111".into(),
        c_phone: "1234567890123456".into(),
        c_since: 1234567890,
        c_credit_bad: true,
        c_credit_lim: 50_000.0,
        c_discount: 0.15,
        c_balance: -10.0,
        c_ytd_payment: 10.0,
        c_payment_cnt: 1,
        c_delivery_cnt: 0,
        c_data: "x".repeat(400),
    }));
    let mut bytes = Vec::new();
    row.wal_encode(&mut bytes);

    for cut in 0..bytes.len() {
        assert!(
            TpccRow::wal_decode(&bytes[..cut]).is_none(),
            "truncation at {cut} should fail, not misparse"
        );
    }
}

#[test]
fn crash_recovery_round_trip_for_boxed_rows() {
    use crate::bat_bench::tpcc_schema::TpccTree;
    use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
    use crate::bat_crud_model::crud_operation::CRUDOperation;
    use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
    use crate::bat_root::index_root::RootIndexType;

    let path =
        std::env::temp_dir().join(format!("batstore_tpcc_wal_test_{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let warehouse_key = k_warehouse(1);
    let customer_key = k_customer(1, 1, 42);
    let stock_key = k_stock(1, 7);

    {
        let tree = TpccTree::make_standard(RootIndexType::default())
            .with_wal(&path, std::time::Duration::from_millis(2))
            .unwrap();

        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(
                warehouse_key,
                TpccRow::Warehouse(Box::new(Warehouse {
                    w_name: "Marburg".into(),
                    w_street_1: "Uniplatz".into(),
                    w_street_2: "".into(),
                    w_city: "Marburg".into(),
                    w_state: "HE".into(),
                    w_zip: "350321111".into(),
                    w_tax: 0.07,
                    w_ytd: 300_000.0,
                }))
            )),
            CRUDOperationResult::Inserted(_)
        ));

        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(
                customer_key,
                TpccRow::Customer(Box::new(Customer {
                    c_first: "Amir".into(),
                    c_middle: "OE".into(),
                    c_last: "BARBAR".into(),
                    c_street_1: "s1".into(),
                    c_street_2: "s2".into(),
                    c_city: "city".into(),
                    c_state: "HE".into(),
                    c_zip: "350321111".into(),
                    c_phone: "1234567890123456".into(),
                    c_since: 1234567890,
                    c_credit_bad: true,
                    c_credit_lim: 50_000.0,
                    c_discount: 0.15,
                    c_balance: -10.0,
                    c_ytd_payment: 10.0,
                    c_payment_cnt: 1,
                    c_delivery_cnt: 0,
                    c_data: "x".repeat(450),
                }))
            )),
            CRUDOperationResult::Inserted(_)
        ));

        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Insert(
                stock_key,
                TpccRow::Stock(Box::new(Stock {
                    s_quantity: 42,
                    s_dist: std::array::from_fn(dist_bytes),
                    s_ytd: 1.0,
                    s_order_cnt: 2,
                    s_remote_cnt: 3,
                    s_data: "ORIGINALxyz".into(),
                    s_su_suppkey: 999,
                }))
            )),
            CRUDOperationResult::Inserted(_)
        ));
    } // tree drops here: every Box is deallocated normally, exactly like a real crash would leave nothing behind but the WAL file.

    let recovered = TpccTree::open_recovered(
        RootIndexType::default(),
        &path,
        std::time::Duration::from_millis(2),
    )
    .unwrap();
    let version = recovered.current_version();

    match recovered.dispatch_crud(CRUDOperation::Point(warehouse_key, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
            let w = r[0].payload.as_warehouse();
            assert_eq!(w.w_name, "Marburg");
            assert_eq!(w.w_tax, 0.07);
        }
        other => panic!("warehouse missing or wrong after recovery: {other}"),
    }

    match recovered.dispatch_crud(CRUDOperation::Point(customer_key, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
            let c = r[0].payload.as_customer();
            assert_eq!(c.c_last, "BARBAR");
            assert_eq!(c.c_data.len(), 450);
            assert!(c.c_credit_bad);
        }
        other => panic!("customer missing or wrong after recovery: {other}"),
    }

    match recovered.dispatch_crud(CRUDOperation::Point(stock_key, version)) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
            let s = r[0].payload.as_stock();
            assert_eq!(s.s_quantity, 42);
            assert_eq!(s.s_dist[3], dist_bytes(3));
            assert_eq!(s.s_data, "ORIGINALxyz");
        }
        other => panic!("stock missing or wrong after recovery: {other}"),
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn tpcc_database_crash_recovery_round_trip_across_tables() {
    use crate::bat_bench::tpcc_schema::{Table, TpccDatabase};
    use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
    use crate::bat_crud_model::crud_operation::CRUDOperation;
    use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
    use crate::bat_root::index_root::RootIndexType;

    let base_path = std::env::temp_dir().join(format!(
        "batstore_tpcc_db_wal_test_{}.log",
        std::process::id()
    ));
    let meta_path = format!("{}.meta", base_path.display());
    let _ = std::fs::remove_file(&base_path);
    let _ = std::fs::remove_file(&meta_path);

    let warehouse_key = k_warehouse(1);
    let customer_key = k_customer(1, 1, 42);

    {
        let db = TpccDatabase::new_with_wal(
            RootIndexType::default(),
            &base_path,
            std::time::Duration::from_millis(2),
        )
        .unwrap();

        assert!(matches!(
            crate::bat_bench::tpcc_schema::dispatch_crud_big(
                &db,
                Table::Warehouse,
                CRUDOperation::Insert(
                    warehouse_key,
                    TpccRow::Warehouse(Box::new(Warehouse {
                        w_name: "Marburg".into(),
                        w_street_1: "Uniplatz".into(),
                        w_street_2: "".into(),
                        w_city: "Marburg".into(),
                        w_state: "HE".into(),
                        w_zip: "350321111".into(),
                        w_tax: 0.07,
                        w_ytd: 300_000.0,
                    }))
                )
            ),
            CRUDOperationResult::Inserted(_)
        ));

        assert!(matches!(
            db.tree_for(Table::Customer)
                .dispatch_crud(CRUDOperation::Insert(
                    customer_key,
                    TpccRow::Customer(Box::new(Customer {
                        c_first: "Amir".into(),
                        c_middle: "OE".into(),
                        c_last: "BARBAR".into(),
                        c_street_1: "s1".into(),
                        c_street_2: "s2".into(),
                        c_city: "city".into(),
                        c_state: "HE".into(),
                        c_zip: "350321111".into(),
                        c_phone: "1234567890123456".into(),
                        c_since: 1234567890,
                        c_credit_bad: true,
                        c_credit_lim: 50_000.0,
                        c_discount: 0.15,
                        c_balance: -10.0,
                        c_ytd_payment: 10.0,
                        c_payment_cnt: 1,
                        c_delivery_cnt: 0,
                        c_data: "x".repeat(450),
                    }))
                )),
            CRUDOperationResult::Inserted(_)
        ));
    } // db drops here: every table's Box'd rows are deallocated normally.

    let recovered = TpccDatabase::open_recovered(
        RootIndexType::default(),
        &base_path,
        std::time::Duration::from_millis(2),
    )
    .unwrap();
    let version = recovered.current_version();

    match crate::bat_bench::tpcc_schema::dispatch_crud_big(
        &recovered,
        Table::Warehouse,
        CRUDOperation::Point(warehouse_key, version),
    ) {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
            let w = r[0].payload.as_warehouse();
            assert_eq!(w.w_name, "Marburg");
            assert_eq!(w.w_tax, 0.07);
        }
        other => panic!("warehouse missing or wrong after recovery: {other}"),
    }

    match recovered
        .tree_for(Table::Customer)
        .dispatch_crud(CRUDOperation::Point(customer_key, version))
    {
        CRUDOperationResult::MatchedRecords(r) if r.len() == 1 => {
            let c = r[0].payload.as_customer();
            assert_eq!(c.c_last, "BARBAR");
            assert_eq!(c.c_data.len(), 450);
            assert!(c.c_credit_bad);
        }
        other => panic!("customer missing or wrong after recovery: {other}"),
    }

    let _ = std::fs::remove_file(&base_path);
    let _ = std::fs::remove_file(&meta_path);
}
