use std::sync::Arc;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::{Database, DbTransaction};
use crate::mv_query::interval::Interval;
use crate::mv_root::index_root::RootIndexType;

const FAN: usize = 8;
type TestDb = Database<FAN, FAN, u64, u64>;
fn inc(k: u64) -> u64 { k.checked_add(1).unwrap_or(u64::MAX) }
fn dec(k: u64) -> u64 { k.checked_sub(1).unwrap_or(u64::MIN) }

#[test]
fn range_scan_resolves_a_many_times_updated_key_to_its_latest_value() {
    let db: Arc<TestDb> = Arc::new(Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX));
    let t = db.create_table("t").table_id().unwrap();
    {
        let setup = DbTransaction::begin(&db);
        assert!(matches!(setup.insert(t, 42, 0u64), CRUDOperationResult::Inserted(_)));
        setup.commit();
    }

    const N: u64 = 500;
    for i in 0..N {
        let tx = DbTransaction::begin(&db);
        let cur = match tx.point(t, 42) { CRUDOperationResult::MatchedRecords(v) => *v[0].payload, other => panic!("iter {i}: {other}") };
        assert_eq!(cur, i, "iter {i}: point-read must reflect all prior commits");
        assert!(matches!(tx.update(t, 42, cur + 1), CRUDOperationResult::Updated(_)));
        tx.commit();
    }

    // Point read (already known to work) vs range scan (the actual code
    // path bench_tpcc_stress_tests.rs's snapshot()/scan_all() use).
    let check = DbTransaction::begin(&db);
    let point_val = match check.point(t, 42) { CRUDOperationResult::MatchedRecords(v) => *v[0].payload, other => panic!("{other}") };
    let range_results = match check.range(t, Interval::new(u64::MIN, u64::MAX)) {
        CRUDOperationResult::MatchedRecords(v) => v,
        other => panic!("{other}"),
    };
    check.commit();

    assert_eq!(point_val, N, "point read must show {N}, got {point_val}");
    assert_eq!(range_results.len(), 1, "range scan must return exactly one row for this one key, got {}", range_results.len());
    let range_val = *range_results[0].payload;
    assert_eq!(range_val, N, "range scan must resolve to the SAME latest value as point read ({N}), got {range_val}");
}

#[test]
fn range_scan_resolves_many_distinct_many_times_updated_keys_to_their_latest_values() {
    let db: Arc<TestDb> = Arc::new(Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX));
    let t = db.create_table("t").table_id().unwrap();
    const NUM_KEYS: u64 = 10;
    const N: u64 = 200;
    {
        let setup = DbTransaction::begin(&db);
        for k in 0..NUM_KEYS {
            assert!(matches!(setup.insert(t, k, 0u64), CRUDOperationResult::Inserted(_)));
        }
        setup.commit();
    }

    for i in 0..N {
        for k in 0..NUM_KEYS {
            let tx = DbTransaction::begin(&db);
            let cur = match tx.point(t, k) { CRUDOperationResult::MatchedRecords(v) => *v[0].payload, other => panic!("iter {i} key {k}: {other}") };
            assert_eq!(cur, i, "iter {i} key {k}: point-read must reflect all prior commits");
            assert!(matches!(tx.update(t, k, cur + 1), CRUDOperationResult::Updated(_)));
            tx.commit();
        }
    }

    let check = DbTransaction::begin(&db);
    let range_results = match check.range(t, Interval::new(u64::MIN, u64::MAX)) {
        CRUDOperationResult::MatchedRecords(v) => v,
        other => panic!("{other}"),
    };
    check.commit();

    assert_eq!(range_results.len(), NUM_KEYS as usize, "range scan must return exactly {NUM_KEYS} rows, got {}", range_results.len());
    let sum: u64 = range_results.iter().map(|r| *r.payload).sum();
    assert_eq!(sum, NUM_KEYS * N, "range-scanned sum must equal {NUM_KEYS}*{N}={}, got {sum}", NUM_KEYS * N);
    for r in &range_results {
        assert_eq!(*r.payload, N, "key {} must resolve to {N} via range scan, got {}", r.key, *r.payload);
    }
}
