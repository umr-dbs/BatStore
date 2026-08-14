//! Investigation scratch test: several threads concurrently doing
//! read-modify-write increments against a *small shared set* of keys (not
//! just one hot key - `verify_range_scan.rs`/earlier lost-update tests
//! already cover the single-key case cleanly), tracking the exact number of
//! successful commits per key via a mutex-protected dense counter array, then comparing against
//! each key's actual final value. No multi-table/multi-step transaction
//! shape yet - this isolates "real concurrency across a handful of shared
//! keys sitting in the same leaf" as the one new variable, before adding
//! New-Order's extra table touches on top.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::{Database, DbTransaction};
use crate::mv_query::interval::Interval;
use crate::mv_root::index_root::RootIndexType;

const FAN: usize = 16;
type TestDb = Database<FAN, FAN, u64, u64>;
fn inc(k: u64) -> u64 {
    k.checked_add(1).unwrap_or(u64::MAX)
}
fn dec(k: u64) -> u64 {
    k.checked_sub(1).unwrap_or(u64::MIN)
}

const NUM_KEYS: u64 = 20;

fn run(num_threads: usize, duration: Duration) {
    let db: Arc<TestDb> = Arc::new(Database::new(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
    ));
    let t = db.create_table("t").table_id().unwrap();
    {
        let mut setup = DbTransaction::begin(&db);
        for k in 0..NUM_KEYS {
            assert!(matches!(
                setup.insert(t, k, 0u64),
                CRUDOperationResult::Inserted(_)
            ));
        }
        setup.commit();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let success_counts = Arc::new(Mutex::new(vec![0u64; NUM_KEYS as usize]));

    let handles: Vec<_> = (0..num_threads)
        .map(|seed| {
            let db = db.clone();
            let stop = stop.clone();
            let success_counts = success_counts.clone();
            thread::spawn(move || {
                let mut rng_state: u64 = 0x9E3779B97F4A7C15u64.wrapping_add(seed as u64);
                let mut next_key = || {
                    // xorshift64 - fast, no external RNG crate dependency needed here.
                    rng_state ^= rng_state << 13;
                    rng_state ^= rng_state >> 7;
                    rng_state ^= rng_state << 17;
                    rng_state % NUM_KEYS
                };
                while !stop.load(Relaxed) {
                    let key = next_key();
                    let mut tx = DbTransaction::begin(&db);
                    let cur = match tx.point(t, key) {
                        CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
                        other => panic!("unexpected point result: {other}"),
                    };
                    match tx.update(t, key, cur + 1) {
                        CRUDOperationResult::Updated(_) => {}
                        CRUDOperationResult::Conflict => {
                            drop(tx);
                            continue;
                        }
                        other => panic!("unexpected update result: {other}"),
                    }
                    if tx.commit().is_some() {
                        success_counts.lock().unwrap()[key as usize] += 1;
                    }
                }
            })
        })
        .collect();

    thread::sleep(duration);
    stop.store(true, Relaxed);
    for h in handles {
        h.join().expect("worker thread must not panic");
    }

    let mut check = DbTransaction::begin(&db);
    let rows = match check.range(t, Interval::new(u64::MIN, u64::MAX)) {
        CRUDOperationResult::MatchedRecords(v) => v,
        other => panic!("unexpected range result: {other}"),
    };
    check.commit();
    assert_eq!(
        rows.len(),
        NUM_KEYS as usize,
        "range scan must return exactly {NUM_KEYS} rows, got {}",
        rows.len()
    );

    let expected = success_counts.lock().unwrap();
    let mut total_expected = 0u64;
    let mut total_actual = 0u64;
    let mut mismatches = Vec::new();
    for r in &rows {
        let actual = *r.payload;
        let exp = expected[r.key as usize];
        total_expected += exp;
        total_actual += actual;
        if actual != exp {
            mismatches.push((r.key, exp, actual));
        }
    }
    println!(
        "total_expected={total_expected} total_actual={total_actual} mismatches={mismatches:?}"
    );
    assert!(
        mismatches.is_empty(),
        "every key's final value must equal its own tracked successful-commit count; mismatches: {mismatches:?} (total_expected={total_expected}, total_actual={total_actual})"
    );
}

#[test]
fn concurrent_read_modify_write_across_a_small_shared_key_set_never_loses_an_update() {
    run(6, Duration::from_millis(1500));
}

const MISC_KEYS_PER_THREAD: u64 = 5;

/// Escalation: same small shared-key contention as above, but each
/// transaction now also does a few reads/writes on its *own private* "misc"
/// keys before and after touching the shared key - mimicking New-Order's
/// shape (Warehouse/District/Customer/Item touches, then Stock, then
/// Orders/NewOrder/CustLastOrder) instead of a bare read-modify-write loop.
fn run_multi_step(num_threads: usize, duration: Duration) {
    let db: Arc<TestDb> = Arc::new(Database::new(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
    ));
    let shared_t = db.create_table("shared").table_id().unwrap();
    let misc_t = db.create_table("misc").table_id().unwrap();
    {
        let mut setup = DbTransaction::begin(&db);
        for k in 0..NUM_KEYS {
            assert!(matches!(
                setup.insert(shared_t, k, 0u64),
                CRUDOperationResult::Inserted(_)
            ));
        }
        for t in 0..num_threads as u64 {
            for i in 0..MISC_KEYS_PER_THREAD {
                let k = 1_000 + t * MISC_KEYS_PER_THREAD + i;
                assert!(matches!(
                    setup.insert(misc_t, k, 0u64),
                    CRUDOperationResult::Inserted(_)
                ));
            }
        }
        setup.commit();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let success_counts = Arc::new(Mutex::new(vec![0u64; NUM_KEYS as usize]));

    let handles: Vec<_> = (0..num_threads)
        .map(|seed| {
            let db = db.clone();
            let stop = stop.clone();
            let success_counts = success_counts.clone();
            let my_misc_keys: Vec<u64> = (0..MISC_KEYS_PER_THREAD)
                .map(|i| 1_000 + seed as u64 * MISC_KEYS_PER_THREAD + i)
                .collect();
            thread::spawn(move || {
                let mut rng_state: u64 = 0x9E3779B97F4A7C15u64.wrapping_add(seed as u64);
                let mut next_key = || {
                    rng_state ^= rng_state << 13;
                    rng_state ^= rng_state >> 7;
                    rng_state ^= rng_state << 17;
                    rng_state % NUM_KEYS
                };
                while !stop.load(Relaxed) {
                    let key = next_key();
                    let mut tx = DbTransaction::begin(&db);

                    // Several earlier, unrelated steps before the shared key.
                    for &k in &my_misc_keys {
                        let cur = match tx.point(misc_t, k) {
                            CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
                            other => panic!("unexpected misc point result: {other}"),
                        };
                        assert!(matches!(
                            tx.update(misc_t, k, cur + 1),
                            CRUDOperationResult::Updated(_)
                        ));
                    }

                    let cur = match tx.point(shared_t, key) {
                        CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
                        other => panic!("unexpected shared point result: {other}"),
                    };
                    match tx.update(shared_t, key, cur + 1) {
                        CRUDOperationResult::Updated(_) => {}
                        CRUDOperationResult::Conflict => {
                            drop(tx);
                            continue;
                        }
                        other => panic!("unexpected shared update result: {other}"),
                    }

                    // More unrelated work after the shared key, before commit.
                    for &k in &my_misc_keys {
                        let _ = tx.point(misc_t, k);
                    }

                    if tx.commit().is_some() {
                        success_counts.lock().unwrap()[key as usize] += 1;
                    }
                }
            })
        })
        .collect();

    thread::sleep(duration);
    stop.store(true, Relaxed);
    for h in handles {
        h.join().expect("worker thread must not panic");
    }

    let mut check = DbTransaction::begin(&db);
    let rows = match check.range(shared_t, Interval::new(u64::MIN, u64::MAX)) {
        CRUDOperationResult::MatchedRecords(v) => v,
        other => panic!("unexpected range result: {other}"),
    };
    check.commit();
    assert_eq!(
        rows.len(),
        NUM_KEYS as usize,
        "range scan must return exactly {NUM_KEYS} rows, got {}",
        rows.len()
    );

    let expected = success_counts.lock().unwrap();
    let mut total_expected = 0u64;
    let mut total_actual = 0u64;
    let mut mismatches = Vec::new();
    for r in &rows {
        let actual = *r.payload;
        let exp = expected[r.key as usize];
        total_expected += exp;
        total_actual += actual;
        if actual != exp {
            mismatches.push((r.key, exp, actual));
        }
    }
    println!(
        "total_expected={total_expected} total_actual={total_actual} mismatches={mismatches:?}"
    );
    assert!(
        mismatches.is_empty(),
        "every key's final value must equal its own tracked successful-commit count; mismatches: {mismatches:?} (total_expected={total_expected}, total_actual={total_actual})"
    );
}

#[test]
fn concurrent_multi_step_transactions_across_a_small_shared_key_set_never_lose_an_update() {
    run_multi_step(6, Duration::from_millis(1500));
}
