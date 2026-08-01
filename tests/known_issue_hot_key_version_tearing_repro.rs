//! Minimal, standalone reproduction of a known, pre-existing structural
//! limitation - kept `#[ignore]`d so `cargo test` stays green by default;
//! run it explicitly to see the issue:
//!
//!   cargo test --bin cMVBT -- --ignored hot_key_version_tearing --nocapture
//!
//! ## What this demonstrates
//!
//! Two threads run small, multi-step transactions concurrently. Every
//! transaction does a few reads/writes on its *own private* "misc" keys
//! (standing in for New-Order's Warehouse/District/Customer/Item steps),
//! then increments *one shared, hot* key exactly once, then does a bit more
//! private work before committing. No transaction ever writes the hot key
//! more than once, and no two threads ever touch the same misc key - the
//! *only* contention anywhere in this test is on the one hot key.
//!
//! After both threads stop, the hot key's actual stored value is compared
//! against the number of transactions that reported a successful commit
//! after having incremented it. On an engine free of this issue those two
//! numbers are always equal - every reported success is reflected in the
//! final value. Here, they routinely aren't (or the run panics outright
//! with `LeafPage::push_uncommitted: index N out of bounds`): some of the
//! hot key's committed increments go missing.
//!
//! ## Why
//!
//! A `DbTransaction`'s snapshot is registered *at `begin()`*, before it
//! does anything else, and stays registered until `commit()`/`abort()` -
//! see `DbTransaction::begin`/`Database::begin_snapshot`. So while thread A
//! is still working through its own misc-key steps (nowhere near the hot
//! key yet), its snapshot is already "live" and already old enough that if
//! thread B commits a write to the hot key in the meantime, the version B
//! just superseded cannot be discarded - A's still-open transaction might
//! legitimately need to read it (`mv_tree::smo::record_survives_gc`). This
//! is completely ordinary, correct MVCC behavior, not a bug by itself.
//!
//! The bug is what happens when enough such "still-needed" versions of one
//! key pile up in the same leaf page: `split()`'s `KEY_SPLIT` path has to
//! divide a page's entries between two sibling leaves at some key boundary
//! - but when the page is dominated by one key's own version chain, there
//! is no such boundary. `nearest_key_boundary`'s own doc names this
//! outright: "tearing is then unavoidable without duplicate-key sibling
//! support, a pre-existing structural limit this doesn't attempt to fix."
//! Some of the hot key's own physical versions end up in the sibling whose
//! fence no longer routes to that key at all, silently orphaning them (or,
//! depending on exact timing, the write path can even run out of physical
//! room before a split resolves things, panicking outright).
//!
//! A small `FAN`/`NUM_RECORDS` (16, vs. production's 125) is used here
//! purely to make the pathological page-filling state reachable in a
//! couple hundred milliseconds instead of requiring a much longer/heavier
//! run - the mechanism is identical at production scale (see
//! `tests/bench_tpcc_stress_tests.rs`'s own doc on the same limitation,
//! hit there under a realistic TPC-C workload, just far less frequently).
//!
//! ## What does *not* explain this
//!
//! No same-transaction repeats are involved anywhere in this repro - each
//! transaction writes the hot key exactly once. `DbTransaction::update`'s
//! self-overwrite fast path (mutates a key's payload in place when a
//! transaction has already written it earlier in the very same
//! transaction) therefore cannot help here and doesn't change this repro's
//! outcome - it fixes a different, narrower contributor to the same class
//! of page pressure, not this one.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_db::{Database, DbTransaction};
use crate::mv_root::index_root::RootIndexType;

const FAN: usize = 16;
type TestDb = Database<FAN, FAN, u64, u64>;

fn inc(k: u64) -> u64 { k.checked_add(1).unwrap_or(u64::MAX) }
fn dec(k: u64) -> u64 { k.checked_sub(1).unwrap_or(u64::MIN) }

const HOT_KEY: u64 = 1;
const MISC_KEYS_PER_THREAD: u64 = 5;

#[test]
#[ignore = "demonstrates a known, pre-existing, documented structural limitation \
            (same-key leaf-split tearing under sustained hot-key contention - \
            see this file's own doc); not a routine pass/fail check"]
fn hot_key_version_tearing_under_concurrent_multi_step_transactions() {
    let db: Arc<TestDb> = Arc::new(Database::new(RootIndexType::default(), inc, dec, u64::MIN, u64::MAX));
    let hot_t = db.create_table("hot").table_id().unwrap();
    let misc_t = db.create_table("misc").table_id().unwrap();

    {
        let setup = DbTransaction::begin(&db);
        assert!(matches!(setup.insert(hot_t, HOT_KEY, 0u64), CRUDOperationResult::Inserted(_)));
        setup.commit();
    }

    let num_threads = 2usize;
    let stop = Arc::new(AtomicBool::new(false));
    let hot_success_count = Arc::new(AtomicU64::new(0));

    // Each thread gets its own private misc-key range up front, so misc
    // writes never conflict with each other - the *only* contention in
    // this whole test is on the one hot key.
    {
        let setup = DbTransaction::begin(&db);
        for t in 0..num_threads as u64 {
            for i in 0..MISC_KEYS_PER_THREAD {
                let k = 1_000 + t * MISC_KEYS_PER_THREAD + i;
                assert!(matches!(setup.insert(misc_t, k, 0u64), CRUDOperationResult::Inserted(_)));
            }
        }
        setup.commit();
    }

    let handles: Vec<_> = (0..num_threads).map(|t| {
        let db = db.clone();
        let stop = stop.clone();
        let hot_success_count = hot_success_count.clone();
        let my_misc_keys: Vec<u64> = (0..MISC_KEYS_PER_THREAD).map(|i| 1_000 + t as u64 * MISC_KEYS_PER_THREAD + i).collect();
        thread::spawn(move || {
            while !stop.load(Relaxed) {
                let tx = DbTransaction::begin(&db);

                // Several earlier, unrelated steps - à la New-Order's
                // Warehouse/District/Customer/Item reads - holding this
                // transaction's snapshot open well before it ever touches
                // the hot key.
                for &k in &my_misc_keys {
                    let cur = match tx.point(misc_t, k) {
                        CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
                        other => panic!("unexpected misc point result: {other}"),
                    };
                    assert!(matches!(tx.update(misc_t, k, cur + 1), CRUDOperationResult::Updated(_)));
                }

                // Touch the hot key exactly once - never a repeat within
                // this transaction.
                let cur = match tx.point(hot_t, HOT_KEY) {
                    CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
                    other => panic!("unexpected hot point result: {other}"),
                };
                match tx.update(hot_t, HOT_KEY, cur + 1) {
                    CRUDOperationResult::Updated(_) => {}
                    CRUDOperationResult::Conflict => { drop(tx); continue; }
                    other => panic!("unexpected hot update result: {other}"),
                }

                // More unrelated work after touching the hot key, before
                // commit - the snapshot stays open a while longer still.
                for &k in &my_misc_keys {
                    let _ = tx.point(misc_t, k);
                }

                if tx.commit().is_some() {
                    hot_success_count.fetch_add(1, Relaxed);
                }
            }
        })
    }).collect();

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Relaxed);
    for h in handles {
        h.join().expect("worker thread panicked - see LeafPage::push_uncommitted's bounds check, this IS the issue this repro demonstrates");
    }

    let final_value = {
        let tx = DbTransaction::begin(&db);
        let v = match tx.point(hot_t, HOT_KEY) {
            CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
            other => panic!("unexpected final point result: {other}"),
        };
        tx.commit();
        v
    };
    let expected = hot_success_count.load(Relaxed);
    println!("expected={expected} final_value={final_value} (missing={})", expected.saturating_sub(final_value));
    assert_eq!(
        final_value, expected,
        "hot key's final stored value must equal the number of transactions that reported a \
         successful commit after incrementing it exactly once each - a mismatch means some \
         committed increments were silently lost to same-key leaf-split tearing"
    );
}
