//! Regression test for a fixed bug in `bat_tree::smo::split`'s `KEY_SPLIT`-
//! vs-`VERSION_SPLIT` decision: it used `survivor_count > capacity` (strict),
//! so a page whose still-needed ("surviving" - see `record_survives_gc`)
//! entries landed at *exactly* capacity took the `VERSION_SPLIT` ("just
//! compact") path instead of `KEY_SPLIT` ("make room") - even though
//! `split()` is only ever called because some pending write needs a free
//! slot in whatever comes out of it (its only callers, `on_overflow_node`/
//! `split_root`, both exist to make room for one). A `VERSION_SPLIT` that
//! can't discard anything (every survivor still needed) then produced a
//! freshly rebuilt page that was *already* 100% full, with nowhere for that
//! pending write to go:
//! - on a root-is-leaf tree (no parent to redo the overflow check on the
//!   fresh result), the pending write panicked outright:
//!   `LeafPage::push_uncommitted: index N out of bounds for NUM_RECORDS=N`.
//! - on a non-root leaf (whose parent *does* recheck before writing to a
//!   freshly split child), the identical, still-fully-protected survivor
//!   set just repeated the same no-op `VERSION_SPLIT` forever instead.
//!
//! Originally suspected to need a "hot"/repeated key to trigger (see the
//! git history of this file's predecessor, `known_issue_hot_key_version_
//! tearing_repro.rs`) - traced with temporary instrumentation and found not
//! to: this test's leaf holds several *distinct* keys (a shared "hot" key
//! plus two threads' own private "misc" keys), and overflows simply because
//! one transaction happens to be somewhat slower to commit than several
//! others sharing its page - enough on its own to push survivor_count to
//! capacity. Fixed in `split()` by comparing `survivor_count >= capacity`
//! instead, reserving the one slot the pending write actually needs.
//!
//! Same-transaction updates overwrite their pending tuple, and
//! first-writer-wins prevents concurrent transactions from accumulating an
//! unresolved chain for one key. Repeated delete/reinsert is covered by the
//! matching transaction regression tests and likewise reuses the pending
//! tuple after its first replacement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::Duration;

use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::{Database, DbTransaction};
use crate::bat_root::index_root::RootIndexType;

const FAN: usize = 16;
type TestDb = Database<FAN, FAN, u64, u64>;

fn inc(k: u64) -> u64 {
    k.checked_add(1).unwrap_or(u64::MAX)
}
fn dec(k: u64) -> u64 {
    k.checked_sub(1).unwrap_or(u64::MIN)
}

const HOT_KEY: u64 = 1;
const MISC_KEYS_PER_THREAD: u64 = 5;

#[test]
fn concurrent_multi_step_transactions_never_overflow_or_lose_a_committed_update() {
    let db: Arc<TestDb> = Arc::new(Database::new(
        RootIndexType::default(),
        inc,
        dec,
        u64::MIN,
        u64::MAX,
    ));
    let hot_t = db.create_table("hot").table_id().unwrap();
    let misc_t = db.create_table("misc").table_id().unwrap();

    {
        let mut setup = DbTransaction::begin(&db);
        assert!(matches!(
            setup.insert(hot_t, HOT_KEY, 0u64),
            CRUDOperationResult::Inserted(_)
        ));
        setup.commit();
    }

    let num_threads = 2usize;
    let stop = Arc::new(AtomicBool::new(false));
    let hot_success_count = Arc::new(AtomicU64::new(0));

    // Each thread gets its own private misc-key range up front, so misc
    // writes never conflict with each other - the only *contended* key in
    // this whole test is the one shared hot key.
    {
        let mut setup = DbTransaction::begin(&db);
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

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            let stop = stop.clone();
            let hot_success_count = hot_success_count.clone();
            let my_misc_keys: Vec<u64> = (0..MISC_KEYS_PER_THREAD)
                .map(|i| 1_000 + t as u64 * MISC_KEYS_PER_THREAD + i)
                .collect();
            thread::spawn(move || {
                while !stop.load(Relaxed) {
                    let mut tx = DbTransaction::begin(&db);

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

                    let cur = match tx.point(hot_t, HOT_KEY) {
                        CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
                        other => panic!("unexpected hot point result: {other}"),
                    };
                    match tx.update(hot_t, HOT_KEY, cur + 1) {
                        CRUDOperationResult::Updated(_) => {}
                        CRUDOperationResult::Conflict => {
                            drop(tx);
                            continue;
                        }
                        other => panic!("unexpected hot update result: {other}"),
                    }

                    for &k in &my_misc_keys {
                        let _ = tx.point(misc_t, k);
                    }

                    if tx.commit().is_some() {
                        hot_success_count.fetch_add(1, Relaxed);
                    }
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Relaxed);
    for h in handles {
        h.join().expect("worker thread must not panic - this is exactly the LeafPage::push_uncommitted bounds panic this test regresses against");
    }

    let final_value = {
        let mut tx = DbTransaction::begin(&db);
        let v = match tx.point(hot_t, HOT_KEY) {
            CRUDOperationResult::MatchedRecords(v) => *v[0].payload,
            other => panic!("unexpected final point result: {other}"),
        };
        tx.commit();
        v
    };
    let expected = hot_success_count.load(Relaxed);
    assert_eq!(
        final_value, expected,
        "hot key's final stored value must equal the number of transactions that reported a \
         successful commit after incrementing it exactly once each"
    );
}
