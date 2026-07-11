use std::cell::Cell;
use std::fmt::Display;
use std::hash::Hash;

use crossbeam_channel::Receiver;
use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::{CRUDOperation, TxAtomicOperation};
use crate::mv_crud_model::crud_operation_result::CRUDOperationInnerReason::{KeyAlreadyDeleted, KeyAlreadyExists, KeyDoesNotExist};
use crate::mv_crud_model::crud_operation_result::{AtomicTxResult, CRUDOperationResult};
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::{Version, VersionInfo};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_utils::interval::Interval;

/// A multi-operation OSIC transaction (paper §3.5, "Putting Everything
/// Together"): one fixed snapshot (`ts_start`), drawn once at `begin` and
/// shared by every read/write issued through it, committed exactly once at
/// the end. Writes are stamped with `(worker_id, ts_start)` as they happen —
/// never revisited at commit — so `commit` is just a single append to this
/// worker's `CommitLog`, i.e. OSIC's "instant commit".
///
/// Each write is also logged to the WAL (if attached) as it happens, under
/// that same fixed stamp — but durability is only *waited for* once, at
/// `commit`/drop, not after every individual write. This is safe because a
/// transaction's writes are all sent to the WAL's channel by the same
/// thread in program order, and the channel preserves per-sender order, so
/// waiting on the *last* write's flush ticket is enough to guarantee every
/// earlier one in the same transaction is durable too (see
/// `mv_wal::writer::WalWriter::log_with_stamp`).
///
/// **Limitation (confirmed out of scope for this pass):** there is no
/// rollback/undo on abort. `insert`/`update`/`delete` enforce first-writer-
/// wins and report a conflict via `CRUDOperationResult::Conflict` instead of
/// applying the write, but any writes already applied earlier in the same
/// transaction are **not** reverted — nor are they un-logged: the WAL has no
/// concept of transaction boundaries, so an abandoned write is replayed on
/// recovery exactly as if it had committed. A real abort would additionally
/// need to physically undo those prior writes (in the tree *and* the WAL)
/// and recycle `ts_start` for a retry (see the paper's "Transaction Abort"),
/// which this transaction doesn't do.
pub struct Transaction<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> {
    tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    committed: bool,
    /// Flush ticket for the most recently logged write, if a WAL is
    /// attached — waited on (once) at `commit`/drop; see the type doc for
    /// why waiting on only the latest one still covers every earlier write.
    wal_ticket: Cell<Option<Receiver<()>>>,
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload
> Transaction<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Draws this transaction's `ts_start` from the tree's Global Logical
    /// Clock and registers it as an active snapshot for the whole lifetime
    /// of the transaction (until `commit` or drop) — the fix that makes
    /// multi-op snapshot isolation actually hold: every read/write issued
    /// through this `Transaction` sees the exact same snapshot, unlike two
    /// independent `dispatch_crud` calls, which could each get a different one.
    pub fn begin(tree: &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>) -> Self {
        let worker_id = tree.worker_id();
        let ts_start = tree.begin_snapshot();

        Self { tree, worker_id, ts_start, committed: false, wal_ticket: Cell::new(None) }
    }

    #[inline(always)]
    pub const fn ts_start(&self) -> Version {
        self.ts_start
    }

    #[inline(always)]
    pub const fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// Point read against this transaction's fixed snapshot.
    pub fn point(&self, key: Key) -> CRUDOperationResult<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.tree.key_point_read_from_root(
            self.tree.retrieve_root_for(self.ts_start),
            key,
            self.worker_id,
            self.ts_start)
    }

    /// Range read against this transaction's fixed snapshot.
    pub fn range(&self, range: Interval<Key>, force_read_all: bool)
        -> CRUDOperationResult<'_, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let scan = RangeQueryIter::new(
            self.tree,
            self.ts_start,
            range,
            false,
            self.worker_id
        );

        if force_read_all {
            CRUDOperationResult::MatchedRecords(scan.collect())
        }
        else{
            CRUDOperationResult::MatchedRecordIter(scan)
        }
}

    /// First-writer-wins check (paper §3.1 "Preliminaries"): the physically
    /// newest version at `key`, if any, must be visible to this
    /// transaction's snapshot — otherwise a concurrent transaction this one
    /// can't see got there first.
    fn newest_visible_to_me(&self, leaf_page: &LeafPage<NUM_RECORDS, Key, Payload>, key: Key) -> bool {
        let mut is_visible
            = |stamp| self.tree.is_visible_stamp(self.worker_id, self.ts_start, stamp);

        leaf_page.as_records()
            .iter()
            .rfind(|r| r.key() == key)
            .map(|record| is_visible(record.version().insertion_stamp()))
            .unwrap_or(true)
    }

    /// Logs `build`'s write under this transaction's fixed `stamp`, storing
    /// the flush ticket (overwriting any earlier one — see the type doc for
    /// why that's safe) for `commit`/drop to wait on. No-op when no WAL is
    /// attached.
    #[inline(always)]
    fn log_write(&self, stamp: TxStamp, build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>) {
        self.wal_ticket.set(self.tree.wal_log_write(stamp, build));
    }

    pub fn insert(&self, key: Key, payload: Payload) -> CRUDOperationResult<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let leaf_guard = self.tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        if !self.newest_visible_to_me(leaf_page, key) {
            return CRUDOperationResult::Conflict;
        }

        if leaf_page.as_records()
            .iter()
            .rfind(|r| r.key == key)
            .map(|r| !r.version.is_deleted())
            .unwrap_or(false)
        {
            return CRUDOperationResult::ZeroAffected(KeyAlreadyExists);
        }

        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        self.log_write(stamp, |_| CRUDOperation::Insert(key, payload.clone()));

        let current_len = leaf_page.len();

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len);

        leaf_page.commit_delta(1, 0);

        CRUDOperationResult::Inserted(stamp.ts_start())
    }

    pub fn update(&self, key: Key, payload: Payload) -> CRUDOperationResult<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let leaf_guard = self.tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        if !self.newest_visible_to_me(leaf_page, key) {
            return CRUDOperationResult::Conflict;
        }

        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        self.log_write(stamp, |_| CRUDOperation::Update(key, payload.clone()));

        let current_len = leaf_page.len();

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len);

        leaf_page.commit_delta(1, 0);

        match leaf_page.delete_after_update(key, stamp) {
            Ok(Some(..)) => {
                leaf_page.commit_delta(-1, 1);
                CRUDOperationResult::Updated(stamp.ts_start())
            }
            Ok(None) => {
                leaf_page.commit_delta(-1, 0);
                leaf_page.undo_uncommitted(current_len);
                CRUDOperationResult::ZeroAffected(KeyDoesNotExist)
            }
            Err(()) => {
                leaf_page.commit_delta(-1, 0);
                leaf_page.undo_uncommitted(current_len);
                CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
            }
        }
    }

    pub fn delete(&self, key: Key) -> CRUDOperationResult<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let leaf_guard = self.tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        if !self.newest_visible_to_me(leaf_page, key) {
            return CRUDOperationResult::Conflict;
        }

        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        self.log_write(stamp, |_| CRUDOperation::Delete(key));

        match leaf_page.delete(key, stamp) {
            Ok(Some(..)) => {
                leaf_page.commit_delta(-1, 1);
                CRUDOperationResult::Deleted(stamp.ts_start())
            }
            Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
            Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted),
        }
    }

    /// Instant commit, Early Lock Release ordering (paper §3.4): appends
    /// `ts_commit` to this worker's `CommitLog` — making every write this
    /// transaction made visible — *before* waiting for them to be durably
    /// flushed to the WAL (if attached; see the type doc for why waiting on
    /// just the last ticket suffices for every earlier write in the same
    /// transaction). Once flushed, blocks until the global durability
    /// watermark has caught up to this transaction's own `ts_start` — i.e.
    /// until everything this transaction could have read is itself
    /// confirmed durable too (dependency tracking) — before returning
    /// `ts_commit` to the caller. No write-set revisit needed, since every
    /// write was already stamped and installed the moment it was applied.
    /// Skips the wait entirely for a transaction that never wrote anything
    /// (no WAL ticket): a read-only transaction has no durability contract
    /// to keep.
    pub fn commit(mut self) -> Version {
        self.committed = true;
        let ticket = self.wal_ticket.take();
        let logged = ticket.is_some();
        let ts_commit = self.tree.commit_tx_elr(self.worker_id, &ticket);
        self.tree.wal_wait_flush(ticket);
        self.tree.finish_elr_commit(self.worker_id, self.ts_start, logged);
        self.tree.end_snapshot(self.ts_start);
        ts_commit
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Drop for Transaction<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        // Dropped without calling `commit` (e.g. the caller gave up after a
        // `Conflict`) — still wait for any logged-but-unflushed write and
        // release the registered snapshot, so neither durability nor
        // GC/commit-log pruning are left hanging. Writes already applied
        // (in the tree *and* the WAL) are *not* rolled back (see this
        // type's doc comment).
        if !self.committed {
            self.tree.wal_wait_flush(self.wal_ticket.take());
            self.tree.end_snapshot(self.ts_start);
        }
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn dispatch_atomic_transaction(&self, atomic_tx: TxAtomicOperation<Key, Payload>)
        -> AtomicTxResult<'_,FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        self.dispatch_crud(atomic_tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
    use crate::mv_crud_model::crud_operation::CRUDOperation;
    use crate::mv_root::index_root::RootIndexType;
    use crate::mv_sync::clock::GlobalClock;
    use crate::mv_sync::commit_log::CommitLog;
    use crate::mv_sync::visibility::{is_visible, SnapshotCache};

    const FAN: usize = 8;
    type TestTree = MVBTSt<FAN, FAN, u64, u64>;

    /// Mirrors the paper's Figure 3 worked example: worker W1 commits two
    /// transactions ("a" then "e"), worker W2 starts a transaction ("d")
    /// that never commits, and a transaction "f" on a third worker takes
    /// its snapshot after a/e committed but while d is still open. f must
    /// see a and e, but not d — exactly the Transitive Commit Invariant
    /// this project's `CommitLog`/`is_visible` are built on.
    #[test]
    fn transitive_commit_invariant_worked_example() {
        let glc = GlobalClock::new();
        let logs = [CommitLog::new(), CommitLog::new(), CommitLog::new()];
        const W1: WorkerId = 0;
        const W2: WorkerId = 1;
        const W3: WorkerId = 2;

        let stamp_a = TxStamp::new(W1, glc.next_timestamp());
        logs[W1 as usize].commit(&glc);

        let stamp_e = TxStamp::new(W1, glc.next_timestamp());
        logs[W1 as usize].commit(&glc);

        // W2 starts "d" but never commits it.
        let stamp_d = TxStamp::new(W2, glc.next_timestamp());

        // f's snapshot: after a/e committed, while d is still in flight.
        let f_ts_start = glc.next_timestamp();
        let mut f_cache = SnapshotCache::new(3);

        assert!(is_visible(&logs, &mut f_cache, W3, f_ts_start, stamp_a),
            "f must see a: committed on W1 before f's snapshot");
        assert!(is_visible(&logs, &mut f_cache, W3, f_ts_start, stamp_e),
            "f must see e: also committed on W1 before f's snapshot");
        assert!(!is_visible(&logs, &mut f_cache, W3, f_ts_start, stamp_d),
            "f must NOT see d: W2 never committed it");
    }

    #[test]
    fn multi_op_transaction_sees_own_writes_and_isolates_others() {
        let tree = TestTree::make_standard(RootIndexType::default());

        let tx1 = Transaction::begin(&tree);
        assert!(matches!(tx1.insert(1, 100), CRUDOperationResult::Inserted(_)));

        // Own writes are visible within the same still-open transaction.
        match tx1.point(1) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 100 => {}
            other => panic!("tx1 should see its own uncommitted write, got {other}"),
        }

        let tree_ref = &tree;

        // A transaction on a different worker, snapshotting before tx1
        // commits, must not see tx1's (still uncommitted) insert.
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let tx2 = Transaction::begin(tree_ref);
                match tx2.point(1) {
                    CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
                    other => panic!("tx2 should not see tx1's uncommitted insert yet, got {other}"),
                }
                tx2.commit();
            }).join().unwrap();
        });

        tx1.commit();

        // A transaction on yet another worker, snapshotting after tx1's
        // commit, must now see it.
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let tx3 = Transaction::begin(tree_ref);
                match tx3.point(1) {
                    CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 100 => {}
                    other => panic!("tx3 should see tx1's now-committed insert, got {other}"),
                }
                tx3.commit();
            }).join().unwrap();
        });
    }

    #[test]
    fn first_writer_wins_conflict() {
        let tree = TestTree::make_standard(RootIndexType::default());
        assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(1, 100)), CRUDOperationResult::Inserted(_)));

        let tx1 = Transaction::begin(&tree);

        // A concurrent transaction on another worker updates and commits
        // key 1 *after* tx1's snapshot was already taken.
        let tree_ref = &tree;
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let tx2 = Transaction::begin(tree_ref);
                assert!(matches!(tx2.update(1, 200), CRUDOperationResult::Updated(_)));
                tx2.commit();
            }).join().unwrap();
        });

        // tx1's snapshot predates tx2's write, so tx1 must lose the race
        // instead of silently overwriting it.
        assert!(matches!(tx1.update(1, 999), CRUDOperationResult::Conflict));
    }

    /// A multi-op `Transaction`'s writes must survive a crash: each
    /// `insert`/`update`/`delete` logs to the WAL as it happens (under the
    /// transaction's one fixed stamp), and `commit` waits for all of them to
    /// be durably flushed before returning — so once `commit()` has
    /// returned, every write the transaction made must reappear after
    /// `open_recovered`, even though recovery mints fresh stamps for
    /// everything it replays and has no notion of transaction boundaries.
    #[test]
    fn multi_op_transaction_writes_are_durable_across_recovery() {
        let path = std::env::temp_dir().join(format!("cmvbt_tx_wal_test_{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        {
            let tree = TestTree::make_standard(RootIndexType::default());
            tree.enable_wal(&path, std::time::Duration::from_millis(2)).unwrap();

            let tx = Transaction::begin(&tree);
            assert!(matches!(tx.insert(1, 100), CRUDOperationResult::Inserted(_)));
            assert!(matches!(tx.insert(2, 200), CRUDOperationResult::Inserted(_)));
            assert!(matches!(tx.update(1, 101), CRUDOperationResult::Updated(_)));
            assert!(matches!(tx.delete(2), CRUDOperationResult::Deleted(_)));
            tx.commit();
            // tree drops here: commit() already waited for durability, so
            // this isn't relied on for correctness, just cleanup ordering.
        }

        let recovered = TestTree::open_recovered(
            RootIndexType::default(), &path, std::time::Duration::from_millis(2)).unwrap();
        let recovered_version = recovered.current_version();

        match recovered.dispatch_crud(CRUDOperation::Point(1, recovered_version)) {
            CRUDOperationResult::MatchedRecords(r) if r.len() == 1 && r[0].payload == 101 => {}
            other => panic!("key 1 should survive recovery with its updated payload, got {other}"),
        }
        match recovered.dispatch_crud(CRUDOperation::Point(2, recovered_version)) {
            CRUDOperationResult::MatchedRecords(r) if r.is_empty() => {}
            other => panic!("key 2 should stay deleted after recovery, got {other}"),
        }

        let _ = std::fs::remove_file(&path);
    }
}
