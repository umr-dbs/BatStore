//! Generalization of `mv_bench::tpcc_txn::TpccTxn` for a `mv_db::Database`:
//! a multi-table OSIC transaction (paper §3.5, "Putting Everything
//! Together") — one fixed snapshot (`ts_start`), drawn once from the
//! database's shared `TxContext` at `begin` and reused by every read/write
//! this transaction issues against any of its tables, committed exactly
//! once at the end. Writes are stamped with `(worker_id, ts_start)` as they
//! happen — never revisited at commit — so `commit` is just a single append
//! to this worker's `CommitLog`, i.e. OSIC's "instant commit".
//!
//! Each write is also logged to the WAL (if attached) as it happens, under
//! that same fixed stamp — fire-and-forget, same as single-op
//! `dispatch_crud` (see `MVBTSt::wal_hardened_version`'s doc): `commit`
//! never waits for any of this transaction's writes to actually flush.
//! `commit` additionally logs one WAL Commit marker for the whole
//! transaction (see `WalEntry::Commit`'s doc) — replay only ever applies a
//! write once it finds that marker, so an aborted transaction's writes
//! (below) are simply never replayed, with no separate reversal needed on
//! the WAL side. Callers that need a durability point-in-time for this
//! transaction should call `wal_hardened_version`/`wait_wal_hardened`
//! themselves after `commit` returns.
//!
//! **Abort**: dropping this transaction without calling `commit()` (e.g.
//! after a `Conflict`, or a business-logic rollback) reverts every write it
//! already applied, automatically, in `Drop` — see `MVBTSt::abort_write`.
//! Each write's *key* (paired with the table it was written on) is
//! remembered (not a full undo log: replaying it back to its live-tree
//! state is enough, since every current caller writes a given key at most
//! once per transaction — a transaction that wrote the same key more than
//! once would still abort safely, just without replaying its intermediate
//! states one at a time). Reversal marks the record invalid (or undeletes
//! it, for a plain `Delete`) rather than physically removing it — see
//! `TxStamp::is_invalid`'s doc — and is purely in-memory: since this
//! transaction never commits, no `wal_log_commit` marker is ever logged for
//! it, so replay skips its writes regardless of whether/when a crash
//! happens relative to this abort.
//!
//! Two deliberate differences from `TpccTxn`, both consequences of
//! `Database`'s tables being looked up at runtime (`Arc<MVBTSt<..>>`, from a
//! locked map) rather than fixed struct fields borrowed for the whole
//! transaction's lifetime:
//! - `range` is always eager (`CRUDOperationResult::MatchedRecords`, never
//!   the lazy `MatchedRecordIter`): a `RangeQueryIter<'a>` borrowing from a
//!   function-local `Arc` clone can't outlive this method call. A caller
//!   wanting a zero-copy streaming scan on one table should fetch
//!   `Database::table(id)` into a local binding themselves and call
//!   `dispatch_crud(RangeIterSi(..))` on it directly.
//! - `commit()` logs exactly one Commit marker (through any one touched
//!   table's tree — they all share the same underlying `Arc<WalWriter>`),
//!   not a loop over every touched table.
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationInnerReason::{KeyAlreadyDeleted, KeyAlreadyExists, KeyDoesNotExist};
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_query::interval::Interval;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::{RecordPoint, RecordPointResult};
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::{Version, VersionInfo};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_wal::record::{TableId, WalPayload};
use std::cell::RefCell;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::DerefMut;
use triomphe::Arc;

use super::database::Database;

pub enum TransactionState {
    InFlight,
    Committed,
    Aborted
}

pub struct DbTransaction<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> {
    db: &'a Database<FAN_OUT, NUM_RECORDS, Key, Payload>,
    worker_id: WorkerId,
    ts_start: Version,
    committed: TransactionState,
    /// `(table, key)` pairs this transaction has actually written (on a
    /// successful `Inserted`/`Updated`/`Deleted` outcome only — never on
    /// `Conflict`/`ZeroAffected`, since nothing was written there to
    /// revert). Walked by `Drop` to abort every one of them if `commit()`
    /// was never called.
    written: RefCell<Vec<(TableId, Key)>>,
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> DbTransaction<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Draws `ts_start` from the database's shared `TxContext` and registers
    /// it as an active snapshot *once*, covering every table this
    /// transaction may go on to touch — not just whichever table happens to
    /// be read/written first (see `TpccTxn::begin`'s identical reasoning).
    pub fn begin(db: &'a Database<FAN_OUT, NUM_RECORDS, Key, Payload>) -> Self {
        let worker_id = db.worker_id();
        let ts_start = db.begin_snapshot();

        Self { db, worker_id, ts_start, committed: TransactionState::InFlight, written: RefCell::new(Vec::new()) }
    }

    #[inline(always)]
    pub const fn ts_start(&self) -> Version {
        self.ts_start
    }

    #[inline(always)]
    pub const fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    fn tree(&self, table: TableId) -> Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        self.db.table(table)
            .unwrap_or_else(|| panic!("mv_db::DbTransaction: unknown TableId {table} (did you forget Database::create_table?)"))
    }

    /// Point read against this transaction's fixed snapshot, on `table`.
    /// Always eager (`MatchedRecords`) — a point read never produces the
    /// lazy iterator variant, but the return type is pinned to `'static`
    /// (rather than elided to `&self`) since the underlying tree is a
    /// function-local `Arc`, not a field borrowed for `'a` — see the module
    /// doc.
    pub fn point(&self, table: TableId, key: Key) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let tree = self.tree(table);
        match tree.key_point_read_from_root(tree.retrieve_root_for(self.ts_start), key, self.worker_id, self.ts_start) {
            CRUDOperationResult::MatchedRecords(v) => CRUDOperationResult::MatchedRecords(v),
            other => panic!("mv_db::DbTransaction::point: expected MatchedRecords, got {other}"),
        }
    }

    /// Range read against this transaction's fixed snapshot, on `table` —
    /// always eager, see the module doc for why.
    pub fn range(&self, table: TableId, range: Interval<Key>) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let tree = self.tree(table);
        let scan = RangeQueryIter::new(&tree, self.ts_start, range, false, self.worker_id);
        CRUDOperationResult::MatchedRecords(scan.collect())
    }

    /// Like `range`, but only ever finds the record with the smallest key
    /// — see `RangeQueryIter::min_by_key`'s doc for why that needs a real
    /// comparison within the first matching leaf rather than just taking
    /// whatever `next()` yields first (leaf pages are append-ordered, not
    /// key-sorted). No lifetime issue despite `range` being forced eager
    /// for the same reason (see the module doc): the `RangeQueryIter`
    /// itself never leaves this function, only the one owned result it
    /// produces does. Callers that only want the minimum (e.g.
    /// `mv_bench::tpcc_txn::deliver_one_district`'s "find the oldest queued
    /// new-order") no longer have to collect the entire range to get it.
    pub fn range_min(&self, table: TableId, range: Interval<Key>) -> Option<RecordPointResult<Key, Payload>> {
        let tree = self.tree(table);
        RangeQueryIter::new(&tree, self.ts_start, range, false, self.worker_id).min_by_key()
    }

    // /// First-writer-wins check, on `table`: the physically newest version at
    // /// `key`, if any, must be visible to this transaction's snapshot — see
    // /// `TpccTxn::newest_visible_to_me`'s identical reasoning (including why
    // /// an invalid/aborted entry is skipped rather than checked).
    // fn newest_visible_to_me(
    //     &self,
    //     tree: &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
    //     leaf_page: &LeafPage<NUM_RECORDS, Key, Payload>,
    //     key: Key,
    // ) -> bool {
    //     let is_visible = |stamp| tree.is_visible_stamp(self.worker_id, self.ts_start, stamp);
    //
    //     leaf_page.as_records()
    //         .iter()
    //         .rfind(|r| r.key() == key && !r.version().insertion_stamp().is_invalid())
    //         .map(|record| is_visible(record.version().insertion_stamp()))
    //         .unwrap_or(true)
    // }

    pub fn insert(&self, table: TableId, key: Key, payload: Payload) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let tree = self.tree(table);
        let leaf_guard = tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        // if !self.newest_visible_to_me(&tree, leaf_page, key) {
        //     return CRUDOperationResult::Conflict;
        // }

        if let Some(crud_error) = leaf_page
            .as_records()
            .iter()
            .rfind(|r| r.key == key)
            .filter(|r| r.version.is_live())
            .map(|r|
                if r.version.insertion_stamp().worker_id() != self.worker_id {
                    CRUDOperationResult::Conflict
                } else {
                    CRUDOperationResult::ZeroAffected(KeyAlreadyExists)
                })
        {
            return crud_error
        }

        let stamp = TxStamp::new(self.worker_id, self.ts_start);
        tree.wal_log_write(stamp, |_| CRUDOperation::Insert(key, payload.clone()));

        let current_len = leaf_page.len();

        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len);

        leaf_page.commit_delta(1, 0);

        self.written.borrow_mut().push((table, key));
        CRUDOperationResult::Inserted(stamp.ts_start())
    }

    pub fn update(&self, table: TableId, key: Key, payload: Payload) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let tree = self.tree(table);
        let leaf_guard = tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        // if !self.newest_visible_to_me(&tree, leaf_page, key) {
        //     return CRUDOperationResult::Conflict;
        // }

        // Skip a physically-newest entry that's `invalid` (a since-aborted
        // transaction's `Insert`/`Update`, left in place until the next SMO —
        // see `LeafPage::apply_invalidate`'s doc): `is_visible_stamp` always
        // reports an invalid stamp as *not visible, to anyone* (see
        // `visibility::is_visible`'s doc — its own aborting writer included),
        // since a reader must never see an aborted write. That's the right
        // answer for a *read*, but wrong here — this is asking "is the
        // current live state safe for me to overwrite", and treating a dead
        // entry's invisibility as "someone else committed something newer"
        // would report `Conflict` forever after any abort touched this key,
        // even though `abort_write` already restored the true live
        // predecessor underneath it. Mirrors `LeafPage::is_live_lineage`,
        // which every abort/undelete path already searches by.
        match leaf_page
            .as_records_mut()
            .iter_mut()
            .rfind(|r| r.key() == key && !r.version.insertion_stamp().is_invalid())
        {
            Some(record) =>
                if tree.is_visible_stamp(self.worker_id, self.ts_start, record.version.insertion_stamp()) {
                    let stamp
                        = TxStamp::new(self.worker_id, self.ts_start);

                    tree.wal_log_write(stamp, |_| CRUDOperation::Update(key, payload.clone()));
                    if !record.version.delete(stamp) {
                        return CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                    }

                    let current_len
                        = leaf_page.len();

                    leaf_page.push_uncommitted(
                        RecordPoint::new(key, VersionInfo::new(stamp), payload),
                        current_len);

                    leaf_page.commit_delta(0, 1);

                    self.written.borrow_mut().push((table, key));
                    CRUDOperationResult::Updated(stamp.ts_start())
                }
                else {
                    CRUDOperationResult::Conflict
                },
            None => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
        }
    }

    pub fn delete(&self, table: TableId, key: Key) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let tree = self.tree(table);
        let leaf_guard = tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        let leaf_page = leaf_deref_mut.as_leaf_page();

        // if !self.newest_visible_to_me(&tree, leaf_page, key) {
        //     return CRUDOperationResult::Conflict;
        // }

        // See `update`'s identical comment just above: skip a physically-
        // newest but `invalid` (since-aborted) entry, or its permanently-
        // invisible stamp reports a spurious `Conflict` for every later
        // writer of this key.
        match leaf_page
            .as_records_mut()
            .iter_mut()
            .rfind(|r| r.key == key && !r.version.insertion_stamp().is_invalid())
        {
            Some(record) => if tree.is_visible_stamp(
                self.worker_id,
                self.ts_start,
                record.version.insertion_stamp())
            {
                let stamp = TxStamp::new(self.worker_id, self.ts_start);
                tree.wal_log_write(stamp, |_| CRUDOperation::Delete(key));

                if record.version.delete(stamp) {
                    leaf_page.commit_delta(-1, 1);
                    self.written.borrow_mut().push((table, key));

                    CRUDOperationResult::Deleted(stamp.ts_start())
                }
                else {
                    CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                }
            }
            else {
                CRUDOperationResult::Conflict
            },
            None => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
        }
    }

    pub const fn is_committed(&self) -> bool {
        matches!(self.committed, TransactionState::Committed)
    }

    pub const fn is_aborted(&self) -> bool {
        matches!(self.committed, TransactionState::Aborted)
    }

    /// Instant commit: appends `ts_commit` to this worker's (shared)
    /// `CommitLog` — making every write this transaction made, across every
    /// table it touched, visible at once — then logs exactly **one** WAL
    /// Commit marker, through whichever table this transaction happened to
    /// write first (every table on this database shares the same
    /// `Arc<WalWriter>`, so it doesn't matter which — unlike `TpccTxn`,
    /// which must log one marker per touched table since each has its own
    /// file). No-op (nothing to log) if this transaction never wrote
    /// anything.
    pub fn commit(mut self) -> Option<Version> {
        if let TransactionState::InFlight = self.committed {
            self.committed = TransactionState::Committed;

            let ts_commit = self.db.ctx.commit_tx( self.worker_id);

            if let Some( & (table, _)) = self.written.borrow().first() {
            let stamp = TxStamp::new( self.worker_id, self.ts_start);
            self.tree(table).wal_log_commit(stamp, ts_commit);
            }

            self.db.end_snapshot(self.ts_start);
            Some(ts_commit)
        }
        else {
            None
        }
    }

    pub fn abort(mut self) -> bool {
        if let TransactionState::InFlight = self.committed {
            self.committed = TransactionState::Aborted;

            // Reverse (LIFO) order, not chronological: reverting is not
            // atomic across every written key at once — each `abort_write`
            // individually re-exposes that one key to any other concurrent
            // transaction the instant it runs, well before the rest of
            // `self.written` has been reverted too. When an earlier write
            // in this transaction effectively acts as a lock/dequeue step
            // that later writes in the same transaction depend on (e.g.
            // `mv_bench::tpcc_txn::deliver_one_district` deletes a NewOrder
            // queue entry first, then updates that order's Orders/OrderLine/
            // Customer rows), reverting in forward order un-deletes — i.e.
            // re-queues — that entry *first*, while this abort still has
            // several other writes left to revert: a concurrent Delivery
            // scan can pick the freshly re-queued order back up and start
            // racing this thread's own in-flight reversal of its
            // OrderLine/Customer rows, corrupting them out from under it.
            // Reverting last-write-first instead means every write this
            // transaction made *after* that lock/dequeue step is already
            // fully reverted by the time the dequeue step's own reversal
            // makes the entry visible to anyone else again — the same
            // ordering a plain undo-log/rollback would use.
            let stamp = TxStamp::new(self.worker_id, self.ts_start);
            for &(table, key) in self.written.borrow().iter().rev() {
                self.tree(table).abort_write(key, stamp);
            }

            self.db.end_snapshot(self.ts_start);
            true
        }
        else {
            false
        }
    }
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> Drop for DbTransaction<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn drop(&mut self) {
        // Dropped without calling `commit`/`abort` (e.g. the caller gave up
        // after a `Conflict` without calling `abort` explicitly) — abort
        // every write this transaction made, on whichever table it made it
        // on, before releasing the registered snapshot (see the type doc
        // and `MVBTSt::abort_write`). An explicit `commit()`/`abort()` call
        // already did this (and released the snapshot) itself, so skip
        // here — not just belt-and-suspenders: re-running would double
        // `end_snapshot` this transaction's `ts_start`.
        if let TransactionState::InFlight = self.committed {
            // Reverse (LIFO) order — see `abort`'s identical doc above for
            // why forward order can expose a partially-unwound transaction
            // to a concurrent one mid-abort.
            let stamp = TxStamp::new(self.worker_id, self.ts_start);
            for &(table, key) in self.written.borrow().iter().rev() {
                self.tree(table).abort_write(key, stamp);
            }
            self.db.end_snapshot(self.ts_start);
        }
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> Database<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// One-op convenience: opens a `DbTransaction`, performs a single
    /// insert/update/delete/point/range, and commits (or, on a write
    /// `Conflict`, just drops the transaction instead — nothing was written
    /// on this fresh, single-op transaction, so `Drop`'s abort loop is a
    /// no-op) — for callers who don't need an explicit multi-op
    /// transaction. Mirrors how `MVBTSt::dispatch_crud`'s single-op
    /// auto-commit path is itself just a trivial one-operation transaction.
    pub fn dispatch_crud(&self, table: TableId, op: CRUDOperation<Key, Payload>) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let tx = DbTransaction::begin(self);

        let result = match op {
            CRUDOperation::Insert(key, payload) => tx.insert(table, key, payload),
            CRUDOperation::Update(key, payload) => tx.update(table, key, payload),
            CRUDOperation::Delete(key) => tx.delete(table, key),
            CRUDOperation::Point(key, _) | CRUDOperation::PointSi(key) => tx.point(table, key),
            CRUDOperation::Range(range, _) | CRUDOperation::RangeSi(range) => tx.range(table, range),
            other => panic!("mv_db::Database::dispatch_crud: unsupported op {other}"),
        };

        if matches!(result, CRUDOperationResult::Conflict) {
            drop(tx);
        } else {
            tx.commit();
        }

        result
    }
}
