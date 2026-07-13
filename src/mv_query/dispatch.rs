use std::hash::Hash;
use std::fmt::Display;
use std::mem;
use std::sync::atomic::Ordering::Relaxed;
use itertools::Itertools;
use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationInnerReason::{KeyAlreadyDeleted, KeyAlreadyExists, KeyDoesNotExist};
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_query::rand_query::RAND_ATTEMPTS_MAX;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;
use crate::mv_test::VERBOSE;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_sync::smart_cell::sched_yield;

pub const RANGE_DISPATCH_LAZY: bool = true;

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Decides whether `Update`/`UpdateRand` should take the "update in
    /// place" fast path (mutate an existing record's payload without
    /// minting a new version) rather than the normal versioned
    /// insert-then-supersede path. Only decides — callers apply the
    /// mutation themselves, since `Update` and `UpdateRand` differ slightly
    /// in how they do so.
    ///
    /// `*Rand` operations (`UpdateRand` here) are used purely for
    /// benchmark/data-generation workloads, never logged to the WAL, and so
    /// are free to take this fast path whenever the heuristic says so. The
    /// WAL-relevant `Update` arm additionally gates this off entirely
    /// whenever a WAL is attached — see the call site.
    pub(crate) fn decide_update_in_place(
        &self,
        leaf_page: &LeafPage<NUM_RECORDS, Key, Payload>,
        key: Key,
    ) -> bool {
        if !self.has_update_in_place() {
            return false;
        }

        match self.tracker().newest_live_si() {
            Some(newest_si) => leaf_page
                .as_records()
                .iter()
                .rfind(|r| r.key() == key)
                .map(|record| record.version.insert_stamp.ts_start() > newest_si)
                .unwrap_or(false),
            None => leaf_page // empty live index: No readers; e.g., only updates!
                .as_records()
                .iter()
                .rfind(|r| r.key() == key)
                .is_some(),
        }
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload
> AtomicTxDispatcher<'a, FAN_OUT, NUM_RECORDS, Key, Payload> for MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn dispatch_crud(&'a self, crud: CRUDOperation<Key, Payload>) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        match crud {
            CRUDOperation::Insert(key, payload) => {
                let leaf_guard =
                    self.traversal_write_olc(key);

                let leaf_deref_mut = leaf_guard
                    .deref_mut();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                if leaf_page.as_records()
                    .iter()
                    .rfind(|r| r.key == key)
                    .map(|r| !r.version.is_deleted())
                    .unwrap_or(false)
                {
                    return CRUDOperationResult::ZeroAffected(KeyAlreadyExists);
                }

                let current_len
                    = leaf_page.len();

                let stamp
                    = self.wal_start_commit(|_| CRUDOperation::Insert(key, payload.clone()));

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len);

                leaf_page.commit_delta(1, 0);

                drop(leaf_guard);
                // Commit (visibility) and return immediately — the WAL
                // record (if any) is flushed asynchronously in a batch by
                // its shard's background thread, not waited on here. See
                // `MVBTSt::wal_hardened_version`'s doc for how to check/wait
                // for durability explicitly instead.
                let _ts_commit = self.commit_tx(stamp.worker_id());

                CRUDOperationResult::Inserted(stamp.ts_start())
            }
            CRUDOperation::Update(key, payload) => {
                let leaf_guard =
                    self.traversal_write_olc(key);

                let leaf_deref_mut = leaf_guard
                    .deref_mut();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let current_len
                    = leaf_page.len();

                // The in-place fast path never mints a version, which can't be
                // represented in the WAL (one CRUDOperation = one version = one log
                // record), so it's skipped entirely whenever a WAL is attached —
                // every logged Update always takes the normal versioned path below.
                if !self.wal_ever_enabled.load(Relaxed) && self.decide_update_in_place(leaf_page, key) {
                    if let Some(record) = leaf_page
                        .as_records_mut()
                        .iter_mut()
                        .rfind(|r| r.key() == key)
                    {
                        *record.payload_mut() = payload;
                        if record.version.is_deleted() {
                            record.version_mut().undelete();

                            leaf_page.commit_delta(1, -1);
                        }

                        return CRUDOperationResult::Updated(self.current_version())
                    }
                }

                let stamp
                    = self.wal_start_commit(|_| CRUDOperation::Update(key, payload.clone()));

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len);

                // soft commit for atomic visibility of new published record
                leaf_page.commit_delta(1, 0);

                match leaf_page.delete_after_update(key, stamp) {
                    Ok(Some(..)) => {
                        // Apply second soft atomic commit for lifetime end
                        leaf_page.commit_delta(-1, 1);

                        drop(leaf_guard);
                        // Fire-and-forget WAL, see the Insert arm above.
                        let _ts_commit = self.commit_tx(stamp.worker_id());

                        CRUDOperationResult::Updated(stamp.ts_start())
                    }
                    Ok(None) => {
                        // Reverse the soft commit above: the pushed record never
                        // became a real, superseding update, so it must not stay
                        // counted as active or `unsafe_degree()`'s fill-ratio reads
                        // drift out of sync with the leaf's true content.
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
            CRUDOperation::Delete(key) => {
                if VERBOSE {
                    println!("dispatch delete key={key}");
                }

                let leaf_guard =
                    self.traversal_write_olc(key);

                if VERBOSE {
                    println!("traverse olc end");
                    println!("[key={key}] - Leaf: ({:?}) records", leaf_guard.active_dead_count());
                }
                let leaf_deref_mut = leaf_guard
                    .deref_mut();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                if VERBOSE {
                    println!("[key={key}] - Begin_commit()");
                }
                if VERBOSE {
                    println!("[key={key}] - Loop start");
                }

                let stamp
                    = self.wal_start_commit(|_| CRUDOperation::Delete(key));

                if VERBOSE {
                    println!("[key={key}] - Commit succeeded: {}, Attempts: 0", stamp.ts_start());
                }

                match leaf_page.delete(key, stamp) {
                    Ok(Some(..)) => {
                        leaf_page.commit_delta(-1, 1);
                        if VERBOSE {
                            println!("After delete Leaf-records:\n{}", leaf_page.as_records().iter().join("\n"));
                        }

                        drop(leaf_guard);
                        // Fire-and-forget WAL, see the Insert arm above.
                        let _ts_commit = self.commit_tx(stamp.worker_id());
                        CRUDOperationResult::Deleted(stamp.ts_start())
                    },
                    Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                    Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                }
            }
            CRUDOperation::Range(range, version)
            if RANGE_DISPATCH_LAZY => match self.dispatch_crud(
                CRUDOperation::RangeIter(range, version)) {
                CRUDOperationResult::MatchedRecordIter(iter) =>
                    CRUDOperationResult::MatchedRecords(iter.collect()),
                other => other
            },
            CRUDOperation::Range(range, version) => {
                let reader_worker = self.worker_id();
                self.on_acquire_reader_snapshot(version);
                let res = self.key_range_read_from_root(
                    self.retrieve_root_for(version),
                    range,
                    reader_worker,
                    version);
                self.on_release_reader_snapshot(version);
                res
            },
            CRUDOperation::Point(key, version) => {
                let reader_worker = self.worker_id();
                self.on_acquire_reader_snapshot(version);
                let res = self.key_point_read_from_root(
                    self.retrieve_root_for(version),
                    key,
                    reader_worker,
                    version);
                self.on_release_reader_snapshot(version);
                res
            },
            CRUDOperation::RangeIter(key, version) =>
                CRUDOperationResult::MatchedRecordIter(RangeQueryIter::new(
                    self,
                    version,
                    key,
                    true,
                    self.worker_id())),
            // `*Si` ("read the current snapshot") variants: unlike
            // `Point`/`Range`/`RangeIter`, which accept a `version` the
            // caller already drew (typically via `current_version()`,
            // arbitrarily long before this call — the right choice for a
            // deliberate, explicit-version read, e.g. a historical query or
            // one sharing an already-open `Transaction`'s snapshot, but
            // racy for "just read whatever's freshest right now": there's a
            // real gap between the caller reading that version and this
            // function registering it, during which a concurrent GC
            // decision can't see this reader yet and may reclaim a page it
            // needs — see `mv_sync::version_handle::begin_snapshot`'s doc,
            // which this mirrors), these draw their own version via
            // `begin_snapshot` — gap-free by construction, since drawing
            // and registering happen as one unit there.
            CRUDOperation::PointSi(key) => {
                let reader_worker = self.worker_id();
                let version = self.begin_snapshot();
                let res = self.key_point_read_from_root(
                    self.retrieve_root_for(version),
                    key,
                    reader_worker,
                    version);
                self.end_snapshot(version);
                res
            },
            CRUDOperation::RangeSi(range) => match self.dispatch_crud(
                CRUDOperation::RangeIterSi(range)) {
                CRUDOperationResult::MatchedRecordIter(iter) =>
                    CRUDOperationResult::MatchedRecords(iter.collect()),
                other => other
            },
            // Uses `draw_snapshot_version_with`, not `begin_snapshot`:
            // `RangeQueryIter::new`'s own `register_reader_si: true` path is
            // what actually registers this version (so that *it* — not this
            // arm — is what releases it later, on completion or drop, since
            // the iterator outlives this function call). Calling
            // `begin_snapshot` here too would register twice per read but
            // only ever release once — a permanent leak that pins
            // `live_min_snapshot` at this version forever, so `free_block`
            // could never reclaim anything again for the lifetime of the
            // tree.
            CRUDOperation::RangeIterSi(key) =>
                CRUDOperationResult::MatchedRecordIter(self.draw_snapshot_version_with(|version| {
                    RangeQueryIter::new(self, version, key, true, self.worker_id())
                })),
            // `*Rand` operations are used purely for benchmark/data-generation
            // workloads (see mv_test.rs) — irrelevant to the actual running
            // system, so they're never routed through the WAL at all.
            CRUDOperation::UpdateRand => {
                let (_fence, leaf_guard) =
                    self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard
                    .deref_mut();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let current_len
                    = leaf_page.len();

                let (live_n, _dead_n)
                    = leaf_page.active_dead_count();

                let mut find_i
                    = rand::random_range(0..live_n as usize);

                let mut key = Key::default();
                let payload = Payload::default();

                for r in leaf_page.as_records() {
                    if !r.version().is_deleted() {
                        if find_i == 0 {
                            key = r.key;
                            break
                        }
                        find_i -= 1;
                    }
                };

                if self.decide_update_in_place(leaf_page, key) {
                    if let Some(record) = leaf_page
                        .as_records_mut()
                        .iter_mut()
                        .rfind(|r| r.key() == key)
                    {
                        record.version_mut().undelete();
                        *record.payload_mut() = payload;
                        leaf_page.commit_delta(1, -1);

                        return CRUDOperationResult::UpdatedRand(key, self.current_version())
                    }
                }

                let stamp
                    = TxStamp::new(self.worker_id(), self.start_tx_commit());

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len);

                // two steps soft commit: Mark new record visible
                leaf_page.commit_delta(1, 0);
                match leaf_page.delete_after_update(key, stamp) {
                    Ok(Some(..)) => {
                        // second step soft commit: Correct counters
                        leaf_page.commit_delta(-1, 1);
                        self.commit_tx(stamp.worker_id());

                        CRUDOperationResult::UpdatedRand(key, stamp.ts_start())
                    }
                    Ok(None) => {
                        // Same counter reversal as the Update arm: the pushed
                        // record never became a real, superseding update.
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
            CRUDOperation::DeleteRand => {
                let (_fence, leaf_guard)
                    = self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard
                    .deref_mut();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let (live_n, _dead_n)
                    = leaf_page.active_dead_count();

                let mut find_i
                    = rand::random_range(0..live_n as usize);

                let mut key
                    = Key::default();

                for r in leaf_page.as_records() {
                    if !r.version().is_deleted() {
                        if find_i == 0 {
                            key = r.key;
                            break
                        }
                        find_i -= 1;
                    }
                };

                let stamp
                    = TxStamp::new(self.worker_id(), self.start_tx_commit());

                if VERBOSE {
                    println!("[key={key}] - Commit succeeded: {}, Attempts: 0", stamp.ts_start());
                }
                match leaf_page.delete(key, stamp) {
                    Ok(Some(..)) => {
                        leaf_page.commit_delta(-1, 1);
                        if VERBOSE {
                            println!("After delete Leaf-records:\n{}", leaf_page.as_records().iter().join("\n"));
                        }

                        self.commit_tx(stamp.worker_id());
                        CRUDOperationResult::DeletedRand(key, stamp.ts_start())
                    },
                    Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                    Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                }
            }
            CRUDOperation::InsertRand => {
                let (fence, leaf_guard) =
                    self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard
                    .deref_mut();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                if size_of::<Key>() != mem::size_of::<u64>() { // Not supported
                    println!(">>>> CRUDOperation::InsertRand only supported on *u64* !");
                    return CRUDOperationResult::Error
                }

                let min = unsafe { *((&fence.lower) as * const _ as *const u64) };
                let max = unsafe { *((&fence.upper) as * const _ as *const u64) };

                let mut rand_attempts = 0;
                let key = loop {
                    let generated = rand::random_range(min..=max);
                    let gen_key = unsafe { *((&generated) as * const _ as * const Key) };

                    match leaf_page.as_records()
                        .iter()
                        .rfind(|r| r.key == gen_key)
                    {
                        None => break Some(gen_key),
                        Some(record) if record.version().is_deleted() =>
                            break Some(gen_key),
                        _ if rand_attempts >= RAND_ATTEMPTS_MAX => break None,
                        _ => {
                            rand_attempts += 1;
                            sched_yield(rand_attempts);
                        }
                    }
                };

                if key.is_none() {
                    println!(">> RandKey Generation Failed!\
                    >> RAND_ATTEMPTS_MAX = {RAND_ATTEMPTS_MAX}\
                    >> Fence = {fence}");

                    return self.dispatch_crud(CRUDOperation::InsertRand)
                }

                let key = key.unwrap();
                debug_assert!(key <= fence.upper && key >= fence.lower);
                if VERBOSE {
                    println!("[RandInsert] - Key: {key}, Fence= min: {min}, max: {max}");
                }
                let payload = Payload::default();

                let current_len
                    = leaf_page.len();

                let stamp
                    = TxStamp::new(self.worker_id(), self.start_tx_commit());

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len);

                leaf_page.commit_delta(1, 0);
                self.commit_tx(stamp.worker_id());

                CRUDOperationResult::InsertedRand(key, stamp.ts_start())
            }
            _ => CRUDOperationResult::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv_page_model::node::{Active, Dead};
    use crate::mv_root::index_root::RootIndexType;

    const FAN: usize = 8;
    type TestTree = MVBTSt<FAN, FAN, u64, u64>;

    fn leaf_counts(tree: &TestTree, key: u64) -> (Active, Dead) {
        let leaf_guard = tree.traversal_write_olc(key);
        let leaf_deref_mut = leaf_guard.deref_mut();
        leaf_deref_mut.as_leaf_page().active_dead_count()
    }

    /// Regression test: `MVBTSt::commit_tx` must NOT prune a worker's
    /// `CommitLog` while block-reclaim GC is disabled — pruning assumes any
    /// record whose `LCB` data gets dropped is itself unreachable, which is
    /// only true when block reclaim is actually removing dead pages in
    /// lockstep (see `TrackerHandleSt`'s type doc). Without GC, dead
    /// records — and an explicit historical read at an old `version` — stay
    /// reachable forever, so the log must grow unboundedly instead of
    /// silently losing the `LCB` data such a read would need.
    #[test]
    fn commit_log_grows_unbounded_without_gc_enabled() {
        let tree = TestTree::make_standard(RootIndexType::default());

        for k in 0..10_000u64 {
            assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(k, k)), CRUDOperationResult::Inserted(_)));
        }

        let worker_id = tree.worker_id();
        let max_workers = tree.commit_logs.len();
        let len = tree.commit_logs[worker_id as usize].len();
        assert!(
            len > max_workers,
            "expected the commit log to grow unbounded with GC off, got only {len} entries (max_workers = {max_workers})"
        );
    }

    /// Counterpart to `commit_log_grows_unbounded_without_gc_enabled`: once
    /// `enable_gc` has actually been called, pruning is sound again (block
    /// reclaim is now removing dead pages in the same lockstep `LCB` pruning
    /// assumes), so the log should stay bounded near `max_workers`.
    #[test]
    fn commit_log_stays_bounded_with_gc_enabled() {
        let tree = TestTree::make_standard(RootIndexType::default());
        tree.enable_gc(false);

        for k in 0..10_000u64 {
            assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(k, k)), CRUDOperationResult::Inserted(_)));
        }

        let worker_id = tree.worker_id();
        let max_workers = tree.commit_logs.len();
        let len = tree.commit_logs[worker_id as usize].len();
        assert!(
            len <= max_workers,
            "expected the commit log to stay pruned near max_workers ({max_workers}) with GC on, got {len} entries"
        );
    }

    /// Regression test for a bug found while building the WAL: `Update`'s
    /// `Ok(None)` (KeyDoesNotExist) and `Err(())` (KeyAlreadyDeleted)
    /// failure branches called `undo_uncommitted` without reversing the
    /// `commit_delta(1, 0)` applied just before, permanently inflating the
    /// leaf's tracked active count even though nothing was actually
    /// inserted. A failed op mutates nothing, so the leaf's tracked
    /// (active, dead) counts must be *exactly* the same before and after —
    /// checked directly, not via a rescan through `as_records()`, which is
    /// itself bounded by the same (possibly-corrupted) length and so can't
    /// independently catch this.
    #[test]
    fn failed_update_leaves_counts_unchanged() {
        let tree = TestTree::make_standard(RootIndexType::default());

        assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(1, 100)), CRUDOperationResult::Inserted(_)));

        // Update on a key that was never inserted: delete_after_update finds
        // no prior record for it at all -> Ok(None).
        let before = leaf_counts(&tree, 1);
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Update(999, 1)),
            CRUDOperationResult::ZeroAffected(_)
        ));
        assert_eq!(leaf_counts(&tree, 1), before, "counts changed after an Ok(None) (KeyDoesNotExist) failure");

        // Insert key=2, delete it, then Update it again: delete_after_update
        // finds the prior record but it's already deleted -> Err(()).
        assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(2, 200)), CRUDOperationResult::Inserted(_)));
        assert!(matches!(tree.dispatch_crud(CRUDOperation::Delete(2)), CRUDOperationResult::Deleted(_)));

        let before2 = leaf_counts(&tree, 2);
        assert!(matches!(
            tree.dispatch_crud(CRUDOperation::Update(2, 201)),
            CRUDOperationResult::ZeroAffected(_)
        ));
        assert_eq!(leaf_counts(&tree, 2), before2, "counts changed after an Err(()) (KeyAlreadyDeleted) failure");
    }

    /// Same bug, repeated many times on a small-fanout tree, then verified
    /// two independent ways: the tracked counts must still match their
    /// pre-batch value, and driving enough real inserts afterwards to force
    /// real splits must both (a) not panic inside smo.rs on a bad fill-ratio
    /// read and (b) leave the exact expected key set behind — not one key
    /// short, and not with a phantom extra key.
    #[test]
    fn repeated_failed_updates_do_not_corrupt_later_state() {
        let tree = TestTree::make_standard(RootIndexType::default());

        assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(1, 100)), CRUDOperationResult::Inserted(_)));

        let before = leaf_counts(&tree, 1);
        for k in 1000..1000 + (FAN as u64) * 4 {
            assert!(matches!(
                tree.dispatch_crud(CRUDOperation::Update(k, 999)),
                CRUDOperationResult::ZeroAffected(_)
            ));
        }
        assert_eq!(leaf_counts(&tree, 1), before, "counts drifted after a batch of failed updates");

        for k in 2..=(FAN as u64) * 3 {
            assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(k, k * 10)), CRUDOperationResult::Inserted(_)));
        }

        // Point queries, not Range: Range has a separate, pre-existing bug
        // with ascending sequential-key splits (some leaves become
        // unreachable from the root's fence intervals) that's unrelated to
        // the counter-drift fix under test here. Also: `current_version()`,
        // not `current_version_for_reader()` — the latter aggregates across
        // a process-global thread registry (see clock.rs), so under `cargo
        // test`'s parallel test threads it can be dragged down by a
        // completely unrelated test's tree/thread.
        let version = tree.current_version();
        for k in 1..=(FAN as u64) * 3 {
            let expected_payload = if k == 1 { 100 } else { k * 10 };
            match tree.dispatch_crud(CRUDOperation::Point(k, version)) {
                CRUDOperationResult::MatchedRecords(records) if records.len() == 1 && records[0].payload == expected_payload => {}
                other => panic!("key {k} missing or wrong after failed updates + real inserts: {other}"),
            }
        }
    }

    /// New invariant from simplifying the WAL to log `CRUDOperation`
    /// directly: since one logged record must equal one minted version,
    /// `Update`'s in-place fast path (which mints none) must never fire
    /// while a WAL is attached — every Update must go through the normal
    /// versioned path and get a fresh version instead.
    #[test]
    fn update_in_place_disabled_while_wal_attached() {
        let path = std::env::temp_dir().join(format!("cmvbt_dispatch_wal_test_{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let tree = TestTree::make_standard(RootIndexType::default());
        tree.enable_gc(true);
        tree.enable_wal(&path, std::time::Duration::from_millis(2)).unwrap();

        let CRUDOperationResult::Inserted(insert_version) = tree.dispatch_crud(CRUDOperation::Insert(1, 100)) else {
            panic!("expected Inserted");
        };

        // No live readers registered, and GC+update-in-place is on: with no
        // WAL this would take the in-place fast path (see
        // wal_disabled_path_unaffected in integration_tests.rs) and reuse
        // the current version. With a WAL attached it must mint a fresh one.
        let CRUDOperationResult::Updated(update_version) = tree.dispatch_crud(CRUDOperation::Update(1, 200)) else {
            panic!("expected Updated");
        };
        assert!(update_version > insert_version, "Update must mint a fresh version while a WAL is attached");

        let _ = std::fs::remove_file(&path);
    }
}
