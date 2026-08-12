use crate::mv_crud_model::crud_api::AtomicTxDispatcher;
use crate::mv_crud_model::crud_operation::{CRUDOperation, TxAtomicOperation};
use crate::mv_crud_model::crud_operation_result::CRUDOperationInnerReason::{
    KeyAlreadyDeleted, KeyAlreadyExists, KeyDoesNotExist,
};
use crate::mv_crud_model::crud_operation_result::{AtomicTxResult, CRUDOperationResult};
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_query::rand_query::RAND_ATTEMPTS_MAX;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::tx_stamp::TxStamp;
use crate::mv_record_model::version_info::VersionInfo;
use crate::mv_sync::smart_cell::sched_yield;
use crate::mv_test::VERBOSE;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_wal::backend::WalBackend;
use itertools::Itertools;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;

pub const RANGE_DISPATCH_LAZY: bool = true;

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
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
    pub(crate) fn decide_update_in_place_record(&self, version: &VersionInfo) -> bool {
        if !self.has_update_in_place() {
            return false;
        }

        match self.ctx.newest_live_si() {
            Some(newest_si) => {
                version.insertion_stamp().ts_start() > newest_si
                    && !version.insertion_stamp().is_invalid()
            }
            None => !version.insertion_stamp().is_invalid(),
        }
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Atomically derives a replacement payload from the latest live value
    /// while holding the leaf write latch. Used by YCSB's single-field
    /// update so it cannot lose another updater's intervening field change.
    pub(crate) fn update_with(
        &self,
        key: Key,
        make_payload: impl FnOnce(&Payload) -> Payload,
    ) -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let leaf_guard = self.traversal_write_olc(key);
        let leaf_page = leaf_guard.deref_mut().as_leaf_page();
        let Some(position) = leaf_page.latest_position(key, false) else {
            return CRUDOperationResult::ZeroAffected(KeyDoesNotExist);
        };
        if !leaf_page.version_at(position).is_live() {
            return CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted);
        }

        let payload = make_payload(leaf_page.payload_at(position));
        if matches!(self.cold.wal.as_ref(), WalBackend::Off)
            && self.decide_update_in_place_record(leaf_page.version_at(position))
        {
            leaf_page.set_payload_at(position, payload);
            return CRUDOperationResult::Updated(self.current_version());
        }

        let stamp = self.wal_start_commit(|_| CRUDOperation::Update(key, payload.clone()));
        if !leaf_page.version_mut_at(position).delete(stamp) {
            return CRUDOperationResult::Error;
        }
        let current_len = leaf_page.len();
        leaf_page.push_uncommitted(
            RecordPoint::new(key, VersionInfo::new(stamp), payload),
            current_len,
        );
        leaf_page.commit_delta(0, 1);
        let ts_commit = self.commit_tx(stamp.worker_id());
        drop(leaf_guard);
        self.wal_log_commit(stamp, ts_commit);
        CRUDOperationResult::Updated(stamp.ts_start())
    }
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload,
> AtomicTxDispatcher<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    for MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn dispatch_crud(
        &'a self,
        crud: CRUDOperation<Key, Payload>,
    ) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        match crud {
            CRUDOperation::Insert(key, payload) => {
                let leaf_guard = self.traversal_write_olc(key);

                let leaf_deref_mut = leaf_guard.deref_mut();

                let leaf_page = leaf_deref_mut.as_leaf_page();

                if let Some(position) = leaf_page.latest_position(key, true) {
                    let version = leaf_page.version_at(position);
                    if version.is_live() {
                        let reader_worker = self.worker_id();
                        let now = self.current_version();
                        return if self.is_visible_stamp(
                            reader_worker,
                            now,
                            version.insertion_stamp(),
                        ) {
                            CRUDOperationResult::ZeroAffected(KeyAlreadyExists)
                        } else {
                            CRUDOperationResult::Conflict
                        };
                    }
                }

                let current_len = leaf_page.len();

                let stamp = self.wal_start_commit(|_| CRUDOperation::Insert(key, payload.clone()));

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len,
                );

                leaf_page.commit_delta(1, 0);

                if crate::mv_tree::smo::TRACE_KEY_DEBUG {
                    crate::mv_tree::smo::push_trace(format!(
                        "TRACE insert thread={:#x} key={key} into leaf={:p}",
                        {
                            use std::hash::{Hash, Hasher};
                            let mut h = std::collections::hash_map::DefaultHasher::new();
                            std::thread::current().id().hash(&mut h);
                            h.finish()
                        },
                        leaf_deref_mut as *const _
                    ));
                }

                // Commit while the leaf is still write-locked. OLC readers
                // cannot validate the new prefix until the guard is
                // released, so the version is already in OSIC's commit log
                // at the first instant it can be observed successfully.
                // record (if any) is flushed asynchronously in a batch by
                // the writer's background thread, not waited on here. See
                // `MVBTSt::wal_hardened_version`'s doc for how to check/wait
                // for durability explicitly instead.
                let ts_commit = self.commit_tx(stamp.worker_id());
                drop(leaf_guard);
                self.wal_log_commit(stamp, ts_commit);

                CRUDOperationResult::Inserted(stamp.ts_start())
            }
            CRUDOperation::Update(key, payload) => {
                let leaf_guard = self.traversal_write_olc(key);

                let leaf_deref_mut = leaf_guard.deref_mut();

                let leaf_page = leaf_deref_mut.as_leaf_page();

                let current_len = leaf_page.len();

                // The in-place fast path never mints a version, which can't be
                // represented in the WAL (one CRUDOperation = one version = one log
                // record), so it's skipped entirely whenever a WAL is attached —
                // every logged Update always takes the normal versioned path below.
                let latest_position = leaf_page.latest_position(key, false);
                if let Some(position) = latest_position {
                    if matches!(self.cold.wal.as_ref(), WalBackend::Off)
                        && self.decide_update_in_place_record(leaf_page.version_at(position))
                    {
                        leaf_page.set_payload_at(position, payload);
                        if leaf_page.version_at(position).is_deleted() {
                            leaf_page.version_mut_at(position).undelete();

                            leaf_page.commit_delta(1, -1);
                        }

                        return CRUDOperationResult::Updated(self.current_version());
                    }
                }

                match latest_position {
                    Some(position) if leaf_page.version_at(position).is_live() => {
                        let reader_worker = self.worker_id();
                        let now = self.current_version();
                        if !self.is_visible_stamp(
                            reader_worker,
                            now,
                            leaf_page.version_at(position).insertion_stamp(),
                        ) {
                            return CRUDOperationResult::Conflict;
                        }
                        let stamp =
                            self.wal_start_commit(|_| CRUDOperation::Update(key, payload.clone()));

                        if !leaf_page.version_mut_at(position).delete(stamp) {
                            return CRUDOperationResult::Error;
                        }

                        leaf_page.push_uncommitted(
                            RecordPoint::new(key, VersionInfo::new(stamp), payload),
                            current_len,
                        );

                        leaf_page.commit_delta(0, 1);
                        let ts_commit = self.commit_tx(stamp.worker_id());
                        drop(leaf_guard);
                        self.wal_log_commit(stamp, ts_commit);

                        CRUDOperationResult::Updated(stamp.ts_start())
                    }
                    Some(_) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted),
                    None => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                }
            }
            CRUDOperation::Delete(key) => {
                if VERBOSE {
                    println!("dispatch delete key={key}");
                }

                let leaf_guard = self.traversal_write_olc(key);

                if VERBOSE {
                    println!("traverse olc end");
                    println!(
                        "[key={key}] - Leaf: ({:?}) records",
                        leaf_guard.active_dead_count()
                    );
                }
                let leaf_deref_mut = leaf_guard.deref_mut();

                let leaf_page = leaf_deref_mut.as_leaf_page();

                if VERBOSE {
                    println!("[key={key}] - Begin_commit()");
                }
                if VERBOSE {
                    println!("[key={key}] - Loop start");
                }

                let position = leaf_page.latest_position(key, true);
                if let Some(position) = position {
                    let reader_worker = self.worker_id();
                    let now = self.current_version();
                    if !self.is_visible_stamp(
                        reader_worker,
                        now,
                        leaf_page.version_at(position).insertion_stamp(),
                    ) {
                        return CRUDOperationResult::Conflict;
                    }
                }

                let stamp = self.wal_start_commit(|_| CRUDOperation::Delete(key));

                match leaf_page.delete(key, stamp) {
                    Ok(Some(..)) => {
                        leaf_page.commit_delta(-1, 1);
                        if VERBOSE {
                            println!(
                                "After delete Leaf-records:\n{}",
                                leaf_page.as_records().iter().join("\n")
                            );
                        }

                        let ts_commit = self.commit_tx(stamp.worker_id());
                        drop(leaf_guard);
                        self.wal_log_commit(stamp, ts_commit);
                        CRUDOperationResult::Deleted(stamp.ts_start())
                    }
                    Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                    Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted),
                }
            }
            CRUDOperation::Range(range, version) if RANGE_DISPATCH_LAZY => {
                match self.dispatch_crud(CRUDOperation::RangeIter(range, version)) {
                    CRUDOperationResult::MatchedRecordIter(iter) => {
                        CRUDOperationResult::MatchedRecords(iter.collect())
                    }
                    other => other,
                }
            }
            CRUDOperation::Range(range, version) => {
                let reader_worker = self.worker_id();
                self.on_acquire_reader_snapshot(version);
                let res = self.key_range_read_from_root(
                    self.retrieve_root_for(version),
                    range,
                    reader_worker,
                    version,
                );
                self.on_release_reader_snapshot(version);
                res
            }
            CRUDOperation::Point(key, version) => {
                let reader_worker = self.worker_id();
                self.on_acquire_reader_snapshot(version);
                let res = self.key_point_read_from_root(
                    self.retrieve_root_for(version),
                    key,
                    reader_worker,
                    version,
                );
                self.on_release_reader_snapshot(version);
                res
            }
            CRUDOperation::RangeIter(key, version) => CRUDOperationResult::MatchedRecordIter(
                RangeQueryIter::new(self, version, key, true, self.worker_id()),
            ),
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
                    version,
                );
                self.end_snapshot(version);
                res
            }
            CRUDOperation::RangeSi(range) => {
                match self.dispatch_crud(CRUDOperation::RangeIterSi(range)) {
                    CRUDOperationResult::MatchedRecordIter(iter) => {
                        CRUDOperationResult::MatchedRecords(iter.collect())
                    }
                    other => other,
                }
            }
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
            CRUDOperation::RangeIterSi(key) => {
                CRUDOperationResult::MatchedRecordIter(self.draw_snapshot_version_with(|version| {
                    RangeQueryIter::new(self, version, key, true, self.worker_id())
                }))
            }
            // `*Rand` operations are used purely for benchmark/data-generation
            // workloads (see mv_test.rs) — irrelevant to the actual running
            // system, so they're never routed through the WAL at all.
            CRUDOperation::UpdateRand => {
                let (_fence, leaf_guard) = self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard.deref_mut();

                let leaf_page = leaf_deref_mut.as_leaf_page();

                let current_len = leaf_page.len();

                let (live_n, _dead_n) = leaf_page.active_dead_count();

                let mut find_i = rand::random_range(0..live_n as usize);

                let mut key = Key::default();
                let payload = Payload::default();

                for r in leaf_page.as_records() {
                    if r.version().is_live() {
                        if find_i == 0 {
                            key = r.key;
                            break;
                        }
                        find_i -= 1;
                    }
                }

                if let Some(position) = leaf_page.latest_position(key, false) {
                    if self.decide_update_in_place_record(leaf_page.version_at(position)) {
                        leaf_page.version_mut_at(position).undelete();
                        leaf_page.set_payload_at(position, payload);
                        leaf_page.commit_delta(1, -1);

                        return CRUDOperationResult::UpdatedRand(key, self.current_version());
                    }
                }

                let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len,
                );

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
                let (_fence, leaf_guard) = self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard.deref_mut();

                let leaf_page = leaf_deref_mut.as_leaf_page();

                let (live_n, _dead_n) = leaf_page.active_dead_count();

                let mut find_i = rand::random_range(0..live_n as usize);

                let mut key = Key::default();

                for r in leaf_page.as_records() {
                    if r.version().is_live() {
                        if find_i == 0 {
                            key = r.key;
                            break;
                        }
                        find_i -= 1;
                    }
                }

                let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());

                if VERBOSE {
                    println!(
                        "[key={key}] - Commit succeeded: {}, Attempts: 0",
                        stamp.ts_start()
                    );
                }
                match leaf_page.delete(key, stamp) {
                    Ok(Some(..)) => {
                        leaf_page.commit_delta(-1, 1);
                        if VERBOSE {
                            println!(
                                "After delete Leaf-records:\n{}",
                                leaf_page.as_records().iter().join("\n")
                            );
                        }

                        self.commit_tx(stamp.worker_id());
                        CRUDOperationResult::DeletedRand(key, stamp.ts_start())
                    }
                    Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                    Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted),
                }
            }
            CRUDOperation::InsertRand => {
                let (fence, leaf_guard) = self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard.deref_mut();

                let leaf_page = leaf_deref_mut.as_leaf_page();

                if size_of::<Key>() != mem::size_of::<u64>() {
                    // Not supported
                    println!(">>>> CRUDOperation::InsertRand only supported on *u64* !");
                    return CRUDOperationResult::Error;
                }

                let min = unsafe { *((&fence.lower) as *const _ as *const u64) };
                let max = unsafe { *((&fence.upper) as *const _ as *const u64) };

                let mut rand_attempts = 0;
                let key = loop {
                    let generated = rand::random_range(min..=max);
                    let gen_key = unsafe { *((&generated) as *const _ as *const Key) };

                    match leaf_page.as_records().iter().rfind(|r| r.key == gen_key) {
                        None => break Some(gen_key),
                        Some(record) if !record.version().is_live() => break Some(gen_key),
                        _ if rand_attempts >= RAND_ATTEMPTS_MAX => break None,
                        _ => {
                            rand_attempts += 1;
                            sched_yield(rand_attempts);
                        }
                    }
                };

                if key.is_none() {
                    println!(
                        ">> RandKey Generation Failed!\
                    >> RAND_ATTEMPTS_MAX = {RAND_ATTEMPTS_MAX}\
                    >> Fence = {fence}"
                    );

                    return self.dispatch_crud(CRUDOperation::InsertRand);
                }

                let key = key.unwrap();
                debug_assert!(key <= fence.upper && key >= fence.lower);
                if VERBOSE {
                    println!("[RandInsert] - Key: {key}, Fence= min: {min}, max: {max}");
                }
                let payload = Payload::default();

                let current_len = leaf_page.len();

                let stamp = TxStamp::new(self.worker_id(), self.start_tx_commit());

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(stamp), payload),
                    current_len,
                );

                leaf_page.commit_delta(1, 0);
                self.commit_tx(stamp.worker_id());

                CRUDOperationResult::InsertedRand(key, stamp.ts_start())
            }
            _ => CRUDOperationResult::Error,
        }
    }
}

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn dispatch_atomic_transaction(
        &self,
        atomic_tx: TxAtomicOperation<Key, Payload>,
    ) -> AtomicTxResult<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.dispatch_crud(atomic_tx)
    }
}
