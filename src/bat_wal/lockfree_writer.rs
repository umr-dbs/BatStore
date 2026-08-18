use std::cmp::Ord;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io;
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::AtomicU64;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use triomphe::Arc;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::{AtomicVersion, Version};
use crate::bat_sync::clock::GlobalClock;
use crate::bat_wal::record::{self, WalEntry, WalRecord};

/// How long the fsync thread waits, between short polls, for every writer
/// that had already reserved a byte range (via `tail.fetch_add`) at the
/// start of the current cycle to finish its `pwrite` before giving up on
/// this cycle. Not a correctness knob — a writer that blows past this just
/// gets its durability credit deferred to the next cycle (see
/// `quiesce_and_publish`'s doc) — only a latency/throughput one.
const QUIESCE_POLL: Duration = Duration::from_micros(20);
const QUIESCE_MAX_WAIT: Duration = Duration::from_millis(5);

/// Alternative to [`WalWriter`](crate::bat_wal::writer::WalWriter) that
/// replaces its channel + single background-thread append path with the
/// pattern its own doc comment used to describe only in the small ("minting
/// a version is a plain atomic fetch_add") sense: appending a record is now
/// *also* just an atomic `fetch_add` (reserving a byte range in `tail`) plus
/// a positioned `pwrite` (`FileExt::write_at`) — every calling thread writes
/// its own bytes straight into the file, with no channel hand-off to a
/// dedicated writer thread and no lock ever taken on the file itself.
///
/// # Why this is safe without a lock
/// POSIX `pwrite` is *positioned* I/O: it never consults or mutates the
/// file's shared byte-offset cursor (unlike plain `write`/an `O_APPEND` fd),
/// so two threads calling it concurrently with disjoint `(offset, len)`
/// ranges — guaranteed here because `tail.fetch_add` hands out disjoint
/// ranges — never race on the same bytes, the same way two threads writing
/// to disjoint slices of an in-memory buffer wouldn't.
///
/// # What this trades away: interior holes on crash
/// The existing `WalWriter` appends strictly in the order its one
/// background thread drains its channel, so *any* incomplete data can only
/// ever be a torn *suffix*. Here, thread A can reserve a low offset and
/// then stall or the whole process can die before its `pwrite` lands,
/// while thread B's later-offset `pwrite` has already completed and even
/// reached disk (the kernel flushes dirty pages in the background on its
/// own schedule, independent of this writer's own fsync loop) — leaving an
/// *interior* hole of unwritten (zero) bytes with valid, durable records
/// both before *and* after it.
///
/// `bat_wal::recovery::replay`/`replay_database`/`replay_two_tables` handle
/// this: they scan via `record::resync_next` rather than a plain
/// `read_frame` loop, which skips forward past a hole (or any other
/// unparseable run of bytes) instead of stopping at the first one — see
/// that function's doc for why that's safe and `tests/wal_integration_tests.rs`'s
/// `replay_recovers_records_after_an_interior_hole` for a file built with a
/// hand-placed hole recovering records on both sides of it. This closes the
/// correctness gap that used to keep this type out of the production
/// WAL-attach path entirely; it still isn't wired into
/// `MVBTSt`/`Database`'s `attach_wal` (that's a separate integration task —
/// swapping a concrete `WalWriter` for a writer that can be either kind
/// throughout `version_handle.rs`/`mvbt.rs`/`database.rs`), and `LocalBatch`
/// still has no timeout-based flush for bursty/sporadic call patterns (see
/// that type's doc) — but the writer's own on-disk format is no longer the
/// blocker.
///
/// # Durability watermark under concurrent completion
/// Because writers can *complete* out of order (thread B's `pwrite` can
/// finish before thread A's, even though A reserved the earlier offset),
/// `hardened_version` can't just be "the last thing this shard's one
/// writer thread flushed", the way `WalWriter`'s is. Instead:
/// - Every writer bumps `completed` (bytes) by its record's length *after*
///   its `pwrite` returns, so `completed == tail` means every reserved byte
///   range has actually been written — the file's `[0, tail)` prefix is
///   provably hole-free at that instant.
/// - The background fsync thread snapshots `tail` at the start of each
///   cycle, waits (bounded by `QUIESCE_MAX_WAIT`) for `completed` to catch
///   up to that snapshot, and only then calls `sync_data` and publishes
///   `hardened` — so `hardened_version()` never claims durability for a
///   range that could still contain a hole.
pub struct LockFreeWalWriter<Key, Payload> {
    file: Arc<File>,
    /// Next byte offset to hand out. Every `enqueue` claims `[old, old+len)`
    /// via `fetch_add` and writes exactly there — disjoint by construction,
    /// so no two calls ever contend for the same bytes.
    tail: Arc<AtomicU64>,
    /// Bytes whose `pwrite` has returned, summed the same way as `tail`.
    /// `completed == tail` is this writer's quiescence condition — see the
    /// struct doc's "Durability watermark" section.
    completed: Arc<AtomicU64>,
    /// Highest *Commit* `ts_start` any completed `pwrite` has applied so
    /// far — an upper bound the fsync thread reads only once it has
    /// confirmed quiescence, so what it reads is guaranteed to correspond
    /// to bytes already inside `[0, tail)` at that point. `0` = no commit
    /// applied yet (matches `WalWriter::hardened`'s "used, nothing
    /// confirmed" sentinel — real `ts_start`s are always `>= 1`).
    applied_commit_max: Arc<AtomicVersion>,
    /// Externally visible durability watermark — same three-state contract
    /// as `WalWriter::hardened` (see that field's doc), but here only ever
    /// touched by the fsync thread, after quiescence.
    hardened: Arc<AtomicVersion>,
    /// Same idea as `applied_commit_max`/`hardened`, but tracking *every*
    /// enqueued record (`Write` or `Commit`) rather than only `Commit`s —
    /// `wait_flushed` needs this, not `hardened`: `WalWriter::wait_flushed`
    /// unblocks once its record's containing batch is fsynced, full stop,
    /// regardless of whether that record was a bare `Write` (most callers —
    /// see this module's own tests, which never log a `Commit` at all).
    /// Gating `wait_flushed` on `hardened` instead would hang forever
    /// waiting for a `Commit` that may never come.
    applied_any_max: Arc<AtomicVersion>,
    flushed_any: Arc<AtomicVersion>,
    /// Closing this (via `Drop`) is what wakes the fsync thread out of its
    /// `recv_timeout` immediately (as `Disconnected`) instead of it having
    /// to wait out a full `flush_interval` on shutdown — the same idiom
    /// `WalWriter` uses for its data channel, just carrying no payload here
    /// since actual records never travel through a channel in this writer.
    _stop: Option<Sender<()>>,
    thread: Option<JoinHandle<()>>,
    /// `fn(Key, Payload)`, not `(Key, Payload)`: this type never actually
    /// stores a `Key`/`Payload` value (every field above is plain
    /// bytes/atomics/handles) — only the `fn`-pointer marker form is
    /// unconditionally `Send + Sync` regardless of `Key`/`Payload`, which
    /// matters because `LockFreeWalBackend`'s sweep thread shares this
    /// type across threads via `Arc`. The tuple form would instead make
    /// `Send`/`Sync` conditional on `Key: Send`/`Payload: Send`, which
    /// would ripple that requirement through every generic caller
    /// (`MVBTSt`, `Database`, `DbTransaction`, ...) for no real reason —
    /// every concrete instantiation (`u64`, `TpccRow`, `YcsbRow`) is
    /// already trivially `Send` anyway.
    _marker: PhantomData<fn(Key, Payload)>,
}

impl<Key: Ord + Copy + Hash + Display, Payload: Clone> LockFreeWalWriter<Key, Payload> {
    /// Opens (creating if needed) the log file at `path` and starts the
    /// background fsync thread. Unlike `WalWriter::open`, the file is *not*
    /// opened with `O_APPEND` — every write is explicitly positioned via
    /// `pwrite`/`write_at`, and mixing `O_APPEND` with positioned writes is
    /// unnecessary at best (the kernel-maintained append cursor this writer
    /// never uses) and platform-dependent at worst.
    pub fn open(path: &Path, flush_interval: Duration) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)?;
        let start_tail = file.metadata()?.len();

        let file = Arc::new(file);
        let tail = Arc::new(AtomicU64::new(start_tail));
        let completed = Arc::new(AtomicU64::new(start_tail));
        let applied_commit_max = Arc::new(AtomicVersion::new(0));
        let hardened = Arc::new(AtomicVersion::new(Version::MAX));
        let applied_any_max = Arc::new(AtomicVersion::new(0));
        let flushed_any = Arc::new(AtomicVersion::new(0));

        let (stop_tx, stop_rx) = bounded::<()>(0);

        let thread = {
            let file = file.clone();
            let tail = tail.clone();
            let completed = completed.clone();
            let applied_commit_max = applied_commit_max.clone();
            let hardened = hardened.clone();
            let applied_any_max = applied_any_max.clone();
            let flushed_any = flushed_any.clone();
            thread::spawn(move || {
                Self::fsync_loop(
                    file, stop_rx, flush_interval, tail, completed,
                    applied_commit_max, hardened, applied_any_max, flushed_any,
                )
            })
        };

        Ok(Self {
            file,
            tail,
            completed,
            applied_commit_max,
            hardened,
            applied_any_max,
            flushed_any,
            _stop: Some(stop_tx),
            thread: Some(thread),
            _marker: PhantomData,
        })
    }

    fn fsync_loop(
        file: Arc<File>,
        stop: Receiver<()>,
        flush_interval: Duration,
        tail: Arc<AtomicU64>,
        completed: Arc<AtomicU64>,
        applied_commit_max: Arc<AtomicVersion>,
        hardened: Arc<AtomicVersion>,
        applied_any_max: Arc<AtomicVersion>,
        flushed_any: Arc<AtomicVersion>,
    ) {
        loop {
            match stop.recv_timeout(flush_interval) {
                Ok(()) => unreachable!("stop channel only ever closes, never sends"),
                Err(RecvTimeoutError::Timeout) => {}
                // Owning writer dropped: one last quiesce-and-publish so
                // nothing enqueued just before shutdown is left unflushed,
                // then exit.
                Err(RecvTimeoutError::Disconnected) => {
                    Self::quiesce_and_publish(&file, &tail, &completed, &applied_commit_max, &hardened, &applied_any_max, &flushed_any);
                    return;
                }
            }

            Self::quiesce_and_publish(&file, &tail, &completed, &applied_commit_max, &hardened, &applied_any_max, &flushed_any);
        }
    }

    /// Waits (bounded) for every byte range reserved as of this call to be
    /// actually written, then `fsync`s and publishes `hardened`/`flushed_any`.
    /// Skips publishing (not the whole cycle's fsync — see below) if
    /// quiescence doesn't happen within `QUIESCE_MAX_WAIT`: a writer that
    /// stalled past its `fetch_add` just defers credit to the next cycle
    /// rather than blocking this one indefinitely or, worse, fsyncing/
    /// publishing past a range that might still contain a hole.
    fn quiesce_and_publish(
        file: &File,
        tail: &AtomicU64,
        completed: &AtomicU64,
        applied_commit_max: &AtomicVersion,
        hardened: &AtomicVersion,
        applied_any_max: &AtomicVersion,
        flushed_any: &AtomicVersion,
    ) {
        let target = tail.load(Acquire);
        if completed.load(Acquire) < target {
            let deadline = Instant::now() + QUIESCE_MAX_WAIT;
            while completed.load(Acquire) < target && Instant::now() < deadline {
                thread::sleep(QUIESCE_POLL);
            }
            if completed.load(Acquire) < target {
                // Still not quiescent - don't fsync/publish past a range
                // that may still contain a hole. Try again next cycle.
                return;
            }
        }

        // Read *after* quiescence is confirmed: every write/commit whose
        // bytes are within `[0, target)` has, by the release-sequence on
        // `completed` (each writer bumps `applied_commit_max`/
        // `applied_any_max` before `completed`, all with `Release`),
        // already published its update by the time this `Acquire` load runs.
        let commit_max = applied_commit_max.load(Acquire);
        let any_max = applied_any_max.load(Acquire);

        // Same "retry indefinitely" policy as `WalWriter::flush_loop` — a
        // transient I/O error gets a chance to clear rather than either
        // silently dropping durability or panicking the process.
        while file.sync_data().is_err() {
            thread::sleep(Duration::from_millis(50));
        }

        if commit_max != 0 {
            hardened.store(commit_max, Relaxed);
        }
        if any_max != 0 {
            flushed_any.store(any_max, Relaxed);
        }
    }

    /// Same contract as `WalWriter::wait_flushed`, with one caveat that
    /// writer doesn't share: `WalWriter::wait_flushed` takes a per-call
    /// ticket, so it unblocks only when *that specific call's* record
    /// flushes. This writer has no per-record ticket (see the struct doc),
    /// so it matches on `ts_start` alone — indistinguishable between a
    /// `Write` and its later `Commit`, which always share one `ts_start`.
    /// Calling this with a stamp that already had *any* record (`Write` or
    /// `Commit`) flushed for it returns immediately, even if the specific
    /// record the caller actually cares about (typically the `Commit`, to
    /// mirror `hardened_version`) hasn't gone through a fsync cycle yet —
    /// poll `hardened_version()` directly instead when that distinction
    /// matters (see `tests/wal_lockfree_writer_tests.rs`'s
    /// `hardened_version_starts_unset_and_only_advances_on_commit`, which
    /// does exactly that). Polls rather than blocking on a wakeup (no
    /// per-record channel ack exists to block on); not on any production
    /// dispatch path, kept for tests that want to block on one specific
    /// record.
    pub fn wait_flushed(&self, ts: Version) {
        while self.flushed_any.load(Relaxed) < ts {
            thread::sleep(QUIESCE_POLL);
        }
    }

    pub fn hardened_version(&self) -> Version {
        self.hardened.load(Relaxed)
    }
}

impl<Key: Ord + Copy + Hash + Display, Payload: Clone + record::WalPayload> LockFreeWalWriter<Key, Payload> {
    /// Lock-free counterpart to `WalWriter::start_commit_logged`.
    pub fn start_commit_logged(
        &self,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> TxStamp {
        let stamp = TxStamp::new(worker_id, clock.next_timestamp());
        self.log_with_stamp(stamp, build);
        stamp
    }

    /// Lock-free counterpart to `WalWriter::log_with_stamp`.
    pub fn log_with_stamp(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        let entry = WalEntry::Write(WalRecord { stamp, op: build(stamp.ts_start()) });

        // Pre-sized via `entry_size_hint` (an exact-or-close estimate of the
        // real encoded size — see that function's doc) rather than a small
        // fixed guess, so this doesn't pay for repeated grow-and-copy
        // reallocations on anything bigger than a `u64` payload (real
        // payloads like `TpccRow`/`YcsbRow` routinely run into the hundreds
        // of bytes).
        let mut framed = Vec::with_capacity(record::entry_size_hint(&entry));
        record::encode_entry_framed(&entry, &mut framed);

        self.enqueue(stamp.ts_start(), false, &framed);
    }

    /// Lock-free counterpart to `WalWriter::log_commit`.
    pub fn log_commit(&self, stamp: TxStamp, ts_commit: Version) {
        let entry = WalEntry::Commit { stamp, ts_commit };
        let mut framed = Vec::with_capacity(record::entry_size_hint::<Key, Payload>(&entry));
        record::encode_entry_framed::<Key, Payload>(&entry, &mut framed);

        self.enqueue(stamp.ts_start(), true, &framed);
    }

    /// Lock-free counterpart to `WalWriter::start_commit_logged_for_table`.
    pub fn start_commit_logged_for_table(
        &self,
        table_id: record::TableId,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> TxStamp {
        let stamp = TxStamp::new(worker_id, clock.next_timestamp());
        self.log_with_stamp_for_table(table_id, stamp, build);
        stamp
    }

    /// Lock-free counterpart to `WalWriter::log_with_stamp_for_table`.
    pub fn log_with_stamp_for_table(
        &self,
        table_id: record::TableId,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        let entry = WalEntry::Write(WalRecord { stamp, op: build(stamp.ts_start()) });
        // +4: the table id this framing adds on top of `encode_entry_framed`'s
        // plain shape — see `record::encode_entry_for_table_framed`'s doc.
        let mut framed = Vec::with_capacity(record::entry_size_hint(&entry) + 4);
        record::encode_entry_for_table_framed(table_id, &entry, &mut framed);

        self.enqueue(stamp.ts_start(), false, &framed);
    }

    /// Lock-free counterpart to `WalWriter::log_commit_for_table`.
    pub fn log_commit_for_table(&self, stamp: TxStamp, ts_commit: Version) {
        let entry = WalEntry::Commit { stamp, ts_commit };
        let mut framed = Vec::with_capacity(record::entry_size_hint::<Key, Payload>(&entry) + 4);
        record::encode_entry_for_table_framed::<Key, Payload>(
            record::TABLE_ID_COMMIT_SENTINEL,
            &entry,
            &mut framed,
        );

        self.enqueue(stamp.ts_start(), true, &framed);
    }

    /// Single-record convenience over `enqueue_bytes` — see that method's
    /// doc for the actual hot path.
    fn enqueue(&self, ts_start: Version, is_commit: bool, framed: &[u8]) {
        let commit_ts = if is_commit { Some(ts_start) } else { None };
        self.enqueue_bytes(ts_start, commit_ts, framed);
    }

    /// Reserves `bytes.len()` at the current `tail` and writes it there via
    /// positioned `pwrite` — the entire lock-free hot path: one
    /// `fetch_add`, one syscall, two more `fetch_add`/`fetch_max` bumps for
    /// bookkeeping. No mutex, no channel send, no background thread on this
    /// call's critical path at all. `bytes` may be a single framed record
    /// (`enqueue`) or several concatenated ones (`LocalBatch::flush_into` —
    /// see that type's doc for why grouping several of *one thread's own*
    /// records into one `pwrite` this way is worth doing) — either way this
    /// method has no notion of "one record", only "some framed bytes ending
    /// at `max_ts_start`, possibly containing a Commit up to
    /// `max_commit_ts_start`".
    fn enqueue_bytes(&self, max_ts_start: Version, max_commit_ts_start: Option<Version>, bytes: &[u8]) {
        let _ = self.hardened.compare_exchange(Version::MAX, 0, Relaxed, Relaxed);

        let len = bytes.len() as u64;
        let offset = self.tail.fetch_add(len, Relaxed);

        let mut written = 0usize;
        while written < bytes.len() {
            match self.file.write_at(&bytes[written..], offset + written as u64) {
                Ok(0) => thread::sleep(Duration::from_millis(50)), // no progress - treat like an error, see below
                Ok(n) => written += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                // Same "retry indefinitely" policy as `WalWriter::flush_loop`
                // — see that method's doc for why neither dropping nor
                // panicking is acceptable here.
                Err(_) => thread::sleep(Duration::from_millis(50)),
            }
        }

        // Must happen *before* the `completed` bump below — see
        // `quiesce_and_publish`'s doc for the release-sequence argument
        // this ordering relies on.
        if let Some(commit_ts) = max_commit_ts_start {
            self.applied_commit_max.fetch_max(commit_ts, Release);
        }
        self.applied_any_max.fetch_max(max_ts_start, Release);
        self.completed.fetch_add(len, Release);
    }

    /// Flushes `batch` (see `LocalBatch`'s doc) in one `enqueue_bytes` call
    /// — one `fetch_add`/`pwrite` for however many records `batch`
    /// accumulated, instead of one each. No-op on an empty batch. Leaves
    /// `batch` empty and ready to accumulate the next group.
    pub fn flush_batch(&self, batch: &mut LocalBatch<Key, Payload>) {
        if batch.bytes.is_empty() {
            return;
        }
        self.enqueue_bytes(batch.max_ts_start, batch.max_commit_ts_start, &batch.bytes);
        batch.bytes.clear();
        batch.records = 0;
        batch.max_ts_start = 0;
        batch.max_commit_ts_start = None;
    }
}

/// A single thread's own uncommitted-to-disk records, accumulated locally
/// before being handed to the writer as one group. This is "group commit"
/// applied per writer thread instead of across all of them via a shared
/// channel + dedicated background thread (that's what `WalWriter` already
/// does): the benchmark in `tests/wal_writer_throughput_bench.rs` found
/// `LockFreeWalWriter`'s one-`pwrite`-per-record hot path loses badly to
/// `WalWriter`'s batched `write_all` past a couple of threads, because it
/// pays a full syscall (and, on a shared growing file, likely some
/// filesystem-level inode-extension serialization — see that benchmark
/// file's write-up) for every single record. Batching a handful of one
/// thread's own records into a single `pwrite` closes most of that gap
/// while keeping the property that made this writer worth trying in the
/// first place: no cross-thread coordination beyond the shared atomic
/// `tail` — `LocalBatch` itself touches nothing shared at all, so N threads
/// batching concurrently never contend with each other except at the
/// `fetch_add` (and, briefly, at `pwrite`) each one does when it flushes.
///
/// Purely a client-side accumulator: owned by whichever thread creates it,
/// never shared. Caller decides the flush policy (`flush_batch` whenever
/// `len()` hits some threshold — see the benchmark for exactly that — or on
/// every call, which degenerates to the same one-`pwrite`-per-record
/// behavior `enqueue` already gives you). A caller that wants a latency
/// bound on records sitting unflushed during a lull (this type has none —
/// unlike `WalWriter`'s `GROUP_COMMIT_LINGER`, nothing here ever flushes a
/// partially-filled batch on a timer) must flush explicitly at whatever
/// point in its own logic marks "this thread has nothing else pending".
pub struct LocalBatch<Key, Payload> {
    bytes: Vec<u8>,
    records: usize,
    max_ts_start: Version,
    max_commit_ts_start: Option<Version>,
    /// `fn(Key, Payload)`, not `(Key, Payload)` — see `LockFreeWalWriter::_marker`'s
    /// doc for why: this type is shared across threads (`LockFreeWalBackend`'s
    /// per-worker `Vec<Mutex<LocalBatch<..>>>`) and never actually owns a
    /// `Key`/`Payload` value.
    _marker: PhantomData<fn(Key, Payload)>,
}

impl<Key, Payload> Default for LocalBatch<Key, Payload> {
    fn default() -> Self {
        Self { bytes: Vec::new(), records: 0, max_ts_start: 0, max_commit_ts_start: None, _marker: PhantomData }
    }
}

impl<Key: Ord + Copy + Hash + Display, Payload: Clone + record::WalPayload> LocalBatch<Key, Payload> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of records accumulated since the last flush — what a caller
    /// compares against its own batch-size threshold to decide when to call
    /// `LockFreeWalWriter::flush_batch`.
    pub fn len(&self) -> usize {
        self.records
    }

    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// Appends one `Write` entry to this batch — same encoding `enqueue`
    /// gives a lone record, just accumulated instead of immediately
    /// written. Does not itself touch the file; call `flush_batch` (on
    /// whatever cadence the caller chooses) to actually write it out.
    pub fn push_write(&mut self, stamp: TxStamp, build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>) {
        let op = build(stamp.ts_start());
        record::encode_entry_framed(&WalEntry::Write(WalRecord { stamp, op }), &mut self.bytes);
        self.max_ts_start = self.max_ts_start.max(stamp.ts_start());
        self.records += 1;
    }

    /// Appends one `Commit` marker to this batch — see `push_write`'s doc.
    pub fn push_commit(&mut self, stamp: TxStamp, ts_commit: Version) {
        record::encode_entry_framed::<Key, Payload>(&WalEntry::Commit { stamp, ts_commit }, &mut self.bytes);
        self.max_ts_start = self.max_ts_start.max(stamp.ts_start());
        self.max_commit_ts_start = Some(self.max_commit_ts_start.map_or(stamp.ts_start(), |m| m.max(stamp.ts_start())));
        self.records += 1;
    }

    /// Table-tagged counterpart to `push_write` — see `record::encode_entry_for_table_framed`.
    pub fn push_write_for_table(
        &mut self,
        table_id: record::TableId,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        let op = build(stamp.ts_start());
        record::encode_entry_for_table_framed(table_id, &WalEntry::Write(WalRecord { stamp, op }), &mut self.bytes);
        self.max_ts_start = self.max_ts_start.max(stamp.ts_start());
        self.records += 1;
    }

    /// Table-tagged counterpart to `push_commit` — always tagged with
    /// `record::TABLE_ID_COMMIT_SENTINEL`, same as `WalWriter::log_commit_for_table`.
    pub fn push_commit_for_table(&mut self, stamp: TxStamp, ts_commit: Version) {
        record::encode_entry_for_table_framed::<Key, Payload>(
            record::TABLE_ID_COMMIT_SENTINEL,
            &WalEntry::Commit { stamp, ts_commit },
            &mut self.bytes,
        );
        self.max_ts_start = self.max_ts_start.max(stamp.ts_start());
        self.max_commit_ts_start = Some(self.max_commit_ts_start.map_or(stamp.ts_start(), |m| m.max(stamp.ts_start())));
        self.records += 1;
    }
}

impl<Key, Payload> Drop for LockFreeWalWriter<Key, Payload> {
    fn drop(&mut self) {
        self._stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
