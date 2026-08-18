use std::cmp::Ord;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use triomphe::Arc;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::{AtomicVersion, Version};
use crate::bat_sync::clock::GlobalClock;
use crate::bat_wal::record::{self, WalEntry, WalRecord};

const GROUP_COMMIT_LINGER: Duration = Duration::from_micros(200);

/// How many independent (channel, background flush thread) pairs a
/// `WalWriter` splits committers across, keyed by `worker_id % NUM_SHARDS` —
/// see `WalWriter`'s own doc for why one shared channel isn't enough.
/// Deliberately small: each shard is a whole OS thread plus its own
/// group-commit batching/fsync cadence, and the fix this targets is
/// *enqueue* contention (many producers hitting one MPSC channel), not I/O
/// throughput — a handful of shards already cuts that contention by the
/// same factor without spawning one flush thread per worker.
const NUM_SHARDS: usize = 4;

struct LogMessage {
    bytes: Vec<u8>,
    /// This record's `ts_start` — folded into `WalWriter::hardened` once the
    /// batch it's part of has actually fsynced (see `flush_loop`), so the
    /// hardened watermark advances per *batch*, not per operation. Only
    /// consulted when `is_commit` is also true — see that field's doc.
    ts_start: Version,
    /// Whether this message is a `WalEntry::Commit` marker rather than a
    /// plain `Write`. `hardened` must only ever advance based on `Commit`
    /// messages: a `Write` alone is never independently meaningful —
    /// `bat_wal::recovery::replay` only applies a write once it finds this
    /// same-`ts_start` marker (see `MVBTSt::wal_log_commit`'s doc) — so a
    /// caller polling `hardened_version()` needs the *marker*, not just the
    /// write bytes, to be durable. Every write path in this codebase always
    /// follows a `Write`/batch of `Write`s with exactly one `Commit` for the
    /// same `ts_start` (single-op dispatch: back-to-back in one call;
    /// `bat_db::DbTransaction`: one `wal_log_commit` after all its
    /// `wal_log_write` calls) — but those can land in *different* flush
    /// batches under scheduling pressure (the writes enqueued, then the
    /// enqueueing thread gets preempted before enqueueing the commit, and
    /// `flush_loop` drains and flushes just the writes in the meantime).
    /// Since every message for one transaction shares the same `ts_start`,
    /// letting a `Write`-only batch advance `hardened` to that `ts_start`
    /// would let `wait_wal_hardened` return before the commit marker —
    /// the actual, replay-relevant durability point — is on disk.
    is_commit: bool,
    /// Fired once `bytes` has been durably fsynced. `None` for the (checked:
    /// every production call site) common case where the caller never asked
    /// for a ticket in the first place — see `WalWriter::enqueue`'s
    /// `want_ticket` doc for why that case skips constructing this channel
    /// at all, rather than building one and just never sending on it.
    ack: Option<Sender<()>>,
}

/// One (channel, background flush thread) pair — see `NUM_SHARDS`. All
/// shards belonging to the same `WalWriter` write through the same
/// `Arc<Mutex<File>>`, so the file's byte layout and this crate's WAL format
/// are completely unaffected by sharding; only the *enqueue* side (the
/// contended part — see `WalWriter`'s doc) is split up.
struct WalShard {
    sender: Option<Sender<LogMessage>>,
    thread: Option<JoinHandle<()>>,
    /// This shard's own three-state watermark (see `WalWriter::hardened_version`'s
    /// doc for the three states and why per-shard state alone isn't enough
    /// to answer a cross-shard query — `submitted` below is also needed).
    /// Written by this shard's own flush thread, plus the `Version::MAX` ->
    /// `0` downgrade `enqueue` does on this specific shard the moment it
    /// gets its first message.
    confirmed: Arc<AtomicVersion>,
    /// Highest *Commit* `ts_start` ever handed to *this* shard's channel —
    /// updated by `enqueue`, synchronously and strictly before the
    /// corresponding message is actually sent, so it always already
    /// reflects everything that could possibly still be in flight for this
    /// shard by the time any concurrent reader observes it. Never touched
    /// by the flush thread; not shared across threads, so no `Arc` needed.
    /// See `WalWriter::hardened_version`'s doc for why this is paired with
    /// `confirmed` rather than comparing `confirmed` alone across shards.
    submitted: AtomicVersion,
}

/// Lock-free-on-the-hot-path WAL writer: minting a version
/// (`GlobalClock::start_commit`) is a plain atomic `fetch_add`, and handing a
/// record to a background writer thread is a channel send — no mutex on the
/// enqueue side. Internally split into `NUM_SHARDS` independent channels
/// (`worker_id % NUM_SHARDS` picks one — see `enqueue`), each drained by its
/// own background thread: profiling found a single shared unbounded
/// `crossbeam-channel` becoming a genuine contention point under many
/// concurrent committers (its internal segment-allocation spin-wait showing
/// up as real wall-clock cost), so one channel per worker thread would
/// defeat the point — sharding spreads that contention across `NUM_SHARDS`
/// independent queues instead. All shards still share one physical file
/// (via `Arc<Mutex<File>>`), so the WAL's on-disk format and recovery are
/// completely unaffected by sharding; `hardened_version` aggregates every
/// shard's own watermark (see that method's doc for why the aggregate is a
/// minimum, not a value every shard publishes into together). The
/// trade-off: two commits can land in the
/// log in either order regardless of which version is numerically smaller
/// (whichever thread's send/channel-drain happens to go first wins), so file
/// byte order no longer implies version order. `replay` (see
/// `bat_wal::recovery`) accounts for this by sorting records by version
/// before applying them.
pub struct WalWriter<Key, Payload> {
    shards: Vec<WalShard>,
    /// `fn(Key, Payload)`, not `(Key, Payload)`: this type never actually
    /// stores a `Key`/`Payload` value (its background thread's captured
    /// state is plain bytes/atomics/handles, no `Key`/`Payload` type ever
    /// crosses the channel — see `LogMessage`), so only the `fn`-pointer
    /// marker form is unconditionally `Send + Sync` regardless of
    /// `Key`/`Payload`. Matters now that `bat_wal::backend::WalBackend`
    /// wraps this alongside `LockFreeWalWriter` in one enum shared across
    /// threads via `Arc` — the tuple form would make the whole enum's
    /// `Send`/`Sync` conditional on `Key: Send`/`Payload: Send`, rippling
    /// that requirement through every generic caller (`MVBTSt`, `Database`,
    /// `DbTransaction`, ...) for no real reason.
    _marker: PhantomData<fn(Key, Payload)>,
}

impl<Key: Ord + Copy + Hash + Display, Payload: Clone> WalWriter<Key, Payload> {
    /// Opens (creating if needed) the log file at `path` for append and
    /// starts `NUM_SHARDS` background flush threads sharing it. Any
    /// pre-existing content (e.g. from a prior run, already replayed via
    /// `recovery::replay`) is left untouched; callers that recovered a
    /// shorter valid prefix must truncate the file to that length *before*
    /// calling this.
    pub fn open(path: &Path, flush_interval: Duration) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let file = Arc::new(Mutex::new(file));

        let shards = (0..NUM_SHARDS)
            .map(|_| {
                let (sender, receiver) = unbounded::<LogMessage>();
                let file = file.clone();
                let confirmed = Arc::new(AtomicVersion::new(Version::MAX));
                let thread = {
                    let confirmed = confirmed.clone();
                    thread::spawn(move || Self::flush_loop(file, receiver, flush_interval, confirmed))
                };
                WalShard { sender: Some(sender), thread: Some(thread), confirmed, submitted: AtomicVersion::new(0) }
            })
            .collect();

        Ok(Self { shards, _marker: PhantomData })
    }

    fn flush_loop(file: Arc<Mutex<File>>, receiver: Receiver<LogMessage>, flush_interval: Duration, confirmed: Arc<AtomicVersion>) {
        loop {
            let first = match receiver.recv_timeout(flush_interval) {
                Ok(msg) => msg,
                Err(RecvTimeoutError::Timeout) => continue,
                // All senders (i.e. the owning WalWriter) dropped: drain is
                // implicit above (recv_timeout still yields any messages
                // still buffered before finally erroring), so once we get
                // here there's nothing left to flush.
                Err(RecvTimeoutError::Disconnected) => return,
            };

            thread::sleep(GROUP_COMMIT_LINGER);

            // Drain whatever else has arrived on *this shard's* channel
            // without blocking, so this flush batches every commit that
            // piled up on this shard during the last `flush_interval` into a
            // single write + fsync. Other shards batch independently, on
            // their own threads.
            let mut batch = vec![first];
            while let Ok(msg) = receiver.try_recv() {
                batch.push(msg);
            }

            let mut buf = Vec::new();
            for msg in &batch {
                buf.extend_from_slice(&msg.bytes);
            }

            // Every shard's flush thread shares this one file: the lock is
            // only held across one batch's write + fsync (already-batched,
            // so contention here is far rarer than the per-enqueue
            // contention sharding the channels avoids), and a shared file
            // means any one shard's `sync_data` durably persists every other
            // shard's bytes written before it too.
            let mut file = file.lock().expect("wal shard file mutex poisoned");
            // Retry indefinitely rather than either dropping the acks
            // (waiters would wake up believing they're durable when they
            // aren't) or giving up silently (waiters would hang forever
            // with no chance of ever succeeding) — a transient I/O hiccup
            // gets a chance to clear before the next attempt.
            while file.write_all(&buf).and_then(|_| file.sync_data()).is_err() {
                thread::sleep(Duration::from_millis(50));
            }
            drop(file);

            // The whole batch just became durable at once — publish the
            // highest *Commit* `ts_start` in it as *this shard's* new
            // confirmed watermark, rather than resolving each record
            // individually. A batch that (still) contains no Commit message
            // at all leaves `confirmed` untouched — see `LogMessage::is_commit`'s
            // doc for why a batch of bare writes must never advance it.
            // `fetch_max` (not `store`): this shard has exactly one
            // background thread ever writing to its own `confirmed`, so
            // batches are already applied in non-decreasing order in
            // practice, but `fetch_max` costs nothing extra and removes any
            // doubt.
            if let Some(max_ts) = batch.iter().filter(|msg| msg.is_commit).map(|msg| msg.ts_start).max() {
                confirmed.fetch_max(max_ts, Relaxed);
            }

            for msg in batch {
                if let Some(ack) = &msg.ack {
                    let _ = ack.send(());
                }
            }
        }
    }

    /// Blocks until the record this `ticket` (from `start_commit_logged`)
    /// belongs to has been durably fsynced. No production dispatch path
    /// calls this anymore (writes are fire-and-forget — see
    /// `MVBTSt::wal_hardened_version`'s doc for why); kept for callers that
    /// genuinely want to wait on one specific record, and exercised by this
    /// module's own test below.
    pub fn wait_flushed(&self, ticket: Receiver<()>) {
        let _ = ticket.recv();
    }

    /// This writer's durability watermark, aggregated across every shard.
    ///
    /// A first attempt at sharding this (publishing every shard's own
    /// `fetch_max` into one watermark shared by all of them) let this
    /// method claim durability for a `ts_start` that a *different* shard
    /// hadn't actually written yet — exactly the failure
    /// `tests/tree_wal_consistency_tests.rs`'s
    /// `concurrent_db_transactions_across_tables_match_shared_wal_exactly`
    /// caught. Taking the **minimum** of each shard's own `confirmed`
    /// instead isn't right either: `ts_start` is one global sequence
    /// shared by every worker regardless of which shard its own
    /// `worker_id % NUM_SHARDS` happens to land on, so a shard simply never
    /// receiving any more work has no reason its own `confirmed` should
    /// ever catch up to some *other* shard's higher `ts_start` — comparing
    /// raw `confirmed` values across shards this way would make this method
    /// (and thus `wait_wal_hardened`) block forever waiting for a value
    /// that specific shard will never see.
    ///
    /// The actual per-shard question isn't "how high is your `confirmed`"
    /// but "are you caught up" — either `confirmed` has already reached the
    /// target being asked about, *or* this shard has flushed everything
    /// it's ever been handed (`confirmed >= submitted`), in which case it
    /// has nothing left that could possibly be undurable, regardless of the
    /// numeric gap to some other shard's higher watermark. A shard that's
    /// caught up this way contributes no constraint to the aggregate (same
    /// as a shard still at `Version::MAX`, never used) rather than a
    /// specific numeric floor — computed here as `Version::MAX` so it
    /// doesn't pull the cross-shard minimum down. `submitted` is safe to
    /// read for this without synchronizing against `confirmed`: `enqueue`
    /// updates it strictly before the corresponding message is sent, so it
    /// can only ever be a stale *overestimate* of what's truly in flight,
    /// never an underestimate — which only makes this method's answer more
    /// conservative, never wrong.
    pub fn hardened_version(&self) -> Version {
        let mut min = Version::MAX;
        for shard in &self.shards {
            let confirmed = shard.confirmed.load(Relaxed);
            if confirmed == Version::MAX {
                continue; // never used - no constraint
            }
            let submitted = shard.submitted.load(Relaxed);
            if confirmed < submitted {
                min = min.min(confirmed);
            }
            // else: this shard is fully drained - no constraint either.
        }
        min
    }
}

/// Split into its own impl block (rather than folded into the one above):
/// only these two methods actually encode a record, so only they need
/// `Payload: WalPayload` — `open`/`flush_loop`/`wait_flushed` are pure
/// byte/file plumbing and stay usable for any `Payload` regardless of
/// whether it has a `WalPayload` impl.
impl<Key: Ord + Copy + Hash + Display, Payload: Clone + record::WalPayload> WalWriter<Key, Payload> {
    /// Mints the next `ts_start` from `clock` — a plain lock-free
    /// `fetch_add`, uncoordinated with any other WAL-logging thread — pairs
    /// it with `worker_id` into a `TxStamp`, and logs it fire-and-forget via
    /// [`log_with_stamp`](Self::log_with_stamp). Used by the single-op,
    /// auto-committing dispatch path — the actual production hot path,
    /// which (checked: every call site in `bat_sync::version_handle`)
    /// never waits on this specific record's own flush. See
    /// [`start_commit_logged_with_ticket`](Self::start_commit_logged_with_ticket)
    /// for the variant that returns a flush ticket, at the cost of a real
    /// per-call channel allocation.
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

    /// Ticketed counterpart to [`start_commit_logged`](Self::start_commit_logged)
    /// — for callers that genuinely need to `wait_flushed` on this one
    /// record (this module's own tests; no production call site uses this).
    pub fn start_commit_logged_with_ticket(
        &self,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> (TxStamp, Receiver<()>) {
        let stamp = TxStamp::new(worker_id, clock.next_timestamp());
        let ticket = self.log_with_stamp_impl(stamp, build, true).expect("ticket requested");
        (stamp, ticket)
    }

    /// Logs one write under an *already-determined* `stamp`, rather than
    /// minting a fresh one — what a multi-op `Transaction` needs: every
    /// write in the same transaction shares its one `ts_start`, so it must
    /// reuse that stamp instead of drawing a new tick per operation (which
    /// would break snapshot isolation and defeat instant commit's "no
    /// write-set revisit"). Fire-and-forget — see `start_commit_logged`'s
    /// doc; [`log_with_stamp_with_ticket`](Self::log_with_stamp_with_ticket)
    /// is the ticketed counterpart, if some future test needs one.
    pub fn log_with_stamp(&self, stamp: TxStamp, build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>) {
        self.log_with_stamp_impl(stamp, build, false);
    }

    /// Ticketed counterpart to [`log_with_stamp`](Self::log_with_stamp).
    pub fn log_with_stamp_with_ticket(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> Receiver<()> {
        self.log_with_stamp_impl(stamp, build, true).expect("ticket requested")
    }

    fn log_with_stamp_impl(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
        want_ticket: bool,
    ) -> Option<Receiver<()>> {
        let entry = WalEntry::Write(WalRecord { stamp, op: build(stamp.ts_start()) });

        // Encodes straight into one buffer (length prefix + body + crc)
        // instead of encoding the body into its own buffer and then
        // copying it into a second, framed one — see
        // `record::encode_entry_framed`. Pre-sized via `entry_size_hint`
        // (an exact-or-close estimate of the real encoded size — see that
        // function's doc) rather than a small fixed guess, so this doesn't
        // pay for repeated grow-and-copy reallocations on anything bigger
        // than a `u64` payload (real payloads like `TpccRow`/`YcsbRow`
        // routinely run into the hundreds of bytes).
        let mut framed = Vec::with_capacity(record::entry_size_hint(&entry));
        record::encode_entry_framed(&entry, &mut framed);

        self.enqueue(stamp.worker_id(), stamp.ts_start(), false, framed, want_ticket)
    }

    /// Logs a **Commit marker** confirming that `stamp`'s transaction
    /// actually committed at `ts_commit` — see `WalEntry::Commit`'s doc for
    /// why this is a separate entry from the write(s) it confirms, and
    /// `bat_wal::recovery::replay`'s doc for how it gates replay.
    /// Fire-and-forget — see `start_commit_logged`'s doc.
    pub fn log_commit(&self, stamp: TxStamp, ts_commit: Version) {
        self.log_commit_impl(stamp, ts_commit, false);
    }

    fn log_commit_impl(&self, stamp: TxStamp, ts_commit: Version, want_ticket: bool) -> Option<Receiver<()>> {
        let entry = WalEntry::Commit { stamp, ts_commit };
        let mut framed = Vec::with_capacity(record::entry_size_hint::<Key, Payload>(&entry));
        record::encode_entry_framed::<Key, Payload>(&entry, &mut framed);

        self.enqueue(stamp.worker_id(), stamp.ts_start(), true, framed, want_ticket)
    }

    /// Table-tagged counterpart to `start_commit_logged`, for a `Database`'s
    /// single shared writer: mints a fresh `ts_start` the same way, but
    /// encodes/logs the record under `table_id` (see
    /// `record::encode_entry_for_table_framed`) so `bat_wal::recovery::
    /// replay_database` can later demux this file's entries back to the
    /// right table. Fire-and-forget — see `start_commit_logged`'s doc.
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

    /// Ticketed counterpart to
    /// [`start_commit_logged_for_table`](Self::start_commit_logged_for_table)
    /// — see `start_commit_logged_with_ticket`'s doc.
    pub fn start_commit_logged_for_table_with_ticket(
        &self,
        table_id: record::TableId,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> (TxStamp, Receiver<()>) {
        let stamp = TxStamp::new(worker_id, clock.next_timestamp());
        let ticket = self.log_with_stamp_for_table_impl(table_id, stamp, build, true).expect("ticket requested");
        (stamp, ticket)
    }

    /// Table-tagged counterpart to `log_with_stamp` — same "log under an
    /// already-determined stamp" contract, just tagging the record with
    /// `table_id` for later demultiplexing by `replay_database`.
    /// Fire-and-forget — see `start_commit_logged`'s doc.
    pub fn log_with_stamp_for_table(
        &self,
        table_id: record::TableId,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        self.log_with_stamp_for_table_impl(table_id, stamp, build, false);
    }

    fn log_with_stamp_for_table_impl(
        &self,
        table_id: record::TableId,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
        want_ticket: bool,
    ) -> Option<Receiver<()>> {
        let entry = WalEntry::Write(WalRecord { stamp, op: build(stamp.ts_start()) });
        // +4: the table id this framing adds on top of `encode_entry_framed`'s
        // plain shape — see `record::encode_entry_for_table_framed`'s doc.
        let mut framed = Vec::with_capacity(record::entry_size_hint(&entry) + 4);
        record::encode_entry_for_table_framed(table_id, &entry, &mut framed);

        self.enqueue(stamp.worker_id(), stamp.ts_start(), false, framed, want_ticket)
    }

    /// Table-tagged counterpart to `log_commit`. A Commit marker is
    /// transaction-scoped, not table-scoped (see `WalEntry::Commit`'s doc
    /// and `record::TABLE_ID_COMMIT_SENTINEL`'s), so this always tags the
    /// entry with that reserved sentinel rather than taking a `table_id`
    /// parameter — `replay_database` ignores it for `Commit` entries
    /// regardless. Fire-and-forget — see `start_commit_logged`'s doc.
    pub fn log_commit_for_table(&self, stamp: TxStamp, ts_commit: Version) {
        self.log_commit_for_table_impl(stamp, ts_commit, false);
    }

    /// Ticketed counterpart to
    /// [`log_commit_for_table`](Self::log_commit_for_table) — see
    /// `start_commit_logged_with_ticket`'s doc.
    pub fn log_commit_for_table_with_ticket(&self, stamp: TxStamp, ts_commit: Version) -> Receiver<()> {
        self.log_commit_for_table_impl(stamp, ts_commit, true).expect("ticket requested")
    }

    fn log_commit_for_table_impl(&self, stamp: TxStamp, ts_commit: Version, want_ticket: bool) -> Option<Receiver<()>> {
        let entry = WalEntry::Commit { stamp, ts_commit };
        let mut framed = Vec::with_capacity(record::entry_size_hint::<Key, Payload>(&entry) + 4);
        record::encode_entry_for_table_framed::<Key, Payload>(record::TABLE_ID_COMMIT_SENTINEL, &entry, &mut framed);

        self.enqueue(stamp.worker_id(), stamp.ts_start(), true, framed, want_ticket)
    }

    /// Shared enqueue path for `log_with_stamp`/`log_commit`/etc: downgrades
    /// `worker_id`'s shard's own `confirmed` off its "never used" sentinel
    /// if needed, bumps that shard's `submitted` if this is a Commit (see
    /// `WalWriter::hardened_version`'s doc for why only Commits count
    /// there, matching `confirmed`), and hands the already-framed bytes to
    /// that shard's background flush thread (`worker_id % NUM_SHARDS` —
    /// see `WalWriter`'s doc). `is_commit` — see `LogMessage::is_commit`'s
    /// doc — must be `true` only for an actual `WalEntry::Commit` marker.
    ///
    /// `want_ticket`: `false` (every production call site, via the
    /// ticket-less methods above) skips constructing the per-call
    /// `bounded(1)` ack channel entirely, rather than building one and
    /// simply discarding it — checked every dispatch call site
    /// (`bat_sync::version_handle`) already discarded the ticket
    /// `log_with_stamp`/`log_commit`/etc used to unconditionally return, so
    /// building that channel was pure waste on the hot path (visible in
    /// profiling as `crossbeam_channel::channel::bounded` plus its own
    /// allocator churn — see `docs/oltp_wal_optimization.md`'s follow-up
    /// section). `true` (the `_with_ticket` methods, used only by this
    /// module's own tests) builds it, same as this type always used to.
    fn enqueue(
        &self,
        worker_id: WorkerId,
        ts_start: Version,
        is_commit: bool,
        framed: Vec<u8>,
        want_ticket: bool,
    ) -> Option<Receiver<()>> {
        let shard = &self.shards[worker_id as usize % self.shards.len()];

        // The moment this write is enqueued, this shard has real
        // outstanding work: if its own watermark is still at the "never
        // used" sentinel (`Version::MAX`), downgrade it to `0` ("used,
        // nothing confirmed yet") so a concurrent `hardened_version()`
        // query can no longer mistake this shard's real pending write for
        // an idle shard it should skip over. A harmless no-op once this
        // shard has flushed at least one batch — its watermark then holds a
        // real, already-confirmed value that must never regress.
        let _ = shard.confirmed.compare_exchange(Version::MAX, 0, Relaxed, Relaxed);

        // Must happen *before* this message is sent below, so any
        // concurrent `hardened_version()` call can only ever see a
        // `submitted` that already accounts for this message, never one
        // that's stale by missing it — see that method's doc for why this
        // ordering is what makes reading `submitted` there safe without
        // synchronizing against `confirmed`.
        if is_commit {
            shard.submitted.fetch_max(ts_start, Relaxed);
        }

        let (ack, ack_rx) = if want_ticket {
            let (tx, rx) = bounded(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Safe to unwrap: a shard's sender is only ever taken (and its
        // channel closed) from `Drop`, which can't run concurrently with
        // this call — `self` is reached through an `Arc`, so `Drop` only
        // runs once no other reference (and thus no other call to this
        // method) exists.
        let _ = shard.sender.as_ref().unwrap().send(LogMessage { bytes: framed, ts_start, is_commit, ack });
        ack_rx
    }
}

impl<Key, Payload> Drop for WalWriter<Key, Payload> {
    fn drop(&mut self) {
        // Drop every shard's sender *before* joining any of their threads:
        // each background thread only exits once its own sender is gone
        // (see `flush_loop`'s `Disconnected` handling), so joining one
        // shard while another's sender is still alive risks joining threads
        // out of an order that could deadlock if they ever shared state
        // beyond the (already-`Arc`'d) file/watermark.
        for shard in &mut self.shards {
            shard.sender.take();
        }
        for shard in &mut self.shards {
            if let Some(thread) = shard.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

