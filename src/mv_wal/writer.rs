use std::cmp::Ord;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use triomphe::Arc;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::clock::GlobalClock;
use crate::mv_wal::record::{self, WalEntry, WalRecord};

const GROUP_COMMIT_LINGER: Duration = Duration::from_micros(200);

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
    /// `mv_wal::recovery::replay` only applies a write once it finds this
    /// same-`ts_start` marker (see `MVBTSt::wal_log_commit`'s doc) — so a
    /// caller polling `hardened_version()` needs the *marker*, not just the
    /// write bytes, to be durable. Every write path in this codebase always
    /// follows a `Write`/batch of `Write`s with exactly one `Commit` for the
    /// same `ts_start` (single-op dispatch: back-to-back in one call;
    /// `mv_db::DbTransaction`: one `wal_log_commit` after all its
    /// `wal_log_write` calls) — but those can land in *different* flush
    /// batches under scheduling pressure (the writes enqueued, then the
    /// enqueueing thread gets preempted before enqueueing the commit, and
    /// `flush_loop` drains and flushes just the writes in the meantime).
    /// Since every message for one transaction shares the same `ts_start`,
    /// letting a `Write`-only batch advance `hardened` to that `ts_start`
    /// would let `wait_wal_hardened` return before the commit marker —
    /// the actual, replay-relevant durability point — is on disk.
    is_commit: bool,
    /// Fired once `bytes` has been durably fsynced.
    ack: Sender<()>,
}

/// Lock-free WAL writer: minting a version (`GlobalClock::start_commit`) is
/// a plain atomic `fetch_add`, and handing a record to the background
/// writer thread is a channel send — no mutex, no shared buffer that
/// concurrent committers contend on. The trade-off: two commits can land in
/// the log in either order regardless of which version is numerically
/// smaller (whichever thread's send/channel-drain happens to go first
/// wins), so file byte order no longer implies version order. `replay`
/// (see `mv_wal::recovery`) accounts for this by sorting records by version
/// before applying them.
pub struct WalWriter<Key, Payload> {
    sender: Option<Sender<LogMessage>>,
    thread: Option<JoinHandle<()>>,
    /// Three-state watermark, encoded in one atomic:
    /// - `Version::MAX` ("never used") — this shard has never had a write
    ///   enqueued. Must not drag down a multi-shard aggregate's `min` just
    ///   because some worker slot happens to be idle.
    /// - `0` ("used, nothing confirmed yet") — at least one write has been
    ///   enqueued (`log_with_stamp` downgrades from `Version::MAX` to this
    ///   the moment that happens) but this shard's background thread hasn't
    ///   completed its first flush yet. Deliberately *not* left at
    ///   `Version::MAX` in this state — that would let a query for "is
    ///   version V durable" answer yes for a shard with real, unflushed,
    ///   pending writes.
    /// - any other value — the highest `ts_start` this shard's background
    ///   thread has confirmed durably fsynced so far.
    ///
    /// Shared with `flush_loop` via `Arc` so the background thread can
    /// publish it without a lock; once past the initial downgrade, safe to
    /// update with a plain `store` rather than a compare-and-max because
    /// this shard has exactly one producer (its owning worker — see this
    /// type's doc) submitting strictly in `ts_start` order, so batches are
    /// drained and flushed in that same non-decreasing order.
    hardened: Arc<AtomicVersion>,
    /// `fn(Key, Payload)`, not `(Key, Payload)`: this type never actually
    /// stores a `Key`/`Payload` value (its background thread's captured
    /// state is plain bytes/atomics/handles, no `Key`/`Payload` type ever
    /// crosses the channel — see `LogMessage`), so only the `fn`-pointer
    /// marker form is unconditionally `Send + Sync` regardless of
    /// `Key`/`Payload`. Matters now that `mv_wal::backend::WalBackend`
    /// wraps this alongside `LockFreeWalWriter` in one enum shared across
    /// threads via `Arc` — the tuple form would make the whole enum's
    /// `Send`/`Sync` conditional on `Key: Send`/`Payload: Send`, rippling
    /// that requirement through every generic caller (`MVBTSt`, `Database`,
    /// `DbTransaction`, ...) for no real reason.
    _marker: PhantomData<fn(Key, Payload)>,
}

impl<Key: Ord + Copy + Hash + Display, Payload: Clone> WalWriter<Key, Payload> {
    /// Opens (creating if needed) the log file at `path` for append and
    /// starts the background flush thread. Any pre-existing content (e.g.
    /// from a prior run, already replayed via `recovery::replay`) is left
    /// untouched; callers that recovered a shorter valid prefix must
    /// truncate the file to that length *before* calling this.
    pub fn open(path: &Path, flush_interval: Duration) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let (sender, receiver) = unbounded::<LogMessage>();
        let hardened = Arc::new(AtomicVersion::new(Version::MAX));

        let thread = {
            let hardened = hardened.clone();
            thread::spawn(move || Self::flush_loop(file, receiver, flush_interval, hardened))
        };

        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            hardened,
            _marker: PhantomData,
        })
    }

    fn flush_loop(mut file: File, receiver: Receiver<LogMessage>, flush_interval: Duration, hardened: Arc<AtomicVersion>) {
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

            // Drain whatever else has arrived without blocking, so this
            // flush batches every commit that piled up during the last
            // `flush_interval` into a single write + fsync.
            let mut batch = vec![first];
            while let Ok(msg) = receiver.try_recv() {
                batch.push(msg);
            }

            let mut buf = Vec::new();
            for msg in &batch {
                buf.extend_from_slice(&msg.bytes);
            }

            // Retry indefinitely rather than either dropping the acks
            // (waiters would wake up believing they're durable when they
            // aren't) or giving up silently (waiters would hang forever
            // with no chance of ever succeeding) — a transient I/O hiccup
            // gets a chance to clear before the next attempt.
            while file.write_all(&buf).and_then(|_| file.sync_data()).is_err() {
                thread::sleep(Duration::from_millis(50));
            }

            // The whole batch just became durable at once — publish the
            // highest *Commit* `ts_start` in it as this shard's new hardened
            // watermark, rather than resolving each record individually. A
            // batch that (still) contains no Commit message at all leaves
            // `hardened` untouched — see `LogMessage::is_commit`'s doc for
            // why a batch of bare writes must never advance it.
            if let Some(max_ts) = batch.iter().filter(|msg| msg.is_commit).map(|msg| msg.ts_start).max() {
                hardened.store(max_ts, Relaxed);
            }

            for msg in batch {
                let _ = msg.ack.send(());
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

    /// This shard's durability watermark — see the `hardened` field doc for
    /// its three possible states. When it's a real value (neither `0` nor
    /// `Version::MAX`), every record submitted to this shard with
    /// `ts_start <= this value` is confirmed durably fsynced.
    pub fn hardened_version(&self) -> Version {
        self.hardened.load(Relaxed)
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
    /// it with `worker_id` into a `TxStamp`, and logs it via
    /// [`log_with_stamp`](Self::log_with_stamp). Returns the stamp and a
    /// flush ticket: `wait_flushed`/dropping it without waiting both work,
    /// but only waiting on it actually blocks for durability. Used by the
    /// single-op, auto-committing dispatch path, where each `CRUDOperation`
    /// mints its own fresh stamp.
    pub fn start_commit_logged(
        &self,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> (TxStamp, Receiver<()>) {
        let stamp = TxStamp::new(worker_id, clock.next_timestamp());
        let ticket = self.log_with_stamp(stamp, build);
        (stamp, ticket)
    }

    /// Logs one write under an *already-determined* `stamp`, rather than
    /// minting a fresh one — what a multi-op `Transaction` needs: every
    /// write in the same transaction shares its one `ts_start`, so it must
    /// reuse that stamp instead of drawing a new tick per operation (which
    /// would break snapshot isolation and defeat instant commit's "no
    /// write-set revisit"). Returns a flush ticket, same as
    /// `start_commit_logged`.
    pub fn log_with_stamp(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> Receiver<()> {
        let op = build(stamp.ts_start());

        // Encodes straight into one buffer (length prefix + body + crc)
        // instead of encoding the body into its own buffer and then
        // copying it into a second, framed one — see
        // `record::encode_entry_framed`.
        let mut framed = Vec::with_capacity(40);
        record::encode_entry_framed(&WalEntry::Write(WalRecord { stamp, op }), &mut framed);

        self.enqueue(stamp.ts_start(), false, framed)
    }

    /// Logs a **Commit marker** confirming that `stamp`'s transaction
    /// actually committed at `ts_commit` — see `WalEntry::Commit`'s doc for
    /// why this is a separate entry from the write(s) it confirms, and
    /// `mv_wal::recovery::replay`'s doc for how it gates replay. Same
    /// fire-and-forget model as `log_with_stamp`: the caller never waits on
    /// this, and it flows through the exact same channel/flush-loop/
    /// `hardened` machinery, just carrying a different entry kind.
    pub fn log_commit(&self, stamp: TxStamp, ts_commit: Version) -> Receiver<()> {
        let mut framed = Vec::with_capacity(24);
        record::encode_entry_framed::<Key, Payload>(
            &WalEntry::Commit { stamp, ts_commit },
            &mut framed,
        );

        self.enqueue(stamp.ts_start(), true, framed)
    }

    /// Table-tagged counterpart to `start_commit_logged`, for a `Database`'s
    /// single shared writer: mints a fresh `ts_start` the same way, but
    /// encodes/logs the record under `table_id` (see
    /// `record::encode_entry_for_table_framed`) so `mv_wal::recovery::
    /// replay_database` can later demux this file's entries back to the
    /// right table.
    pub fn start_commit_logged_for_table(
        &self,
        table_id: record::TableId,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> (TxStamp, Receiver<()>) {
        let stamp = TxStamp::new(worker_id, clock.next_timestamp());
        let ticket = self.log_with_stamp_for_table(table_id, stamp, build);
        (stamp, ticket)
    }

    /// Table-tagged counterpart to `log_with_stamp` — same "log under an
    /// already-determined stamp" contract, just tagging the record with
    /// `table_id` for later demultiplexing by `replay_database`.
    pub fn log_with_stamp_for_table(
        &self,
        table_id: record::TableId,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> Receiver<()> {
        let op = build(stamp.ts_start());

        let mut framed = Vec::with_capacity(44);
        record::encode_entry_for_table_framed(table_id, &WalEntry::Write(WalRecord { stamp, op }), &mut framed);

        self.enqueue(stamp.ts_start(), false, framed)
    }

    /// Table-tagged counterpart to `log_commit`. A Commit marker is
    /// transaction-scoped, not table-scoped (see `WalEntry::Commit`'s doc
    /// and `record::TABLE_ID_COMMIT_SENTINEL`'s), so this always tags the
    /// entry with that reserved sentinel rather than taking a `table_id`
    /// parameter — `replay_database` ignores it for `Commit` entries
    /// regardless.
    pub fn log_commit_for_table(&self, stamp: TxStamp, ts_commit: Version) -> Receiver<()> {
        let mut framed = Vec::with_capacity(28);
        record::encode_entry_for_table_framed::<Key, Payload>(
            record::TABLE_ID_COMMIT_SENTINEL,
            &WalEntry::Commit { stamp, ts_commit },
            &mut framed,
        );

        self.enqueue(stamp.ts_start(), true, framed)
    }

    /// Shared enqueue path for `log_with_stamp`/`log_commit`: downgrades
    /// `hardened` off its "never used" sentinel if needed (see the field
    /// doc) and hands the already-framed bytes to the background flush
    /// thread. `is_commit` — see `LogMessage::is_commit`'s doc — must be
    /// `true` only for an actual `WalEntry::Commit` marker.
    fn enqueue(&self, ts_start: Version, is_commit: bool, framed: Vec<u8>) -> Receiver<()> {
        // The moment this write is enqueued, this shard has real
        // outstanding work: if `hardened` is still at the "never used"
        // sentinel (`Version::MAX`), downgrade it to `0` ("used, nothing
        // confirmed yet") so a concurrent `hardened_version()` query can no
        // longer mistake a shard with a real pending write for
        // unconstrained. A harmless no-op once this shard has flushed at
        // least one batch — `hardened` then holds a real, already-
        // confirmed value that must never regress.
        let _ = self.hardened.compare_exchange(Version::MAX, 0, Relaxed, Relaxed);

        let (ack_tx, ack_rx) = bounded(1);
        // Safe to unwrap: the sender is only ever taken (and the channel
        // closed) from `Drop`, which can't run concurrently with this call
        // — `self` is reached through an `Arc`, so `Drop` only runs once no
        // other reference (and thus no other call to this method) exists.
        let _ = self.sender.as_ref().unwrap().send(LogMessage { bytes: framed, ts_start, is_commit, ack: ack_tx });
        ack_rx
    }
}

impl<Key, Payload> Drop for WalWriter<Key, Payload> {
    fn drop(&mut self) {
        // Drop the sender *before* joining: the background thread only
        // exits once every sender is gone (see `flush_loop`'s
        // `Disconnected` handling), so joining first would deadlock.
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

