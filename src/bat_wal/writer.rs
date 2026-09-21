use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::{AtomicVersion, Version};
use crate::bat_sync::clock::GlobalClock;
use crate::bat_wal::record::{self, WalEntry, WalRecord};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded, unbounded};
use std::cmp::Ord;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use triomphe::Arc;

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
    _marker: PhantomData<fn(Key, Payload)>,
}

impl<Key: Ord + Copy + Hash + Display, Payload: Clone> WalWriter<Key, Payload> {
    pub fn open(path: &Path, flush_interval: Duration) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let file = Arc::new(Mutex::new(file));

        let shards = (0..NUM_SHARDS)
            .map(|_| {
                let (sender, receiver) = unbounded::<LogMessage>();
                let file = file.clone();
                let confirmed = Arc::new(AtomicVersion::new(Version::MAX));
                let thread = {
                    let confirmed = confirmed.clone();
                    thread::spawn(move || {
                        Self::flush_loop(file, receiver, flush_interval, confirmed)
                    })
                };
                WalShard {
                    sender: Some(sender),
                    thread: Some(thread),
                    confirmed,
                    submitted: AtomicVersion::new(0),
                }
            })
            .collect();

        Ok(Self {
            shards,
            _marker: PhantomData,
        })
    }

    fn flush_loop(
        file: Arc<Mutex<File>>,
        receiver: Receiver<LogMessage>,
        flush_interval: Duration,
        confirmed: Arc<AtomicVersion>,
    ) {
        loop {
            let first = match receiver.recv_timeout(flush_interval) {
                Ok(msg) => msg,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            };

            thread::sleep(GROUP_COMMIT_LINGER);

            let mut batch = vec![first];
            while let Ok(msg) = receiver.try_recv() {
                batch.push(msg);
            }

            let mut buf = Vec::new();
            for msg in &batch {
                buf.extend_from_slice(&msg.bytes);
            }

            let mut file = file.lock().expect("wal shard file mutex poisoned");
            while file.write_all(&buf).and_then(|_| file.sync_data()).is_err() {
                thread::sleep(Duration::from_millis(50));
            }
            drop(file);

            if let Some(max_ts) = batch
                .iter()
                .filter(|msg| msg.is_commit)
                .map(|msg| msg.ts_start)
                .max()
            {
                confirmed.fetch_max(max_ts, Relaxed);
            }

            for msg in batch {
                if let Some(ack) = &msg.ack {
                    let _ = ack.send(());
                }
            }
        }
    }

    pub fn wait_flushed(&self, ticket: Receiver<()>) {
        let _ = ticket.recv();
    }

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
impl<Key: Ord + Copy + Hash + Display, Payload: Clone + record::WalPayload>
    WalWriter<Key, Payload>
{
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
        let ticket = self
            .log_with_stamp_impl(stamp, build, true)
            .expect("ticket requested");
        (stamp, ticket)
    }

    pub fn log_with_stamp(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        self.log_with_stamp_impl(stamp, build, false);
    }

    /// Ticketed counterpart to [`log_with_stamp`](Self::log_with_stamp).
    pub fn log_with_stamp_with_ticket(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> Receiver<()> {
        self.log_with_stamp_impl(stamp, build, true)
            .expect("ticket requested")
    }

    fn log_with_stamp_impl(
        &self,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
        want_ticket: bool,
    ) -> Option<Receiver<()>> {
        let entry = WalEntry::Write(WalRecord {
            stamp,
            op: build(stamp.ts_start()),
        });

        let mut framed = Vec::with_capacity(record::entry_size_hint(&entry));
        record::encode_entry_framed(&entry, &mut framed);

        self.enqueue(
            stamp.worker_id(),
            stamp.ts_start(),
            false,
            framed,
            want_ticket,
        )
    }

    pub fn log_commit(&self, stamp: TxStamp, ts_commit: Version) {
        self.log_commit_impl(stamp, ts_commit, false);
    }

    fn log_commit_impl(
        &self,
        stamp: TxStamp,
        ts_commit: Version,
        want_ticket: bool,
    ) -> Option<Receiver<()>> {
        let entry = WalEntry::Commit { stamp, ts_commit };
        let mut framed = Vec::with_capacity(record::entry_size_hint::<Key, Payload>(&entry));
        record::encode_entry_framed::<Key, Payload>(&entry, &mut framed);

        self.enqueue(
            stamp.worker_id(),
            stamp.ts_start(),
            true,
            framed,
            want_ticket,
        )
    }

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
        let ticket = self
            .log_with_stamp_for_table_impl(table_id, stamp, build, true)
            .expect("ticket requested");
        (stamp, ticket)
    }

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
        let entry = WalEntry::Write(WalRecord {
            stamp,
            op: build(stamp.ts_start()),
        });
        // +4: the table id this framing adds on top of `encode_entry_framed`'s
        // plain shape — see `record::encode_entry_for_table_framed`'s doc.
        let mut framed = Vec::with_capacity(record::entry_size_hint(&entry) + 4);
        record::encode_entry_for_table_framed(table_id, &entry, &mut framed);

        self.enqueue(
            stamp.worker_id(),
            stamp.ts_start(),
            false,
            framed,
            want_ticket,
        )
    }

    pub fn log_commit_for_table(&self, stamp: TxStamp, ts_commit: Version) {
        self.log_commit_for_table_impl(stamp, ts_commit, false);
    }

    /// Ticketed counterpart to
    /// [`log_commit_for_table`](Self::log_commit_for_table) — see
    /// `start_commit_logged_with_ticket`'s doc.
    pub fn log_commit_for_table_with_ticket(
        &self,
        stamp: TxStamp,
        ts_commit: Version,
    ) -> Receiver<()> {
        self.log_commit_for_table_impl(stamp, ts_commit, true)
            .expect("ticket requested")
    }

    fn log_commit_for_table_impl(
        &self,
        stamp: TxStamp,
        ts_commit: Version,
        want_ticket: bool,
    ) -> Option<Receiver<()>> {
        let entry = WalEntry::Commit { stamp, ts_commit };
        let mut framed = Vec::with_capacity(record::entry_size_hint::<Key, Payload>(&entry) + 4);
        record::encode_entry_for_table_framed::<Key, Payload>(
            record::TABLE_ID_COMMIT_SENTINEL,
            &entry,
            &mut framed,
        );

        self.enqueue(
            stamp.worker_id(),
            stamp.ts_start(),
            true,
            framed,
            want_ticket,
        )
    }

    fn enqueue(
        &self,
        worker_id: WorkerId,
        ts_start: Version,
        is_commit: bool,
        framed: Vec<u8>,
        want_ticket: bool,
    ) -> Option<Receiver<()>> {
        let shard = &self.shards[worker_id as usize % self.shards.len()];

        let _ = shard
            .confirmed
            .compare_exchange(Version::MAX, 0, Relaxed, Relaxed);

        if is_commit {
            shard.submitted.fetch_max(ts_start, Relaxed);
        }

        let (ack, ack_rx) = if want_ticket {
            let (tx, rx) = bounded(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let _ = shard.sender.as_ref().unwrap().send(LogMessage {
            bytes: framed,
            ts_start,
            is_commit,
            ack,
        });
        ack_rx
    }
}

impl<Key, Payload> Drop for WalWriter<Key, Payload> {
    fn drop(&mut self) {
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
