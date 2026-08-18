use std::cmp::Ord;
use std::fmt::Display;
use std::hash::Hash;
use std::io;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use parking_lot::Mutex;
use triomphe::Arc;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::clock::GlobalClock;
use crate::bat_wal::lockfree_writer::{LocalBatch, LockFreeWalWriter};
use crate::bat_wal::record::{self, WalPayload};
use crate::bat_wal::writer::WalWriter;

/// `LockFreeWalWriter` plus per-worker local batching (see `LocalBatch`'s
/// doc for why grouping a thread's own records into one `pwrite` matters)
/// and a background sweep that bounds how long a partial batch can sit
/// unflushed — `LocalBatch` on its own has no timeout, only a size
/// threshold, so a worker that logs a handful of writes and then goes idle
/// (rather than immediately filling its batch) would otherwise leave them
/// unflushed indefinitely.
///
/// Batches are indexed directly by `WorkerId`, mirroring
/// `bat_gc::block_tracer::BlockTracer`'s "one shard per worker" layout — one
/// `Mutex<LocalBatch>` per worker slot, so concurrent workers never contend
/// on each other's batch (only the rare interleaving with this struct's own
/// sweep thread briefly touching the same slot). Locked with a real mutex
/// rather than `try_lock`-and-skip: hold times are tiny (a memcpy push, or
/// a `flush_batch` call), so a worker occasionally blocking behind the
/// sweep thread for that long is cheaper than the sweep thread silently
/// skipping a slot it couldn't lock and leaving that worker's batch
/// unflushed for another whole `flush_interval`.
pub struct LockFreeWalBackend<Key, Payload> {
    writer: Arc<LockFreeWalWriter<Key, Payload>>,
    batches: Arc<Vec<Mutex<LocalBatch<Key, Payload>>>>,
    batch_size: usize,
    _stop: Option<Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

/// Split out, like `LockFreeWalWriter`'s own two impl blocks: `hardened_version`
/// needs no `Payload: WalPayload` bound.
impl<Key: Ord + Copy + Hash + Display + 'static, Payload: Clone + 'static> LockFreeWalBackend<Key, Payload> {
    pub fn hardened_version(&self) -> Version {
        self.writer.hardened_version()
    }
}

impl<Key: Ord + Copy + Hash + Display + 'static, Payload: Clone + WalPayload + 'static>
    LockFreeWalBackend<Key, Payload>
{
    /// `batch_size`: a worker's own batch flushes as soon as it reaches this
    /// many records (bounds how large one flush's `pwrite` gets and how much
    /// memory an unusually bursty worker can pile up between sweeps).
    /// `max_workers`: sizes the per-worker slot array — callers pass
    /// `bat_tree::mvbt::default_max_workers()`, the same runtime worker count every other
    /// worker-indexed structure in this codebase (`SnapshotCache`,
    /// `BlockTracer`) uses.
    pub fn open(path: &Path, flush_interval: Duration, batch_size: usize, max_workers: usize) -> io::Result<Self> {
        let writer = Arc::new(LockFreeWalWriter::open(path, flush_interval)?);
        let batches: Arc<Vec<Mutex<LocalBatch<Key, Payload>>>> =
            Arc::new((0..max_workers).map(|_| Mutex::new(LocalBatch::new())).collect());

        let (stop_tx, stop_rx) = bounded::<()>(0);

        let thread = {
            let writer = writer.clone();
            let batches = batches.clone();
            thread::spawn(move || Self::sweep_loop(writer, batches, flush_interval, stop_rx))
        };

        Ok(Self { writer, batches, batch_size, _stop: Some(stop_tx), thread: Some(thread) })
    }

    fn sweep_loop(
        writer: Arc<LockFreeWalWriter<Key, Payload>>,
        batches: Arc<Vec<Mutex<LocalBatch<Key, Payload>>>>,
        flush_interval: Duration,
        stop: Receiver<()>,
    ) {
        loop {
            match stop.recv_timeout(flush_interval) {
                Ok(()) => unreachable!("stop channel only ever closes, never sends"),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    Self::sweep_once(&writer, &batches);
                    return;
                }
            }
            Self::sweep_once(&writer, &batches);
        }
    }

    fn sweep_once(writer: &LockFreeWalWriter<Key, Payload>, batches: &[Mutex<LocalBatch<Key, Payload>>]) {
        for slot in batches {
            let mut batch = slot.lock();
            if !batch.is_empty() {
                writer.flush_batch(&mut batch);
            }
        }
    }

    fn batch_slot(&self, worker_id: WorkerId) -> &Mutex<LocalBatch<Key, Payload>> {
        &self.batches[worker_id as usize]
    }

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

    pub fn log_with_stamp(&self, stamp: TxStamp, build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>) {
        let mut batch = self.batch_slot(stamp.worker_id()).lock();
        batch.push_write(stamp, build);
        if batch.len() >= self.batch_size {
            self.writer.flush_batch(&mut batch);
        }
    }

    pub fn log_commit(&self, stamp: TxStamp, ts_commit: Version) {
        let mut batch = self.batch_slot(stamp.worker_id()).lock();
        batch.push_commit(stamp, ts_commit);
        if batch.len() >= self.batch_size {
            self.writer.flush_batch(&mut batch);
        }
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

    pub fn log_with_stamp_for_table(
        &self,
        table_id: record::TableId,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        let mut batch = self.batch_slot(stamp.worker_id()).lock();
        batch.push_write_for_table(table_id, stamp, build);
        if batch.len() >= self.batch_size {
            self.writer.flush_batch(&mut batch);
        }
    }

    pub fn log_commit_for_table(&self, stamp: TxStamp, ts_commit: Version) {
        let mut batch = self.batch_slot(stamp.worker_id()).lock();
        batch.push_commit_for_table(stamp, ts_commit);
        if batch.len() >= self.batch_size {
            self.writer.flush_batch(&mut batch);
        }
    }
}

impl<Key, Payload> Drop for LockFreeWalBackend<Key, Payload> {
    fn drop(&mut self) {
        // Stop (and join) the sweep thread *before* `writer` drops: its last
        // sweep flushes every worker's remaining batch into `writer`, which
        // must happen before `writer`'s own `Drop` does its final
        // quiesce-and-fsync, or those bytes would sit in a `LocalBatch` that
        // never reaches the file at all.
        self._stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Which append/durability strategy a tree's WAL uses — see
/// `bat_wal::writer`/`bat_wal::lockfree_writer`'s module docs for the two
/// designs and `tests/wal_writer_throughput_bench.rs` for measurements of
/// the difference. Both produce the identical on-disk wire format
/// (`record::encode_entry_framed`/`encode_entry_for_table_framed`), so
/// `bat_wal::recovery::replay*` doesn't need to know or care which one wrote
/// a given file, and a database can even be recovered by one kind and then
/// reattached with the other.
pub enum WalBackend<Key, Payload> {
    Off,
    Batched(WalWriter<Key, Payload>),
    LockFree(LockFreeWalBackend<Key, Payload>),
}

/// Split out, like `WalWriter`'s own two impl blocks: `hardened_version`
/// needs no `Payload: WalPayload` bound, so it stays callable even where
/// that bound isn't otherwise in scope (see `MVBTSt::wal_hardened_version`).
impl<Key: Ord + Copy + Hash + Display + 'static, Payload: Clone + 'static> WalBackend<Key, Payload> {
    pub fn hardened_version(&self) -> Version {
        match self {
            Self::Off => 0,
            Self::Batched(w) => w.hardened_version(),
            Self::LockFree(w) => w.hardened_version(),
        }
    }
}

impl<
    Key: Ord + Copy + Hash + Display + 'static,
    Payload: Clone + WalPayload + 'static,
> WalBackend<Key, Payload> {
    pub fn open_batched(path: &Path, flush_interval: Duration) -> io::Result<Self> {
        Ok(Self::Batched(WalWriter::open(path, flush_interval)?))
    }

    pub fn open_lockfree(path: &Path, flush_interval: Duration, batch_size: usize, max_workers: usize) -> io::Result<Self> {
        Ok(Self::LockFree(LockFreeWalBackend::open(path, flush_interval, batch_size, max_workers)?))
    }

    pub fn start_commit_logged(
        &self,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> TxStamp {
        match self {
            Self::Off => TxStamp::new(worker_id, clock.next_timestamp()),
            Self::Batched(w) => w.start_commit_logged(clock, worker_id, build),
            Self::LockFree(w) => w.start_commit_logged(clock, worker_id, build),
        }
    }

    pub fn log_with_stamp(&self, stamp: TxStamp, build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>) {
        match self {
            Self::Off => {}
            Self::Batched(w) => { w.log_with_stamp(stamp, build); }
            Self::LockFree(w) => w.log_with_stamp(stamp, build),
        }
    }

    pub fn log_commit(&self, stamp: TxStamp, ts_commit: Version) {
        match self {
            Self::Off => {}
            Self::Batched(w) => { w.log_commit(stamp, ts_commit); }
            Self::LockFree(w) => w.log_commit(stamp, ts_commit),
        }
    }

    pub fn start_commit_logged_for_table(
        &self,
        table_id: record::TableId,
        clock: &GlobalClock,
        worker_id: WorkerId,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) -> TxStamp {
        match self {
            Self::Off => TxStamp::new(worker_id, clock.next_timestamp()),
            Self::Batched(w) => w.start_commit_logged_for_table(table_id, clock, worker_id, build),
            Self::LockFree(w) => w.start_commit_logged_for_table(table_id, clock, worker_id, build),
        }
    }

    pub fn log_with_stamp_for_table(
        &self,
        table_id: record::TableId,
        stamp: TxStamp,
        build: impl FnOnce(Version) -> CRUDOperation<Key, Payload>,
    ) {
        match self {
            Self::Off => {}
            Self::Batched(w) => { w.log_with_stamp_for_table(table_id, stamp, build); }
            Self::LockFree(w) => w.log_with_stamp_for_table(table_id, stamp, build),
        }
    }

    pub fn log_commit_for_table(&self, stamp: TxStamp, ts_commit: Version) {
        match self {
            Self::Off => {}
            Self::Batched(w) => { w.log_commit_for_table(stamp, ts_commit); }
            Self::LockFree(w) => w.log_commit_for_table(stamp, ts_commit),
        }
    }
}
