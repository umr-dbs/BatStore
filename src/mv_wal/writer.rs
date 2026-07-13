use std::cmp::Ord;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::clock::GlobalClock;
use crate::mv_wal::record::{self, WalRecord};

const GROUP_COMMIT_LINGER: Duration = Duration::from_micros(200);

struct LogMessage {
    bytes: Vec<u8>,
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
    _marker: PhantomData<(Key, Payload)>,
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

        let thread = thread::spawn(move || Self::flush_loop(file, receiver, flush_interval));

        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            _marker: PhantomData,
        })
    }

    fn flush_loop(mut file: File, receiver: Receiver<LogMessage>, flush_interval: Duration) {
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

            for msg in batch {
                let _ = msg.ack.send(());
            }
        }
    }

    /// Blocks until the record this `ticket` (from `start_commit_logged`)
    /// belongs to has been durably fsynced.
    pub fn wait_flushed(&self, ticket: Receiver<()>) {
        let _ = ticket.recv();
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

        let mut body = Vec::with_capacity(32);
        record::encode(&WalRecord { stamp, op }, &mut body);

        let mut framed = Vec::with_capacity(body.len() + 8);
        record::frame(&body, &mut framed);

        let (ack_tx, ack_rx) = bounded(1);
        // Safe to unwrap: the sender is only ever taken (and the channel
        // closed) from `Drop`, which can't run concurrently with this call
        // — `self` is reached through an `Arc`, so `Drop` only runs once no
        // other reference (and thus no other call to this method) exists.
        let _ = self.sender.as_ref().unwrap().send(LogMessage { bytes: framed, ack: ack_tx });
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn group_commit_flushes_and_wait_unblocks() {
        let path = std::env::temp_dir().join(format!("cmvbt_wal_test_{}.log", std::process::id()));
        let _ = fs::remove_file(&path);

        let writer: WalWriter<u64, u64> =
            WalWriter::open(&path, Duration::from_millis(5)).unwrap();
        let clock = GlobalClock::new();

        let (s1, t1)
            = writer.start_commit_logged(&clock, 0, |_v| CRUDOperation::Insert(1, 100));

        let (s2, t2)
            = writer.start_commit_logged(&clock, 0, |_v| CRUDOperation::Delete(2));
        assert!(s2.ts_start() > s1.ts_start());

        writer.wait_flushed(t1);
        writer.wait_flushed(t2);

        drop(writer);

        let bytes = fs::read(&path).unwrap();
        assert!(!bytes.is_empty());

        let mut offset = 0;
        let mut seen = Vec::new();
        while let Some((body, consumed)) = record::read_frame(&bytes[offset..]) {
            let record: WalRecord<u64, u64> = record::decode(body).unwrap();
            seen.push(record);
            offset += consumed;
        }
        assert_eq!(offset, bytes.len());
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].stamp.ts_start(), s1.ts_start());
        assert_eq!(seen[1].stamp.ts_start(), s2.ts_start());

        let _ = fs::remove_file(&path);
    }
}
