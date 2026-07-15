use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use arc_swap::ArcSwapOption;
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_gc::tracker_handle::{TrackerHandle, TrackerHandleSt};
use crate::mv_page_model::Height;
use crate::mv_root::index_root::{RootIndex, RootIndexType};
use crate::mv_sync::tx_context::TxContext;
use crate::mv_wal::writer::WalWriter;

pub const FAN_OUT: usize        = 125;
/// `RecordPoint<Key, Payload>` is 32B for `Key = Payload = u64` (`VersionInfo`
/// packs down to 16B — see `mv_record_model::tx_stamp::TxStamp`'s doc), so
/// 125 keeps the leaf's record array at the same ~4000B budget `FAN_OUT`'s
/// internal-node arrays target.
pub const NUM_RECORDS: usize    = 125;
pub type Key                    = u64;
pub type Payload                = u64;
// pub type Payload = PayloadIndirection;
pub type MVBT                   = MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub const INIT_TREE_HEIGHT: Height = 1;

/// Default size of a tree's fixed OSIC worker pool (§3.1: commit log size =
/// #workers) — one per physical core, mirroring the paper's one-worker-per-
/// core deployment model.
pub fn default_max_workers() -> usize { crate::mv_sync::visibility::MAX_WORKERS_CAP }
// pub const MAX_TREE_HEIGHT: Height = Height::MAX;

pub struct MVBTSt<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> {
    pub(crate) root: RootIndex<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub block_manager: BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
    /// The transactional core (clock, commit logs, worker registry,
    /// active-snapshot tracking) — see `TxContext`'s type doc. Private and
    /// unshared for every tree built via `make_standard`/`Default::default`/
    /// `open_recovered`; shared (via `Arc::clone`) across several per-table
    /// trees only when built through `make_with_shared_ctx`, so a
    /// transaction spanning those tables stays atomic/snapshot-isolated as
    /// one unit (see `mv_bench::tpcc_schema::TpccDatabase`).
    pub(crate) ctx: Arc<TxContext>,
    pub(crate) inc_key: fn(Key) -> Key,
    pub(crate) dec_key: fn(Key) -> Key,
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
    /// One `WalWriter` per worker (paper §4: "logging is distributed across
    /// threads, each having [its own log]") — sized to `worker_registry`'s
    /// fixed pool, indexed by `WorkerId`, so concurrent workers' commits
    /// fsync independent files instead of serializing through one shared
    /// background flush thread. `enable_wal`/`disable_wal` swap the whole
    /// `Vec` atomically (all shards on/off together); see `wal_shard_path`
    /// for the on-disk naming.
    pub(crate) wal: ArcSwapOption<Vec<WalWriter<Key, Payload>>>,
    /// Set once, the first time `enable_wal` is ever called — lets the write
    /// dispatch path (`wal_start_commit`/`wal_log_write`) skip touching `wal`
    /// at all on a tree that has never had a WAL attached, instead of paying
    /// `ArcSwapOption::load`'s guard mechanism (measured ~4.5% of write-path
    /// time in a WAL-off profile) on every single write for a lookup that
    /// always turns out `None`. Never reset by `disable_wal` — once a tree
    /// has ever had a WAL, later writes fall back to the real (cheap, no-op)
    /// `wal.load()` check rather than trying to re-derive "definitely off"
    /// from one flag, keeping this fast path's correctness trivial to see.
    pub(crate) wal_ever_enabled: AtomicBool,
}

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Clone + Default + Display + Sync + 'static
> Sync for MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Send for MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Display + Clone + Default + Sync + 'static
> Default for MVBTSt<FAN_OUT, NUM_RECORDS, u64, Payload> {
    fn default() -> Self {
        Self::make_standard(RootIndexType::default())
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, u64, Payload>
{
    pub fn count_roots(&self) -> usize {
        self.root.count_roots()
    }
    
    #[inline]
    pub fn make_standard(
        root_index_type: RootIndexType) -> Self
    {
        fn inc_key(k: u64) -> u64 {
            k.checked_add(1).unwrap_or(u64::MAX)
        }

        fn dec_key(k: u64) -> u64 {
            k.checked_sub(1).unwrap_or(u64::MIN)
        }

        Self::make(root_index_type, inc_key, dec_key, u64::MIN, u64::MAX)
    }

    // pub fn olc() -> Self {
    //     Self::make_standard(OLC(), ClockType::SYNC)
    // }
}

/// Split out from the `make_standard`/`count_roots` block above: this is the
/// only method here that needs `Payload: WalPayload` (it calls `enable_wal`
/// and `mv_wal::recovery::replay`), so only it should require that bound —
/// `make_standard`/`count_roots` stay usable for any `Payload`, WAL-capable
/// or not.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload
> MVBTSt<FAN_OUT, NUM_RECORDS, u64, Payload>
{
    /// Builds a fresh tree, replays any existing per-worker WAL shards found
    /// at `wal_path` into it (see `mv_wal::recovery::replay`), truncates
    /// each shard file to its own valid prefix (dropping any torn tail left
    /// by a crash mid-fsync), then attaches live per-worker writers so
    /// subsequent mutations keep appending to those same shard files. Call
    /// this instead of `make_standard` + `enable_wal` whenever the log might
    /// already contain data from a prior run. No separate clock bump is
    /// needed: replaying each op already mints it a fresh version through
    /// the normal path, so the clock is already correctly positioned by the
    /// time `replay` returns.
    pub fn open_recovered(
        root_index_type: RootIndexType,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
    ) -> std::io::Result<Self> {
        let tree = Self::make_standard(root_index_type);

        let valid_lengths = crate::mv_wal::recovery::replay(&tree, wal_path)?;

        for (shard_path, valid_len) in &valid_lengths {
            if let Ok(file) = std::fs::OpenOptions::new().write(true).open(shard_path) {
                file.set_len(*valid_len)?;
            }
        }

        tree.enable_wal(wal_path, flush_interval)?;

        Ok(tree)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Turns on this table's own block reclaim
    /// (`TrackerHandleSt::block_reclaim_enabled`, gates dead-page reuse for
    /// this tree specifically) *and* this tree's `ctx`'s copy of the same
    /// flag (gates whether `commit_tx` prunes `ctx`'s shared `CommitLog`s
    /// and whether active-snapshot tracking runs at all — see `TxContext`'s
    /// doc). When `ctx` is shared by several tables (see
    /// `make_with_shared_ctx`), calling this on just one of them still only
    /// flips *that table's* dead-page reclaim, but flips the *shared*
    /// pruning flag for every table sharing `ctx` — callers responsible for
    /// a whole multi-table database must toggle GC uniformly across all of
    /// its tables (see `mv_bench::tpcc_schema::TpccDatabase::enable_gc`), not
    /// call this per table, or pruning becomes unsound for tables whose own
    /// reclaim never got turned on.
    pub fn enable_gc(&self, update_in_place: bool) {
        self.block_manager.tracker().set_block_reclaim_enabled(true);
        self.block_manager.set_update_in_place(update_in_place);
        self.ctx.set_block_reclaim_enabled(true);
    }

    pub fn disable_gc(&self) {
        self.block_manager.tracker().set_block_reclaim_enabled(false);
        self.ctx.set_block_reclaim_enabled(false);
    }

    pub fn truncate_commit_log(&self, enabled: bool) {
        self.ctx.set_truncate_commit_log(enabled);
    }

    pub fn root_star_index(&self) -> RootIndexType {
        self.root.index_type()
    }

    #[inline(always)]
    pub(crate) fn tracker(&self) -> &TrackerHandleSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.block_manager.tracker()
    }

    #[inline(always)]
    pub(crate) fn has_update_in_place(&self) -> bool {
        self.block_manager.has_update_in_place()
    }

    #[inline]
    fn make(root_index_type: RootIndexType,
            inc_key: fn(Key) -> Key,
            dec_key: fn(Key) -> Key,
            min_key: Key,
            max_key: Key,
    ) -> Self {
        let max_workers = default_max_workers().max(1);
        Self::make_with_shared_ctx(
            root_index_type, inc_key, dec_key, min_key, max_key,
            Arc::new(TxContext::new(max_workers)))
    }

    /// Same as `make`, but takes a pre-built `ctx` instead of creating a
    /// private one — the entry point for several per-table trees that must
    /// share one transactional core (see `TxContext`'s doc and
    /// `mv_bench::tpcc_schema::TpccDatabase`). Every single-tree constructor
    /// (`make_standard`, `Default::default`, `open_recovered`) still funnels
    /// through plain `make` above, so they're unaffected by this existing.
    /// `pub(crate)`: callers outside `mv_tree` construct trees through
    /// benchmark-specific wrappers (e.g. `TpccDatabase::new`) that build the
    /// shared `ctx` once and pass it to every table.
    #[inline]
    pub(crate) fn make_with_shared_ctx(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        ctx: Arc<TxContext>,
    ) -> Self {
        let bm = BlockAllocManager::new();
        Self {
            root: RootIndex::new(root_index_type, &bm, &ctx),
            block_manager: bm,
            ctx,
            inc_key,
            dec_key,
            min_key,
            max_key,
            wal: ArcSwapOption::empty(),
            wal_ever_enabled: AtomicBool::new(false),
        }
    }
}

/// Split from the block above: `enable_wal`/`disable_wal` are the only
/// methods that ever construct/attach a `WalWriter<Key, Payload>`, which
/// (via `start_commit_logged`/`log_with_stamp`) requires `Payload:
/// WalPayload` — everything else on `MVBTSt` (`enable_gc`, `make`, ...)
/// stays usable for any `Payload`, WAL-capable or not.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Attaches one live per-worker WAL writer for every worker in this
    /// tree's fixed pool, each appending to its own shard file derived from
    /// `path` (see `wal_shard_path`) — after any bytes already there (a
    /// fresh file, or the valid prefix a prior `mv_wal::recovery::replay`
    /// left behind). Each shard's group-commit batches are fsynced every
    /// `flush_interval`, independently of every other shard. Cheap/no-op
    /// when never called: the dispatch write path only touches the WAL when
    /// this returns `Some`.
    pub fn enable_wal(&self, path: &std::path::Path, flush_interval: std::time::Duration) -> std::io::Result<()> {
        let max_workers = self.ctx.max_workers();
        let mut shards = Vec::with_capacity(max_workers);
        for worker_id in 0..max_workers {
            shards.push(WalWriter::open(&wal_shard_path(path, worker_id), flush_interval)?);
        }
        self.wal.store(Some(Arc::new(shards)));
        self.wal_ever_enabled.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Detaches every per-worker WAL writer, if any, blocking until each
    /// one's background flush thread drains and fsyncs any remaining
    /// buffered records.
    pub fn disable_wal(&self) {
        self.wal.store(None);
    }
}

/// On-disk path for worker `worker_id`'s WAL shard, derived from the base
/// `path` a caller passes to `enable_wal`/`open_recovered` by appending a
/// zero-padded worker index — e.g. `wal.log` -> `wal.log.0000`,
/// `wal.log.0001`, ... A plain string suffix (not an extension swap) so any
/// base path works regardless of whether it already has an extension.
pub(crate) fn wal_shard_path(base: &std::path::Path, worker_id: usize) -> std::path::PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(format!(".{worker_id:04}"));
    std::path::PathBuf::from(s)
}