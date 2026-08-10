use std::fmt::Display;
use std::hash::Hash;
use std::sync;
use std::sync::atomic::AtomicBool;
use arc_swap::ArcSwapOption;
use triomphe::Arc;
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_gc::tracker_handle::{TrackerHandle, TrackerHandleSt};
use crate::mv_page_model::Height;
use crate::mv_root::index_root::{RootIndex, RootIndexType};
use crate::mv_sync::tx_context::TxContext;
use crate::mv_wal::backend::WalBackend;

/// `RecordPoint<Key, Payload>` is 32B for `Key = Payload = u64` (`VersionInfo`
/// packs down to 16B — see `mv_record_model::tx_stamp::TxStamp`'s doc), so
/// 123 keeps the leaf's record array at the same ~4000B budget `FAN_OUT`'s
/// internal-node arrays target - and, unlike 125, lands the *actual*
/// heap-allocated unit exactly on a 4096B page.
///
/// The real allocation is `OptCell<Block<..>>` (see `SmartCell`'s pointee),
/// not `Block` alone: `OptCell` adds an 8B `cell_version` alongside the
/// block, and `Block`'s own `Node` header rounds up to a 64B-aligned total
/// (`#[repr(C, align(64))]`). At 125, `Block` itself already lands on
/// exactly 4096B, leaving no room for that extra 8B without spilling into
/// a second 64B slice - so `OptCell<Block>` actually came out to 4160B,
/// which isn't a page-sized jemalloc size class either, so jemalloc rounds
/// it up further and packs allocations with no page-alignment guarantee at
/// all: confirmed empirically (real allocations) at only 16/64 (25%) landing
/// page-aligned, each one otherwise wasting ~960B against jemalloc's actual
/// size class and most straddling two physical pages. At 123, `Block` is
/// 4032B, leaving exactly enough room for `OptCell`'s extra 8B (rounded to
/// 4096B) to land back on a page boundary with zero waste - confirmed
/// empirically at 64/64 (100%) page-aligned.
pub const FAN_OUT: usize        = 123;
pub const NUM_RECORDS: usize    = 123;
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
    /// One unified `WalBackend` for this tree — either kind (see that
    /// type's doc): a `WalWriter`, where every worker enqueues into its
    /// single channel (a lock-free MPSC queue, so concurrent workers still
    /// enqueue without blocking each other) and its one background thread
    /// group-commits everyone's pending records together into one file; or
    /// a `LockFreeWalWriter`, where every worker instead reserves its own
    /// byte range and writes it directly. `enable_wal`/`enable_wal_lockfree`/
    /// `disable_wal` swap this atomically.
    pub(crate) wal: ArcSwapOption<WalBackend<Key, Payload>>,
    /// `Some(id)` when this tree is one table of a `mv_db::Database`, whose
    /// tables all share one `WalBackend` (the *same* `Arc` cloned into every
    /// table's `wal` field via `attach_wal`) and must tag their WAL entries
    /// so `mv_wal::recovery::replay_database` can demultiplex the single
    /// interleaved file back to the right table. `None` for every
    /// standalone/single-tree caller (`make_standard`, `Default::default`,
    /// `open_recovered`, and every existing `mv_bench`/`mv_test` tree,
    /// including `TpccDatabase`'s per-table-file tables, which keep their
    /// existing one-writer-per-table design) — see
    /// `mv_sync::version_handle`'s WAL-logging methods for how this branches
    /// between the plain and table-tagged wire encodings.
    pub(crate) table_id: Option<crate::mv_wal::record::TableId>,
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
    /// Builds a fresh tree, replays any existing WAL found at `wal_path`
    /// into it (see `mv_wal::recovery::replay`), truncates the file to its
    /// own valid prefix (dropping any torn tail left by a crash
    /// mid-fsync), then attaches a live writer so subsequent mutations keep
    /// appending to that same file. Call this instead of `make_standard` +
    /// `enable_wal` whenever the log might already contain data from a
    /// prior run. No separate clock bump is needed: replaying each op
    /// already mints it a fresh version through the normal path, so the
    /// clock is already correctly positioned by the time `replay` returns.
    pub fn open_recovered(
        root_index_type: RootIndexType,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
    ) -> std::io::Result<Self> {
        let tree = Self::make_standard(root_index_type);

        let valid_len = crate::mv_wal::recovery::replay(&tree, wal_path)?;

        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(wal_path) {
            file.set_len(valid_len)?;
        }

        tree.enable_wal(wal_path, flush_interval)?;

        Ok(tree)
    }

    /// Same as `open_recovered`, but reattaches via `enable_wal_lockfree`
    /// instead of `enable_wal` — recovery itself (`mv_wal::recovery::replay`,
    /// via `record::resync_next`) doesn't care which writer produced the
    /// file, since both share the same on-disk wire format; only which
    /// writer picks up *afterwards* differs.
    pub fn open_recovered_lockfree(
        root_index_type: RootIndexType,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
        batch_size: usize,
    ) -> std::io::Result<Self> {
        let tree = Self::make_standard(root_index_type);

        let valid_len = crate::mv_wal::recovery::replay(&tree, wal_path)?;

        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(wal_path) {
            file.set_len(valid_len)?;
        }

        tree.enable_wal_lockfree(wal_path, flush_interval, batch_size)?;

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

    pub fn allow_historic_query(&self, enabled: bool) {
        self.disable_gc();
        self.ctx.set_truncate_commit_log(!enabled);
    }

    pub fn root_star_index(&self) -> RootIndexType {
        self.root.index_type()
    }

    /// `Some(id)` if this tree is one table of a `mv_db::Database` — the
    /// index `Database::create_table` assigned it, i.e. its position in
    /// that database's table list — `None` for every standalone/single-tree
    /// caller. See this struct's `table_id` field doc.
    pub fn table_id(&self) -> Option<crate::mv_wal::record::TableId> {
        self.table_id
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
            Arc::new(TxContext::new(max_workers)), None)
    }

    /// Same as `make`, but takes a pre-built `ctx` instead of creating a
    /// private one — the entry point for several per-table trees that must
    /// share one transactional core (see `TxContext`'s doc and
    /// `mv_bench::tpcc_schema::TpccDatabase`). Every single-tree constructor
    /// (`make_standard`, `Default::default`, `open_recovered`) still funnels
    /// through plain `make` above, so they're unaffected by this existing.
    /// `table_id`: `Some(id)` for a table belonging to a `mv_db::Database`
    /// (see this struct's `table_id` field doc); `None` for every other
    /// caller, including `TpccDatabase`, which keeps its own
    /// one-`WalWriter`-per-table design. `pub(crate)`: callers outside
    /// `mv_tree` construct trees through wrappers (`TpccDatabase::new`,
    /// `mv_db::Database::create_table`) that build the shared `ctx` once and
    /// pass it to every table.
    #[inline]
    pub(crate) fn make_with_shared_ctx(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        ctx: Arc<TxContext>,
        table_id: Option<crate::mv_wal::record::TableId>,
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
            table_id,
        }
    }
}

/// Split from the block above: `enable_wal`/`enable_wal_lockfree`/
/// `disable_wal` are the only methods that ever construct/attach a
/// `WalBackend<Key, Payload>`, which (via `start_commit_logged`/
/// `log_with_stamp`) requires `Payload: WalPayload` — everything else on
/// `MVBTSt` (`enable_gc`, `make`, ...) stays usable for any `Payload`,
/// WAL-capable or not.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + crate::mv_wal::record::WalPayload
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Attaches an already-open writer — the shared primitive behind
    /// `enable_wal`/`enable_wal_lockfree` below, and also used directly by
    /// `mv_db::Database` to hand every one of its tables a *clone of the
    /// same* `Arc<WalBackend>` (rather than each table opening its own),
    /// which is what makes a Database's WAL genuinely one shared file
    /// instead of one per table.
    pub(crate) fn attach_wal(&self, writer: sync::Arc<WalBackend<Key, Payload>>) {
        self.wal.store(Some(writer));
        self.wal_ever_enabled.store(true, sync::atomic::Ordering::Relaxed);
    }

    /// Opens and attaches one live, batched (`WalWriter`) WAL writer at
    /// `path` — after any bytes already there (a fresh file, or the valid
    /// prefix a prior `mv_wal::recovery::replay` left behind). Cheap/no-op
    /// when never called: the dispatch write path only touches the WAL
    /// when this returns `Some`. Every worker enqueues into this single
    /// writer's channel; its group-commit batches are fsynced every
    /// `flush_interval`. See `enable_wal_lockfree` for the alternative,
    /// lock-free backend.
    pub fn enable_wal(&self, path: &std::path::Path, flush_interval: std::time::Duration) -> std::io::Result<()> {
        self.attach_wal(sync::Arc::new(WalBackend::open_batched(path, flush_interval)?));
        Ok(())
    }

    /// Same as `enable_wal`, but backed by `LockFreeWalWriter` (via
    /// `WalBackend::LockFree`) instead: every worker locally batches its
    /// own writes (see `mv_wal::lockfree_writer::LocalBatch`) up to
    /// `batch_size` records, flushed early if that fills up or otherwise on
    /// the next periodic sweep (bounded by `flush_interval`), with its own
    /// `pwrite` rather than funneling through one shared background thread.
    /// See `tests/wal_writer_throughput_bench.rs` for when this actually
    /// wins over `enable_wal` (short answer: past a couple of concurrent
    /// worker threads, provided `batch_size` isn't left at `1`).
    pub fn enable_wal_lockfree(
        &self,
        path: &std::path::Path,
        flush_interval: std::time::Duration,
        batch_size: usize,
    ) -> std::io::Result<()> {
        self.attach_wal(sync::Arc::new(WalBackend::open_lockfree(path, flush_interval, batch_size, default_max_workers())?));
        Ok(())
    }

    /// Detaches the WAL writer, if any, blocking until its background flush
    /// thread drains and fsyncs any remaining buffered records.
    pub fn disable_wal(&self) {
        self.wal.store(None);
    }
}

/// Split out purely because `mv_viz::dump::dump_tree_to_file` needs `Key:
/// 'static` restated explicitly (the struct definition above already implies
/// it, but individual `impl` blocks only get what they themselves declare) —
/// every other method on `MVBTSt` stays available without it.
#[cfg(feature = "tree-viz")]
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Dumps this tree's full root* list and the (de-duplicated) block graph
    /// they reach to a JSON file at `path` for `tools/tree_visualizer.html` —
    /// see `mv_viz::dump::dump_tree_to_file`'s doc for the format and the
    /// quiescent-read-only caveat.
    pub fn dump_to_file(&self, path: impl AsRef<std::path::Path>, max_depth: Option<usize>) -> std::io::Result<()> {
        crate::mv_viz::dump::dump_tree_to_file(self, path, max_depth)
    }
}
