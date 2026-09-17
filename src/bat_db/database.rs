use std::fmt::Display;
use std::hash::Hash;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread;
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use smallvec::SmallVec;
use triomphe::Arc;
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::tx_context::TxContext;
use crate::bat_tree::mvbt::{default_max_workers, MVBTSt};
use crate::bat_wal::backend::WalBackend;
use crate::bat_wal::record::{TableId, WalPayload};
use crate::bat_wal::recovery;

#[cfg(feature = "tree-viz")]
#[derive(Clone, serde::Serialize)]
pub struct DumpColumn {
    pub name: String,
    /// Display type, for example "integer", "text", or "decimal".
    pub data_type: String,
}

/// Best-effort lowers the calling thread's OS scheduling priority to the
/// lowest niceness Linux allows (`19`, via `setpriority(PRIO_PROCESS, tid,
/// ..)` on the thread's own kernel tid — Linux gives each thread, not just
/// each process, its own schedulable priority under `PRIO_PROCESS`). Meant
/// to be called once, at the very start of a background housekeeping
/// thread's body (currently only the vacuum thread `set_vacuum` spawns) —
/// makes the OS scheduler yield that thread's CPU time to
/// foreground transaction threads under contention, rather than sharing it
/// evenly, so background compaction can't degrade foreground latency just
/// by running at the wrong moment. A no-op (never fails loudly) on any
/// non-Linux target or if the syscalls themselves fail — this is a
/// best-effort scheduling hint, not a correctness requirement, so a
/// platform without it just runs the thread at normal priority.
pub(crate) fn lower_current_thread_priority() {
    #[cfg(target_os = "linux")]
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::pid_t;
        libc::setpriority(libc::PRIO_PROCESS, tid as libc::id_t, 19);
    }
}

/// How many tables' worth of `TableEntry` are stored inline inside
/// `Database::tables`' own `Arc` allocation, rather than in a second,
/// separately-heap-allocated buffer the way a plain `Vec` always would be —
/// see `TableList`'s doc for why that second indirection is worth avoiding.
/// Set to the actual table count `Database::create_table` is ever called
/// with across this codebase's own workloads: TPC-C/HTAP's `TpccDatabase`
/// creates exactly 12 tables here (`tpcc_schema::TpccDatabase::
/// create_all_tables`'s doc — `Table::Warehouse`/`Table::District` are
/// deliberately excluded from this list, resolved through their own
/// `big_trees` instead). Past this capacity, `SmallVec` transparently spills
/// to its own heap buffer (see `TableList`'s doc) — correctness never
/// depends on staying under it, only performance does, so a workload that
/// legitimately needs more tables than this just pays one extra indirection
/// rather than breaking.
pub const INLINE_TABLE_CAPACITY: usize = 12;

#[derive(Clone)]
struct TableEntry<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> {
    name: String,
    tree: Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>,
}

/// `Database::tables`' element storage. A plain `Vec<TableEntry<..>>` behind
/// an `ArcSwap` would mean *two* heap indirections to reach a given table:
/// one to dereference the `Arc` and reach the `Vec`'s own header, a second
/// to follow the `Vec`'s separately-allocated buffer pointer to the actual
/// elements. `SmallVec` with an inline capacity embeds the element buffer
/// directly inside the same allocation as the `Vec`-like header — which,
/// here, is itself already inside the `Arc`'s allocation — so as long as
/// the table count stays at or under `INLINE_TABLE_CAPACITY` (the expected
/// case, always, per this module's doc: creating a table is a rare,
/// database-init-time event), reading a table costs exactly one heap
/// indirection (the `Arc`'s own), not two. Past that capacity, `SmallVec`
/// transparently spills to its own separate heap buffer — behaving exactly
/// like `Vec` again — so correctness never depends on staying under the
/// inline capacity, only performance does.
type TableList<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload,
> = SmallVec<[TableEntry<FAN_OUT, NUM_RECORDS, Key, Payload>; INLINE_TABLE_CAPACITY]>;

/// A collection of named tables (each an `MVBTSt` tree) sharing one
/// transactional core (`TxContext`) and one WAL — see this module's doc for
/// how this differs from `bat_bench::tpcc_schema::TpccDatabase`.
///
/// Every table is directly indexable by its `TableId` — simply its position
/// in `tables` — rather than looked up through a hash map: a table's id is
/// assigned once, sequentially, the moment it's created (see
/// `create_table`), and persisted in that same order to a small catalog
/// file colocated with the WAL (see `catalog_path`), so recovery can
/// reconstruct the exact same name-to-index mapping without hashing or any
/// other name resolution — it just recreates tables in the order the
/// catalog file lists them.
///
/// All of `Database`'s state (`tables`, the shared `wal`, `gc`, and
/// `meta_path`) is held in lock-free `arc_swap` cells — the same primitive
/// `MVBTSt::wal` itself already uses — rather than behind a `Mutex`/
/// `RwLock`. Every read (`table`, `table_named`, and every `DbTransaction`
/// op) is a single atomic pointer load followed by plain slice indexing or
/// a linear scan; no lock is ever taken. `create_table` (the only path that
/// mutates `tables`) publishes a new, one-longer table list via
/// `ArcSwap::rcu` — a lock-free compare-and-swap retry loop, not a lock —
/// which is sound specifically *because* creating a table is meant to be
/// rare (database-init time, essentially never afterwards): a retry
/// rebuilds the new tree from scratch, which would be wasteful under real
/// contention but costs nothing in the expected case of zero concurrent
/// creators.
pub struct Database<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> {
    pub(crate) ctx: Arc<TxContext>,
    root_index_type: RootIndexType,
    inc_key: fn(Key) -> Key,
    dec_key: fn(Key) -> Key,
    min_key: Key,
    max_key: Key,
    /// Every table on this database, directly indexable by `TableId` (its
    /// position in this list) — see this struct's and `TableList`'s doc.
    /// Wrapped in a `sync::Arc` (on top of the `ArcSwap` itself) so the
    /// vacuum thread (see `set_vacuum`'s doc) can hold its own cheap clone
    /// of the *handle* — not just a one-time snapshot of its contents — and
    /// so keep re-reading the live table list on every sweep.
    tables: sync::Arc<ArcSwap<TableList<FAN_OUT, NUM_RECORDS, Key, Payload>>>,
    /// The one writer shared by every table on this database — the *same*
    /// `Arc` cloned into each table's own `MVBTSt::wal` field (see
    /// `MVBTSt::attach_wal`) — or empty if WAL is off.
    wal: Arc<WalBackend<Key, Payload>>,
    /// This database's table-catalog path (see `catalog_path`) once WAL has
    /// been enabled at least once — there is nothing to persist a catalog
    /// *for* before then. `None` for a purely in-memory database.
    meta_path: Option<PathBuf>,
    /// `Some(update_in_place)` once `enable_gc` was called (applied to any
    /// table created afterwards); `None` (GC off) otherwise.
    gc: ArcSwapOption<bool>,
    /// The currently-running vacuum (idle-compaction) sweep thread's stop
    /// flag (see `set_vacuum`), or `None` if it's off. Swapped to a fresh
    /// flag whenever `set_vacuum` (re)starts the thread and to `None` when
    /// it stops it, so at most one sweep thread is ever running for this
    /// database at a time.
    vacuum_stop: ArcSwapOption<AtomicBool>,
    /// Per-table shared scan-worker pools (see `bat_tree::scan_pool::
    /// ScanWorkerPool`'s doc), assigned via `enable_scan_pool`/
    /// `disable_scan_pool` and indexed by `TableId`, same as `tables`.
    /// `None`/missing for a table with no pool assigned (every table,
    /// initially). A plain `Mutex`, not `tables`' lock-free `ArcSwap`
    /// pattern: assigning a pool is a rare, setup-adjacent call, never on
    /// any hot path, so there's nothing to gain from lock-freedom here —
    /// same reasoning as `vacuum_stop` just above.
    scan_pools: sync::Mutex<Vec<Option<Arc<crate::bat_tree::scan_pool::ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>>>>>,
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> Database<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// A fresh, empty database — no tables yet, its own private `TxContext`,
    /// WAL/GC both off. `inc_key`/`dec_key`/`min_key`/`max_key` are shared by
    /// every table subsequently created via `create_table` (same role as
    /// `MVBTSt::make_standard`'s fixed `u64` increment/decrement/bounds).
    pub fn new(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
    ) -> Self {
        Self::new_with_max_workers(
            root_index_type, inc_key, dec_key, min_key, max_key,
            default_max_workers(),
        )
    }

    /// `new` with an explicit worker-registry capacity. Benchmark drivers know
    /// their exact loader/worker/OLAP thread budget and use this to avoid sizing
    /// every per-worker structure to the whole machine.
    pub fn new_with_max_workers(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        max_workers: usize,
    ) -> Self {
        let max_workers = max_workers.max(1);
        Self::new_with_max_workers_and_backend(
            root_index_type, inc_key, dec_key, min_key, max_key, max_workers,
            Arc::new(WalBackend::Off), None,
        )
    }

    pub(crate) fn new_with_max_workers_and_backend(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        max_workers: usize,
        wal: Arc<WalBackend<Key, Payload>>,
        meta_path: Option<PathBuf>,
    ) -> Self {
        Self {
            ctx: Arc::new(TxContext::new(max_workers)),
            root_index_type,
            inc_key,
            dec_key,
            min_key,
            max_key,
            tables: sync::Arc::new(ArcSwap::from_pointee(TableList::new())),
            wal,
            meta_path,
            gc: ArcSwapOption::empty(),
            vacuum_stop: ArcSwapOption::empty(),
            scan_pools: sync::Mutex::new(Vec::new()),
        }
    }

    pub fn new_with_wal(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        wal_path: &Path,
        flush_interval: Duration,
    ) -> io::Result<Self> {
        Self::new_with_max_workers_and_wal(
            root_index_type, inc_key, dec_key, min_key, max_key,
            default_max_workers(), wal_path, flush_interval,
        )
    }

    pub fn new_with_max_workers_and_wal(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        max_workers: usize,
        wal_path: &Path,
        flush_interval: Duration,
    ) -> io::Result<Self> {
        let meta_path = catalog_path(wal_path);
        write_catalog(&meta_path, std::iter::empty::<&str>())?;
        Ok(Self::new_with_max_workers_and_backend(
            root_index_type, inc_key, dec_key, min_key, max_key, max_workers,
            Arc::new(WalBackend::open_batched(wal_path, flush_interval)?), Some(meta_path),
        ))
    }

    pub fn new_with_max_workers_and_wal_lockfree(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        max_workers: usize,
        wal_path: &Path,
        flush_interval: Duration,
        batch_size: usize,
    ) -> io::Result<Self> {
        let meta_path = catalog_path(wal_path);
        write_catalog(&meta_path, std::iter::empty::<&str>())?;
        Ok(Self::new_with_max_workers_and_backend(
            root_index_type, inc_key, dec_key, min_key, max_key, max_workers,
            Arc::new(WalBackend::open_lockfree(
                wal_path, flush_interval, batch_size, max_workers.max(1),
            )?), Some(meta_path),
        ))
    }

    /// Creates (or returns the existing) table named `name`, assigning it
    /// the next sequential `TableId` — its index in this database's table
    /// list. Idempotent by name *within one process run*: calling again
    /// with the same name returns the same tree rather than erroring or
    /// creating a second one. This is **not** a substitute for consistent
    /// ordering across a restart — recovery must recreate tables in their
    /// originally-recorded order, which is exactly what `open_recovered`
    /// does (by reading the catalog file itself), not by calling this
    /// directly with names in whatever order a caller happens to pick.
    ///
    /// If this database's WAL is enabled, `name` is durably appended to the
    /// catalog file *before* the table is published (see `tables`' doc) —
    /// so a crash can never leave a table visible to future writers that
    /// the catalog doesn't already know about. The new table also inherits
    /// whatever WAL/GC state this database currently has, applied before
    /// publishing — the gap `bat_bench::tpcc_schema::TpccDatabase` never has
    /// to close, since its 14 tables are all built before anything is
    /// toggled.
    pub fn create_table(&self, name: &str) -> Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        if let Some(existing) = self.table_named(name) {
            return existing;
        }

        if let Some(path) = self.meta_path.as_ref() {
            append_catalog_entry(path, name)
                .expect("bat_db::Database::create_table: failed to append to the table catalog");
        }

        self.create_table_unpublished(name)
    }

    /// Builds a fresh tree for `name` — inheriting whatever WAL/GC state
    /// this database currently has — and publishes it via `ArcSwap::rcu`.
    /// `rcu`'s closure may run more than once under genuine concurrent
    /// creation (retried on a lost race, never under a lock), so the tree
    /// (and its baked-in `table_id`, which must exactly match its final
    /// index) is built fresh inside the closure on every attempt, with only
    /// the *last* attempt's tree — the one that actually got published —
    /// kept, via `built`.
    ///
    /// Used by both `create_table` (which persists `name` to the catalog
    /// first) and `open_recovered` (which recreates tables from an
    /// *already-persisted* catalog, so must not re-append them).
    fn create_table_unpublished(&self, name: &str) -> Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        let gc = self.gc.load();
        let mut built: Option<Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>> = None;

        self.tables.rcu(|current| {
            let id: TableId = current.len() as TableId;

            let tree = Arc::new(MVBTSt::make_with_shared_ctx(
                self.root_index_type,
                self.inc_key,
                self.dec_key,
                self.min_key,
                self.max_key,
                self.ctx.clone(),
                Some(id),
                self.wal.clone(),
            ));
            if let Some(update_in_place) = gc.as_ref() {
                tree.enable_gc(**update_in_place);
            }

            built = Some(tree.clone());

            let mut next = (**current).clone();
            next.push(TableEntry { name: name.to_string(), tree });
            next
        });

        built.expect("ArcSwap::rcu always invokes its closure at least once")
    }

    /// Direct, lock-free slice index by `TableId` — an `Arc` clone (a cheap
    /// refcount bump) of whichever table currently sits at that position,
    /// or `None` if `id` is out of range.
    pub fn table(&self, id: TableId) -> Option<Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>> {
        self.tables.load().get(id as usize).map(|entry| entry.tree.clone())
    }

    /// Linear scan by name — fine given `Database`'s own design assumption
    /// that the total table count stays small (tables are meant to be
    /// created once, at database-init time). Prefer `table`/a `TableId`
    /// (via `MVBTSt::table_id`) on any path that runs more than a handful
    /// of times.
    pub fn table_named(&self, name: &str) -> Option<Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>> {
        self.tables.load().iter().find(|entry| entry.name == name).map(|entry| entry.tree.clone())
    }

    /// Every table's name paired with its tree's stable identity address
    /// (`Arc::as_ptr`, not the `Arc` handle's own stack slot — the same
    /// pointee address every `Arc` clone of that table's tree shares for the
    /// life of the process, since a table is created once and never moved or
    /// freed). Diagnostic use only, e.g. resolving `bat_test`'s
    /// per-tree-address restart attribution back to a table name.
    pub fn table_names_by_addr(&self) -> Vec<(usize, String)> {
        self.tables.load().iter()
            .map(|entry| (Arc::as_ptr(&entry.tree) as usize, entry.name.clone()))
            .collect()
    }

    pub(crate) fn wal_writer(&self) -> Arc<WalBackend<Key, Payload>> {
        self.wal.clone()
    }

    /// Toggles block reclaim uniformly across every table on this database
    /// — see `MVBTSt::enable_gc`'s doc for why partial/per-table toggling
    /// would make pruning the shared commit logs unsound (every table
    /// shares this database's one `TxContext`).
    /// `vacuum`, when `Some((dead_ratio_threshold, sweep_interval))`, also
    /// (re)starts the background vacuum thread (see `set_vacuum`'s doc)
    /// with those parameters; `None` stops it if one is running. The
    /// vacuum thread only ever reclaims space GC has made collectible in
    /// the first place, so it's exposed here as GC's own background half
    /// rather than as a separately toggled feature — one call turns both on
    /// or off together.
    pub fn enable_gc(&self, update_in_place: bool, vacuum: Option<(f64, Duration)>) {
        for entry in self.tables.load().iter() {
            entry.tree.enable_gc(update_in_place);
        }
        self.gc.store(Some(sync::Arc::new(update_in_place)));

        self.set_vacuum(vacuum);
    }

    pub fn disable_gc(&self) {
        for entry in self.tables.load().iter() {
            entry.tree.disable_gc();
        }
        self.gc.store(None);
        self.set_vacuum(None);
    }

    /// Starts or stops the vacuum thread on its own, independent of the
    /// per-tree GC flag `enable_gc`/`disable_gc` toggle — e.g. for a caller
    /// that wants GC's inline reclaim on for a whole run but the vacuum
    /// sweep only during a specific measured window
    /// (`bat_bench::tpcc_driver::run_tpcc`'s load-vs-timed-phase split is
    /// exactly this). `enable_gc`/`disable_gc` are the common-case wrapper
    /// that keeps both toggled together; this is the escape hatch for when
    /// their lifecycles need to diverge.
    ///
    /// `None` stops the currently-running sweep thread, if any — a no-op
    /// otherwise. Signals the thread to exit and returns immediately; it
    /// does not wait for the thread's current sleep/sweep to finish, since
    /// this is best-effort housekeeping, not something callers need to
    /// synchronize with.
    ///
    /// `Some((dead_ratio_threshold, sweep_interval))` (re)starts the sweep
    /// thread: first stopping any previously running one (so at most one
    /// ever runs at a time), then spawning a fresh thread that repeatedly
    /// sweeps every table on this database (see `MVBTSt::compact_idle_pass`'s
    /// doc), forcing a compaction on any leaf whose dead/(active+dead) ratio
    /// is at or above `dead_ratio_threshold`, sleeping `sweep_interval`
    /// between sweeps — the fix for a read-heavy table (few, infrequent
    /// writes to any one leaf) otherwise sitting at a garbage-inflated ratio
    /// indefinitely, since nothing on the ordinary write path ever revisits
    /// such a leaf. This is GC's background half, not a separate feature —
    /// it only ever reclaims space GC's own bookkeeping
    /// (`active_dead_count`) has already made collectible — so it's
    /// started/stopped through `enable_gc`/`disable_gc`'s `vacuum` parameter
    /// rather than its own public on/off switch.
    ///
    /// Runs at the lowest OS scheduling priority this platform supports
    /// (see `lower_current_thread_priority`'s doc) — a vacuum sweep
    /// competing with foreground transaction threads for CPU would defeat
    /// its own "idle" premise, so it's set to yield to them under
    /// contention rather than share evenly.
    ///
    /// Re-reads `tables` (via its cheaply-cloneable `sync::Arc<ArcSwap<..>>`
    /// handle) at the start of every sweep, not just once at spawn time —
    /// so a table created after the thread starts is picked up on the very
    /// next sweep, without needing to call `set_vacuum` again.
    ///
    /// The spawned thread permanently consumes one `WorkerId` from this
    /// database's fixed worker pool the moment its first sweep actually
    /// writes anything (see `bat_sync::worker::WorkerRegistry`'s doc) —
    /// callers sizing `max_workers` (`new_with_max_workers`) need to budget
    /// for it. It also outlives `&self` — it holds its own `Arc` clone of
    /// the `tables` handle, not a reference back to this `Database` — so a
    /// caller that drops this database without calling `disable_gc` first
    /// leaves the thread running forever, keeping those tables (and this
    /// database's `ctx`) alive through its own clone.
    pub fn set_vacuum(&self, vacuum: Option<(f64, Duration)>) {
        if let Some(stop) = self.vacuum_stop.swap(None) {
            stop.store(true, Relaxed);
        }

        let Some((dead_ratio_threshold, sweep_interval)) = vacuum else {
            return;
        };

        let stop = sync::Arc::new(AtomicBool::new(false));
        self.vacuum_stop.store(Some(stop.clone()));

        let tables = self.tables.clone();
        thread::spawn(move || {
            lower_current_thread_priority();
            while !stop.load(Relaxed) {
                for entry in tables.load().iter() {
                    entry.tree.compact_idle_pass(dead_ratio_threshold);
                    if stop.load(Relaxed) {
                        break;
                    }
                }
                thread::sleep(sweep_interval);
            }
        });
    }

    /// Uniformly disables GC and configures whether historic
    /// (pre-GC-horizon) queries stay possible across every table on this
    /// database — see `MVBTSt::allow_historic_query`'s doc. Same "must be
    /// applied uniformly across every table sharing this database's `ctx`"
    /// reasoning as `enable_gc`/`disable_gc`.
    pub fn allow_historic_query(&self, enabled: bool) {
        for entry in self.tables.load().iter() {
            entry.tree.allow_historic_query(enabled);
        }
        self.gc.store(None);
    }

    /// Assigns `id`'s table a dedicated shared scan-worker pool (see
    /// `bat_tree::scan_pool::ScanWorkerPool`'s doc) of `num_workers`
    /// threads, for any query wanting to fan a scan out across it via
    /// `ScanWorkerPool::dispatch`/`try_dispatch` — see `scan_pool` to fetch
    /// it back out. `expected_concurrent_queries` is passed straight
    /// through to `ScanWorkerPool::spawn` — see that method's and
    /// `ScanWorkerPool::fair_query_fanout`'s docs for what it's for. Panics
    /// if `id` names no table. Replacing an already-assigned pool drops
    /// the old one, whose worker threads then exit on their own (see
    /// `ScanWorkerPool`'s doc) — this doesn't wait for that.
    pub fn enable_scan_pool(&self, id: TableId, num_workers: usize, expected_concurrent_queries: Option<usize>) {
        let tree = self.table(id).expect("Database::enable_scan_pool: no table with this TableId");
        let pool = Arc::new(crate::bat_tree::scan_pool::ScanWorkerPool::spawn(
            tree,
            num_workers,
            expected_concurrent_queries,
        ));
        let mut pools = self.scan_pools.lock().unwrap();
        let idx = id as usize;
        if pools.len() <= idx {
            pools.resize_with(idx + 1, || None);
        }
        pools[idx] = Some(pool);
    }

    /// Drops the pool `enable_scan_pool` assigned to `id`, if any — a no-op
    /// otherwise. See that method's doc for the (unwaited) worker thread
    /// shutdown this triggers.
    pub fn disable_scan_pool(&self, id: TableId) {
        let mut pools = self.scan_pools.lock().unwrap();
        if let Some(slot) = pools.get_mut(id as usize) {
            *slot = None;
        }
    }

    /// The pool `enable_scan_pool` assigned to `id`, if any.
    pub fn scan_pool(
        &self,
        id: TableId,
    ) -> Option<Arc<crate::bat_tree::scan_pool::ScanWorkerPool<FAN_OUT, NUM_RECORDS, Key, Payload>>> {
        self.scan_pools.lock().unwrap().get(id as usize).cloned().flatten()
    }

    /// Reads off the shared clock — same value regardless of which table's
    /// tree it's read through, since every table shares this database's one
    /// `ctx`.
    pub fn current_version(&self) -> Version {
        self.ctx.current_version()
    }

    /// Export every named table's index and visible rows at one shared SI
    /// snapshot. Call at a quiescent point: structural page dumping uses raw
    /// page borrows. The caller supplies the application schema and payload
    /// encoder because `Database` deliberately has no SQL column catalog.
    #[cfg(feature = "tree-viz")]
    pub fn dump_explorer_bundle(
        &self,
        path: impl AsRef<Path>,
        schemas: &[Vec<DumpColumn>],
        encode_row: impl Fn(TableId, &Payload) -> serde_json::Map<String, serde_json::Value>,
    ) -> io::Result<()> {
        use crate::bat_db::transaction::DbTransaction;
        use crate::bat_query::interval::Interval;
        use serde_json::{json, Value};

        let tables = self.tables.load();
        if schemas.len() != tables.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "one column schema is required for each table"));
        }
        let mut tx = DbTransaction::begin(self);
        let snapshot_version = tx.ts_start().to_string();
        let mut exported = Vec::with_capacity(tables.len());
        for (index, entry) in tables.iter().enumerate() {
            let id = index as TableId;
            let mut rows = Vec::new();
            tx.range_for_each(id, Interval::new(self.min_key, self.max_key), |key, payload| {
                let mut row = encode_row(id, payload);
                row.insert("key".into(), Value::String(key.to_string()));
                rows.push(Value::Object(row));
            });
            exported.push(json!({
                "id": id,
                "name": entry.name,
                "columns": schemas[index],
                "rows": rows,
                "tree": crate::bat_viz::dump::build_tree_dump(&entry.tree, None),
            }));
        }
        tx.abort();
        let glc_next = self.current_version();
        let bundle = json!({
            "format": "batstore-explorer-bundle-v1",
            "snapshot_version": snapshot_version,
            "glc_next": glc_next.to_string(),
            "glc_last": glc_next.saturating_sub(1).to_string(),
            "max_worker_id": self.ctx.max_worker_id(),
            "tables": exported,
        });
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, &bundle).map_err(io::Error::other)
    }

    pub fn root_star_index(&self) -> RootIndexType {
        self.root_index_type
    }

    pub(crate) fn worker_id(&self) -> WorkerId {
        self.ctx.worker_id()
    }

    pub(crate) fn begin_snapshot(&self) -> Version {
        self.ctx.begin_snapshot()
    }

    pub(crate) fn end_snapshot(&self, ts_start: Version) {
        self.ctx.end_snapshot(ts_start)
    }
}

/// On-disk path for a `Database`'s table catalog: one table name per line,
/// in creation order — a table's `TableId` is simply its 0-based line
/// number, so recovery never hashes or otherwise resolves a name to an id,
/// only reads this file top to bottom and recreates tables in that order.
/// Colocated with the WAL by suffixing its path, since the catalog must be
/// readable *before* any tree exists to replay the WAL into.
///
/// Plain newline-delimited text rather than actual JSON, despite the
/// "meta.json" name suggested when this feature was scoped: this project
/// has no JSON dependency, table names are simple identifiers with no
/// escaping concerns, and a flat list is genuinely appendable a byte-range
/// at a time (`append_catalog_entry`), unlike a JSON array, which would need
/// a full-file rewrite on every new table to stay valid. Swap this for a
/// real `serde_json`-backed encoding if the crate ever adds that dependency
/// for other reasons.
fn catalog_path(wal_path: &Path) -> PathBuf {
    let mut s = wal_path.as_os_str().to_owned();
    s.push(".meta");
    PathBuf::from(s)
}

fn write_catalog<'a>(path: &Path, names: impl Iterator<Item = &'a str>) -> io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    for name in names {
        writeln!(file, "{name}")?;
    }
    file.sync_data()
}

fn append_catalog_entry(path: &Path, name: &str) -> io::Result<()> {
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{name}")?;
    file.sync_data()
}

fn read_catalog(path: &Path) -> io::Result<Vec<String>> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(content.lines().filter(|line| !line.is_empty()).map(str::to_string).collect()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + Send + 'static,
    Payload: Display + Clone + Default + Sync + 'static + WalPayload,
> Database<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Builds a fresh, empty database, reads its table catalog (see
    /// `catalog_path`) at the path derived from `wal_path` and recreates
    /// every table it lists, in the exact order recorded (so each lands
    /// back at the same `TableId`/index it originally had — no separate
    /// name resolution needed), replays the one shared log file into them
    /// via `bat_wal::recovery::replay_database`, truncates it to its own
    /// valid prefix, then attaches one live writer — the `Database`
    /// counterpart to `MVBTSt::open_recovered`/`TpccDatabase::open_recovered`.
    /// A missing catalog file is treated as zero tables (a fresh database).
    pub fn open_recovered(
        root_index_type: RootIndexType,
        inc_key: fn(Key) -> Key,
        dec_key: fn(Key) -> Key,
        min_key: Key,
        max_key: Key,
        wal_path: &Path,
        flush_interval: Duration,
    ) -> io::Result<Self> {
        let db = Self::new_with_max_workers_and_backend(
            root_index_type, inc_key, dec_key, min_key, max_key,
            default_max_workers(),
            Arc::new(WalBackend::open_batched(wal_path, flush_interval)?),
            Some(catalog_path(wal_path)),
        );

        for name in read_catalog(&catalog_path(wal_path))? {
            db.create_table_unpublished(&name);
        }

        {
            let snapshot = db.tables.load();
            let trees: Vec<&MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>> =
                snapshot.iter().map(|entry| entry.tree.as_ref()).collect();

            let valid_len = recovery::replay_database(&trees, wal_path)?;
            if let Ok(file) = std::fs::OpenOptions::new().write(true).open(wal_path) {
                file.set_len(valid_len)?;
            }
        }

        Ok(db)
    }
}
