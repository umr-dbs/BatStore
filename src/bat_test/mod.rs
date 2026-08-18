use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation::CRUDOperation;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_query::SnapShot;
use crate::bat_query::dispatch::RANGE_DISPATCH_LAZY;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_sync::version_handle;
use crate::bat_tree::mvbt::FAN_OUT;
use crate::bat_tree::mvbt::NUM_RECORDS;
use crate::bat_tree::mvbt::{Key, MVBT, MVBTSt, Payload};
use crossbeam_channel::{Receiver, TryRecvError, unbounded};
use itertools::{Either, Itertools};
use parking_lot::Mutex;
use rand::RngExt;
use rand::distr::{Alphanumeric, Distribution};
use rand::prelude::SliceRandom;
use rand_distr::Zipf;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::{ThreadId, spawn};
use std::time::{Duration, Instant, SystemTime};
use std::{fs, mem, thread};
use triomphe::Arc;

pub const VERBOSE: bool = false;
pub const LOG_REORG: bool = false;
/// Gates the `smo.rs` split/merge diagnostic `eprintln!`s left over from the
/// lost-write investigation (thread id, page address, obsoleted/pushed
/// fences per split/merge) — flip to `true` to bring them back without
/// having to re-thread them by hand.
pub const DIAG: bool = false;
const SYSTEM_STR: &str = "MVTree";
pub static MERGES_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);
pub static SPLITS_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);
pub static MERGE_ROOT_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);
pub static SPLITS_ROOT_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);

// pub static mut RESTARTS_COUNTER: [AtomicUsize; 100] = [
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
// ];

/// Diagnostic: attributes OLC write-traversal restarts (failed optimistic
/// validations / lock-CAS failures) to the page and key that caused them.
/// The question this answers: does contention concentrate on a *few pages
/// each hosting many distinct keys* (a LeanStore-style contention-split
/// candidate), or on *one key repeatedly* (e.g. TPC-C's Warehouse.YTD /
/// District.NEXT_O_ID) — a page split can't help the latter, since it can't
/// separate a key from itself. Off by default (and, being a `const bool`,
/// dead-code-eliminated at every call site when off — same idiom as
/// `LOG_REORG`/`VERBOSE` above). Flip to `true`, run a benchmark, join all
/// worker threads, then call `dump_restart_trace`.
pub const RESTART_TRACE: bool = false;

/// How many attempts a `traversal_write_olc` call took before it finally
/// succeeded, bucketed (index = `attempts.min(ATTEMPT_HISTOGRAM_CAP - 1)`,
/// so the last bucket is "this many or more"). Answers a different question
/// than the per-page trace above: not *where* contention concentrates, but
/// *how many times* a typical write actually has to retry before winning —
/// i.e. whether `sched_yield`'s backoff curve (`smart_cell.rs`:
/// `JITTER_BACKOFF_THRESHOLD` and its spin/`sched_yield`/jittered-sleep
/// tiers) is actually well-matched to the attempt counts writes see in
/// practice, or whether most contention resolves well inside one tier while
/// the others are dead weight (or vice versa: attempts routinely blow past
/// the tiers, meaning the backoff itself might be part of the problem, not
/// just downstream of it). Plain atomics, not thread-sharded, since each
/// bucket is a single counter incremented rarely enough (once per completed
/// write, not once per restart) that cross-thread contention on it isn't a
/// concern the way per-restart recording above would be.
pub const ATTEMPT_HISTOGRAM_CAP: usize = 64;
pub static WRITE_ATTEMPTS_HISTOGRAM: [AtomicU64; ATTEMPT_HISTOGRAM_CAP] =
    [const { AtomicU64::new(0) }; ATTEMPT_HISTOGRAM_CAP];

#[inline(always)]
pub fn record_write_attempts(attempts: usize) {
    if !RESTART_TRACE {
        return;
    }
    WRITE_ATTEMPTS_HISTOGRAM[attempts.min(ATTEMPT_HISTOGRAM_CAP - 1)].fetch_add(1, Relaxed);
}

/// Writes the attempts-to-success histogram to `path` as CSV
/// (`attempts,count`, last row is `{CAP-1}+`), and prints p50/p90/p99/max
/// bucket to stdout for a quick look without opening the file.
pub fn dump_attempt_histogram(path: &str) {
    let counts: Vec<u64> = WRITE_ATTEMPTS_HISTOGRAM
        .iter()
        .map(|a| a.load(Relaxed))
        .collect();
    let total: u64 = counts.iter().sum();

    let mut f = BufWriter::new(
        fs::File::create(path).expect("dump_attempt_histogram: failed to create output file"),
    );
    writeln!(f, "attempts,count").unwrap();
    for (i, c) in counts.iter().enumerate() {
        if i == ATTEMPT_HISTOGRAM_CAP - 1 {
            writeln!(f, "{i}+,{c}").unwrap();
        } else {
            writeln!(f, "{i},{c}").unwrap();
        }
    }

    let percentile = |p: f64| -> Option<usize> {
        let target = (total as f64 * p).ceil() as u64;
        let mut running = 0u64;
        for (i, c) in counts.iter().enumerate() {
            running += c;
            if running >= target {
                return Some(i);
            }
        }
        None
    };

    println!(
        "dump_attempt_histogram: wrote {path} ({total} completed writes; p50={:?} p90={:?} p99={:?} p100_bucket={:?})",
        percentile(0.50),
        percentile(0.90),
        percentile(0.99),
        counts.iter().rposition(|&c| c > 0),
    );
}

/// Diagnostic: measures the OLAP-scan-side counterpart to `RESTART_TRACE`
/// above. `bat_query::iter_query::RangeQueryIter::refill` visits a leaf's
/// *entire* physical record array (live and dead/superseded versions alike,
/// per `LeafPage::as_records()`'s doc) and filters it down to whatever's
/// actually visible to the scan's snapshot — so a leaf that's accumulated a
/// lot of garbage since its last compaction (see `bat_tree::smo::split`'s
/// `ByVersion` case, and `BigTreeSize`'s doc for why `Table::Warehouse`/
/// `Table::District`'s bigger leaves defer that compaction longer) makes
/// every scan over it pay for records it will just throw away. This counts
/// `records visited` (the whole leaf, every call) vs. `records matched`
/// (what actually passed the visibility+range filter) globally, across every
/// tree — cheap enough to enable widely (two `fetch_add`s per leaf visited,
/// not per record), unlike `RESTART_TRACE`'s per-page attribution. Off by
/// default and dead-code-eliminated at every call site when off, same idiom
/// as `RESTART_TRACE`. Flip to `true`, run a scan-heavy benchmark, then call
/// `dump_scan_trace`.
pub const SCAN_TRACE: bool = false;

static SCAN_LEAVES_VISITED: AtomicU64 = AtomicU64::new(0);
static SCAN_RECORDS_VISITED: AtomicU64 = AtomicU64::new(0);
static SCAN_RECORDS_MATCHED: AtomicU64 = AtomicU64::new(0);

/// Records one leaf visited by a range scan: `visited` is the leaf's whole
/// physical record count (live + dead), `matched` is how many of those
/// passed the scan's visibility+range filter. No-op, and dead-code
/// eliminated, unless `SCAN_TRACE` is `true`.
#[inline(always)]
pub fn record_leaf_scan(visited: usize, matched: usize) {
    if !SCAN_TRACE {
        return;
    }
    SCAN_LEAVES_VISITED.fetch_add(1, Relaxed);
    SCAN_RECORDS_VISITED.fetch_add(visited as u64, Relaxed);
    SCAN_RECORDS_MATCHED.fetch_add(matched as u64, Relaxed);
}

/// Resets `record_leaf_scan`'s accumulators — same rationale as
/// `reset_restart_trace`: call before a fresh run's first scan so a
/// multi-run process (e.g. a backend-comparison loop) doesn't carry over a
/// prior run's counts.
pub fn reset_scan_trace() {
    SCAN_LEAVES_VISITED.store(0, Relaxed);
    SCAN_RECORDS_VISITED.store(0, Relaxed);
    SCAN_RECORDS_MATCHED.store(0, Relaxed);
}

/// Prints the accumulated leaves/records-visited-vs-matched totals and their
/// ratio (how many physical records a scan had to look at for every one it
/// actually returned) to stdout. Only ever non-zero when `SCAN_TRACE` is
/// `true`.
pub fn dump_scan_trace() {
    let leaves = SCAN_LEAVES_VISITED.load(Relaxed);
    let visited = SCAN_RECORDS_VISITED.load(Relaxed);
    let matched = SCAN_RECORDS_MATCHED.load(Relaxed);
    let ratio = if matched == 0 {
        f64::NAN
    } else {
        visited as f64 / matched as f64
    };
    println!(
        "dump_scan_trace: {leaves} leaves visited, {visited} records visited, \
        {matched} records matched (visited/matched = {ratio:.2}x)"
    );
}

/// Diagnostic: investigates the `verify_concurrent_shared_keys` livelock
/// (~4% hang rate, reproduced on unmodified `main`) by checking whether
/// `bat_tree::smo::split`'s `VERSION_SPLIT` branch actually shrinks a leaf's
/// survivor set across successive re-splits of the *same key range*, or just
/// churns — copies a same-or-larger survivor set into a fresh page that
/// overflows again almost immediately. Keyed by the leaf's fence (stable
/// across a version-split, unlike the page address, which changes every
/// time). Off by default, dead-code-eliminated when off, same idiom as
/// `RESTART_TRACE`/`SCAN_TRACE` above.
pub const SPLIT_CONVERGENCE_TRACE: bool = false;

static VERSION_SPLIT_PREV_SURVIVORS: std::sync::LazyLock<Mutex<HashMap<String, usize>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
static VERSION_SPLIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static VERSION_SPLIT_NON_SHRINKING: AtomicU64 = AtomicU64::new(0);

/// Records one `VERSION_SPLIT` of a leaf: `fence_key` identifies the key
/// range (stable across repeated re-splits of the same leaf lineage),
/// `survivor_count` is exactly what's about to be copied into the fresh
/// page. Compares against the last recorded `survivor_count` for the same
/// `fence_key` — if it didn't shrink, this `VERSION_SPLIT` made no real
/// progress. No-op, and dead-code eliminated, unless `SPLIT_CONVERGENCE_TRACE`
/// is `true`.
#[inline(always)]
pub fn record_version_split(fence_key: String, survivor_count: usize) {
    if !SPLIT_CONVERGENCE_TRACE {
        return;
    }
    VERSION_SPLIT_TOTAL.fetch_add(1, Relaxed);
    let mut prev = VERSION_SPLIT_PREV_SURVIVORS.lock();
    if let Some(&last) = prev.get(&fence_key) {
        if survivor_count >= last {
            VERSION_SPLIT_NON_SHRINKING.fetch_add(1, Relaxed);
            eprintln!(
                "DIAG version_split non-shrinking fence={fence_key} prev_survivors={last} now_survivors={survivor_count}"
            );
        }
    }
    prev.insert(fence_key, survivor_count);
}

/// Prints the accumulated `VERSION_SPLIT` convergence stats. Only ever
/// non-zero when `SPLIT_CONVERGENCE_TRACE` is `true`.
pub fn dump_split_convergence_trace() {
    let total = VERSION_SPLIT_TOTAL.load(Relaxed);
    let non_shrinking = VERSION_SPLIT_NON_SHRINKING.load(Relaxed);
    println!(
        "dump_split_convergence_trace: {total} VERSION_SPLITs, {non_shrinking} non-shrinking \
        ({:.1}%)",
        if total == 0 {
            0.0
        } else {
            100.0 * non_shrinking as f64 / total as f64
        }
    );
}

/// Count of write-traversal restarts caused by root contention specifically
/// (every writer touches the root, so this is expected to be nonzero; it's
/// tracked separately from per-page attribution below since there's only
/// ever one root and no "key" is available at that call site).
pub static ROOT_RESTARTS: AtomicU64 = AtomicU64::new(0);

/// Same event as `ROOT_RESTARTS`, but broken down by *which table's* root —
/// keyed by the table's own tree address (`&MVBTSt<..>`'s address, identical
/// to `Arc::as_ptr` of the `Arc<MVBTSt<..>>` `Database` holds for that table
/// — see `record_root_restart_for_table`'s call site), since a bare total
/// can't distinguish "every table contends on root about equally" from "one
/// table's root dominates everything else." Same thread-local-then-merge-
/// on-drop shape as `RestartLocal` above, just without the per-key
/// breakdown (every writer to a table touches that table's root regardless
/// of key, so a per-key split wouldn't be informative here).
struct RootRestartsLocal(HashMap<usize, u64>);

static ROOT_RESTARTS_BY_TABLE: std::sync::LazyLock<Mutex<HashMap<usize, u64>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

impl Drop for RootRestartsLocal {
    fn drop(&mut self) {
        if self.0.is_empty() {
            return;
        }
        let mut global = ROOT_RESTARTS_BY_TABLE.lock();
        for (addr, count) in self.0.drain() {
            *global.entry(addr).or_insert(0) += count;
        }
    }
}

thread_local! {
    static ROOT_RESTARTS_LOCAL: RefCell<RootRestartsLocal> = RefCell::new(RootRestartsLocal(HashMap::new()));
}

/// Records one root-contention restart attributed to `tree_addr` (a
/// table's tree address — see this field's own doc). No-op, and dead-code
/// eliminated, unless `RESTART_TRACE` is `true`.
#[inline(always)]
pub fn record_root_restart_for_table(tree_addr: usize) {
    if !RESTART_TRACE {
        return;
    }
    ROOT_RESTARTS_LOCAL.with(|local| {
        *local.borrow_mut().0.entry(tree_addr).or_insert(0) += 1;
    });
}

/// Writes the per-table root-restart breakdown to `path` as CSV
/// (`table_name,tree_addr,root_restarts`), sorted by count descending.
/// `table_names` resolves a tree address to a name (e.g.
/// `Database::table_names_by_addr()`) — an address with no matching name
/// (table dropped, or the caller didn't pass a mapping) is printed as its
/// raw hex address instead. Must be called after every worker thread that
/// might have recorded a root restart has already been `join`ed (same
/// caveat as `dump_restart_trace`).
pub fn dump_root_restarts_by_table(path: &str, table_names: &[(usize, String)]) {
    let global = ROOT_RESTARTS_BY_TABLE.lock();
    let mut rows: Vec<(&usize, &u64)> = global.iter().collect();
    rows.sort_by(|a, b| b.1.cmp(a.1));

    let mut f = BufWriter::new(
        fs::File::create(path).expect("dump_root_restarts_by_table: failed to create output file"),
    );
    writeln!(f, "table_name,tree_addr,root_restarts").unwrap();
    for (addr, count) in rows {
        let name = table_names
            .iter()
            .find(|(candidate, _)| candidate == addr)
            .map_or("<unresolved>", |(_, name)| name.as_str());
        writeln!(f, "{name},0x{addr:x},{count}").unwrap();
    }

    println!(
        "dump_root_restarts_by_table: wrote {path} ({} tables)",
        global.len()
    );
}

struct RestartPageStats {
    total: u64,
    by_site_key: HashMap<(&'static str, String), u64>,
}

/// Per-thread, so recording a restart never contends with any other
/// thread's own recording — merged into `RESTART_GLOBAL` only once, when
/// this thread's TLS is torn down (see `Drop` below). Correct only if
/// `dump_restart_trace` is called after every worker thread that might have
/// recorded a restart has already been `join`ed.
struct RestartLocal(HashMap<usize, RestartPageStats>);

impl Drop for RestartLocal {
    fn drop(&mut self) {
        if self.0.is_empty() {
            return;
        }
        let mut global = RESTART_GLOBAL.lock();
        for (addr, stats) in self.0.drain() {
            let entry = global.entry(addr).or_insert_with(|| RestartPageStats {
                total: 0,
                by_site_key: HashMap::new(),
            });
            entry.total += stats.total;
            for (k, c) in stats.by_site_key {
                *entry.by_site_key.entry(k).or_insert(0) += c;
            }
        }
    }
}

static RESTART_GLOBAL: std::sync::LazyLock<Mutex<HashMap<usize, RestartPageStats>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

thread_local! {
    static RESTART_LOCAL: RefCell<RestartLocal> = RefCell::new(RestartLocal(HashMap::new()));
}

/// Records one restart attributed to `page_addr` (a stable identity for the
/// life of the run today, since block-reclaim GC is off by default — see
/// `SmartCell`'s doc) and `key`, tagged with the call site (`"leaf_write_lock"`,
/// `"on_overflow_node"`, etc.) that observed it. No-op, and dead-code
/// eliminated, unless `RESTART_TRACE` is `true`.
#[inline(always)]
pub fn record_restart(page_addr: usize, key: &impl Display, site: &'static str) {
    if !RESTART_TRACE {
        return;
    }
    let key = key.to_string();
    RESTART_LOCAL.with(|local| {
        let mut local = local.borrow_mut();
        let entry = local
            .0
            .entry(page_addr)
            .or_insert_with(|| RestartPageStats {
                total: 0,
                by_site_key: HashMap::new(),
            });
        entry.total += 1;
        *entry.by_site_key.entry((site, key)).or_insert(0) += 1;
    });
}

/// Total number of distinct `(page_addr, site, key)` entries currently held
/// across every page in `RESTART_GLOBAL` — a direct measure of this
/// diagnostic's own memory footprint, for tests that want to check it
/// without dumping a CSV. Only ever non-zero when `RESTART_TRACE` is `true`.
pub fn restart_trace_footprint() -> usize {
    RESTART_GLOBAL
        .lock()
        .values()
        .map(|s| s.by_site_key.len())
        .sum()
}

/// Clears every accumulator `record_restart`/`record_root_restart_for_table`/
/// `record_write_attempts` feed. These are process-lifetime `static`s that
/// only ever grow (see `RestartLocal`/`RootRestartsLocal`'s doc for why
/// they're merged into a global on thread-exit rather than reset there) —
/// harmless for a single benchmark run that exits the process afterward, but
/// a driver that calls `run_tpcc`/`run_ycsb` more than once in the same
/// process (e.g. `tests/tpcc_wal_backend_bench.rs`'s backend-comparison
/// loop) would otherwise keep accumulating every prior run's restart data
/// on top of the current run's, unbounded, for as long as `RESTART_TRACE` is
/// on. Call at the start of a fresh run, before any worker thread can record
/// anything, so each run's `dump_restart_trace`/`dump_attempt_histogram`/
/// `dump_root_restarts_by_table` output reflects only that run.
pub fn reset_restart_trace() {
    RESTART_GLOBAL.lock().clear();
    ROOT_RESTARTS_BY_TABLE.lock().clear();
    ROOT_RESTARTS.store(0, Relaxed);
    for a in &WRITE_ATTEMPTS_HISTOGRAM {
        a.store(0, Relaxed);
    }
}

/// Writes the merged restart attribution to `path` as CSV
/// (`page_addr,page_total_restarts,page_distinct_site_keys,site,key,count`),
/// one row per (page, site, key) triple, sorted by page total descending.
/// Must be called after every worker thread has been `join`ed (see
/// `RestartLocal`'s doc) — a thread still running has its data sitting in
/// that thread's own TLS, not yet merged into `RESTART_GLOBAL`.
pub fn dump_restart_trace(path: &str) {
    let global = RESTART_GLOBAL.lock();

    let mut pages: Vec<(&usize, &RestartPageStats)> = global.iter().collect();
    pages.sort_by(|a, b| b.1.total.cmp(&a.1.total));

    let mut f = BufWriter::new(
        fs::File::create(path).expect("dump_restart_trace: failed to create output file"),
    );
    writeln!(
        f,
        "page_addr,page_total_restarts,page_distinct_site_keys,site,key,count"
    )
    .unwrap();
    for (addr, stats) in pages {
        let mut entries: Vec<(&(&'static str, String), &u64)> = stats.by_site_key.iter().collect();
        entries.sort_by(|a, b| b.1.cmp(a.1));
        for ((site, key), count) in entries {
            writeln!(
                f,
                "0x{:x},{},{},{},{},{}",
                addr,
                stats.total,
                stats.by_site_key.len(),
                site,
                key,
                count
            )
            .unwrap();
        }
    }

    println!(
        "dump_restart_trace: wrote {path} ({} distinct pages, root_restarts={})",
        global.len(),
        ROOT_RESTARTS.load(Relaxed)
    );
}

pub struct ThreadWorkerInfo {
    pub thread_id: ThreadId,
    pub crud: CRUDOperation<Key, Payload>,
    pub fps: usize,
    pub load: f64,
    pub tick_ops: usize,
    pub total_ops: usize,
}
fn olap_tests(
    index: Arc<MVBT>,
    num_olaps: usize,
    tx_per_thread: usize,
    skew: f32,
    range: Either<Key, Arc<AtomicU64>>,
    fixed_si: bool,
    control_signal: Option<Receiver<ThreadWorkerInfo>>,
) -> (usize, u128) {
    if control_signal.is_none() {
        println!(
            "> Starting OLAPs...{num_olaps} threads, \
        {tx_per_thread} scans per thread."
        );
    } else {
        println!(
            "> Starting OLAPs...{num_olaps} threads, \
         with control signal for continuous scans per thread"
        );
    }

    if range.is_left() {
        println!(
            "> Scan key-range is fixed to 0..={}",
            range.as_ref().left().unwrap()
        )
    } else {
        println!("> Scan key-range is dynamic to 0..=LastKey")
    }

    if num_olaps == 0 {
        return (0, 0);
    }

    let v_index = format!(
        "mv_{}",
        match index.root_star_index() {
            RootIndexType::FrugalList => "fg",
            RootIndexType::SkipList => "sk",
            RootIndexType::BTree => "bt",
            RootIndexType::LinkedList => "ll",
        }
    );

    let lazy = if RANGE_DISPATCH_LAZY { "_lazy" } else { "" };

    let mut olaps = vec![];

    let file_log = format!("{v_index}{lazy}_olap_skew_{skew}.csv");
    let _nc = fs::remove_file(file_log.as_str());
    let mut olap_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(file_log.as_str())
        .unwrap();

    olap_file
        .write_all(
            b"target_snapshot,\
            current_snapshot,\
            target_root_number,\
            current_roots_count,\
            sleep_time,\
            range_start,\
            range_end,\
            count_results,\
            latency\n",
        )
        .unwrap();

    let g_counter = Arc::new(AtomicUsize::new(0));

    let start_olap_time = Instant::now();
    for _ in 0..num_olaps {
        let index = index.clone();

        let signal = control_signal.clone();

        let range = range.clone();

        let count_olaps = g_counter.clone();

        olaps.push(spawn(move || {
            let mut results = vec![];
            let mut tx_c = 0;
            // let range = range.left().unwrap_or(0);
            while tx_c < tx_per_thread {
                let key_min = 0;
                let key_max = Key::MAX;

                let current_si = index.current_version();
                let si = if fixed_si {
                    current_si
                } else {
                    rand::random_range(version_handle::START_VERSION..=current_si)
                };

                let (current_root_position, roots_count) = (0, 0);
                // = index.retrieve_root_number_for(si);
                // println!("Min = {key_min}, max = {key_max}");

                let op = CRUDOperation::Range((key_min..key_max).into(), si);
                // let op = CRUDOperation::Point(key_min, si);
                let time_start = SystemTime::now();

                let crud = index.dispatch_crud(op);

                let time_spent = SystemTime::now()
                    .duration_since(time_start)
                    .unwrap()
                    .as_nanos();

                let count_results = match crud {
                    CRUDOperationResult::MatchedRecords(data) => data.len(),
                    _ => panic!(),
                };

                let _ = count_olaps.fetch_add(1, Relaxed);

                results.push((
                    si,
                    current_si,
                    0u128,
                    key_min,
                    key_min,
                    count_results,
                    time_spent,
                    current_root_position,
                    roots_count,
                ));

                if let Some(signal) = signal.as_ref() {
                    match signal.try_recv() {
                        Err(TryRecvError::Disconnected) => break,
                        _ => continue,
                    }
                }

                tx_c += 1;
            }

            results
        }))
    }

    let olaps = olaps
        .into_iter()
        .map(|j| j.join().unwrap())
        .flatten()
        .collect::<Vec<_>>();

    let time_olap = start_olap_time.elapsed().as_nanos();
    // mem::drop(updaters);

    olaps.into_iter().for_each(
        |(
            target_si,
            current_si,
            sleep_time,
            key_min,
            key_max,
            count_results,
            time_spent,
            current_root_psotion,
            c_roots_count,
        )| {
            olap_file
                .write_all(
                    format!(
                        "\
                            {target_si},\
                            {current_si},\
                            {current_root_psotion},\
                            {c_roots_count},\
                            {sleep_time},\
                            {key_min},\
                            {key_max},\
                            {count_results},\
                            {time_spent}\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
        },
    );

    (g_counter.load(SeqCst), time_olap)
}

const INSERT: u8 = 0;
const UPDATE: u8 = 1;
const DELETE: u8 = 2;

pub(crate) fn main_insert_rate_limiter(parms: Vec<String>) {
    // let log = parms[2].parse::<bool>().unwrap_or(false);
    // let runtime_sec = parms[3].parse::<u64>().unwrap_or(10);
    // let num_workers = parms[4].parse::<usize>().unwrap_or(10);
    // let fps = parms[5].parse::<usize>().unwrap_or(100);
    // let crud = CRUDOperation::InsertRand;
    // let index = Arc::new(MVBT::default());
    // let olap_workers = parms[6].parse::<usize>().unwrap_or(10);
    // let olaps_per_worker = parms[7].parse::<usize>().unwrap_or(10);
    // let olap_skew_workers = parms[8].parse::<f32>().unwrap_or(0f32);
    // let olaps_key_range = parms[9].parse::<Key>().unwrap_or(Key::MAX);
    // let olaps_si_freshest = parms[10].parse::<bool>().unwrap_or(false);
    // let (info_sender, info_receiver)
    //     = unbounded();
    //
    // let file_name
    //     = format!("mv_runtime_{runtime_sec}_workers_{num_workers}_fps_{fps}_crud_{crud}.csv");
    //
    // let _ = fs::remove_file(file_name.as_str());
    // let mut log_file = BufWriter::new(OpenOptions::new()
    //     .write(true)
    //     .append(true)
    //     .create(true)
    //     .open(file_name.as_str()).unwrap());
    //
    // log_file.write_all(b"tid,crud,fps,load,tick_ops,total_ops\n").unwrap();
    //
    // let start_time = Instant::now();
    // let workers = (0..num_workers)
    //     .map(|_| ThreadWorker::new(
    //         index.clone(),
    //         fps,
    //         crud.clone(),
    //         log,
    //         info_sender.clone()))
    //     .collect_vec();
    //
    // let signal = info_receiver.clone();
    // spawn(move || olap_tests(
    //     index,
    //     olap_workers,
    //     olaps_per_worker,
    //     olap_skew_workers,
    //     Either::Left(olaps_key_range),
    //     olaps_si_freshest,
    //     Some(signal)));
    //
    // while start_time.elapsed().as_secs() < runtime_sec {
    //     match info_receiver.try_recv() {
    //         Ok(info) =>
    //             log_file.write_all(format!("{}\n", info).as_bytes()).unwrap(),
    //         _ => thread::yield_now()
    //     }
    // }
    //
    // println!("Total Ops = {}", workers
    //     .into_iter()
    //     .map(|t| t.stop())
    //     .collect_vec()
    //     .into_iter()
    //     .map(|handle| handle.join().unwrap())
    //     .sum::<usize>());
    //
    // mem::drop(info_receiver);
}
pub(crate) fn main_test(parms: Vec<String>) {
    let n = parms[2].parse().unwrap();
    let num_olaps = parms[3].parse::<usize>().unwrap();
    let olaps_per_worker = parms[4].parse::<usize>().unwrap();
    let skew = parms[5].parse::<f32>().unwrap();
    let key_range = parms[6].parse().unwrap_or(Key::MAX);
    let root_star_index = match parms[7].as_str() {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default(),
    };
    println!("RootStar = {}", root_star_index);

    let tree = Arc::new(MVBT::make_standard(root_star_index));
    let mut check = HashMap::new();
    let mut errors = 0;

    let p = AtomicU64::new(0);
    while check.len() < n {
        let key = rand::random_range(0..100_000_000);

        if !check.contains_key(&key) {
            match tree.dispatch_crud(CRUDOperation::Insert(key, p.fetch_add(1, SeqCst))) {
                CRUDOperationResult::Inserted(v) => {
                    check.insert(key, v);
                }
                _ => {
                    println!("Error insert key={key}");
                    errors += 1
                }
            };
        }
    }
    for (k, _) in check.iter() {
        (0..1_00).for_each(|_| {
            match tree.dispatch_crud(CRUDOperation::Update(*k, p.fetch_add(1, SeqCst))) {
                CRUDOperationResult::Updated(_) => {}
                _ => panic!(),
            }
        });
    }

    for (k, v) in check.iter() {
        (0..1_00).for_each(
            |o| match tree.dispatch_crud(CRUDOperation::Point(*k, *v + o)) {
                CRUDOperationResult::MatchedRecords(r) => {
                    if r.len() == 1 && *r[0].payload <= *v + o {
                    } else {
                        println!(
                            "Found Version = {}\nQuery Version = {}",
                            *r[0].payload,
                            *v + o
                        );
                    }
                }
                _ => panic!(),
            },
        );
    }

    // test root retrival time.
    mem::drop(check);

    thread::sleep(Duration::from_millis(100));

    println!("Roots present = {}", tree.count_roots());
    let start_root = SystemTime::now();
    tree.retrieve_root_for(1);
    let end_root = SystemTime::now().duration_since(start_root).unwrap();
    println!("{root_star_index} -> Root access: {end_root:?}");

    olap_tests(
        tree,
        num_olaps,
        olaps_per_worker,
        0f32,
        Either::Left(key_range),
        false,
        None,
    );
    return;
    let start_time_iter = SystemTime::now();
    let iter_range = tree.dispatch_crud(CRUDOperation::RangeIter((0..=Key::MAX).into(), 10));

    let iter_res = match iter_range {
        CRUDOperationResult::MatchedRecordIter(iter) => iter,
        _ => panic!(),
    };

    let mut data_from_iter = iter_res.collect_vec();

    let end_time_iter = SystemTime::now().duration_since(start_time_iter).unwrap();
    println!("Time elapsed Iter: {:?}", end_time_iter);
    data_from_iter.sort_by_key(|r| r.key);

    let start_time_range = SystemTime::now();
    let res_all = tree.dispatch_crud(CRUDOperation::Range((0..=Key::MAX).into(), 10));

    let all_res = match res_all {
        CRUDOperationResult::MatchedRecords(vec) => vec,
        _ => panic!(),
    };

    let end_time_range = SystemTime::now().duration_since(start_time_range).unwrap();
    println!("Time elapsed Range: {:?}", end_time_range);
    let mut data_from_all = all_res;
    data_from_all.sort_by_key(|r| r.key);

    println!(
        "Results Iter = {}, Results All = {}",
        data_from_iter.len(),
        data_from_all.len()
    );

    for (k1, k2) in data_from_iter.iter().zip(data_from_all.iter()) {
        if k1.key != k2.key {
            panic!("Key mismatch");
        }
    }
    // olap_tests(tree, num_olaps, olaps_per_worker, skew, key_range, false, None)
}
pub(crate) fn main_sorted_insert(parms: Vec<String>) {
    let query_file_name = parms[2].clone();
    let n: usize = parms[3].parse().unwrap();
    let _nc = fs::remove_file(query_file_name.as_str());

    let mut query_file = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{query_file_name}"))
            .unwrap(),
    );

    let mut querys = 0_usize;

    let mut io_handle = |key: Key| {
        let mut buff = [INSERT, 0, 0, 0, 0, 0, 0, 0, 0];
        buff[1..].copy_from_slice(key.to_le_bytes().as_slice());

        querys += 1;
        query_file.write_all(buff.as_slice()).unwrap()
    };

    (0..n as Key).into_iter().for_each(|op| io_handle(op));
    query_file.flush().unwrap();

    println!("Generated {querys} / {n} keys in sorted order in {query_file_name}!")
}

pub fn main_load_ycsb(parms: Vec<String>) {
    println!("###### Command: {} ######", parms.iter().skip(1).join(" "));

    let query_file_name = parms[2].to_string();
    let concurrent = true;
    let num_olaps: usize = parms[4].parse().unwrap();

    let scans_per_thread = parms[5].parse().unwrap();

    let skew: f64 = parms[6].parse().unwrap();
    let range = parms[7].parse().unwrap_or(Key::MAX);
    let root_star_index = match parms[8].as_str() {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default(),
    };

    let gc = parms[9].parse::<bool>().unwrap_or(false);
    let update_in_place = if gc {
        parms[10].parse::<bool>().unwrap_or(false)
    } else {
        false
    };

    let index = Arc::new(MVBTSt::<FAN_OUT, NUM_RECORDS, Key, Payload>::make_standard(
        root_star_index,
    ));

    let mut gc_str = "Off".to_string();
    if gc {
        index.enable_gc(update_in_place);
        gc_str = format!("On (UIP = {})", update_in_place);
    }

    let oltp_threads = if concurrent { scans_per_thread } else { 1 };
    println!(
        "- QueryFile = {query_file_name}\n\
                - Concurrent = {concurrent}\n\
                - OLTP Threads = {oltp_threads}\n\
                - OLAP Threads = {num_olaps} (Cores = {}, Threads = {})\n\
                - Scans/Thread = {}\n\
                - Skew = {skew}\n\
                - Range = {range}\n\
                - Root* = {root_star_index}\n\
                - GC = {gc_str}",
        num_cpus::get_physical(),
        num_cpus::get(),
        if concurrent {
            format!("Continuous\n- OLTP Threads = {scans_per_thread}")
        } else {
            format!("{scans_per_thread}")
        }
    );

    let oltp_there = fs::exists("oltp.csv").unwrap();
    let mut oltp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("oltp.csv")
        .unwrap();

    if !oltp_there {
        oltp_file
            .write_all(
                b"\
            is_concurrent,\
            oltp_threads,\
            olap_threads,\
            v_index,\
            skew,\
            gc,\
            update_in_place,\
            slice_per_thread,\
            rest_slice,\
            blocks_allocated,\
            blocks_reused,\
            total_num_scan_tx,\
            total_num_oltp_tx,\
            total_oltp_time,\
            total_olap_time\n",
            )
            .unwrap();
    }
    if concurrent {
        (0..15_000_000).for_each(|i| {
            let _ = index.dispatch_crud(CRUDOperation::Insert(
                rand::random_range(0..Key::MAX),
                Payload::default(),
            ));
        });
        // index.block_manager.alloc_count.store(0, Ordering::SeqCst);
        // index.block_manager.reuse_count.store(0, Ordering::SeqCst);

        // TODO: End experimental setting
        let counter_inserts = Arc::new(AtomicU64::new(0));

        oltp_file
            .write_all(
                format!(
                    "\
            true,\
            {oltp_threads},\
            {num_olaps},\
            BatStore({root_star_index}),\
            {skew},\
            {gc},\
            {update_in_place},\
            dynamic,\
            0"
                )
                .as_bytes(),
            )
            .unwrap();

        let start_time_oltp = Instant::now();
        let oltp_joins = (0..oltp_threads)
            .into_iter()
            .map(|_| {
                let index = index.clone();
                let counter_inserts = counter_inserts.clone();
                spawn(move || {
                    let mut count_crud = 0;
                    while counter_inserts.fetch_add(1, Relaxed) < 10_000_000 {
                        let _ = index.dispatch_crud(CRUDOperation::Insert(
                            rand::random_range(0..Key::MAX),
                            Payload::default(),
                        ));

                        count_crud += 1;
                    }

                    count_crud
                })
            })
            .collect_vec();

        let oltp_executed = oltp_joins
            .into_iter()
            .map(|j| j.join().unwrap())
            .sum::<usize>();

        let oltp_total_time = start_time_oltp.elapsed().as_nanos();

        let (num_scans_executed, olap_total_time) = (0, 0);

        // let reuse_blocks
        //     = index.block_manager.reuse_count.load(SeqCst);
        // let alloc_blocks
        //     = index.block_manager.alloc_count.load(SeqCst);

        let reuse_blocks = 0;
        let alloc_blocks = 0;

        let oltp_executed = counter_inserts.load(SeqCst) as _;
        oltp_file
            .write_all(
                format!(
                    ",\
        {alloc_blocks},\
        {reuse_blocks},\
        {num_scans_executed},\
        {oltp_executed},\
        {oltp_total_time},\
        {olap_total_time}\n"
                )
                .as_bytes(),
            )
            .unwrap();

        println!(
            "- Executed {} OLTPs from {query_file_name}\n\
        - Executed = {} OLAPs",
            format_insertions(oltp_executed),
            format_insertions(num_scans_executed)
        );

        println!(
            "###### End Command: {} ######",
            parms.iter().skip(1).join(" ")
        );
    }

    oltp_file.flush().unwrap();
    // println!("{}", NODES_REQUEST.load(SeqCst));
}

pub(crate) fn main_load(parms: Vec<String>) {
    println!("###### Command: {} ######", parms.iter().skip(1).join(" "));

    let query_file_name = parms[2].to_string();
    let concurrent = parms[3].parse::<bool>().unwrap();
    let num_olaps = parms[4].parse().unwrap();

    let scans_per_thread = parms[5].parse().unwrap();

    let skew = parms[6].parse().unwrap();
    let range = parms[7].parse().unwrap_or(Key::MAX);
    let root_star_index = match parms[8].as_str() {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default(),
    };

    let gc = parms[9].parse::<bool>().unwrap_or(false);
    let update_in_place = if gc {
        parms[10].parse::<bool>().unwrap_or(false)
    } else {
        false
    };

    let init_keys = parms[11].parse::<usize>().unwrap_or(100_000);

    let wal = parms[12].parse::<bool>().unwrap_or(false);

    let wal_dir = parms[13].parse::<String>().unwrap_or("wal".to_string());

    let _ = fs::remove_file(wal_dir.as_str());

    let wal_epoch = parms[14].parse::<u64>().unwrap_or(1000);

    let index = Arc::new(if wal {
        MVBTSt::make_standard(root_star_index)
            .with_wal(
                Path::new(wal_dir.as_str()),
                Duration::from_millis(wal_epoch),
            )
            .expect("Error creating WAL")
    } else {
        MVBTSt::make_standard(root_star_index)
    });

    let mut gc_str = "Off".to_string();
    if gc {
        index.enable_gc(update_in_place);
        gc_str = format!("On (UIP = {})", update_in_place);
    }

    let oltp_threads = if concurrent { scans_per_thread } else { 1 };
    println!(
        "- QueryFile = {query_file_name}\n\
                - Concurrent = {concurrent}\n\
                - OLTP Threads = {oltp_threads}\n\
                - OLAP Threads = {num_olaps} (Cores = {}, Threads = {})\n\
                - Scans/Thread = {}\n\
                - Skew = {skew}\n\
                - Range = {range}\n\
                - Root* = {root_star_index}\n\
                - GC = {gc_str}",
        num_cpus::get_physical(),
        num_cpus::get(),
        if concurrent {
            format!("Continuous\n- OLTP Threads = {scans_per_thread}")
        } else {
            format!("{scans_per_thread}")
        }
    );

    let oltp_there = fs::exists("oltp.csv").unwrap();
    let mut oltp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("oltp.csv")
        .unwrap();

    if !oltp_there {
        oltp_file
            .write_all(
                b"\
            is_concurrent,\
            oltp_threads,\
            olap_threads,\
            v_index,\
            skew,\
            gc,\
            update_in_place,\
            slice_per_thread,\
            rest_slice,\
            blocks_allocated,\
            blocks_reused,\
            total_num_scan_tx,\
            total_num_oltp_tx,\
            total_oltp_time,\
            total_olap_time\n",
            )
            .unwrap();
    }

    if concurrent {
        let query_file_name_clone = query_file_name.clone();
        let mut oltp = load_query_into_memory(query_file_name_clone.as_str());

        oltp.drain(0..init_keys).for_each(|i| {
            let _ = index.dispatch_atomic_transaction(i);
        });
        // index.block_manager.alloc_count.store(0, Ordering::SeqCst);
        // index.block_manager.reuse_count.store(0, Ordering::SeqCst);

        // TODO: End experimental setting
        let oltp_threads = scans_per_thread;
        let slice = oltp.len() / oltp_threads;

        let mut work_oltp = (0..oltp_threads)
            .map(|_| oltp.drain(..slice).collect_vec())
            .collect_vec();

        let rest_slice = oltp.len();
        work_oltp.first_mut().unwrap().extend(oltp);
        oltp_file
            .write_all(
                format!(
                    "\
            true,\
            {oltp_threads},\
            {num_olaps},\
            BatStore({root_star_index}),\
            {skew},\
            {gc},\
            {update_in_place},\
            {slice},\
            {rest_slice}"
                )
                .as_bytes(),
            )
            .unwrap();

        let start_time_oltp = Instant::now();
        let oltp_joins = work_oltp
            .into_iter()
            .map(|work| {
                let index = index.clone();
                spawn(move || {
                    let mut count_crud = 0;
                    work.into_iter().for_each(|crud| {
                        let _ = index.dispatch_crud(crud);
                        count_crud += 1;
                    });
                    count_crud
                })
            })
            .collect_vec();

        let (olap_signal, olap_sink) = unbounded();

        let index_olaps = index.clone();
        let olaps = spawn(move || {
            olap_tests(
                index_olaps,
                num_olaps,
                1,
                skew,
                Either::Left(range),
                false,
                Some(olap_sink),
            )
        });

        let oltp_executed = oltp_joins
            .into_iter()
            .map(|j| j.join().unwrap())
            .sum::<usize>();

        let oltp_total_time = start_time_oltp.elapsed().as_nanos();
        drop(olap_signal);
        let (num_scans_executed, olap_total_time) = olaps.join().unwrap();

        // let reuse_blocks
        //     = index.block_manager.reuse_count.load(SeqCst);
        // let alloc_blocks
        //     = index.block_manager.alloc_count.load(SeqCst);

        let reuse_blocks = 0;
        let alloc_blocks = 0;

        oltp_file
            .write_all(
                format!(
                    ",\
        {alloc_blocks},\
        {reuse_blocks},\
        {num_scans_executed},\
        {oltp_executed},\
        {oltp_total_time},\
        {olap_total_time}\n"
                )
                .as_bytes(),
            )
            .unwrap();

        println!(
            "- Executed {} OLTPs from {query_file_name}\n\
        - Executed = {} OLAPs",
            format_insertions(oltp_executed),
            format_insertions(num_scans_executed)
        );

        println!(
            "###### End Command: {} ######",
            parms.iter().skip(1).join(" ")
        );
    } else {
        let mut oltp_tx_buff = load_query_into_memory(query_file_name.as_str());

        // TODO: Explicit for Experiment
        oltp_tx_buff.drain(0..init_keys).for_each(|i| {
            let _ = index.dispatch_crud(i);
        });

        let num = oltp_tx_buff.len();
        let start_oltp_time = Instant::now();

        oltp_tx_buff.into_iter().for_each(|crud| {
            let _ = index.dispatch_crud(crud);
        });

        let oltp_total_time = start_oltp_time.elapsed().as_nanos();

        println!(
            "- Executed {} CRUD operations from {query_file_name}, \
                 starting OLAPs...",
            format_insertions(num)
        );

        let (num_scans_executed, olap_total_time) = olap_tests(
            index.clone(),
            num_olaps,
            scans_per_thread,
            skew,
            Either::Left(range),
            false,
            None,
        );

        // let reuse_blocks
        //     = index.block_manager.reuse_count.load(SeqCst);
        // let alloc_blocks
        //     = index.block_manager.alloc_count.load(SeqCst);

        let reuse_blocks = 0;
        let alloc_blocks = 0;

        oltp_file
            .write_all(
                format!(
                    "\
            false,\
            1,\
            {num_olaps},\
            BatStore({root_star_index}),\
            {skew},\
            {gc},\
            {update_in_place},\
            {num},\
            0,\
            {alloc_blocks},\
            {reuse_blocks},\
            {num_scans_executed},\
            {num},\
            {oltp_total_time},\
            {olap_total_time}\n"
                )
                .as_bytes(),
            )
            .unwrap();

        println!(
            "- Executed = {} OLAPs",
            format_insertions(num_scans_executed)
        );
        println!(
            "###### End Command: {} ######",
            parms.iter().skip(1).join(" ")
        );
    }

    oltp_file.flush().unwrap();
    // println!("{}", NODES_REQUEST.load(SeqCst));
}
pub(crate) fn main_load_cc_new(parms: Vec<String>) {
    let query_file_name = parms[2].to_string();

    let num_olaps = parms[3].parse().unwrap();
    let workers_per_thread = parms[4].parse().unwrap();
    let skew = parms[5].parse().unwrap();
    let root_star_index = match parms[6].as_str() {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default(),
    };
    let index = Arc::new(MVBTSt::make_standard(root_star_index));

    println!("root_start_index = {}", root_star_index);

    let atomic_key = Arc::new(AtomicU64::new(0));

    let index_c = index.clone();
    let (olap_signal, olap_sink) = unbounded();

    let atomic_key_clone = atomic_key.clone();
    let query_file_name_clone = query_file_name.clone();
    let num = spawn(move || {
        load_query(
            query_file_name_clone.as_str(),
            index_c,
            Some(atomic_key_clone),
        )
    });

    let olaps = spawn(move || {
        olap_tests(
            index,
            num_olaps,
            workers_per_thread,
            skew,
            Either::Right(atomic_key),
            true,
            Some(olap_sink),
        )
    });

    let num = num.join().unwrap();
    mem::drop(olap_signal);

    olaps.join().unwrap();

    println!(
        "Finished executing {} CRUD operations from {query_file_name}",
        format_insertions(num)
    );
}
pub(crate) fn main_generate(parms: Vec<String>) {
    let query_file_name = parms[2].as_str();
    let init_population: usize = parms[3].parse().unwrap();
    let total_blocks: usize = parms[4].parse().unwrap();
    let block_inserts: usize = parms[5].parse().unwrap();
    let block_updates: usize = parms[6].parse().unwrap();
    let block_deletes: usize = parms[7].parse().unwrap();

    let skew = parms[8].parse::<f64>().unwrap();

    println!(
        "Generating init_pop = {init_population}\n\
                total_blocks = {total_blocks}\n\
                block_inserts = {block_inserts}\n\
                block_updates = {block_updates}\n\
                block_deletes = {block_deletes}\n\
                skew = {skew}\n"
    );
    generate_query(
        query_file_name,
        init_population,
        total_blocks,
        block_inserts,
        block_updates,
        block_deletes,
        skew,
    );
    println!("Finished generate.")
}
pub(crate) fn main_append(parms: Vec<String>) {
    let query_file_name = parms[2].as_str();
    let total_blocks: usize = parms[4].parse().unwrap();
    let block_inserts: usize = parms[5].parse().unwrap();
    let block_updates: usize = parms[6].parse().unwrap();
    let block_deletes: usize = parms[7].parse().unwrap();

    println!(
        "Appending-Mode\n\
                total_blocks = {total_blocks}\n\
                block_inserts = {block_inserts}\n\
                block_updates = {block_updates}\n\
                block_deletes = {block_deletes}"
    );
    generate_query(
        query_file_name,
        0,
        total_blocks,
        block_inserts,
        block_updates,
        block_deletes,
        0f64,
    );
    println!("Finished generate.")
}

fn generate_query(
    query_file_name: &str,
    init_population: usize,
    total_blocks: usize,
    block_inserts: usize,
    block_updates: usize,
    block_deletes: usize,
    skew: f64,
) {
    let bat_tree = Arc::new(MVBT::default());

    let mut map = HashSet::with_capacity(init_population);

    let mut init_pop_order = Vec::with_capacity(init_population);

    for _ in 0..init_population {
        'l: loop {
            let key = rand::random_range(0..Key::MAX);
            if !map.contains(&key) {
                bat_tree.dispatch_crud(CRUDOperation::Insert(key, Payload::default()));
                map.insert(key);
                init_pop_order.push(CRUDOperation::Insert(key, Payload::default()));

                break 'l;
            }
        }
    }
    mem::drop(map);

    if init_population > 0 {
        let _nc = fs::remove_file(format!("{query_file_name}"));
    } else {
        load_query(query_file_name, bat_tree.clone(), None);
    }

    println!("Finished generating {} init keys", init_population);
    let mut query_file = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{query_file_name}"))
            .unwrap(),
    );

    let mut querys = 0_usize;

    let mut io_handle = |crud: CRUDOperation<Key, Payload>| {
        let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        match crud {
            CRUDOperation::Insert(key, ..) => {
                buff[0] = INSERT;
                buff[1..].copy_from_slice(key.to_le_bytes().as_slice());
            }
            CRUDOperation::Update(key, ..) => {
                buff[0] = UPDATE;
                buff[1..].copy_from_slice(key.to_le_bytes().as_slice());
            }
            CRUDOperation::Delete(key, ..) => {
                buff[0] = DELETE;
                buff[1..].copy_from_slice(key.to_le_bytes().as_slice());
            }
            _ => panic!("Unknown CRUD Operation for blocks"),
        }

        querys += 1;
        query_file.write_all(buff.as_slice()).unwrap()
    };

    init_pop_order.into_iter().for_each(|op| {
        // println!("Executing {}", op);
        io_handle(op)
    });

    let block = {
        let mut crud = Vec::with_capacity(block_inserts + block_updates + block_deletes);

        crud.extend((0..block_inserts).map(|_| CRUDOperation::<Key, Payload>::InsertRand));
        crud.extend((0..block_updates).map(|_| CRUDOperation::<Key, Payload>::UpdateRand));
        crud.extend((0..block_deletes).map(|_| CRUDOperation::<Key, Payload>::DeleteRand));
        crud
    };

    let zipf = Zipf::new(Key::MAX as f64, skew);
    let payload = Payload::default();

    let gen_block = || {
        let mut crud = block.clone();
        crud.shuffle(&mut rand::rng());

        if skew == 0_f64 {
            crud
        } else {
            let key = zipf.as_ref().unwrap().sample(&mut rand::rng()) as Key;
            crud.iter_mut().for_each(|c| match c {
                CRUDOperation::UpdateRand => *c = CRUDOperation::Update(key, payload),
                CRUDOperation::DeleteRand => *c = CRUDOperation::Delete(key),
                CRUDOperation::InsertRand => *c = CRUDOperation::Insert(key, payload),
                _ => panic!("Unknown CRUD Operation for blocks"),
            });
            crud
        }
    };

    for _ in 0..total_blocks {
        for op in gen_block() {
            match bat_tree.dispatch_crud(op.clone()) {
                CRUDOperationResult::InsertedRand(key, _) => {
                    io_handle(CRUDOperation::Insert(key, 0))
                }
                CRUDOperationResult::UpdatedRand(key, _) => {
                    io_handle(CRUDOperation::Update(key, 0))
                }
                CRUDOperationResult::DeletedRand(key, _) => {
                    io_handle(CRUDOperation::Delete::<_, Payload>(key))
                }
                CRUDOperationResult::Error => {
                    panic!("Error on rand query; generate_query(): CRUD({op}) ---> Result(Error)")
                }
                _ => io_handle(op),
            }
        }
    }

    query_file.flush().unwrap();
    if init_population > 0 {
        println!("Generated: {} CRUD Ops", format_insertions(querys))
    } else {
        let total_crud = query_file.into_inner().unwrap().metadata().unwrap().len() / 9;
        println!(
            "Appended: {} CRUD Ops. Total: {} CRUD Ops",
            format_insertions(querys),
            format_insertions(total_crud as _)
        )
    }
}

fn load_query_into_memory(query_file: &str) -> Vec<CRUDOperation<Key, Payload>> {
    let mut query_file = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(format!("{query_file}"))
            .unwrap(),
    );

    let payload = Payload::default();
    let mut loaded = vec![];

    loop {
        let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        match query_file.read_exact(buff.as_mut_slice()) {
            Ok(..) => match buff[0] {
                INSERT => {
                    let key = Key::from_le_bytes((&buff[1..]).try_into().unwrap());
                    let crud = CRUDOperation::Insert(key, payload);
                    loaded.push(crud);
                }
                UPDATE => {
                    let crud = CRUDOperation::Update(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()),
                        payload,
                    );

                    loaded.push(crud);
                }
                DELETE => {
                    let crud =
                        CRUDOperation::Delete(Key::from_le_bytes(buff[1..].try_into().unwrap()));

                    loaded.push(crud);
                }
                _ => panic!("Unknown CRUD Operation for blocks in load query into memory!"),
            },
            Err(..) => break,
        }
    }

    assert!(query_file.read_exact([0].as_mut_slice()).is_err());

    loaded
}
fn load_query(query_file: &str, index: Arc<MVBT>, report_signal: Option<Arc<AtomicU64>>) -> usize {
    let mut query_file = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(format!("{query_file}"))
            .unwrap(),
    );

    let mut query_count = 0;
    let payload = Payload::default();

    loop {
        let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        match query_file.read_exact(buff.as_mut_slice()) {
            Ok(..) => match buff[0] {
                INSERT => {
                    let key = Key::from_le_bytes((&buff[1..]).try_into().unwrap());
                    let crud = CRUDOperation::Insert(key, payload);

                    let r = index.dispatch_crud(crud);
                    if let CRUDOperationResult::Inserted(..) = r {
                        if let Some(ref sender) = report_signal {
                            sender.store(key, Ordering::Release);
                        }
                    } else {
                        panic!("Error loading query insert number = {}: {r}", query_count)
                    }
                }
                UPDATE => {
                    let crud = CRUDOperation::Update(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()),
                        payload,
                    );

                    let r = index.dispatch_crud(crud);
                    if let CRUDOperationResult::Updated(..) = r {
                    } else {
                        panic!("Error loading query update number = {}: {r}", query_count)
                    }
                }
                DELETE => {
                    let crud =
                        CRUDOperation::Delete(Key::from_le_bytes(buff[1..].try_into().unwrap()));

                    let r = index.dispatch_crud(crud);
                    if let CRUDOperationResult::Deleted(..) = r {
                    } else {
                        panic!("Error loading query delete number = {}: {r}", query_count)
                    }
                }
                _ => panic!("Unknown CRUD Operation for blocks in load query!"),
            },
            Err(..) => break,
        }

        query_count += 1
    }

    assert!(query_file.read_exact([0].as_mut_slice()).is_err());
    query_count
}

pub const PAYLOAD_STR_LEN_MIN: usize = 704;
pub const PAYLOAD_STR_LEN_MAX: usize = 7078;
pub const PAYLOAD_ATTR_STR_COUNT: usize = 67;

fn rnd_str(len_min: usize, len_max: usize) -> String {
    let len = rand::rng().random_range(len_min..=len_max);
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn rnd_str_vec(items: usize, str_len_min: usize, str_len_max: usize) -> Vec<String> {
    (0..items)
        .map(|i| rnd_str(str_len_min, str_len_max))
        .collect()
}
#[derive(Clone)]
pub struct PayloadIndirection(Box<PayloadData>);

#[derive(Clone)]
pub struct PayloadData {
    attributes: Vec<String>,
}

impl PayloadData {
    pub fn attr(&self, i: usize) -> &str {
        self.attributes.get(i).unwrap()
    }
}

impl Default for PayloadIndirection {
    fn default() -> Self {
        Self(Box::new(PayloadData {
            attributes: rnd_str_vec(
                PAYLOAD_ATTR_STR_COUNT,
                PAYLOAD_STR_LEN_MIN,
                PAYLOAD_STR_LEN_MAX,
            ),
        }))
    }
}

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}

pub fn format_insertions(mut i: usize) -> String {
    let mut parts = Vec::new();

    let units = [(1_000_000_000, "B"), (1_000_000, "Mio"), (1_000, "K")];

    for &(value, suffix) in &units {
        if i >= value {
            let count = i / value;
            parts.push(format!("{} {}", count, suffix));
            i %= value;
        }
    }

    if i > 0 {
        parts.push(i.to_string());
    }

    if parts.is_empty() {
        "0".to_string()
    } else {
        parts.join(" + ")
    }
}

/// Builds a small demo `MVBT` (plain `u64` keys/payloads) with enough
/// inserts to force a couple of root* splits, deletes a sub-range so leaf
/// pages show a realistic active/dead mix, then dumps its full root* list +
/// block graph via `bat_viz::dump::dump_tree_to_file` - a quick way to get a
/// file `tools/tree_visualizer.html` can load without wiring up a real
/// benchmark. Usage: `viz_demo [out.json] [num_keys] [max_depth]`.
#[cfg(feature = "tree-viz")]
pub(crate) fn main_viz_demo(parms: Vec<String>) {
    use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
    use crate::bat_crud_model::crud_operation::TxAtomicOperation;
    use crate::bat_crud_model::crud_operation_result::AtomicTxResult;
    use crate::bat_tree::mvbt::MVBT;
    use crate::bat_viz::dump::dump_tree_to_file;

    let out_path = parms.get(2).map(String::as_str).unwrap_or("tree_dump.json");
    let num_keys: u64 = parms.get(3).and_then(|s| s.parse().ok()).unwrap_or(15_000);
    let max_depth: Option<usize> = parms.get(4).and_then(|s| s.parse().ok());

    println!("Building demo tree: {num_keys} inserts (+ some deletes), dumping to '{out_path}'...");

    let tree = MVBT::default();

    for key in 0..num_keys {
        match tree.dispatch_atomic_transaction(TxAtomicOperation::Insert(key, key)) {
            AtomicTxResult::Inserted(..) => {}
            other => panic!("insert failed for key {key}: {other}"),
        }
    }

    for key in (0..num_keys / 4).step_by(7) {
        match tree.dispatch_atomic_transaction(TxAtomicOperation::Delete(key)) {
            AtomicTxResult::Deleted(..) => {}
            other => panic!("delete failed for key {key}: {other}"),
        }
    }

    dump_tree_to_file(&tree, out_path, max_depth).expect("failed to write tree dump");

    println!(
        "Wrote '{out_path}' - {} root* version(s). Open tools/tree_visualizer.html and load this file.",
        tree.count_roots()
    );
}

// Test files physically live in `tests/` (not `src/bat_test/`) so all of the
// project's tests are collected in one place; `#[path]` keeps them wired in
// as unit tests compiled into the bin crate, since none of this is reachable
// from a real `tests/` integration test without a `[lib]` target (see
// `tests/loom_registration_ordering.rs`'s doc for the one test that's a true,
// self-contained integration test). Each file's `[[test]]`-less status is
// enforced via `autotests = false` in `Cargo.toml`, so cargo doesn't also try
// to build these as their own standalone integration-test crates.
#[cfg(test)]
#[path = "../../tests/bench_s_htap_correctness_tests.rs"]
mod bench_s_htap_correctness_tests;
#[cfg(test)]
#[path = "../../tests/bench_s_htap_stress_tests.rs"]
mod bench_s_htap_stress_tests;
#[cfg(test)]
#[path = "../../tests/bench_tpcc_correctness_tests.rs"]
mod bench_tpcc_correctness_tests;
#[cfg(test)]
#[path = "../../tests/bench_tpcc_stress_tests.rs"]
mod bench_tpcc_stress_tests;
#[cfg(test)]
#[path = "../../tests/bench_tpcc_txn_tests.rs"]
mod bench_tpcc_txn_tests;
#[cfg(test)]
#[path = "../../tests/bench_tpcc_wal_codec_tests.rs"]
mod bench_tpcc_wal_codec_tests;
#[cfg(test)]
#[path = "../../tests/bench_tpch_correctness_tests.rs"]
mod bench_tpch_correctness_tests;
#[cfg(test)]
#[path = "../../tests/bench_tpch_stress_tests.rs"]
mod bench_tpch_stress_tests;
#[cfg(test)]
#[path = "../../tests/bench_wal_recovery_stress_tests.rs"]
mod bench_wal_recovery_stress_tests;
#[cfg(test)]
#[path = "../../tests/bench_ycsb_correctness_tests.rs"]
mod bench_ycsb_correctness_tests;
#[cfg(test)]
#[path = "../../tests/bench_ycsb_stress_tests.rs"]
mod bench_ycsb_stress_tests;
#[cfg(test)]
#[path = "../../tests/crud_persistence_tests.rs"]
mod crud_persistence_tests;
#[cfg(test)]
#[path = "../../tests/db_integration_tests.rs"]
mod db_integration_tests;
#[cfg(test)]
#[path = "../../tests/db_transaction_abort_tests.rs"]
mod db_transaction_abort_tests;
#[cfg(test)]
#[path = "../../tests/iter_query_tests.rs"]
mod iter_query_tests;
#[cfg(test)]
#[path = "../../tests/leaf_page_abort_tests.rs"]
mod leaf_page_abort_tests;
#[cfg(test)]
#[path = "../../tests/leaf_split_off_by_one_regression_tests.rs"]
mod leaf_split_off_by_one_regression_tests;
#[cfg(test)]
#[path = "../../tests/query_dispatch_tests.rs"]
mod query_dispatch_tests;
#[cfg(test)]
#[path = "../../tests/query_transaction_tests.rs"]
mod query_transaction_tests;
#[cfg(test)]
#[path = "../../tests/restart_trace_leak_repro.rs"]
mod restart_trace_leak_repro;
#[cfg(test)]
#[path = "../../tests/smo_race_investigation_tests.rs"]
mod smo_race_investigation_tests;
#[cfg(test)]
#[path = "../../tests/sync_commit_log_tests.rs"]
mod sync_commit_log_tests;
#[cfg(test)]
#[path = "../../tests/todays_optimization_regression_tests.rs"]
mod todays_optimization_regression_tests;
#[cfg(test)]
#[path = "../../tests/tpcc_wal_backend_bench.rs"]
mod tpcc_wal_backend_bench;
#[cfg(test)]
#[path = "../../tests/tpcc_wal_perf_tests.rs"]
mod tpcc_wal_perf_tests;
#[cfg(test)]
#[path = "../../tests/tree_wal_consistency_tests.rs"]
mod tree_wal_consistency_tests;
#[cfg(test)]
#[path = "../../tests/verify_concurrent_shared_keys.rs"]
mod verify_concurrent_shared_keys;
#[cfg(test)]
#[path = "../../tests/verify_range_scan.rs"]
mod verify_range_scan;
#[cfg(test)]
#[path = "../../tests/wal_integration_tests.rs"]
mod wal_integration_tests;
#[cfg(test)]
#[path = "../../tests/wal_lockfree_writer_tests.rs"]
mod wal_lockfree_writer_tests;
#[cfg(test)]
#[path = "../../tests/wal_record_tests.rs"]
mod wal_record_tests;
#[cfg(test)]
#[path = "../../tests/wal_recovery_tests.rs"]
mod wal_recovery_tests;
#[cfg(test)]
#[path = "../../tests/wal_writer_tests.rs"]
mod wal_writer_tests;
#[cfg(test)]
#[path = "../../tests/wal_writer_throughput_bench.rs"]
mod wal_writer_throughput_bench;
#[cfg(test)]
#[path = "../../tests/ycsb_autocommit_vs_txn_bench.rs"]
mod ycsb_autocommit_vs_txn_bench;
#[cfg(test)]
#[path = "../../tests/ycsb_wal_backend_bench.rs"]
mod ycsb_wal_backend_bench;
#[cfg(test)]
#[path = "../../tests/ycsb_wal_perf_tests.rs"]
mod ycsb_wal_perf_tests;
