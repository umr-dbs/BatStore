//! TPC-C schema for the BatStore benchmark harness.
//!
//! Each of the nine TPC-C tables (plus two maintained secondary indexes) gets
//! its own [`MVBTSt`] tree/index — a [`TpccDatabase`] is a thin, domain-named
//! wrapper around a [`crate::bat_db::Database`] holding all 14 as named
//! tables, sharing that database's one transactional core (`TxContext`) *and*
//! its one shared WAL. Sharing the transactional core — not sharing one
//! physical tree — is what lets a [`crate::bat_bench::tpcc_txn::TpccTxn`]
//! spanning several "tables" (e.g. NewOrder touching Warehouse/District/
//! Customer/Order/NewOrder/OrderLine/Stock) commit atomically and stay
//! snapshot-isolated as a whole, matching how the referenced benchmarks
//! (TPC-C + OLAP scans, e.g. Alhomssi & Leis, VLDB'23) treat a business
//! transaction as one unit — while giving each table an independently-sized,
//! independently-scanned index, closer to how a real storage engine
//! physically separates relations. Sharing the WAL means a `TpccTxn`
//! spanning several tables now logs exactly *one* Commit marker for the
//! whole transaction (see `crate::bat_db::DbTransaction::commit`'s doc),
//! instead of one marker per touched table.
//!
//! [`Table`] is a convenience enum over this database's 14 tables — it
//! carries no data of its own; `TpccDatabase` resolves it to a `TableId`
//! (the underlying `Database`'s actual, plain sequential per-table index)
//! once at construction, cached in `TpccDatabase::table_ids` and indexed by
//! `Table as usize` (safe: `Table`'s declaration order matches `Table::ALL`'s).
//!
//! Also carries CH-benCHmark's (Cole et al., "The Mixed Workload CH-benCHmark",
//! DBTest 2011) three TPC-H-derived dimension tables — SUPPLIER, NATION,
//! REGION — as three more tables on the same `TpccDatabase`, feeding the
//! analytical queries in `bat_bench::tpch_queries`.
//!
//! Key layout: since table selection is now "which table" (a [`Table`] value
//! resolving to a `TableId`), not "which key range", every key is just that
//! table's primary-key columns packed MSB-first (so a byte-ordered range scan
//! matches the natural column order, e.g. scanning all districts of a
//! warehouse or all order-lines of an order) — no table tag bits needed.
//!
//! Several row fields (addresses, `i_data`, `s_dist`, ...) are never read by
//! the 5 transaction profiles, same as in the real spec — they exist for
//! realistic row footprint (page density, leaf fan-out) rather than being
//! touched by transaction logic, so `dead_code` is silenced module-wide.
#![allow(dead_code)]

use crate::bat_crud_model::crud_api::AtomicTxDispatcher;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_db::Database;
use crate::bat_query::interval::Interval;
use crate::bat_root::index_root::RootIndexType;
use crate::bat_tree::mvbt::FAN_OUT;
use crate::bat_wal::backend::WalBackend;
use crate::bat_wal::record::TableId;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread;
use std::time::Duration;
use triomphe::Arc;

pub type TpccKey = u64;

/// A single table's tree. Reuses the base tree's `FAN_OUT` (internal-node
/// arrays only ever hold `Key`/`Version`/`BlockRef`, never `Payload` - see
/// `InternalPage` - so they're identically sized for every payload type as
/// long as `Key = u64`).
///
/// `NUM_RECORDS` also reuses the base tree's value rather than being
/// recomputed for `TpccRow`'s size: `PayloadSlot<Payload>` is always exactly
/// one `usize` (8B) - it
/// inlines `Payload` bitwise only when `Payload` is itself exactly
/// `usize`-sized/aligned (true for the base tree's `u64` payload), and
/// otherwise heap-boxes it behind that one word (true for `TpccRow`, an
/// enum far bigger than 8B). Either way, one SoA leaf slot is 32B: 8B in
/// the key region plus 24B in the parallel version/payload region. Thus the
/// primary 4KB allocation fits `NUM_RECORDS` (123) records regardless of
/// payload type. Its compact two-word validity bitmap is inline; only the
/// deliberately oversized experimental leaf variants allocate it separately.
pub const TPCC_FAN_OUT: usize = FAN_OUT;
pub const TPCC_NUM_RECORDS: usize = crate::bat_tree::mvbt::NUM_RECORDS;

/// Initial TPC-C orders are laid out monotonically over this historical interval instead
/// of all receiving the loader's current wall-clock timestamp. Besides being a more useful
/// analytical data set, ordering history by `o_id` makes `ORDER_LINE`'s delivery-date zone
/// map physically selective because neighboring leaves cover neighboring date intervals.
pub const INITIAL_ORDER_HISTORY_MILLIS: i64 = 30 * 24 * 60 * 60 * 1_000;

/// Fixed predicates from BenchBase's CH-benCHmark Q1/Q6 SQL. Timestamps are UTC Unix
/// milliseconds for 2007-01-02, 1999-01-01, and 2020-01-01 respectively.
pub const BENCHBASE_Q1_DELIVERY_AFTER_MILLIS: i64 = 1_167_696_000_000;
pub const BENCHBASE_Q6_DATE_LO_MILLIS: i64 = 915_148_800_000;
pub const BENCHBASE_Q6_DATE_HI_MILLIS: i64 = 1_577_836_800_000;
pub const BENCHBASE_Q6_QUANTITY_LO: u32 = 1;
pub const BENCHBASE_Q6_QUANTITY_HI: u32 = 100_000;

/// Timestamp for a zero-based initial order ordinal in a monotonically distributed history.
pub(crate) fn initial_order_timestamp(anchor_millis: i64, ordinal: u32, count: u32) -> i64 {
    let denominator = count.saturating_sub(1).max(1) as i128;
    let offset = INITIAL_ORDER_HISTORY_MILLIS as i128 * ordinal as i128 / denominator;
    anchor_millis
        .saturating_sub(INITIAL_ORDER_HISTORY_MILLIS)
        .saturating_add(offset as i64)
}

/// Selective HTAP date predicates over the history produced above. Q1 includes the older
/// half of initial orders; Q6 selects the quarter immediately before that cutoff. Both
/// bounds align with `o_id`-ordered leaf ranges, allowing the delivery-date zone map to
/// reject whole leaves instead of merely filtering records after visiting them.
pub(crate) fn htap_query_date_bounds(anchor_millis: i64) -> (i64, i64, i64) {
    let q1_cutoff = anchor_millis.saturating_sub(INITIAL_ORDER_HISTORY_MILLIS / 2);
    let q6_lo = anchor_millis.saturating_sub(INITIAL_ORDER_HISTORY_MILLIS * 3 / 4);
    (q1_cutoff, q6_lo, q1_cutoff)
}

pub type TpccTree = crate::bat_tree::mvbt::MVBTSt<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;
/// `TpccDatabase::enable_scan_pool`/`scan_pool`'s pool type — a
/// `bat_tree::scan_pool::ScanWorkerPool` fixed to `TpccTree`'s own type
/// parameters, so callers (`parallel_scan`) don't have to spell those out.
pub type TpccScanWorkerPool = crate::bat_tree::scan_pool::ScanWorkerPool<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;

/// Deliberately much larger than `TPCC_NUM_RECORDS`: Warehouse and District
/// are TPC-C's smallest tables by row count (one row per warehouse / ten per
/// warehouse) yet among its hottest by write volume (every New-Order hits
/// `district.next_o_id`, every Payment hits `warehouse.ytd`/`district.ytd`)
/// — in a multiversion tree, that combination means a leaf holding only a
/// handful of *logical* keys still accumulates a physical version chain
/// that crosses the base tree's leaf capacity every few dozen writes,
/// forcing a compaction that briefly write-locks the one page nearly every
/// concurrent transaction routes through. That capacity
/// (`overflow_records_count() == NUM_RECORDS`, see `bat_block::block_handle`)
/// is already the hard, physically-full ceiling — no slack left to tune
/// without more room to begin with (see `bat_tree::smo::unsafe_degree`/
/// `unsafe_degree_root`'s doc). Measured: under the default TPC-C config,
/// `district`+`warehouse` alone accounted for ~99.97% of all root-page
/// write-traversal restarts (`bat_test::ROOT_RESTARTS`/
/// `record_root_restart_for_table`'s per-table breakdown).
///
/// This isn't just a bump to `TPCC_NUM_RECORDS` itself: that would inflate
/// every one of the other 12 tables' leaves too, and undo the exact 4KB
/// page-alignment tuning `NUM_RECORDS` was chosen for (see its own doc) —
/// wasted for tables that were never the problem. Giving only these two
/// tables their own, much larger leaf capacity is a deliberate,
/// memory-for-contention trade-off that only makes sense because they're
/// this small and this hot; see `TreeClass`'s doc for how "which table"
/// resolves to which of the two tree types with no dynamic dispatch.
///
/// **Bigger capacity is not free**, though: `overflow_records_count() ==
/// NUM_RECORDS` is also *when a leaf's dead/superseded versions get
/// compacted away*, and `bat_query::iter_query::RangeQueryIter` scans a
/// leaf's *entire* physical record array (`LeafPage::as_records()`,
/// `len() == active + dead`) filtering live-vs-dead per record — so a
/// bigger `NUM_RECORDS` means more accumulated dead-version garbage sits in
/// the leaf between compactions, and every range scan over that table (the
/// default TPC-C run's `scan_delay_sweep`/`fresh_full_scan` OLAP modes both
/// scan `warehouse`/`district`) pays for wading through it. Measured on the
/// same default config: OLAP scan throughput dropped from ~100% of baseline
/// at the untouched 123-record size down to ~21% at a 512KiB leaf, while
/// root-restart reduction only improved from ~4x to ~32x over that same
/// range — steeply diminishing returns. There's no way to decouple "avoid
/// frequent compactions" from "keep dead-version garbage bounded" within
/// this design: they're the same operation. `BigTreeSize` exists to make
/// that trade-off an explicit, chosen point instead of a hand-picked
/// number.
///
/// Each `KiB8`..`KiB512` variant's `NUM_RECORDS` is chosen the same way the
/// base tree's 123 was (see that constant's doc): empirically, so
/// `OptCell<Block<..>>` (`Block` plus its 8B `cell_version`) lands exactly
/// on a page-size multiple with zero waste, rather than spilling into the
/// next allocator size class the way a round number (256, 1024, ...) would
/// (confirmed: `size_of::<OptCell<Block<TPCC_FAN_OUT, 256, ..>>>()` is
/// 8384B, not 8192B — it *overshoots* the boundary it looks like it should
/// hit).
///
/// `KiB1`..`KiB4` go the other direction — smaller than the untouched
/// 123/123 base tree, down toward the contention/height trade-off's other
/// extreme. They can't reuse `TPCC_FAN_OUT` the way `KiB8`..`KiB512` do:
/// `Block`'s size is `header(64B) + max(internal_size, leaf_size)` (a real
/// Rust `union` of `InternalPage`/`LeafPage`, see `bat_page_model::node`),
/// and `internal_size` is driven by `FAN_OUT`, not `NUM_RECORDS` — at
/// `FAN_OUT = TPCC_FAN_OUT = 123`, `internal_size` alone is already 3944B,
/// which floors `Block` at 4032B no matter how small `NUM_RECORDS` gets.
/// Shrinking the leaf below 4KiB therefore means shrinking the internal
/// fan-out right along with it — each of these three variants uses a
/// symmetric `FAN_OUT = NUM_RECORDS` (the same shape the 123/123 base tree
/// already has), which is also the zero-waste choice: `InternalPage`'s and
/// `LeafPage`'s per-entry cost are both exactly 32B (see `RecordPoint`'s and
/// `Interval`/`Version`/`BlockRef`'s sizes), so `internal_size(N) ==
/// leaf_size(N)` and neither arm of the union wastes space against the
/// other.
///
/// **There is no variant below `KiB1` (`N=27`).** Every split-push
/// into an internal node *appends* 2 fresh entries rather than overwriting
/// (`bat_tree::smo`'s `on_overflow_node`) — the superseded entry just goes
/// dead until GC compacts it away — so a freshly-split node (2 live
/// children) needs `FAN_OUT >= 4` just to have room for a single further
/// push, and each subsequent push costs 2 more raw slots against a total
/// budget of only `FAN_OUT` (`overflow_units_count() == FAN_OUT - 1`, see
/// `bat_block::block_handle`). At `FAN_OUT = 3` that margin is exactly zero:
/// a fresh root already sits at the overflow threshold, permanently, before
/// it can accept even one push — confirmed as a genuine, concurrency-
/// independent deadlock, not mere slowness (traced through `smo.rs`'s
/// `lacks_room_for_split_entries`/`on_overflow_node`/`unsafe_degree_root`).
/// Larger-but-still-small `FAN_OUT` compiles and is not *provably*
/// deadlocked the same way, but empirically it degrades from "fine" to
/// "indistinguishable from hung" once concurrency rises: the *active*-vs-
/// *total* split-strategy decision only grows the "real" entry count by 1
/// per push while burning 2 raw slots, so a small root is an extremely hot,
/// nearly-saturated bottleneck that every concurrent writer funnels
/// through. Measured directly: `FAN_OUT` = 5, 7, 9, *and 11* all ran cleanly
/// at 1 warehouse/2 terminals but never completed (150s+ wall time for a
/// 20s run, <15s of actual CPU work across all threads — a genuine stall,
/// not slow-but-live progress) at just 2 warehouses/4 terminals. `FAN_OUT =
/// 27` (`KiB1`) was the smallest value that held up at that same load
/// *and* at 4 warehouses/8 terminals — still treat it as more contention-
/// sensitive than `KiB2`/`KiB4`, not as unconditionally safe at
/// arbitrary concurrency; nothing here was tested past 8 terminals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BigTreeSize {
    /// 27 records/leaf and 27 keys/internal-node, exactly 1KiB — the
    /// smallest variant confirmed to hold up under concurrent load; see
    /// this enum's own doc for why nothing smaller is offered.
    KiB1,
    /// 59 records/leaf and 59 keys/internal-node, exactly 2KiB.
    KiB2,
    /// 123 records/leaf and 123 keys/internal-node, exactly 4KiB — the same
    /// shape as the untouched base/standard-table tree (`TPCC_FAN_OUT`/
    /// `TPCC_NUM_RECORDS`), just built as its own separate `TreeClass::Big`
    /// instance. The "no augmentation at all" reference point below `KiB8`.
    KiB4,
    /// 251 records/leaf, exactly 8KiB (`FAN_OUT` stays `TPCC_FAN_OUT`, only
    /// the leaf grows). Cheapest of the "grow the leaf" variants, but only
    /// trims district/warehouse root restarts ~1.2-1.3x over the untouched
    /// 123-record leaf — barely more than "off," kept as a reference point.
    KiB8,
    /// 507 records/leaf, exactly 16KiB. ~2x/~1.6x root-restart reduction.
    KiB16,
    /// 1019 records/leaf, exactly 32KiB. The measured sweet spot: OLAP scan
    /// throughput fully recovered (~100% of the untouched-leaf baseline)
    /// while district/warehouse root restarts are still down ~4.4x/~2.6x.
    #[default]
    KiB32,
    /// 2043 records/leaf, exactly 64KiB. ~8x/~4.6x root-restart reduction,
    /// at a real but moderate OLAP scan cost (~82% of baseline).
    KiB64,
    /// 16379 records/leaf, exactly 512KiB. Maximum measured contention
    /// reduction (~32x/~18x), at a steep OLAP scan cost (~21% of baseline
    /// — see this constant group's own doc for the mechanism).
    KiB512,
}

/// `FAN_OUT`/`NUM_RECORDS` for the sub-4KiB variants — symmetric (see
/// `BigTreeSize`'s doc for why), so one constant per variant serves both
/// generic parameters.
pub const TPCC_BIG_KIB1_N: usize = 27;
pub const TPCC_BIG_KIB2_N: usize = 59;
pub const TPCC_BIG_KIB4_N: usize = 123;

pub const TPCC_BIG_KIB8_NUM_RECORDS: usize = 251;
pub const TPCC_BIG_KIB16_NUM_RECORDS: usize = 507;
pub const TPCC_BIG_KIB32_NUM_RECORDS: usize = 1019;
pub const TPCC_BIG_KIB64_NUM_RECORDS: usize = 2043;
pub const TPCC_BIG_KIB512_NUM_RECORDS: usize = 16379;

/// One concrete tree type per `BigTreeSize` variant — see that enum's doc.
/// Built via `MVBTSt::make_with_shared_ctx` sharing the very same
/// `Arc<TxContext>` as `TpccDatabase::db` (see `TpccDatabase::make_big_trees`)
/// — only the physical leaf capacity differs from `TpccTree` (and, for
/// `KiB1`..`KiB4`, the internal fan-out too — see `BigTreeSize`'s doc), not
/// the transactional core, so a `TpccTxn` spanning both a standard and a big
/// table still commits/aborts as one atomic, snapshot-isolated unit.
pub type TpccBigTreeKiB1 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_BIG_KIB1_N, TPCC_BIG_KIB1_N, TpccKey, TpccRow>;
pub type TpccBigTreeKiB2 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_BIG_KIB2_N, TPCC_BIG_KIB2_N, TpccKey, TpccRow>;
pub type TpccBigTreeKiB4 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_BIG_KIB4_N, TPCC_BIG_KIB4_N, TpccKey, TpccRow>;
pub type TpccBigTreeKiB8 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_FAN_OUT, TPCC_BIG_KIB8_NUM_RECORDS, TpccKey, TpccRow>;
pub type TpccBigTreeKiB16 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_FAN_OUT, TPCC_BIG_KIB16_NUM_RECORDS, TpccKey, TpccRow>;
pub type TpccBigTreeKiB32 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_FAN_OUT, TPCC_BIG_KIB32_NUM_RECORDS, TpccKey, TpccRow>;
pub type TpccBigTreeKiB64 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_FAN_OUT, TPCC_BIG_KIB64_NUM_RECORDS, TpccKey, TpccRow>;
pub type TpccBigTreeKiB512 =
    crate::bat_tree::mvbt::MVBTSt<TPCC_FAN_OUT, TPCC_BIG_KIB512_NUM_RECORDS, TpccKey, TpccRow>;

/// The two `TreeClass::Big` trees actually built for one `TpccDatabase`,
/// at whichever `BigTreeSize` it was constructed with. A closed enum over
/// concrete monomorphizations — like `RootIndexType` already is for root
/// indexing — not a `dyn Trait`: `NUM_RECORDS` is a `const` generic and
/// must be fixed at compile time, so "choose a size at runtime" means
/// "choose among a handful of pre-compiled candidates," not "pick an
/// arbitrary integer." See `TpccDatabase::dispatch_big` for how an
/// operation reaches the right variant's trees without an 8-way match at
/// every call site.
pub(crate) enum BigTrees {
    KiB1 {
        warehouse: Arc<TpccBigTreeKiB1>,
        district: Arc<TpccBigTreeKiB1>,
    },
    KiB2 {
        warehouse: Arc<TpccBigTreeKiB2>,
        district: Arc<TpccBigTreeKiB2>,
    },
    KiB4 {
        warehouse: Arc<TpccBigTreeKiB4>,
        district: Arc<TpccBigTreeKiB4>,
    },
    KiB8 {
        warehouse: Arc<TpccBigTreeKiB8>,
        district: Arc<TpccBigTreeKiB8>,
    },
    KiB16 {
        warehouse: Arc<TpccBigTreeKiB16>,
        district: Arc<TpccBigTreeKiB16>,
    },
    KiB32 {
        warehouse: Arc<TpccBigTreeKiB32>,
        district: Arc<TpccBigTreeKiB32>,
    },
    KiB64 {
        warehouse: Arc<TpccBigTreeKiB64>,
        district: Arc<TpccBigTreeKiB64>,
    },
    KiB512 {
        warehouse: Arc<TpccBigTreeKiB512>,
        district: Arc<TpccBigTreeKiB512>,
    },
}

/// Picks `warehouse` or `district` out of one `BigTrees` arm — generic over
/// the arm's own concrete tree type, so this one function serves all 5
/// variants. Panics for any other `Table`; only ever called from a
/// `TreeClass::Big`-guarded path.
fn pick_big<'x, T>(table: Table, warehouse: &'x Arc<T>, district: &'x Arc<T>) -> &'x T {
    match table {
        Table::Warehouse => warehouse,
        Table::District => district,
        _ => unreachable!("pick_big: {table:?} is not a TreeClass::Big table"),
    }
}

/// Spawns `TpccDatabase::set_vacuum`'s big-tree sweep thread,
/// generic over whichever concrete `FAN_OUT`/`NUM_RECORDS` the caller's
/// `BigTrees` arm resolved to — one function serves all 8 variants, same
/// rationale as `pick_big`. Loops `warehouse`/`district` directly (not
/// `Table::ALL`/`dispatch_big`'s dynamic table lookup) since there are only
/// ever exactly these two.
fn spawn_big_idle_compaction_thread<const FAN_OUT: usize, const NUM_RECORDS: usize>(
    warehouse: &Arc<crate::bat_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>>,
    district: &Arc<crate::bat_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>>,
    dead_ratio_threshold: f64,
    sweep_interval: Duration,
    stop: std::sync::Arc<AtomicBool>,
) {
    let warehouse = warehouse.clone();
    let district = district.clone();
    thread::spawn(move || {
        crate::bat_db::database::lower_current_thread_priority();
        while !stop.load(Relaxed) {
            warehouse.compact_idle_pass(dead_ratio_threshold);
            if stop.load(Relaxed) {
                break;
            }
            district.compact_idle_pass(dead_ratio_threshold);
            thread::sleep(sweep_interval);
        }
    });
}

/// One `TreeClass::Big`-only operation, generic over whichever concrete
/// `FAN_OUT`/`NUM_RECORDS` `TpccDatabase::dispatch_big` resolves it against —
/// see that method's doc. Implementors are small, single-use structs holding
/// an operation's parameters (e.g. `TpccTxn`'s `InsertOp { worker_id,
/// ts_start, key, payload }`), each implementing `run` by calling straight
/// into the same `bat_db::transaction::{insert_on_tree, ..}` free functions
/// `DbTransaction` itself uses.
///
/// Generic over `FAN_OUT` too (not just `NUM_RECORDS`, as before the
/// sub-4KiB `BigTreeSize` variants existed): those variants shrink the
/// internal-node fan-out along with leaf capacity — see `BigTreeSize`'s
/// doc — so they no longer share `TPCC_FAN_OUT` with the KiB8..KiB512
/// variants above them.
pub(crate) trait BigTreeOp {
    type Output;
    fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(
        self,
        tree: &crate::bat_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>,
    ) -> Self::Output;
}

/// Re-wraps a `CRUDOperationResult` produced against a `TreeClass::Big`
/// tree (a different `NUM_RECORDS` than the database's standard tables)
/// under the standard tree's own const generics, so callers that need to
/// return/compare results across both classes (`TpccTxn`'s methods,
/// `dispatch_crud_big` below) see one uniform type. Sound because nothing
/// that goes through `BigTreeOp`/`dispatch_big` ever produces
/// `MatchedRecordIter` — the one variant that actually carries
/// `NUM_RECORDS`/`FAN_OUT`-shaped data (a live, zero-copy `RangeQueryIter`
/// borrowing from the tree's own blocks — see `bat_db::transaction`'s module
/// doc: "range is always eager"). Every other variant carries no such data,
/// so re-wrapping it under different const generics changes nothing about
/// its actual content.
pub(crate) fn normalize<'a, const FAN_OUT_FROM: usize, const NUM_RECORDS_FROM: usize>(
    r: CRUDOperationResult<'a, FAN_OUT_FROM, NUM_RECORDS_FROM, TpccKey, TpccRow>,
) -> CRUDOperationResult<'static, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow> {
    match r {
        CRUDOperationResult::MatchedRecords(v) => CRUDOperationResult::MatchedRecords(v),
        CRUDOperationResult::Inserted(v) => CRUDOperationResult::Inserted(v),
        CRUDOperationResult::Updated(v) => CRUDOperationResult::Updated(v),
        CRUDOperationResult::Deleted(v) => CRUDOperationResult::Deleted(v),
        CRUDOperationResult::InsertedRand(k, v) => CRUDOperationResult::InsertedRand(k, v),
        CRUDOperationResult::UpdatedRand(k, v) => CRUDOperationResult::UpdatedRand(k, v),
        CRUDOperationResult::DeletedRand(k, v) => CRUDOperationResult::DeletedRand(k, v),
        CRUDOperationResult::ZeroAffected(reason) => CRUDOperationResult::ZeroAffected(reason),
        CRUDOperationResult::Conflict => CRUDOperationResult::Conflict,
        CRUDOperationResult::Error => CRUDOperationResult::Error,
        CRUDOperationResult::MatchedRecordIter(_) => unreachable!(
            "tpcc_schema::normalize: size-class dispatch never produces a lazy MatchedRecordIter (range is always eager)"
        ),
    }
}

/// `MVBTSt::dispatch_crud`'s counterpart for `TreeClass::Big` tables — a
/// single-op, auto-committing convenience for callers that don't need a
/// full `TpccTxn` (population via `bat_bench::tpcc_load`, tests seeding a
/// row directly). `bat_bench::tpcc_txn`'s own business-transaction logic
/// never calls this — it always dispatches through `TpccDatabase::dispatch_big`
/// via a purpose-built `BigTreeOp` impl instead, since a business
/// transaction needs write-tracking/abort semantics this convenience
/// doesn't provide.
pub fn dispatch_crud_big(
    db: &TpccDatabase,
    table: Table,
    op: crate::bat_crud_model::crud_operation::CRUDOperation<TpccKey, TpccRow>,
) -> CRUDOperationResult<'static, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow> {
    struct DispatchCrudOp(crate::bat_crud_model::crud_operation::CRUDOperation<TpccKey, TpccRow>);
    impl BigTreeOp for DispatchCrudOp {
        type Output =
            CRUDOperationResult<'static, TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>;
        fn run<const FAN_OUT: usize, const NUM_RECORDS: usize>(
            self,
            tree: &crate::bat_tree::mvbt::MVBTSt<FAN_OUT, NUM_RECORDS, TpccKey, TpccRow>,
        ) -> Self::Output {
            normalize(tree.dispatch_crud(self.0))
        }
    }
    db.dispatch_big(table, DispatchCrudOp(op))
}

/// WAL table-id tags for the two `TreeClass::Big` trees. They live outside
/// `Database`'s own table list (see `TreeClass`'s doc), so they need their
/// own reserved slice of the one shared log file's `TableId` tag space,
/// disjoint from whatever `Database::create_table` assigns its own 12
/// standard tables (0..12, one per non-`Big` `Table` variant, in
/// `Table::ALL` order). Continuing right after that range keeps the whole
/// tag space simple and non-overlapping. Independent of `BigTreeSize`: the
/// tag identifies *which table* a WAL entry belongs to, not which size its
/// tree happened to be built at (recovery can freely replay into a
/// differently-sized tree than the one that originally logged the entry —
/// `NUM_RECORDS` never appears in the wire format).
const WAREHOUSE_BIG_TABLE_ID: TableId = 12;
const DISTRICT_BIG_TABLE_ID: TableId = 13;

/// Which physical tree type a `Table` resolves to — see `BigTreeSize`'s
/// doc for why `Warehouse`/`District` specifically need `Big`. `TpccTxn`
/// matches on this (a plain enum tag, not a `dyn Trait` — no
/// vtable/virtual dispatch, and since a given `Table` always resolves to
/// the same variant for the life of the program, branch prediction settles
/// almost immediately) to route each operation to the right concrete tree,
/// reusing the exact same `bat_db::transaction::{insert_on_tree,
/// update_on_tree, ...}` free functions `DbTransaction` itself is built
/// on, just instantiated at a different `NUM_RECORDS` — so `Big`-class
/// tables get identical self-overwrite/abort/WAL semantics, not a
/// hand-rolled parallel implementation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TreeClass {
    Standard,
    Big,
}

/// Database scale/shape — the TPC-C spec's standard cardinalities, with
/// warehouse count and a few sizes made configurable for quicker benchmark
/// runs than a full audited scale factor would take to load.
#[derive(Clone, Copy, Debug)]
pub struct TpccConfig {
    pub num_warehouses: u32,
    pub districts_per_warehouse: u8,
    pub customers_per_district: u32,
    pub num_items: u32,
    pub initial_orders_per_district: u32,
    /// The highest-`o_id` slice of `initial_orders_per_district` that gets a
    /// NEW_ORDER row at load time (spec: last 900 of 3,000).
    pub initial_new_orders: u32,
    /// CH-benCHmark's SUPPLIER table size (see `bat_bench::tpch_queries`
    /// module docs): fixed independent of warehouse count, matching
    /// CH-benCHmark's own choice of a TPC-H SF1-sized (10,000-row) supplier
    /// pool regardless of scale factor.
    pub num_suppliers: u32,
}

impl Default for TpccConfig {
    fn default() -> Self {
        Self {
            num_warehouses: 4,
            districts_per_warehouse: 10,
            customers_per_district: 3_000,
            num_items: 100_000,
            initial_orders_per_district: 3_000,
            initial_new_orders: 900,
            num_suppliers: 10_000,
        }
    }
}

#[cfg(test)]
mod htap_date_tests {
    use super::*;

    #[test]
    fn initial_order_dates_span_history_monotonically() {
        let anchor = 10 * INITIAL_ORDER_HISTORY_MILLIS;
        let dates: Vec<_> = (0..5)
            .map(|ordinal| initial_order_timestamp(anchor, ordinal, 5))
            .collect();
        assert_eq!(dates[0], anchor - INITIAL_ORDER_HISTORY_MILLIS);
        assert_eq!(dates[4], anchor);
        assert!(dates.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn htap_bounds_select_distinct_history_slices() {
        let anchor = 10 * INITIAL_ORDER_HISTORY_MILLIS;
        let (q1_cutoff, q6_lo, q6_hi) = htap_query_date_bounds(anchor);
        assert_eq!(q1_cutoff, anchor - INITIAL_ORDER_HISTORY_MILLIS / 2);
        assert_eq!(q6_lo, anchor - INITIAL_ORDER_HISTORY_MILLIS * 3 / 4);
        assert_eq!(q6_hi, q1_cutoff);
        assert!(q6_lo < q6_hi);
    }
}

/// Selects one of `TpccDatabase`'s 14 tables/trees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Table {
    Warehouse,
    District,
    Customer,
    /// A maintained secondary index standing in for the ORDER table's real
    /// secondary index on (o_w_id,o_d_id,o_c_id,o_id), so OrderStatus can
    /// find "the customer's most recent order" in O(1) instead of a
    /// descending scan.
    CustLastOrder,
    /// (w_id, d_id, last_code, first_code, c_id) -> presence marker; the
    /// "by last name" customer lookup Payment/OrderStatus need.
    CustomerNameIdx,
    History,
    NewOrder,
    Orders,
    OrderLine,
    Item,
    Stock,
    /// CH-benCHmark's TPC-H-derived dimension tables (`bat_bench::tpch_queries`
    /// module docs): SUPPLIER links to STOCK via `Stock::s_su_suppkey`,
    /// NATION/REGION are the standard fixed TPC-H reference tables.
    Supplier,
    Nation,
    Region,
}

impl Table {
    pub const ALL: [Table; 14] = [
        Table::Warehouse,
        Table::District,
        Table::Customer,
        Table::CustLastOrder,
        Table::CustomerNameIdx,
        Table::History,
        Table::NewOrder,
        Table::Orders,
        Table::OrderLine,
        Table::Item,
        Table::Stock,
        Table::Supplier,
        Table::Nation,
        Table::Region,
    ];

    /// Lowercase name — this table's actual identity, used as the
    /// `bat_db::Database::create_table` argument `TpccDatabase` resolves
    /// every `Table` variant to a `TableId` through, and for diagnostics.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Table::Warehouse => "warehouse",
            Table::District => "district",
            Table::Customer => "customer",
            Table::CustLastOrder => "cust_last_order",
            Table::CustomerNameIdx => "customer_name_idx",
            Table::History => "history",
            Table::NewOrder => "new_order",
            Table::Orders => "orders",
            Table::OrderLine => "order_line",
            Table::Item => "item",
            Table::Stock => "stock",
            Table::Supplier => "supplier",
            Table::Nation => "nation",
            Table::Region => "region",
        }
    }

    /// See `TreeClass`'s doc.
    #[inline(always)]
    pub(crate) const fn class(self) -> TreeClass {
        match self {
            Table::Warehouse | Table::District => TreeClass::Big,
            _ => TreeClass::Standard,
        }
    }
}

/// All 14 TPC-C/CH-benCHmark tables — a thin, domain-named wrapper over a
/// [`crate::bat_db::Database`], see this module's doc.
pub struct TpccDatabase {
    pub(crate) db: Database<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>,
    /// `Table -> TableId`, resolved once at construction and indexed by
    /// `Table as usize` — see `Table`'s doc. Meaningless (left `0`, never
    /// read) for `Table::Warehouse`/`Table::District`: those two resolve
    /// through `big_trees` instead, entirely outside `db`'s own table list
    /// — see `TreeClass`'s doc.
    pub(crate) table_ids: [TableId; 14],
    /// `Table::Warehouse`/`Table::District`'s trees, at whichever
    /// `BigTreeSize` this database was built with — see `BigTrees`'s doc.
    pub(crate) big_trees: BigTrees,
    /// The currently-running big-tree idle-compaction sweep thread's stop
    /// flag (see `set_vacuum`), or `None` if it's off. `db`'s
    /// own `set_vacuum` (see
    /// `bat_db::Database`) already covers every *standard* table; this
    /// covers `Table::Warehouse`/`Table::District` specifically, since
    /// `big_trees` lives outside `db`'s table list entirely and needs its
    /// own sweep thread. A plain `Mutex`, not `db`'s lock-free `ArcSwap`
    /// pattern: enabling/disabling this is a rare, database-init-adjacent
    /// call, never on any hot path, so there's nothing to gain from
    /// lock-freedom here.
    idle_compaction_stop: Mutex<Option<std::sync::Arc<AtomicBool>>>,
}

fn inc_key(k: TpccKey) -> TpccKey {
    k.checked_add(1).unwrap_or(TpccKey::MAX)
}
fn dec_key(k: TpccKey) -> TpccKey {
    k.checked_sub(1).unwrap_or(TpccKey::MIN)
}

impl TpccDatabase {
    /// Same as `new_with_big_tree_size`, at `BigTreeSize::default()`
    /// (`KiB32` — the measured sweet spot, see that enum's doc). Kept as
    /// the default constructor so every pre-existing caller (tests, the
    /// TPC-C driver's own default path) keeps working unchanged.
    pub fn new(root_index_type: RootIndexType) -> Self {
        Self::new_with_big_tree_size(root_index_type, BigTreeSize::default())
    }

    pub fn new_with_wal(
        root_index_type: RootIndexType,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
    ) -> std::io::Result<Self> {
        Self::new_with_big_tree_size_and_max_workers_and_wal(
            root_index_type,
            BigTreeSize::default(),
            crate::bat_tree::mvbt::default_max_workers(),
            wal_path,
            flush_interval,
            None,
        )
    }

    pub fn new_with_big_tree_size(
        root_index_type: RootIndexType,
        big_tree_size: BigTreeSize,
    ) -> Self {
        Self::new_with_big_tree_size_and_max_workers(
            root_index_type,
            big_tree_size,
            crate::bat_tree::mvbt::default_max_workers(),
        )
    }

    pub fn new_with_big_tree_size_and_max_workers(
        root_index_type: RootIndexType,
        big_tree_size: BigTreeSize,
        max_workers: usize,
    ) -> Self {
        let db = Database::new_with_max_workers(
            root_index_type,
            inc_key,
            dec_key,
            TpccKey::MIN,
            TpccKey::MAX,
            max_workers,
        );
        let table_ids = Self::create_all_tables(&db);
        let big_trees = Self::make_big_trees(root_index_type, &db, big_tree_size, None);
        Self {
            db,
            table_ids,
            big_trees,
            idle_compaction_stop: Mutex::new(None),
        }
    }

    pub fn new_with_big_tree_size_and_max_workers_and_wal(
        root_index_type: RootIndexType,
        big_tree_size: BigTreeSize,
        max_workers: usize,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
        lockfree_batch_size: Option<usize>,
    ) -> std::io::Result<Self> {
        let db = match lockfree_batch_size {
            Some(batch_size) => Database::new_with_max_workers_and_wal_lockfree(
                root_index_type,
                inc_key,
                dec_key,
                TpccKey::MIN,
                TpccKey::MAX,
                max_workers,
                wal_path,
                flush_interval,
                batch_size,
            )?,
            None => Database::new_with_max_workers_and_wal(
                root_index_type,
                inc_key,
                dec_key,
                TpccKey::MIN,
                TpccKey::MAX,
                max_workers,
                wal_path,
                flush_interval,
            )?,
        };
        let table_ids = Self::create_all_tables(&db);
        let big_trees = Self::make_big_trees(root_index_type, &db, big_tree_size, None);
        Ok(Self {
            db,
            table_ids,
            big_trees,
            idle_compaction_stop: Mutex::new(None),
        })
    }

    /// Creates every one of the 12 *standard-class* tables (see
    /// `TreeClass`), in `Table::ALL`'s fixed order — or, for a database
    /// recovered from an already-populated catalog, simply looks each one up
    /// (`Database::create_table` is idempotent by name, see its doc) — and
    /// returns the resulting `Table -> TableId` cache. Shared by `new`
    /// (always actually creates) and `open_recovered` (recreates from the
    /// catalog `Database::open_recovered` already read; this loop is then a
    /// no-op lookup for every name already present, or a real create for a
    /// genuinely fresh — no prior WAL — database). `Warehouse`/`District`
    /// are deliberately skipped here — `db`'s own catalog only ever needs to
    /// know about its own 12 tables; the big trees are built separately by
    /// `make_big_trees` and never touch `db.create_table` at all, so their
    /// existence can't shift any standard table's `TableId` regardless of
    /// which order this loop visits `Table::ALL` in.
    fn create_all_tables(
        db: &Database<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>,
    ) -> [TableId; 14] {
        let mut table_ids = [0 as TableId; 14];
        for t in Table::ALL {
            if t.class() == TreeClass::Standard {
                let tree = db.create_table(t.as_str());
                if t == Table::OrderLine {
                    tree.set_zone_map_projection(order_line_delivery_d_zone_map_projection);
                }
                table_ids[t as usize] = tree
                    .table_id()
                    .expect("bat_db::Database::create_table always assigns its new table a TableId");
            }
        }
        table_ids
    }

    /// Builds the two `TreeClass::Big` trees at `size`, sharing `db`'s own
    /// `Arc<TxContext>` (see `BigTreeSize`'s doc) so a `TpccTxn` spanning a
    /// big and a standard table still commits/aborts atomically. Called by
    /// both `new_with_big_tree_size` (fresh) and `open_recovered_with_big_tree_size`
    /// (recovery then replays into the result separately — see
    /// `TreeClass`'s doc — since these two trees aren't part of `db`'s own
    /// catalog for `Database::open_recovered` to have already
    /// recreated/replayed).
    fn make_big_trees(
        root_index_type: RootIndexType,
        db: &Database<TPCC_FAN_OUT, TPCC_NUM_RECORDS, TpccKey, TpccRow>,
        size: BigTreeSize,
        wal_override: Option<Arc<WalBackend<TpccKey, TpccRow>>>,
    ) -> BigTrees {
        let ctx = db.ctx.clone();
        let wal = wal_override.unwrap_or_else(|| db.wal_writer());
        macro_rules! build {
            ($ty:ty) => {
                (
                    Arc::new(<$ty>::make_with_shared_ctx(
                        root_index_type,
                        inc_key,
                        dec_key,
                        TpccKey::MIN,
                        TpccKey::MAX,
                        ctx.clone(),
                        Some(WAREHOUSE_BIG_TABLE_ID),
                        wal.clone(),
                    )),
                    Arc::new(<$ty>::make_with_shared_ctx(
                        root_index_type,
                        inc_key,
                        dec_key,
                        TpccKey::MIN,
                        TpccKey::MAX,
                        ctx,
                        Some(DISTRICT_BIG_TABLE_ID),
                        wal.clone(),
                    )),
                )
            };
        }
        match size {
            BigTreeSize::KiB1 => {
                let (warehouse, district) = build!(TpccBigTreeKiB1);
                BigTrees::KiB1 {
                    warehouse,
                    district,
                }
            }
            BigTreeSize::KiB2 => {
                let (warehouse, district) = build!(TpccBigTreeKiB2);
                BigTrees::KiB2 {
                    warehouse,
                    district,
                }
            }
            BigTreeSize::KiB4 => {
                let (warehouse, district) = build!(TpccBigTreeKiB4);
                BigTrees::KiB4 {
                    warehouse,
                    district,
                }
            }
            BigTreeSize::KiB8 => {
                let (warehouse, district) = build!(TpccBigTreeKiB8);
                BigTrees::KiB8 {
                    warehouse,
                    district,
                }
            }
            BigTreeSize::KiB16 => {
                let (warehouse, district) = build!(TpccBigTreeKiB16);
                BigTrees::KiB16 {
                    warehouse,
                    district,
                }
            }
            BigTreeSize::KiB32 => {
                let (warehouse, district) = build!(TpccBigTreeKiB32);
                BigTrees::KiB32 {
                    warehouse,
                    district,
                }
            }
            BigTreeSize::KiB64 => {
                let (warehouse, district) = build!(TpccBigTreeKiB64);
                BigTrees::KiB64 {
                    warehouse,
                    district,
                }
            }
            BigTreeSize::KiB512 => {
                let (warehouse, district) = build!(TpccBigTreeKiB512);
                BigTrees::KiB512 {
                    warehouse,
                    district,
                }
            }
        }
    }

    /// Runs a `TreeClass::Big`-only operation against whichever concrete
    /// tree `table` resolves to, for whatever `BigTreeSize` this database
    /// was built with — the one place that matches on `BigTrees`'s variant,
    /// so `TpccTxn`'s operations don't each repeat a 5-way match. `op.run`
    /// is generic over `NUM_RECORDS`, monomorphized once per variant at
    /// compile time — still no dynamic dispatch, just one static dispatch
    /// site instead of many. Panics (via `pick_big`) if `table` isn't
    /// `Table::Warehouse`/`Table::District`.
    pub(crate) fn dispatch_big<Op: BigTreeOp>(&self, table: Table, op: Op) -> Op::Output {
        match &self.big_trees {
            BigTrees::KiB1 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
            BigTrees::KiB2 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
            BigTrees::KiB4 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
            BigTrees::KiB8 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
            BigTrees::KiB16 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
            BigTrees::KiB32 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
            BigTrees::KiB64 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
            BigTrees::KiB512 {
                warehouse,
                district,
            } => op.run(pick_big(table, warehouse, district)),
        }
    }

    /// Only for `TreeClass::Standard` tables — see `TreeClass`'s doc.
    /// Panics (rather than silently indexing `table_ids`' meaningless `0`
    /// default) if called with `Table::Warehouse`/`Table::District`, whose
    /// trees live outside `db`'s table list entirely; use `dispatch_big`
    /// for those instead.
    #[inline(always)]
    pub fn tree_for(&self, table: Table) -> Arc<TpccTree> {
        assert_eq!(
            table.class(),
            TreeClass::Standard,
            "TpccDatabase::tree_for: {table:?} is a TreeClass::Big table — use dispatch_big instead"
        );
        self.db
            .table(self.table_ids[table as usize])
            .expect("TpccDatabase creates every standard-class Table::ALL entry at construction")
    }

    /// Reads off the shared clock — same value regardless of which table's
    /// tree it's read through, since every table (including the two
    /// `TreeClass::Big` ones) shares this database's one `ctx`.
    pub fn current_version(&self) -> crate::bat_record_model::version_info::Version {
        self.db.current_version()
    }

    /// Toggles block reclaim uniformly across every table on this database
    /// — see `MVBTSt::enable_gc`'s doc for why partial/per-table toggling
    /// would make pruning the shared commit logs unsound. Includes the two
    /// `TreeClass::Big` trees: they share `ctx`'s pruning flag with every
    /// standard table (see `make_big_trees`), so leaving their own
    /// `block_reclaim_enabled` out of step would be exactly the unsound
    /// half-toggled state `MVBTSt::enable_gc`'s doc warns about.
    pub fn enable_gc(&self, update_in_place: bool, vacuum: Option<(f64, Duration)>) {
        // `vacuum`'s own thread(s) are started separately, below, via
        // `set_vacuum` — it has to cover both `db`'s standard tables and
        // this struct's own `big_trees`, which `db.enable_gc` alone can't
        // reach (see `set_vacuum`'s doc).
        self.db.enable_gc(update_in_place, None);
        match &self.big_trees {
            BigTrees::KiB1 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
            BigTrees::KiB2 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
            BigTrees::KiB4 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
            BigTrees::KiB8 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
            BigTrees::KiB16 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
            BigTrees::KiB32 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
            BigTrees::KiB64 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
            BigTrees::KiB512 {
                warehouse,
                district,
            } => {
                warehouse.enable_gc(update_in_place);
                district.enable_gc(update_in_place);
            }
        }
        self.set_vacuum(vacuum);
    }

    pub fn disable_gc(&self) {
        self.db.disable_gc();
        match &self.big_trees {
            BigTrees::KiB1 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
            BigTrees::KiB2 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
            BigTrees::KiB4 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
            BigTrees::KiB8 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
            BigTrees::KiB16 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
            BigTrees::KiB32 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
            BigTrees::KiB64 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
            BigTrees::KiB512 {
                warehouse,
                district,
            } => {
                warehouse.disable_gc();
                district.disable_gc();
            }
        }
        self.set_vacuum(None);
    }

    /// Starts or stops idle/proactive compaction across all 14 tables — GC's
    /// background half (see `bat_db::Database::set_vacuum`'s doc), not a
    /// separately toggled feature, so this is the single entry point
    /// `enable_gc`/`disable_gc` themselves call as well as what a caller
    /// reaches for when it needs the vacuum sweep's lifecycle to diverge
    /// from GC's own (see `bat_db::Database::set_vacuum`'s doc for why that
    /// comes up in practice). `Some((dead_ratio_threshold, sweep_interval))`
    /// (re)starts both sweep threads with those parameters; `None` stops
    /// whichever are running. `db.set_vacuum` alone only reaches the 12
    /// standard tables; this additionally starts/stops its own sweep thread
    /// for `Table::Warehouse`/`Table::District` specifically, since
    /// `big_trees` lives outside `db`'s table list entirely (see
    /// `TreeClass`'s doc). Snapshots the *current* `big_trees` set once, at
    /// call time, the same "tables/trees are created once, at
    /// database-init time" assumption `db.set_vacuum` itself relies on.
    ///
    /// Up to two background threads total when `Some`, each a permanent
    /// `WorkerId` — this one plus `db.set_vacuum`'s own — see that method's
    /// doc.
    pub fn set_vacuum(&self, vacuum: Option<(f64, Duration)>) {
        self.db.set_vacuum(vacuum);
        self.disable_big_idle_compaction();

        let Some((dead_ratio_threshold, sweep_interval)) = vacuum else {
            return;
        };
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        *self.idle_compaction_stop.lock().unwrap() = Some(stop.clone());

        match &self.big_trees {
            BigTrees::KiB1 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
            BigTrees::KiB2 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
            BigTrees::KiB4 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
            BigTrees::KiB8 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
            BigTrees::KiB16 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
            BigTrees::KiB32 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
            BigTrees::KiB64 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
            BigTrees::KiB512 { warehouse, district } => {
                spawn_big_idle_compaction_thread(warehouse, district, dead_ratio_threshold, sweep_interval, stop)
            }
        }
    }

    fn disable_big_idle_compaction(&self) {
        if let Some(stop) = self.idle_compaction_stop.lock().unwrap().take() {
            stop.store(true, Relaxed);
        }
    }

    /// Assigns `table`'s tree a dedicated shared scan-worker pool — a thin,
    /// `Table`-keyed wrapper over `bat_db::Database::enable_scan_pool`
    /// (see that method's and `bat_tree::scan_pool::ScanWorkerPool`'s docs;
    /// this is a `db`-wide feature, not something specific to
    /// `TpccDatabase`). `expected_concurrent_queries` — typically the
    /// caller's own OLAP thread count, since that's usually known by the
    /// time a workload begins — feeds `ScanWorkerPool::fair_query_fanout`,
    /// so each concurrently-querying caller asks for its fair share of
    /// this pool instead of every query grabbing a fixed slice regardless
    /// of how many others are sharing it; `None` if that count isn't
    /// known. `table` must be `TreeClass::Standard` (see `tree_for`'s doc,
    /// which this panics through for `Warehouse`/`District`, since those
    /// two live outside `db`'s table list entirely).
    pub fn enable_scan_pool(&self, table: Table, num_workers: usize, expected_concurrent_queries: Option<usize>) {
        assert_eq!(
            table.class(),
            TreeClass::Standard,
            "TpccDatabase::enable_scan_pool: {table:?} is a TreeClass::Big table"
        );
        self.db.enable_scan_pool(self.table_ids[table as usize], num_workers, expected_concurrent_queries);
    }

    /// Drops the pool `enable_scan_pool` assigned to `table`, if any — a
    /// no-op otherwise. See `bat_db::Database::disable_scan_pool`'s doc for
    /// the (unwaited) worker thread shutdown this triggers. Panics for
    /// `Warehouse`/`District` — see `enable_scan_pool`'s doc; without this,
    /// `table_ids`' meaningless `0` for those two would silently disable
    /// whichever real table happens to hold `TableId` 0.
    pub fn disable_scan_pool(&self, table: Table) {
        assert_eq!(
            table.class(),
            TreeClass::Standard,
            "TpccDatabase::disable_scan_pool: {table:?} is a TreeClass::Big table"
        );
        self.db.disable_scan_pool(self.table_ids[table as usize]);
    }

    /// The pool `enable_scan_pool` assigned to `table`, if any. Panics for
    /// `Warehouse`/`District` — see `disable_scan_pool`'s doc.
    pub fn scan_pool(&self, table: Table) -> Option<Arc<TpccScanWorkerPool>> {
        assert_eq!(
            table.class(),
            TreeClass::Standard,
            "TpccDatabase::scan_pool: {table:?} is a TreeClass::Big table"
        );
        self.db.scan_pool(self.table_ids[table as usize])
    }

    /// Retain historical versions and commit-log entries across all tables.
    /// Configure before running workers; enabling this overrides GC settings.
    pub fn allow_historic_query(&self, enabled: bool) {
        self.set_vacuum(None);
        self.db.allow_historic_query(enabled);
        match &self.big_trees {
            BigTrees::KiB1 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
            BigTrees::KiB2 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
            BigTrees::KiB4 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
            BigTrees::KiB8 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
            BigTrees::KiB16 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
            BigTrees::KiB32 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
            BigTrees::KiB64 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
            BigTrees::KiB512 {
                warehouse,
                district,
            } => {
                warehouse.allow_historic_query(enabled);
                district.allow_historic_query(enabled);
            }
        }
    }

    pub fn root_star_index(&self) -> RootIndexType {
        self.db.root_star_index()
    }
}

impl TpccDatabase {
    /// Builds a fresh database, replays the *single* shared WAL file found
    /// at `wal_path` — via `bat_db::Database::open_recovered`, which reads
    /// its own table catalog to know which tables to recreate, in their
    /// original order, with no per-table file/path bookkeeping needed here
    /// — then attaches a live writer. The `TpccDatabase` counterpart to
    /// `bat_db::Database::open_recovered`. Unlike the old one-file-per-table
    /// design, a `TpccTxn` spanning several tables now logs exactly one
    /// Commit marker for the whole transaction (see
    /// `bat_db::DbTransaction::commit`'s doc), so recovery no longer has the
    /// old "a crash between two tables' markers can leave one table's share
    /// of a transaction replayed and another's not" gap.
    /// Same as `open_recovered_with_big_tree_size`, at `BigTreeSize::default()`.
    /// `NUM_RECORDS` never appears in the WAL wire format (see
    /// `WAREHOUSE_BIG_TABLE_ID`'s doc), so recovering at a *different*
    /// `BigTreeSize` than whatever originally wrote the log is completely
    /// safe — every logged op just replays into a freshly-sized tree.
    pub fn open_recovered(
        root_index_type: RootIndexType,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
    ) -> std::io::Result<Self> {
        Self::open_recovered_with_big_tree_size(
            root_index_type,
            wal_path,
            flush_interval,
            BigTreeSize::default(),
        )
    }

    pub fn open_recovered_with_big_tree_size(
        root_index_type: RootIndexType,
        wal_path: &std::path::Path,
        flush_interval: std::time::Duration,
        big_tree_size: BigTreeSize,
    ) -> std::io::Result<Self> {
        let db = Database::open_recovered(
            root_index_type,
            inc_key,
            dec_key,
            TpccKey::MIN,
            TpccKey::MAX,
            wal_path,
            flush_interval,
        )?;
        let table_ids = Self::create_all_tables(&db);

        // `db`'s own `open_recovered` only knows about (and only replayed)
        // its own 12-table catalog — the two `TreeClass::Big` trees live
        // outside it entirely (see `TreeClass`'s doc) and need their own
        // replay pass over the same shared log file, routed by their own
        // reserved `TableId` tags. `db`'s replay already truncated the file
        // to its valid prefix, so this second, independent scan of that same
        // (now-stable) prefix finds the identical valid length — nothing
        // left to truncate again here.
        let mut big_trees = Self::make_big_trees(
            root_index_type,
            &db,
            big_tree_size,
            Some(Arc::new(WalBackend::Off)),
        );
        let writer = db.wal_writer();
        macro_rules! replay_and_configure {
            ($warehouse:expr, $district:expr) => {{
                crate::bat_wal::recovery::replay_two_tables(
                    &$warehouse,
                    WAREHOUSE_BIG_TABLE_ID,
                    &$district,
                    DISTRICT_BIG_TABLE_ID,
                    wal_path,
                )?;
                Arc::get_mut($warehouse)
                    .expect("big tree must be unshared during recovery construction")
                    .set_wal_before_share(writer.clone());
                Arc::get_mut($district)
                    .expect("big tree must be unshared during recovery construction")
                    .set_wal_before_share(writer.clone());
            }};
        }
        match &mut big_trees {
            BigTrees::KiB1 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
            BigTrees::KiB2 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
            BigTrees::KiB4 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
            BigTrees::KiB8 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
            BigTrees::KiB16 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
            BigTrees::KiB32 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
            BigTrees::KiB64 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
            BigTrees::KiB512 {
                warehouse,
                district,
            } => replay_and_configure!(warehouse, district),
        }

        Ok(Self {
            db,
            table_ids,
            big_trees,
            idle_compaction_stop: Mutex::new(None),
        })
    }
}

// ---------------------------------------------------------------------
// Table range helpers
// ---------------------------------------------------------------------
//
// Table selection is now "which tree" (see `Table`/`TpccDatabase::tree_for`),
// not "which key range", so every one of these is just the trivial
// full-range scan of that table's own tree. Kept as thin named wrappers so
// `olap_scan.rs`/`tpch_queries.rs` call sites don't change shape, just their
// target tree.

#[inline(always)]
fn full_range() -> Interval<TpccKey> {
    Interval::new(TpccKey::MIN, TpccKey::MAX)
}

pub fn warehouse_table_range() -> Interval<TpccKey> {
    full_range()
}
pub fn district_table_range() -> Interval<TpccKey> {
    full_range()
}
pub fn order_line_table_range() -> Interval<TpccKey> {
    full_range()
}
pub fn stock_table_range() -> Interval<TpccKey> {
    full_range()
}
pub fn orders_table_range() -> Interval<TpccKey> {
    full_range()
}
pub fn supplier_table_range() -> Interval<TpccKey> {
    full_range()
}
pub fn nation_table_range() -> Interval<TpccKey> {
    full_range()
}
pub fn region_table_range() -> Interval<TpccKey> {
    full_range()
}

// ---------------------------------------------------------------------
// Key builders
// ---------------------------------------------------------------------

// Bit widths for primary-key columns, generous but not maximal: sized for
// benchmark-scale runs (hundreds of warehouses, tens of millions of orders),
// not the TPC-C spec's audited maximums.
const D_ID_BITS: u32 = 4; // districts/warehouse (spec: 10)
const C_ID_BITS: u32 = 16; // customers/district (spec: 3,000)
const O_ID_BITS: u32 = 32; // orders/district over the whole run (grows unboundedly)
const OL_NO_BITS: u32 = 4; // order-lines/order (spec: 5-15)
const I_ID_BITS: u32 = 24; // items (spec: 100,000)
const LAST_CODE_BITS: u32 = 10; // C_LAST syllable code, exactly 0..=999
const FIRST_CODE_BITS: u32 = 16; // ordinal surrogate for c_first, tie-break only

#[inline(always)]
pub const fn k_warehouse(w_id: u32) -> TpccKey {
    w_id as u64
}

#[inline(always)]
pub const fn k_district(w_id: u32, d_id: u8) -> TpccKey {
    ((w_id as u64) << D_ID_BITS) | d_id as u64
}

#[inline(always)]
pub const fn k_customer(w_id: u32, d_id: u8, c_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + C_ID_BITS)) | ((d_id as u64) << C_ID_BITS) | c_id as u64
}

#[inline(always)]
pub const fn k_customer_name_idx(
    w_id: u32,
    d_id: u8,
    last_code: u16,
    first_code: u16,
    c_id: u32,
) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + LAST_CODE_BITS + FIRST_CODE_BITS + C_ID_BITS))
        | ((d_id as u64) << (LAST_CODE_BITS + FIRST_CODE_BITS + C_ID_BITS))
        | ((last_code as u64) << (FIRST_CODE_BITS + C_ID_BITS))
        | ((first_code as u64) << C_ID_BITS)
        | c_id as u64
}

/// `[lower, upper]` bounds covering every `(first_code, c_id)` for a fixed
/// `(w_id, d_id, last_code)` prefix — used by Payment/OrderStatus's "by last
/// name" lookup.
pub const fn k_customer_name_idx_prefix_bounds(
    w_id: u32,
    d_id: u8,
    last_code: u16,
) -> (TpccKey, TpccKey) {
    (
        k_customer_name_idx(w_id, d_id, last_code, 0, 0),
        k_customer_name_idx(w_id, d_id, last_code, u16::MAX, (1 << C_ID_BITS) - 1),
    )
}

/// Extracts `c_id` back out of a customer-name-index key (the low
/// `C_ID_BITS` bits), used once a range scan over
/// `k_customer_name_idx_prefix_bounds` has picked the matching entry.
#[inline(always)]
pub const fn decode_customer_name_idx_c_id(key: TpccKey) -> u32 {
    (key & ((1u64 << C_ID_BITS) - 1)) as u32
}

#[inline(always)]
pub const fn k_item(i_id: u32) -> TpccKey {
    i_id as u64
}

#[inline(always)]
pub const fn k_stock(w_id: u32, i_id: u32) -> TpccKey {
    ((w_id as u64) << I_ID_BITS) | i_id as u64
}

#[inline(always)]
pub const fn k_order(w_id: u32, d_id: u8, o_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + O_ID_BITS)) | ((d_id as u64) << O_ID_BITS) | o_id as u64
}

/// Decodes an ORDERS-table key back into `(w_id, d_id, o_id)` — the inverse
/// of `k_order`, used by `tpch_queries` after a full-table scan to recover
/// each order's identity for its follow-up `order_line` range scan.
#[inline(always)]
pub const fn decode_order_key(key: TpccKey) -> (u32, u8, u32) {
    let o_id = (key & ((1u64 << O_ID_BITS) - 1)) as u32;
    let d_id = ((key >> O_ID_BITS) & ((1u64 << D_ID_BITS) - 1)) as u8;
    let w_id = (key >> (O_ID_BITS + D_ID_BITS)) as u32;
    (w_id, d_id, o_id)
}

#[inline(always)]
pub const fn k_new_order(w_id: u32, d_id: u8, o_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + O_ID_BITS)) | ((d_id as u64) << O_ID_BITS) | o_id as u64
}

/// `[lower, upper]` bounds covering every `o_id` for a fixed `(w_id, d_id)` —
/// the Delivery transaction's "find the oldest queued new-order" scan.
pub const fn k_new_order_district_bounds(w_id: u32, d_id: u8) -> (TpccKey, TpccKey) {
    (
        k_new_order(w_id, d_id, 0),
        k_new_order(w_id, d_id, u32::MAX),
    )
}

#[inline(always)]
pub const fn k_order_line(w_id: u32, d_id: u8, o_id: u32, ol_number: u8) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + O_ID_BITS + OL_NO_BITS))
        | ((d_id as u64) << (O_ID_BITS + OL_NO_BITS))
        | ((o_id as u64) << OL_NO_BITS)
        | ol_number as u64
}

/// `[lower, upper]` bounds covering every `ol_number` (1..=15) of one order.
///
/// The upper sentinel must be masked to `OL_NO_BITS` (`u8::MAX` doesn't fit:
/// `OL_NO_BITS` is 4 bits wide, not a full byte, unlike e.g. `FIRST_CODE_BITS`
/// which *is* exactly `u16`-wide and so can use `u16::MAX` directly in
/// `k_customer_name_idx_prefix_bounds`) - an unmasked 255 there OR's bits
/// into `o_id`'s own low nibble (`k_order_line`'s `o_id << OL_NO_BITS`
/// starts right where `ol_number`'s bits end), rounding the upper bound's
/// `o_id` up to the next `o_id | 0b1111` and leaking into however many
/// subsequent orders' order-lines happen to fall in that widened range.
pub const fn k_order_line_bounds(w_id: u32, d_id: u8, o_id: u32) -> (TpccKey, TpccKey) {
    const MAX_OL_NUMBER: u8 = (1u8 << OL_NO_BITS) - 1;
    (
        k_order_line(w_id, d_id, o_id, 0),
        k_order_line(w_id, d_id, o_id, MAX_OL_NUMBER),
    )
}

/// Extracts `ol_number` (the low `OL_NO_BITS` bits) back out of an
/// ORDER_LINE key — `tpch_queries::q1` groups by this without going through
/// `k_order_line`'s inputs first (it scans the whole table directly).
#[inline(always)]
pub const fn decode_order_line_number(key: TpccKey) -> u8 {
    (key & ((1u64 << OL_NO_BITS) - 1)) as u8
}

#[inline(always)]
pub const fn k_cust_last_order(w_id: u32, d_id: u8, c_id: u32) -> TpccKey {
    ((w_id as u64) << (D_ID_BITS + C_ID_BITS)) | ((d_id as u64) << C_ID_BITS) | c_id as u64
}

#[inline(always)]
pub fn k_history(seq: u64) -> TpccKey {
    seq
}

#[inline(always)]
pub const fn k_supplier(su_id: u32) -> TpccKey {
    su_id as u64
}

#[inline(always)]
pub const fn decode_supplier_id(key: TpccKey) -> u32 {
    key as u32
}

#[inline(always)]
pub const fn k_nation(n_id: u8) -> TpccKey {
    n_id as u64
}

#[inline(always)]
pub const fn decode_nation_id(key: TpccKey) -> u8 {
    key as u8
}

#[inline(always)]
pub const fn k_region(r_id: u8) -> TpccKey {
    r_id as u64
}

#[inline(always)]
pub const fn decode_region_id(key: TpccKey) -> u8 {
    key as u8
}

// ---------------------------------------------------------------------
// Row payloads
// ---------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Warehouse {
    pub w_name: String,
    pub w_street_1: String,
    pub w_street_2: String,
    pub w_city: String,
    pub w_state: String,
    pub w_zip: String,
    pub w_tax: f64,
    pub w_ytd: f64,
}

#[derive(Clone, Debug)]
pub struct District {
    pub d_name: String,
    pub d_street_1: String,
    pub d_street_2: String,
    pub d_city: String,
    pub d_state: String,
    pub d_zip: String,
    pub d_tax: f64,
    pub d_ytd: f64,
    pub d_next_o_id: u32,
}

#[derive(Clone, Debug)]
pub struct Customer {
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street_1: String,
    pub c_street_2: String,
    pub c_city: String,
    pub c_state: String,
    pub c_zip: String,
    pub c_phone: String,
    pub c_since: i64,
    pub c_credit_bad: bool,
    pub c_credit_lim: f64,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_ytd_payment: f64,
    pub c_payment_cnt: u32,
    pub c_delivery_cnt: u32,
    pub c_data: String,
}

#[derive(Clone, Debug)]
pub struct History {
    pub h_c_id: u32,
    pub h_c_d_id: u8,
    pub h_c_w_id: u32,
    pub h_d_id: u8,
    pub h_w_id: u32,
    pub h_date: i64,
    pub h_amount: f64,
    pub h_data: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NewOrderMarker {
    pub no_o_id: u32,
}

#[derive(Clone, Debug)]
pub struct Order {
    pub o_c_id: u32,
    pub o_entry_d: i64,
    pub o_carrier_id: Option<u32>,
    pub o_ol_cnt: u8,
    pub o_all_local: bool,
}

#[derive(Clone, Debug)]
pub struct OrderLine {
    pub ol_i_id: u32,
    pub ol_supply_w_id: u32,
    pub ol_delivery_d: Option<i64>,
    pub ol_quantity: u8,
    pub ol_amount: f64,
    pub ol_dist_info: String,
}

/// Order-preserving `i64 -> u64` encoding for `MVBTSt::set_zone_map_projection`
/// / `RangeQueryIter::with_zone_predicate` (both require a `u64`-space
/// bound): flips the sign bit so `i64::MIN..=i64::MAX`'s ordering survives
/// the reinterpretation as `u64`, the standard trick for embedding a signed
/// total order into an unsigned one. Shared between `order_line_delivery_d_
/// zone_map_projection` (the write side, called from `create_all_tables`)
/// and `bat_bench::tpch_queries`'s Q1/Q6 (the read side) — both *must* use
/// this exact same encoding, or a leaf's zone map and a query's predicate
/// would silently disagree about what a given bound means.
pub(crate) fn encode_signed_zone_value(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// `ORDER_LINE`'s zone-map projection, tracking `ol_delivery_d` — the exact
/// column CH-benCHmark Q1/Q6 filter on (see `bat_bench::parallel_scan`'s
/// `q1_parallel`/`q6_parallel`). `None` (undelivered) doesn't widen the
/// zone map at all, which is what lets an all-undelivered leaf be skipped
/// outright for a delivered-date predicate — see `LeafZoneMap`'s doc.
fn order_line_delivery_d_zone_map_projection(row: &TpccRow) -> Option<u64> {
    row.as_order_line().ol_delivery_d.map(encode_signed_zone_value)
}

#[derive(Clone, Debug)]
pub struct Item {
    pub i_im_id: u32,
    pub i_name: String,
    pub i_price: f64,
    pub i_data: String,
}

#[derive(Clone, Debug)]
pub struct Stock {
    pub s_quantity: i32,
    /// Always exactly 24 bytes (TPC-C spec §1.3, `rnd_astring_exact::<24>()`
    /// at load time) - stored inline rather than as `[String; 10]` so
    /// cloning a `Stock` (every `NewOrder` line item's stock update does
    /// this, see `tpcc_txn.rs`) is a flat memcpy instead of 10 separate
    /// heap allocations that `perf` showed dominating `Stock::clone`.
    pub s_dist: [[u8; 24]; 10],
    pub s_ytd: f64,
    pub s_order_cnt: u32,
    pub s_remote_cnt: u32,
    pub s_data: String,
    /// CH-benCHmark's addition linking STOCK to SUPPLIER (see
    /// `bat_bench::tpch_queries` module docs): which of the fixed supplier
    /// pool fulfills this `(w_id, i_id)`'s stock, assigned deterministically
    /// at load time (`tpcc_load::su_suppkey_for`).
    pub s_su_suppkey: u32,
}

/// CH-benCHmark's TPC-H-derived SUPPLIER table (standard TPC-H `supplier`
/// columns, minus the unused `s_suppkey`/`s_nationkey` foreign-key
/// decoration this port doesn't need beyond `s_nationkey` itself).
#[derive(Clone, Debug)]
pub struct Supplier {
    pub s_name: String,
    pub s_address: String,
    pub s_nationkey: u8,
    pub s_phone: String,
    pub s_acctbal: f64,
    pub s_comment: String,
}

/// Standard (fixed, 25-row) TPC-H NATION reference table.
#[derive(Clone, Debug)]
pub struct Nation {
    pub n_name: String,
    pub n_regionkey: u8,
    pub n_comment: String,
}

/// Standard (fixed, 5-row) TPC-H REGION reference table.
#[derive(Clone, Debug)]
pub struct Region {
    pub r_name: String,
    pub r_comment: String,
}

/// Payload shared by every one of `TpccDatabase`'s 14 tables — the larger row
/// kinds (`Customer`, `Stock`) are boxed so the enum itself — and thus every
/// leaf record, including the small ones (`Warehouse`, `NewOrderMarker`, ...)
/// — stays compact; the same pattern the base tree already uses for large
/// generic payloads (see `bat_test::PayloadIndirection`). Keeping one shared
/// enum (rather than a distinct native Rust struct payload per table) is
/// what lets every table be the same monomorphized `TpccTree`, so
/// `TpccDatabase` can be a plain struct of same-typed fields and
/// `bat_bench::tpcc_txn::TpccTxn` a single uniform transaction type reused for
/// every table.
#[derive(Clone, Debug, Default)]
pub enum TpccRow {
    #[default]
    Empty,
    Warehouse(Box<Warehouse>),
    District(Box<District>),
    Customer(Box<Customer>),
    /// Secondary customer-name-index entries store nothing beyond the key
    /// (which already encodes `c_id`); the value is just a presence marker.
    CustomerNameIdx,
    History(Box<History>),
    NewOrder(NewOrderMarker),
    Order(Box<Order>),
    OrderLine(Box<OrderLine>),
    Item(Box<Item>),
    Stock(Box<Stock>),
    /// (w_id,d_id,c_id) -> most recent o_id, see `Table::CustLastOrder`.
    CustLastOrder(u32),
    Supplier(Box<Supplier>),
    Nation(Box<Nation>),
    Region(Box<Region>),
}

impl Display for TpccRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TpccRow::Empty => write!(f, "Empty"),
            TpccRow::Warehouse(w) => write!(f, "Warehouse({})", w.w_name),
            TpccRow::District(d) => write!(f, "District({})", d.d_name),
            TpccRow::Customer(c) => write!(f, "Customer({} {})", c.c_first, c.c_last),
            TpccRow::CustomerNameIdx => write!(f, "CustomerNameIdx"),
            TpccRow::History(_) => write!(f, "History"),
            TpccRow::NewOrder(no) => write!(f, "NewOrder(o_id={})", no.no_o_id),
            TpccRow::Order(o) => write!(f, "Order(c_id={}, ol_cnt={})", o.o_c_id, o.o_ol_cnt),
            TpccRow::OrderLine(ol) => write!(f, "OrderLine(i_id={})", ol.ol_i_id),
            TpccRow::Item(i) => write!(f, "Item({})", i.i_name),
            TpccRow::Stock(s) => write!(f, "Stock(qty={})", s.s_quantity),
            TpccRow::CustLastOrder(o_id) => write!(f, "CustLastOrder(o_id={o_id})"),
            TpccRow::Supplier(s) => write!(f, "Supplier({})", s.s_name),
            TpccRow::Nation(n) => write!(f, "Nation({})", n.n_name),
            TpccRow::Region(r) => write!(f, "Region({})", r.r_name),
        }
    }
}

// Field-projection helpers so transaction code doesn't need to match on
// TpccRow everywhere.
impl TpccRow {
    pub fn as_warehouse(&self) -> &Warehouse {
        match self {
            TpccRow::Warehouse(w) => w,
            _ => panic!("expected Warehouse row"),
        }
    }
    pub fn as_district(&self) -> &District {
        match self {
            TpccRow::District(d) => d,
            _ => panic!("expected District row"),
        }
    }
    pub fn as_customer(&self) -> &Customer {
        match self {
            TpccRow::Customer(c) => c,
            _ => panic!("expected Customer row"),
        }
    }
    pub fn as_order(&self) -> &Order {
        match self {
            TpccRow::Order(o) => o,
            _ => panic!("expected Order row"),
        }
    }
    pub fn as_order_line(&self) -> &OrderLine {
        match self {
            TpccRow::OrderLine(ol) => ol,
            _ => panic!("expected OrderLine row"),
        }
    }
    pub fn as_item(&self) -> &Item {
        match self {
            TpccRow::Item(i) => i,
            _ => panic!("expected Item row"),
        }
    }
    pub fn as_stock(&self) -> &Stock {
        match self {
            TpccRow::Stock(s) => s,
            _ => panic!("expected Stock row"),
        }
    }
    pub fn as_cust_last_order(&self) -> u32 {
        match self {
            TpccRow::CustLastOrder(o_id) => *o_id,
            _ => panic!("expected CustLastOrder row"),
        }
    }
    pub fn as_supplier(&self) -> &Supplier {
        match self {
            TpccRow::Supplier(s) => s,
            _ => panic!("expected Supplier row"),
        }
    }
    pub fn as_nation(&self) -> &Nation {
        match self {
            TpccRow::Nation(n) => n,
            _ => panic!("expected Nation row"),
        }
    }
    pub fn as_region(&self) -> &Region {
        match self {
            TpccRow::Region(r) => r,
            _ => panic!("expected Region row"),
        }
    }
}
