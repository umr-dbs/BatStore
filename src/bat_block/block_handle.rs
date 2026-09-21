use std::cell::Cell;
use std::collections::LinkedList;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering::Acquire, fence};

use crate::bat_block::block::Block;
use crate::bat_gc::tracker_handle::{TrackerHandle, TrackerHandleSt};
use crate::bat_page_model::node::Node;
use crate::bat_page_model::{BlockID, BlockRef, ObjectCount};
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::safe_cell::SafeCell;
use crate::bat_sync::smart_cell::SmartCell;
use crate::bat_sync::tx_context::TxContext;
use parking_lot::Mutex;
use triomphe::Arc;

const ENABLE_SMALL_BLOCK: bool = false;
const MAX_ZEROS_PER_BLOCK: usize = 3964; // = data region in a bat_block // outdated due to omitted bat_block-id

/// Default starting numerical value for a valid BlockID.
// pub const START_BLOCK_ID: BlockID = BlockID::MIN;

pub const _1KB: usize = 1024;
pub const _2KB: usize = 2 * _1KB;
pub const _4KB: usize = 4 * _1KB;
pub const _8KB: usize = 8 * _1KB;
pub const _16KB: usize = 16 * _1KB;
pub const _32KB: usize = 32 * _1KB;

pub type AtomicBlockID = AtomicU64;
pub const START_BLOCK_ID: BlockID = BlockID::MIN;

pub const fn bsz_alignment_min<Key, Payload>() -> usize
where
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Default + Clone,
{
    mem::align_of::<Arc<()>>() + // ptr size
        mem::align_of::<usize>() + // dispatcher alignment
        mem::size_of::<usize>() * 2 + // arc extras in data area in Tree
        mem::align_of::<Block<0, 0, Key, Payload>>() + // alignment for bat_block
        mem::size_of::<ObjectCount>() + // len indicator
        mem::size_of::<usize>() * 2 + // arc extras in data area
        // mem::size_of::<SmartFlavor<()>>() + // align of SmartFlavor = size of empty data
        mem::size_of::<SmartCell<()>>() // align of SmartCell = size of usize
}

type DeadPages<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload> =
    Arc<Mutex<LinkedList<(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>>>;

// type DeadPages<const FAN_OUT: usize, const NUM_RECORDS: usize, Key>
// = Arc<SafeCell<BPlusTree<250, 250, Version, BlockRef<FAN_OUT, NUM_RECORDS, Key>>>>;

// pub static NODES_REQUEST: AtomicUsize = AtomicUsize::new(0);
pub struct BlockAllocManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static,
> {
    /// Always present — see `TrackerHandleSt`'s type doc for why this isn't
    /// `Option` anymore: active-snapshot tracking (needed for `CommitLog`
    /// pruning) must not depend on whether block reclaim (`enable_gc`) was
    /// ever turned on.
    tracker: TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>,
    update_in_place: Cell<bool>,
    // pub reuse_count: AtomicUsize,
    // pub alloc_count: AtomicUsize,
    // block_id_counter: AtomicBlockID,
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> Clone for BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn clone(&self) -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            tracker: Arc::new(TrackerHandleSt::new()),
            update_in_place: Cell::new(false),
            // reuse_count: AtomicUsize::new(0),
            // alloc_count: AtomicUsize::new(0),
        }
    }
}

/// Default implementation for BlockManager with default BlockSettings.
impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default,
> Default for BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn default() -> Self {
        BlockAllocManager::new()
    }
}

/// Main functionality implementation for BlockManager.
impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static,
> BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn reset_alloc_reuse_counts(&self) {}

    #[inline(always)]
    pub(crate) fn tracker(&self) -> &TrackerHandleSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.tracker.as_ref()
    }

    #[inline(always)]
    pub(crate) const fn has_update_in_place(&self) -> bool {
        self.update_in_place.get()
    }

    #[inline(always)]
    pub const fn allocation_leaf(&self) -> usize {
        NUM_RECORDS
    }

    #[inline(always)]
    pub const fn allocation_directory(&self) -> usize {
        FAN_OUT
    }

    #[inline(always)]
    pub const fn max_records() -> usize {
        NUM_RECORDS
    }

    #[inline(always)]
    pub const fn overflow_records_count() -> usize {
        Self::max_records()
    }

    #[inline(always)]
    pub const fn max_keys() -> usize {
        FAN_OUT
    }

    #[inline(always)]
    pub const fn overflow_keys_count() -> usize {
        Self::max_keys() - 1
    }

    /// Main Constructor requiring supplied BlockSettings.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            tracker: Arc::new(TrackerHandleSt::new()),
            update_in_place: Cell::new(false),
            // reuse_count: AtomicUsize::new(0),
            // alloc_count: AtomicUsize::new(0),
        }
    }

    pub fn set_update_in_place(&self, update_in_place: bool) {
        self.update_in_place.set(update_in_place);
    }

    #[inline(always)]
    pub fn register_dead_col(
        &self,
        worker_id: WorkerId,
        dead: [(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>); 2],
    ) {
        self.tracker.register_died_page_col(worker_id, dead);
    }

    #[inline(always)]
    pub fn register_dead(
        &self,
        worker_id: WorkerId,
        dead_v: Version,
        dead_p: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ) {
        self.tracker.register_died_page(worker_id, dead_v, dead_p);
    }

    #[inline(always)]
    fn alloc_block(
        &self,
        ctx: &TxContext,
        leaf: bool,
    ) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        #[cfg(feature = "gc-stats")]
        let requested_at = std::time::Instant::now();
        // NODES_REQUEST.fetch_add(1, Relaxed);
        let result = match self.tracker.free_block(ctx) {
            Some(block) => {
                // self.reuse_count.fetch_add(1, Relaxed);

                let m_page = block.unsafe_borrow_mut().node_data.get_mut();

                // println!("Reuse");
                m_page.on_reuse();

                if leaf {
                    m_page.mark_leaf()
                } else {
                    m_page.mark_internal()
                }

                // See `RETIRED_FLAG_VERSION`'s doc: reverses `mark_retired` so
                // this reused block isn't permanently un-lockable.
                block.clear_retired();

                block
            }
            None => {
                self.tracker.record_fresh_alloc(ctx.worker_id());
                if self.tracker.block_reclaim_enabled() {
                    let spare = (1..self.tracker.alloc_batch_size()).map(|_| {
                        Block {
                            node_data: SafeCell::new(Node::new_leaf()),
                        }
                        .into_cell()
                    });
                    self.tracker.queue_fresh_blocks(ctx.worker_id(), spare);
                }
                Block {
                    // block_id: self.next_block_id(),
                    node_data: SafeCell::new(if leaf {
                        Node::new_leaf()
                    } else {
                        Node::new_internal()
                    }),
                }
                .into_cell()
            }
        };
        #[cfg(feature = "gc-stats")]
        self.tracker.record_request_latency(
            ctx.worker_id(),
            requested_at.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        result
    }

    #[inline]
    pub(crate) fn new_empty_leaf(
        &self,
        ctx: &TxContext,
    ) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_block(ctx, true)
    }

    /// Crafts a new aligned Index-Block.
    #[inline]
    pub(crate) fn new_empty_index_block(
        &self,
        ctx: &TxContext,
    ) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_block(ctx, false)
    }
}
