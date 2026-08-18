use std::fmt::Display;
use std::hash::Hash;
use crate::bat_block::block::Block;
use crate::bat_page_model::BlockRef;
use crate::bat_sync::smart_cell::{OptCell, SmartCell};

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn into_cell(self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        // Deliberately leaked, not `Arc`-managed: see `SmartCell`'s doc.
        // Every block that ever exists is allocated exactly once, here.
        SmartCell(Box::into_raw(Box::new(OptCell::new(self))) as *const _)
    }
}