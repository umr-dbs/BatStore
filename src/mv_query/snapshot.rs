use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use crate::mv_query::SnapShot;
use crate::mv_tree::mvbt::MVBTSt;

pub struct ReaderIsolatedSnapShot<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
>(
    pub SnapShot,
    pub &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
);

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> ReaderIsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub const fn snapshot(&self) -> SnapShot {
        self.0
    }

    #[inline(always)]
    pub const fn mv_tree(&self) -> &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.1
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Draws and registers a fresh OSIC snapshot (see `begin_snapshot`) for
    /// reading "as of now". Callers must eventually pair this with
    /// `on_release_reader_snapshot`/`end_snapshot` (see `RangeQueryIter` for
    /// the RAII-safe equivalent) so `CommitLog` pruning doesn't drop an
    /// entry this snapshot's future `LCB` queries still need.
    #[inline(always)]
    pub fn snapshot_current(&self) -> ReaderIsolatedSnapShot<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        ReaderIsolatedSnapShot(self.begin_snapshot(), unsafe { mem::transmute(self) })
    }
}