use crate::bat_query::SnapShot;
use crate::bat_tree::mvbt::MVBTSt;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;

pub struct ReaderIsolatedSnapShot<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    pub SnapShot,
    pub &'a MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
);

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> ReaderIsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub const fn snapshot(&self) -> SnapShot {
        self.0
    }

    #[inline(always)]
    pub const fn bat_tree(&self) -> &MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.1
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn snapshot_current(
        &self,
    ) -> ReaderIsolatedSnapShot<'static, FAN_OUT, NUM_RECORDS, Key, Payload> {
        ReaderIsolatedSnapShot(self.begin_snapshot(), unsafe { mem::transmute(self) })
    }
}
