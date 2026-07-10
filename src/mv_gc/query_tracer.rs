use std::ops::Deref;
use crossbeam_skiplist::SkipSet;
use crate::mv_query::SnapShot;

/// Every transaction on a tree draws its `ts_start` from that tree's single
/// `GlobalClock` via a unique `fetch_add`, so two entries can never collide
/// — no secondary per-thread tie-breaker (the old `Tid`) is needed to keep
/// this set's entries distinct.
#[derive(Ord, Eq, PartialEq, PartialOrd, Clone, Copy)]
pub(crate) struct ReaderQuery(SnapShot);

impl Into<ReaderQuery> for SnapShot {
    fn into(self) -> ReaderQuery {
        ReaderQuery::new(self)
    }
}

impl ReaderQuery {
    #[inline]
    const fn new(version: SnapShot) -> ReaderQuery {
        Self(version)
    }

    #[inline]
    const fn snapshot(&self) -> SnapShot {
        self.0
    }
}
type QueryTracer = SkipSet<ReaderQuery>;

// #[derive(Default, Clone)]
// pub struct NullValue;
//
// impl Display for NullValue {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "()")
//     }
// }

pub(crate) struct TransactionTrace(QueryTracer);

impl Deref for TransactionTrace {
    type Target = QueryTracer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TransactionTrace {
    pub(crate) fn new() -> Self {
        Self(QueryTracer::new())
    }

    #[inline(always)]
    pub(crate) fn peek_min(&self) -> Option<SnapShot> {
        self.front()
            .map(|entry| entry.snapshot())
    }

    #[inline(always)]
    pub(crate) fn peek_max(&self) -> Option<SnapShot> {
        self.back()
            .map(|entry| entry.snapshot())
    }

    /// Enumerates every currently active `ts_start`, for `CommitLog`
    /// pruning: an entry is only ever safe to drop if it isn't the LCB of
    /// any snapshot this yields (see `MVBTSt::commit_tx`).
    #[inline(always)]
    pub(crate) fn active_snapshots(&self) -> impl Iterator<Item = SnapShot> + '_ {
        self.iter().map(|entry| entry.snapshot())
    }

    #[inline(always)]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) {
        let reader_query: ReaderQuery = snapshot.into();
        let _res = self.insert(reader_query.clone());
        // println!("[{:?}] - Inserted ReaderQuery: (v: {}, tid: {})",
        //          thread::current().id(),
        //          res.0, res.1);
    }

    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snap_shot: SnapShot) {
        let reader_query = snap_shot.into();
        if let None = self.remove(&reader_query) {
            // println!("[{:?}] - Failed Reader Snapshot Removal of: (v: {}, tid: {}) was not found",
            //          thread::current().id(),
            //          reader_query.0,
            //          reader_query.1);
        }
        else {
            // println!("[{:?}] - Successful Reader Snapshot Removal of: (v: {}, tid: {}).",
            //          thread::current().id(),
            //          reader_query.0,
            //          reader_query.1);
        }
    }
}