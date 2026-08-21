use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::{Range, RangeInclusive};

pub type U64interval = Interval<u64>;
// pub type VersionInterval = Interval<Version>;

#[derive(Eq, PartialEq, Hash, Default, Clone, Copy)]
pub struct Interval<E: Ord + Copy + Hash + Display> {
    pub lower: E,
    pub upper: E,
}

impl U64interval {
    #[inline(always)]
    pub(crate) const fn blank() -> Self {
        Self {
            lower: u64::MAX,
            upper: u64::MIN,
        }
    }

    #[inline(always)]
    pub(crate) const fn max() -> Self {
        Self {
            lower: u64::MIN,
            upper: u64::MAX,
        }
    }

    #[inline(always)]
    pub(crate) const fn is_upper_max(&self) -> bool {
        self.upper == u64::MAX
    }

    #[inline(always)]
    pub(crate) const fn is_lower_min(&self) -> bool {
        self.lower == u64::MIN
    }
}

impl<E: Ord + Copy + Hash + Display> Interval<E> {
    pub const fn new(lower: E, upper: E) -> Self {
        Self { lower, upper }
    }

    #[inline(always)]
    pub fn set_lower(&mut self, e: E) {
        self.lower = e;
    }

    #[inline(always)]
    pub fn set_upper(&mut self, e: E) {
        self.upper = e;
    }

    #[inline(always)]
    pub const fn lower(&self) -> E {
        self.lower
    }

    #[inline(always)]
    pub const fn upper(&self) -> E {
        self.upper
    }

    #[inline(always)]
    pub fn merge(mut self, interval: &Self) -> Self {
        self.lower = interval.lower.min(self.lower);
        self.upper = interval.upper.max(self.upper);
        self
    }

    #[inline(always)]
    pub fn merge_mut(&mut self, interval: &Self) -> &mut Self {
        self.lower = interval.lower.min(self.lower);
        self.upper = interval.upper.max(self.upper);
        self
    }

    #[inline(always)]
    pub fn merged(&mut self, interval: &Self) {
        self.lower = interval.lower.min(self.lower);
        self.upper = interval.upper.max(self.upper);
    }

    #[inline(always)]
    pub fn expand(mut self, e: E) -> Self {
        self.lower = self.lower.min(e);
        self.upper = self.upper.max(e);
        self
    }

    #[inline(always)]
    pub fn expanded(&mut self, e: E) {
        self.lower = self.lower.min(e);
        self.upper = self.upper.max(e);
    }

    #[inline(always)]
    pub fn expand_mut(&mut self, e: E) -> &mut Self {
        self.lower = self.lower.min(e);
        self.upper = self.upper.max(e);
        self
    }

    #[inline(always)]
    pub fn intersection(&self, other: &Self) -> Self {
        Self::new(
            E::max(self.lower, other.lower),
            E::min(self.upper, other.upper),
        )
    }

    #[inline(always)]
    pub fn covers(&self, other: &Self) -> bool {
        self.lower <= other.lower && self.upper >= other.upper
    }

    #[inline(always)]
    pub fn covers_or_merge(&mut self, other: &Self) -> bool {
        self.covers(other).then(|| true).unwrap_or_else(|| {
            self.merged(other);
            false
        })
    }

    #[inline(always)]
    pub fn overlap(&self, other: &Self) -> bool {
        !self.is_disjoint(other)
    }

    #[inline(always)]
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.lower > other.upper || other.lower > self.upper
    }

    #[inline(always)]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.lower >= other.lower && self.upper <= other.upper
    }

    #[inline(always)]
    pub fn contains(&self, value: E) -> bool {
        value >= self.lower && value <= self.upper
    }
}

impl<E: Ord + Copy + Hash + Display> Into<Interval<E>> for (E, E) {
    fn into(self) -> Interval<E> {
        Interval::new(self.0, self.1)
    }
}

impl<E: Ord + Copy + Hash + Display> Into<Interval<E>> for RangeInclusive<E> {
    fn into(self) -> Interval<E> {
        Interval::new(*self.start(), *self.end())
    }
}

impl Into<U64interval> for Range<u64> {
    fn into(self) -> U64interval {
        U64interval::new(self.start, self.end.checked_sub(1).unwrap_or(u64::MIN))
    }
}

impl<E: Ord + Copy + Hash + Display> PartialOrd<Self> for Interval<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.lower.partial_cmp(&other.lower)
    }
}

impl<E: Ord + Copy + Hash + Display> Ord for Interval<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.lower.cmp(&other.lower)
    }
}

impl<E: Ord + Copy + Hash + Display> Display for Interval<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(low: {}, high: {})", self.lower, self.upper)
    }
}

/// Whether a key type can be evenly split into sub-intervals for
/// intra-query parallelism (see `bat_tree::scan_pool::ScanWorkerPool::
/// dispatch_evenly`) — deliberately opt-in per type, not derivable from
/// `Ord`/`Copy` alone.
///
/// A numeric key that packs several logical fields into different bit
/// ranges (e.g. `bat_bench::tpcc_schema::TpccKey`'s `w_id`/`d_id`/`o_id`/
/// `ol_number`) can have its *real* data occupy only a narrow, unevenly
/// distributed sliver of its full numeric domain — bisecting the type's
/// whole `MIN..=MAX` range blindly would hand most sub-ranges an empty or
/// near-empty slice and dump all the real work on one or two others,
/// which is worse than not splitting at all (see `split_evenly`'s doc on
/// `u64` below for the specific hazard). A type whose values form a
/// single flat numeric domain over whatever *range* a caller actually
/// passes in — sequential ids, hashes, anything without sub-fields, and
/// critically, a `range` that's already the real populated bounds rather
/// than the type's absolute `MIN..MAX` — can implement this safely.
/// Anything else should leave it unimplemented (or return `None`
/// unconditionally) and let its scans stay sequential, or use a
/// domain-aware partitioner written by hand for that specific key layout,
/// the way `bat_bench::parallel_scan::partition_order_line_range` already
/// does for `ORDER_LINE` — see that function's doc.
///
/// The default (used by every implementor unless overridden) always
/// returns `None`: no automatic splitting, so callers built on top of it
/// (like `dispatch_evenly`) fall back to running the query
/// single-threaded rather than risk a badly imbalanced partition.
pub trait RangeSplit: Ord + Copy + Hash + Display + Sized {
    /// Splits `range` into `fanout.max(1)` contiguous sub-intervals
    /// covering exactly `range` with no gaps or overlaps (a sub-interval
    /// with no values left gets a genuinely empty, inverted interval —
    /// `lower > upper` — rather than being omitted, matching
    /// `partition_order_line_range`'s convention: every caller of this
    /// always gets back exactly `fanout` pieces to dispatch, never fewer).
    /// Returns `None` if this type doesn't support splitting at all, or
    /// `range` itself is already empty (`range.lower > range.upper`) —
    /// there's nothing to divide up either way.
    fn split_evenly(range: Interval<Self>, fanout: usize) -> Option<Vec<Interval<Self>>> {
        let _ = (range, fanout);
        None
    }

    /// A cheap, exact count of how many values `range` covers — used by
    /// `dispatch_evenly` to decide whether a scan is even worth
    /// parallelizing *before* paying to split and dispatch it (splitting
    /// a handful of rows across a pool costs more in channel/oneshot
    /// overhead than it saves — see `bat_tree::scan_pool`'s
    /// `MIN_LEN_FOR_SPLIT_DISPATCH` for the measurement behind that).
    ///
    /// Same caveat as `split_evenly` — this is only meaningful when
    /// `range` is already the real, tight bounds of what's being
    /// scanned: for a bit-packed key's full type-level `MIN..MAX` span,
    /// this reports an enormous count regardless of how few rows are
    /// actually populated, so it can't tell "worth parallelizing" from
    /// "not" any better than `split_evenly` could balance the work.
    ///
    /// The default (`None`) means "no opinion" — `dispatch_evenly`
    /// interprets that permissively (proceeds as if the range were large
    /// enough), not as a reason to skip splitting.
    fn approx_len(range: Interval<Self>) -> Option<u64> {
        let _ = range;
        None
    }
}

/// Splits `[range.lower, range.upper]` into `fanout` contiguous,
/// near-equal-width numeric blocks (the earliest blocks absorbing any
/// remainder) by pure arithmetic — no knowledge of how `u64` values are
/// actually being used as a key.
///
/// That last point is exactly the hazard this trait's own doc warns
/// about: this impl is *safe and effective* when `range` is already the
/// real, tight bounds of the data being scanned (e.g. `[0, record_count)`
/// for a plain sequential-id table), but *not* a substitute for a
/// domain-aware partitioner when `range` is a bit-packed key's full
/// `MIN..MAX` type-level span — most of that span would just be unused
/// key space between real records, and bisecting it evenly would starve
/// most workers of any real work. Callers that already have a tight,
/// real range (most straightforward tables) get well-balanced splits for
/// free; callers scanning a sparse/bit-packed key's full nominal range
/// should keep using a hand-written partitioner instead (or first narrow
/// `range` to the real populated bounds before calling this).
impl RangeSplit for u64 {
    fn split_evenly(range: Interval<u64>, fanout: usize) -> Option<Vec<Interval<u64>>> {
        if range.lower > range.upper {
            return None;
        }
        let fanout = fanout.max(1);

        // u128 intermediates: `range == [0, u64::MAX]` would overflow a
        // `u64` span (`upper - lower + 1` wraps to 0) right at the one
        // input where getting this wrong is easiest to miss.
        let span = (range.upper as u128) - (range.lower as u128) + 1;
        let base = span / fanout as u128;
        let rem = span % fanout as u128;

        let mut ranges = Vec::with_capacity(fanout);
        let mut next = range.lower as u128;
        for i in 0..fanout {
            let count = base + if (i as u128) < rem { 1 } else { 0 };
            if count == 0 {
                ranges.push(Interval::new(range.upper, range.lower));
                continue;
            }
            let lo = next;
            let hi = next + count - 1;
            next = hi + 1;
            ranges.push(Interval::new(lo as u64, hi as u64));
        }
        Some(ranges)
    }

    fn approx_len(range: Interval<u64>) -> Option<u64> {
        if range.lower > range.upper {
            return Some(0);
        }
        // Same u128 overflow guard as `split_evenly`, then saturate back
        // down to `u64` — a caller gating on this only cares whether it
        // clears some threshold, so a saturated "however large `u64` can
        // say" for the one input (`[0, u64::MAX]`) that would otherwise
        // overflow is exactly as useful as the true value.
        let span = (range.upper as u128) - (range.lower as u128) + 1;
        Some(span.min(u64::MAX as u128) as u64)
    }
}
