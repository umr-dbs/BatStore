use std::fmt::Display;
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Release};
use crate::bat_query::SnapShot;
use crate::bat_record_model::version_info::Version;
use crate::bat_root::tree_root::ValueRootInner;

pub(crate) type FrugalRootList<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload
> = AtomicFrugalList<ValueRootInner<FAN_OUT, NUM_RECORDS, Key, Payload>>;

type TowerLevel = usize;
// Non-owning: aliases a node whose allocation is owned by some `next` link.
type FrugalNodeLink<Payload> = Option<NonNull<FrugalNodeSt<Payload>>>;

const FLAT_LEVEL: TowerLevel = 0; // all linear links
const SENTINEL_LEVEL: TowerLevel = TowerLevel::MAX; // head starter node

fn pick_level(p: f64) -> TowerLevel {
    let mut lvl: TowerLevel = 1;          // tower nodes start at 1; FLAT_LEVEL=0 is for append_next
    while rand::random_bool(p) { lvl += 1; }
    lvl
}

pub struct AtomicFrugalList<
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    // Appends are already serialized by the write latch of the enclosing
    // SmartCell (see index_root::RootIndex::append_root / smo::split_root's
    // `_master_guard`), so a plain store/load pair is sufficient here - no
    // CAS retry loop needed for a single-writer/many-readers publish.
    head: AtomicPtr<FrugalNodeSt<Payload>>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Default for AtomicFrugalList<Payload> {
    fn default() -> Self {
        Self { head: AtomicPtr::new(Box::into_raw(Box::new(FrugalNodeSt::default()))) }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Drop for AtomicFrugalList<Payload> {
    fn drop(&mut self) {
        // Walk the owning `next` chain iteratively (not recursively, to avoid
        // stack overflow on a long history) freeing each node exactly once.
        // `v_ridgy` is a non-owning alias into this same chain and must not
        // be freed through.
        let mut curr = NonNull::new(*self.head.get_mut());
        while let Some(node_ptr) = curr {
            let mut boxed = unsafe { Box::from_raw(node_ptr.as_ptr()) };
            curr = boxed.next.take();
        }
    }
}

#[derive(Default)]
pub struct FrugalNodeSt<
    Payload: Clone + Default + Display + Send + Sync + 'static>
{
    pub next: FrugalNodeLink<Payload>, // owning: freed by AtomicFrugalList::drop
    pub v_ridgy: FrugalNodeLink<Payload>, // non-owning skip link to a prior version

    pub payload: Payload,
    pub insert_version: Version,
    pub level: TowerLevel,
}

// Sound because a node, once published via `head.store`, is never mutated
// and never individually freed - the whole chain is freed at once by
// AtomicFrugalList::drop, which requires exclusive (&mut) access to the list.
unsafe impl<Payload: Clone + Default + Display + Send + Sync + 'static> Send for FrugalNodeSt<Payload> {}
unsafe impl<Payload: Clone + Default + Display + Send + Sync + 'static> Sync for FrugalNodeSt<Payload> {}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicFrugalList<Payload>
{
    #[inline(always)]
    fn head_ref(&self) -> &FrugalNodeSt<Payload> {
        unsafe { &*self.head.load(Acquire) }
    }

    #[inline(always)]
    pub fn current_root(&self) -> (Payload, SnapShot) {
        let head = self.head_ref();

        (head.payload.clone(), head.insert_version)
    }

    #[inline(always)]
    pub fn iter(&self) -> FrugalVersionIterator<'_, Payload> {
        FrugalVersionIterator {
            current: Some(self.head_ref())
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: Version) -> Self {
        let sentinel = FrugalNodeSt::new(
            payload,
            insert_version,
            SENTINEL_LEVEL // acts as sentinel, i.e., any coin toss matches a v_ridgy eventually
        );

        Self {
            head: AtomicPtr::new(Box::into_raw(Box::new(sentinel)))
        }
    }

    #[inline(always)]
    pub fn push(&self, payload: Payload, insert_version: Version) {
        self.append(payload, insert_version);
    }

    #[inline]
    fn append(&self, payload: Payload, insert_version: Version) {
        const COIN_TOSS_PROBABILITY: f64 = 0.5;

        if rand::random_bool(COIN_TOSS_PROBABILITY) {
            self.append_tower(payload, insert_version)
        }
        else {
            self.append_next(payload, insert_version)
        }
    }

    #[inline(always)]
    fn append_next(&self, payload: Payload, insert_version: Version) {
        let head
            = NonNull::new(self.head.load(Acquire)).unwrap();

        let new_head = Box::new(FrugalNodeSt::new_with(
            payload,
            insert_version,
            FLAT_LEVEL,
            Some(head), // next
            Some(head), // v_ridgy
        ));

        self.head.store(Box::into_raw(new_head), Release);
    }

    #[inline(always)]
    fn append_tower(&self, payload: Payload, insert_version: Version) {
        let head
            = NonNull::new(self.head.load(Acquire)).unwrap();

        let mut curr = head;

        let new_tower_level
            = pick_level(0.5);

        while unsafe { curr.as_ref() }.level < new_tower_level {
            curr = match unsafe { curr.as_ref() }.v_ridgy {
                Some(next) => next,
                None => unreachable!("frugal sentinel never seen!")
            };
        }

        let new_tower_node = Box::new(FrugalNodeSt::new_with(
            payload,
            insert_version,
            new_tower_level,
            Some(head), // next
            Some(curr), // v_ridgy
        ));

        self.head.store(Box::into_raw(new_tower_node), Release);
    }

    #[inline(always)]
    pub fn find_from(curr: &FrugalNodeSt<Payload>,
                     look_up_version: Version) -> Option<&FrugalNodeSt<Payload>>
    {
        let mut curr = curr;

        while curr.level < SENTINEL_LEVEL && curr.insert_version > look_up_version {
            curr = match curr.v_ridgy {
                Some(v_ridgy) if unsafe { v_ridgy.as_ref() }.insert_version > look_up_version =>
                    unsafe { v_ridgy.as_ref() },
                _ => unsafe { curr.next.unwrap().as_ref() },
            };
        }

        (curr.insert_version <= look_up_version).then_some(curr)
    }

    #[inline]
    pub fn find(&self, look_up_version: Version) -> Option<&FrugalNodeSt<Payload>> {
        Self::find_from(self.head_ref(), look_up_version)
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> FrugalNodeSt<Payload>
{
    #[inline(always)]
    fn new(payload: Payload, insert_version: Version, level: TowerLevel) -> Self {
        Self::new_with(payload, insert_version, level, None, None)
    }

    #[inline(always)]
    pub fn new_with(payload: Payload,
                    insert_version: Version,
                    level: TowerLevel,
                    next: FrugalNodeLink<Payload>,
                    v_ridgy: FrugalNodeLink<Payload>) -> Self
    {
        Self {
            next,
            v_ridgy,
            payload,
            insert_version,
            level,
        }
    }
}

pub struct FrugalVersionIterator<
    'a,
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    current: Option<&'a FrugalNodeSt<Payload>>,
}

impl<'a, Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for FrugalVersionIterator<'a, Payload> {
    type Item = &'a FrugalNodeSt<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        let curr = self.current.take()?;
        self.current = curr.next.map(|p| unsafe { p.as_ref() });
        Some(curr)
    }
}
