use std::hash::Hash;
use std::ptr;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use crate::mv_record_model::version_info::VersionInfo;

// pub type Payload = Box<()>;

/// Stores a `Payload` value either inlined directly — bit-transmuted into a
/// `usize`-sized slot, no allocation at all — when `Payload` happens to
/// already be exactly `usize`-sized and no more strictly aligned (true for
/// every payload type this codebase currently uses, e.g. `u64`), or behind
/// a single raw, heap-allocated pointer otherwise. A genuinely
/// variable-length payload (e.g. one meant to represent a vector) should be
/// modeled as its own `Payload` type that manages a raw, length-prefixed
/// buffer directly (`[u64 length][items...]`) rather than embedding
/// something like `std::vec::Vec<T>`, which would already be its own extra
/// indirection underneath this one.
///
/// `Clone` clones the *referenced* `Payload` value and re-wraps it — never a
/// shallow copy of the raw slot, which for the boxed case would let two
/// `RecordPoint`s alias (and corrupt each other's, or double-free, the same
/// payload). Every live code path in this codebase respects that: a
/// `PayloadSlot` is either moved by value (sole ownership transferred, e.g.
/// `LeafPage::push_uncommitted`) or `.clone()`'d into a fresh, independent
/// allocation (e.g. `LeafPage::bulk_push`/`bulk_push_from_slice_ref`/`from`,
/// used when a split copies live records into a new page) — never
/// byte-copied/aliased. `Drop` below relies on that invariant.
struct PayloadSlot<Payload> {
    raw: usize,
    _marker: PhantomData<Payload>,
}

impl<Payload> PayloadSlot<Payload> {
    const INLINE: bool =
        size_of::<Payload>() == size_of::<usize>() && align_of::<Payload>() <= align_of::<usize>();

    #[inline(always)]
    fn new(payload: Payload) -> Self {
        let raw = if Self::INLINE {
            let mut raw: usize = 0;
            unsafe { (&mut raw as *mut usize as *mut Payload).write(payload) };
            raw
        } else {
            Box::into_raw(Box::new(payload)) as usize
        };

        Self { raw, _marker: PhantomData }
    }

    #[inline(always)]
    fn get(&self) -> &Payload {
        unsafe {
            if Self::INLINE {
                &*(&self.raw as *const usize as *const Payload)
            } else {
                &*(self.raw as *const Payload)
            }
        }
    }

    #[inline(always)]
    fn get_mut(&mut self) -> &mut Payload {
        unsafe {
            if Self::INLINE {
                &mut *(&mut self.raw as *mut usize as *mut Payload)
            } else {
                &mut *(self.raw as *mut Payload)
            }
        }
    }
}

impl<Payload> Drop for PayloadSlot<Payload> {
    /// Frees the boxed payload (a no-op for the inline case, which never
    /// allocated one). Sound under this struct's doc invariant: nothing
    /// else ever holds a copy of `raw` for a boxed payload, so exactly one
    /// `PayloadSlot` reaches this `drop` per allocation.
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            if Self::INLINE {
                ptr::drop_in_place(&mut self.raw as *mut usize as *mut Payload);
            } else {
                drop(Box::from_raw(self.raw as *mut Payload));
            }
        }
    }
}

impl<Payload: Clone> Clone for PayloadSlot<Payload> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self::new(self.get().clone())
    }
}

impl<Payload: Default> Default for PayloadSlot<Payload> {
    #[inline(always)]
    fn default() -> Self {
        Self::new(Payload::default())
    }
}

#[derive(Default, Clone)]
// #[repr(packed)]
pub struct RecordPoint<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> {
    pub key: Key,
    pub version: VersionInfo,
    payload: PayloadSlot<Payload>,
}

pub struct RecordPointResult<Key: Ord + Copy + Hash + Default, Payload: Clone> {
    pub key: Key,
    pub payload: Payload,
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordPointResult<Key, Payload> {
    #[inline]
    pub fn from(r: &RecordPoint<Key, Payload>) -> Self {
        Self {
            key: r.key(),
            payload: r.payload().clone()
        }
    }

    #[inline]
    pub const fn new(key: Key, payload: Payload) -> Self {
        Self {
            key,
            payload
        }
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordPoint<Key, Payload> {
    #[inline(always)]
    pub fn new(key: Key, version: VersionInfo, payload: Payload) -> Self {
        Self {
            key,
            version,
            payload: PayloadSlot::new(payload),
        }
    }

    #[inline(always)]
    pub const fn key(&self) -> Key {
        self.key
    }

    #[inline(always)]
    pub const fn key_ref(&self) -> &Key {
        &self.key
    }

    #[inline(always)]
    pub const fn version(&self) -> &VersionInfo {
        &self.version
    }

    #[inline(always)]
    pub fn payload(&self) -> &Payload {
        self.payload.get()
    }

    #[inline(always)]
    pub(crate) fn payload_mut(&mut self) -> &mut Payload {
        self.payload.get_mut()
    }

    #[inline(always)]
    pub fn version_mut(&mut self) -> &mut VersionInfo {
        &mut self.version
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Clone + Default> Display
for RecordPoint<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPoint(Key: {}, Version: {})",
               self.key(),
               self.version())
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Clone> Display
for RecordPointResult<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPointResult(Key: {})", self.key)
    }
}