use std::alloc::Layout;
use std::hash::Hash;
use std::{alloc, mem, ptr};
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::mem::{align_of, size_of, ManuallyDrop};
use std::ops::{Add, Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut, slice_from_raw_parts};
use crate::mv_record_model::unsafe_clone::UnsafeClone;
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
/// `RecordPoint`s alias (and corrupt each other's) the same payload once
/// either is mutated. Like every other allocation in this codebase, a
/// boxed payload is never explicitly freed (see `SmartCell`'s doc for why
/// that's sound while GC's block reclaim stays off) — dropping a
/// `PayloadSlot` is a no-op either way.
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

// impl<Key: Ord + Copy + Hash + Default> Drop for RecordPointResult<Key> {
//     fn drop(&mut self) {
//         unsafe {
//             let _ = Payload::from_raw(self.payload.as_mut());
//
//             // ManuallyDrop::drop(&mut self.payload)
//             // let layout = Layout::from_size_align_unchecked(
//             //     mem::size_of::<usize>(),
//             //     mem::align_of::<u8>());
//
//             // alloc::dealloc(self.payload.deref_mut().deref_mut(), layout);
//         }
//     }
// }

// impl<Key: Ord + Copy + Hash + Default> Clone for RecordPoint<Key> {
//     fn clone(&self) -> Self {
//         Self {
//             key: self.key(),
//             version: self.version().clone(),
//             payload: ManuallyDrop::new(self.payload().clone()),
//         }
//     }
// }

// impl<Key: Ord + Copy + Hash + Default> Drop for RecordPoint<Key> {
//     fn drop(&mut self) {
//         unsafe {
//             let _ = Payload::from_raw(self.payload.as_mut());
//             // ManuallyDrop::drop(&mut self.payload)
//
//             // let layout = Layout::from_size_align_unchecked(
//             //     mem::size_of::<usize>(),
//             //     mem::align_of::<u8>());
//             //
//             // alloc::dealloc(self.payload_mut().deref_mut(), layout);
//         }
//     }
// }

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

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> UnsafeClone
for RecordPoint<Key, Payload> {
    #[inline(always)]
    unsafe fn unsafe_clone(&self) -> Self {
        mem::transmute_copy(self)
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