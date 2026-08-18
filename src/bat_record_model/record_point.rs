use crate::bat_record_model::version_info::VersionInfo;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, align_of, size_of};
use std::ptr;
use triomphe::Arc;

// pub type Payload = Box<()>;

/// Stores a `Payload` value either inlined directly — bit-transmuted into a
/// `usize`-sized slot, no allocation at all — when `Payload` happens to
/// already be exactly `usize`-sized and no more strictly aligned (true for
/// every payload type this codebase currently uses, e.g. `u64`), or behind
/// a single refcounted `triomphe::Arc` otherwise (`triomphe`, not
/// `std::sync`, since nothing here ever needs a `Weak` handle and dropping
/// that second counter halves the control-block overhead). A genuinely
/// variable-length payload (e.g. one meant to represent a vector) should be
/// modeled as its own `Payload` type that manages a raw, length-prefixed
/// buffer directly (`[u64 length][items...]`) rather than embedding
/// something like `std::vec::Vec<T>`, which would already be its own extra
/// indirection underneath this one.
///
/// `Clone` shares the *referenced* `Payload` value (a strong-count bump for
/// the non-inline case) rather than deep-copying it — deliberately, so
/// materializing a read result or copying a live record into a new page on
/// split (`LeafPage::bulk_push`/`bulk_push_from_slice_ref`/`from`) doesn't
/// pay for a full copy of e.g. a multi-field TPC-C row when nothing along
/// that path ever mutates it. Real, exclusive mutation only ever happens via
/// `set` below, which always installs a brand-new allocation rather than
/// writing through the (possibly shared) existing one — so nothing ever
/// observes a `PayloadSlot` change value out from under it. A raw
/// byte-copy of a `PayloadSlot` (as opposed to a proper `.clone()`) would
/// still be unsound — two slots believing they each own a strong count on
/// the same allocation without either having actually bumped it — `Drop`
/// below relies on every live `PayloadSlot` having gone through `new`/
/// `clone`/`set`, never a bitwise copy.
pub(crate) struct PayloadSlot<Payload> {
    raw: usize,
    _marker: PhantomData<Payload>,
}

impl<Payload> PayloadSlot<Payload> {
    const INLINE: bool =
        size_of::<Payload>() == size_of::<usize>() && align_of::<Payload>() <= align_of::<usize>();

    #[inline(always)]
    pub(crate) fn new(payload: Payload) -> Self {
        let raw = if Self::INLINE {
            let mut raw: usize = 0;
            unsafe { (&mut raw as *mut usize as *mut Payload).write(payload) };
            raw
        } else {
            Arc::into_raw(Arc::new(payload)) as usize
        };

        Self {
            raw,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn get(&self) -> &Payload {
        unsafe {
            if Self::INLINE {
                &*(&self.raw as *const usize as *const Payload)
            } else {
                &*(self.raw as *const Payload)
            }
        }
    }

    /// Installs `payload` as this slot's new value, dropping (releasing, for
    /// the refcounted case) whatever was here before. There is deliberately
    /// no `get_mut`/in-place mutation: the non-inline case is a
    /// `triomphe::Arc` that other `PayloadSlot`s may be concurrently
    /// sharing (see this type's doc), so handing out `&mut Payload` into it
    /// would let a mutation through one slot corrupt every other slot still
    /// reading the same allocation. `set` sidesteps that entirely by always
    /// allocating a fresh, uniquely-owned value rather than writing through
    /// the old one — exactly what both of its callers already do (a whole-
    /// value overwrite in the `update_in_place` fast path), so this changes
    /// no observable behavior, just how the replaced value is disposed of.
    #[inline(always)]
    pub(crate) fn set(&mut self, payload: Payload) {
        *self = Self::new(payload);
    }
}

impl<Payload> Drop for PayloadSlot<Payload> {
    /// Frees the inlined payload directly, or releases this slot's strong
    /// reference to the shared allocation (freeing it only if this was the
    /// last one) for the non-inline case — sound as long as every live
    /// `PayloadSlot` was produced by `new`/`clone`/`set`, per this type's
    /// doc, so `raw` always corresponds to a strong reference this slot
    /// genuinely holds.
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            if Self::INLINE {
                ptr::drop_in_place(&mut self.raw as *mut usize as *mut Payload);
            } else {
                drop(Arc::from_raw(self.raw as *const Payload));
            }
        }
    }
}

impl<Payload: Clone> Clone for PayloadSlot<Payload> {
    #[inline(always)]
    fn clone(&self) -> Self {
        let raw = if Self::INLINE {
            let mut raw: usize = 0;
            unsafe { (&mut raw as *mut usize as *mut Payload).write(self.get().clone()) };
            raw
        } else {
            // Bump the shared allocation's strong count instead of deep-
            // cloning the referenced `Payload` — `peek` reconstructs an
            // `Arc` view of the allocation this slot already (genuinely)
            // holds a strong reference to, purely to call `Arc::clone` on
            // it; wrapping it in `ManuallyDrop` stops that temporary's own
            // destructor from releasing the very reference `self` still
            // owns once this function returns.
            let peek = ManuallyDrop::new(unsafe { Arc::from_raw(self.raw as *const Payload) });
            Arc::into_raw(Arc::clone(&peek)) as usize
        };

        Self {
            raw,
            _marker: PhantomData,
        }
    }
}

impl<Payload: Default> Default for PayloadSlot<Payload> {
    #[inline(always)]
    fn default() -> Self {
        Self::new(Payload::default())
    }
}

/// Lets every existing `result.payload.as_stock()`/`.as_customer()`-style
/// call site (there are dozens, across every benchmark driver) keep working
/// unchanged now that `RecordPointResult::payload` holds a `PayloadSlot`
/// instead of an owned `Payload` — autoderef resolves those the same way it
/// would through a `Box`/`Arc`. No `DerefMut`: see `set`'s doc for why
/// nothing should ever get `&mut Payload` into a slot that may be sharing
/// its allocation with other clones.
impl<Payload> std::ops::Deref for PayloadSlot<Payload> {
    type Target = Payload;
    #[inline(always)]
    fn deref(&self) -> &Payload {
        self.get()
    }
}

/// Forwarded to the referenced `Payload`, same as `Deref` above — so
/// existing call sites comparing/printing a `RecordPointResult::payload`
/// against a plain `Payload` value (test assertions especially) keep
/// working unchanged.
impl<Payload: PartialEq> PartialEq for PayloadSlot<Payload> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<Payload: PartialEq> PartialEq<Payload> for PayloadSlot<Payload> {
    #[inline(always)]
    fn eq(&self, other: &Payload) -> bool {
        self.get() == other
    }
}

impl<Payload: std::fmt::Debug> std::fmt::Debug for PayloadSlot<Payload> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.get(), f)
    }
}

#[derive(Default, Clone)]
// #[repr(packed)]
pub struct RecordPoint<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> {
    pub key: Key,
    pub version: VersionInfo,
    payload: PayloadSlot<Payload>,
}

/// `payload` is a `PayloadSlot`, not a bare `Payload`: materializing a read
/// result this way — `from`'s whole reason to exist — is exactly the "clone
/// a record" path `PayloadSlot::clone` is optimized for (a strong-count bump
/// for non-inline payloads, see that type's doc), so a query returning
/// thousands of `RecordPointResult`s no longer deep-copies each one's row.
pub struct RecordPointResult<Key: Ord + Copy + Hash + Default, Payload: Clone> {
    pub key: Key,
    pub payload: PayloadSlot<Payload>,
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordPointResult<Key, Payload> {
    #[inline]
    pub fn from(r: &RecordPoint<Key, Payload>) -> Self {
        Self {
            key: r.key(),
            payload: r.payload.clone(),
        }
    }

    #[inline]
    pub fn new(key: Key, payload: Payload) -> Self {
        Self {
            key,
            payload: PayloadSlot::new(payload),
        }
    }

    #[inline(always)]
    pub(crate) fn from_payload_slot(key: Key, payload: &PayloadSlot<Payload>) -> Self {
        Self {
            key,
            payload: payload.clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn from_leaf(
        record: crate::bat_page_model::leaf_page::LeafRecordRef<'_, Key, Payload>,
    ) -> Self {
        Self::from_payload_slot(record.key(), record.payload_slot())
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
    pub(crate) fn set_payload(&mut self, payload: Payload) {
        self.payload.set(payload);
    }

    #[inline(always)]
    pub fn version_mut(&mut self) -> &mut VersionInfo {
        &mut self.version
    }

    #[inline(always)]
    pub(crate) fn into_parts(self) -> (Key, VersionInfo, PayloadSlot<Payload>) {
        (self.key, self.version, self.payload)
    }

    #[inline(always)]
    pub(crate) fn clone_from_leaf(
        record: crate::bat_page_model::leaf_page::LeafRecordRef<'_, Key, Payload>,
    ) -> Self {
        Self {
            key: record.key(),
            version: record.version().clone(),
            payload: record.payload_slot().clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn payload_slot(&self) -> &PayloadSlot<Payload> {
        &self.payload
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Clone + Default> Display
    for RecordPoint<Key, Payload>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordPoint(Key: {}, Version: {})",
            self.key(),
            self.version()
        )
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Clone> Display
    for RecordPointResult<Key, Payload>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPointResult(Key: {})", self.key)
    }
}
