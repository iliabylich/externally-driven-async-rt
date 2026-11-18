use core::{fmt, marker::PhantomData};

const _: () = assert!(usize::BITS == 64, "only 64-bit targets are supported");

#[must_use]
pub(crate) struct VersionedIndex<T> {
    pub(crate) idx: u32,
    pub(crate) version: u16,
    _marker: PhantomData<fn() -> T>,
}

impl<T> VersionedIndex<T> {
    pub(crate) fn new(idx: u32, version: u16) -> Self {
        Self {
            idx,
            version,
            _marker: PhantomData,
        }
    }

    pub(crate) const fn pack(self) -> usize {
        let [i1, i2, i3, i4] = self.idx.to_le_bytes();
        let [v1, v2] = self.version.to_le_bytes();
        usize::from_le_bytes([i1, i2, i3, i4, v1, v2, 0, 0])
    }

    pub(crate) const fn unpack(bits: usize) -> Self {
        let [i1, i2, i3, i4, v1, v2, _, _] = bits.to_le_bytes();
        Self {
            idx: u32::from_le_bytes([i1, i2, i3, i4]),
            version: u16::from_le_bytes([v1, v2]),
            _marker: PhantomData,
        }
    }
}

impl<T> Clone for VersionedIndex<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for VersionedIndex<T> {}

impl<T> PartialEq for VersionedIndex<T> {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx && self.version == other.version
    }
}

impl<T> Eq for VersionedIndex<T> {}

impl<T> fmt::Debug for VersionedIndex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VersionedIndex({}v{})", self.idx, self.version)
    }
}
