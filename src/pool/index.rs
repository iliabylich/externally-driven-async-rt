use core::{fmt, marker::PhantomData};

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
