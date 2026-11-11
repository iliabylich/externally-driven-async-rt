use core::{fmt, marker::PhantomData};

const _: () = assert!(usize::BITS == 64, "only 64-bit targets are supported");

#[must_use]
pub(crate) struct Index<T> {
    slot: u32,
    generation: u32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Index<T> {
    pub(crate) fn from_parts(slot: u32, generation: u32) -> Self {
        Self {
            slot,
            generation,
            _marker: PhantomData,
        }
    }

    pub(crate) const fn slot(self) -> u32 {
        self.slot
    }

    pub(crate) const fn generation(self) -> u32 {
        self.generation
    }

    pub(crate) const fn to_bits(self) -> usize {
        let [s0, s1, s2, s3] = self.slot.to_le_bytes();
        let [g0, g1, g2, g3] = self.generation.to_le_bytes();
        usize::from_le_bytes([s0, s1, s2, s3, g0, g1, g2, g3])
    }

    pub(crate) const fn from_bits(bits: usize) -> Self {
        let [s0, s1, s2, s3, g0, g1, g2, g3] = bits.to_le_bytes();
        Self {
            slot: u32::from_le_bytes([s0, s1, s2, s3]),
            generation: u32::from_le_bytes([g0, g1, g2, g3]),
            _marker: PhantomData,
        }
    }
}

impl<T> Clone for Index<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Index<T> {}

impl<T> PartialEq for Index<T> {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot && self.generation == other.generation
    }
}

impl<T> Eq for Index<T> {}

impl<T> fmt::Debug for Index<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index({}v{})", self.slot, self.generation)
    }
}
