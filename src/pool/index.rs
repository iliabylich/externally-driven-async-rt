use core::{fmt, marker::PhantomData};

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

    pub(crate) fn slot(self) -> u32 {
        self.slot
    }

    pub(crate) fn generation(self) -> u32 {
        self.generation
    }

    pub(crate) fn to_bits(self) -> u64 {
        (u64::from(self.generation) << 32) | u64::from(self.slot)
    }

    pub(crate) fn from_bits(bits: u64) -> Self {
        Self {
            slot: bits as u32,
            generation: (bits >> 32) as u32,
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
