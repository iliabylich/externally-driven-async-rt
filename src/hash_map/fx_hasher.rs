use core::hash::Hasher;

const MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

#[must_use]
pub(crate) struct FxHasher(u64);

impl FxHasher {
    pub(crate) const fn new() -> Self {
        Self(0)
    }
}

impl Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0.rotate_left(5) ^ u64::from(*byte)).wrapping_mul(MULTIPLIER);
        }
    }
}
