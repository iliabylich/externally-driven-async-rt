#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Token(u64);

impl Token {
    pub(crate) fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub(crate) fn to_bits(self) -> u64 {
        self.0
    }
}
