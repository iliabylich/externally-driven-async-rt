#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Token(usize);

impl Token {
    pub(crate) fn from_bits(bits: usize) -> Self {
        Self(bits)
    }

    pub(crate) fn to_bits(self) -> usize {
        self.0
    }
}
