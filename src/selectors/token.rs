#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Token(pub(super) u64);

#[derive(Default)]
pub(super) struct TokenGenerator {
    next: u64,
}

impl TokenGenerator {
    pub(super) fn next(&mut self) -> Token {
        let token = Token(self.next);
        self.next += 1;
        token
    }
}
