use super::Token;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectorEvent {
    Readable(Token),
    Writable(Token),
    Timer,
}
