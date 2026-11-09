use crate::selector::Token;

pub(crate) const EVENTS_PER_CALL: usize = if cfg!(test) { 4 } else { 64 };

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectorEvent {
    Readable(Token),
    Writable(Token),
    Timer,
}
