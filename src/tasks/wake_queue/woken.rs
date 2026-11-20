#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum Woken {
    Root,
    Task(u32),
}
