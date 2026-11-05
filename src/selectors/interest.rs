#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Interest {
    pub(crate) read: bool,
    pub(crate) write: bool,
}
