#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum RootStatus {
    Running,
    Finished,
}
