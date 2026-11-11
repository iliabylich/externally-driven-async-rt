#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum JoinStatus {
    Running,
    Finished,
}
