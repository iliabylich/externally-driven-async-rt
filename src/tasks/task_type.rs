#[derive(Debug, Clone, Copy)]
pub(crate) enum TaskType {
    Root,
    NonRoot,
}

impl TaskType {
    pub(crate) const fn future(self) -> &'static str {
        match self {
            Self::Root => "the root future",
            Self::NonRoot => "a task future",
        }
    }

    pub(crate) const fn size_const(self) -> &'static str {
        match self {
            Self::Root => "ROOT_TASK_SIZE",
            Self::NonRoot => "TASK_SIZE",
        }
    }
}
