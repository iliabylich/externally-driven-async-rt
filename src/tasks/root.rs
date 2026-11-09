use crate::tasks::TaskId;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Root {
    Unset,
    Running(TaskId),
    Finished,
}
