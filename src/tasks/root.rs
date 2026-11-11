use crate::tasks::TaskId;

#[derive(Debug, Clone, Copy)]
#[must_use]
pub(crate) enum Root<const TASK_SIZE: usize> {
    Unset,
    Running(TaskId<TASK_SIZE>),
    Finished,
}
