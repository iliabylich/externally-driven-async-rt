use crate::tasks::TaskId;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Root<const TASK_SIZE: usize> {
    Unset,
    Running(TaskId<TASK_SIZE>),
    Finished,
}
