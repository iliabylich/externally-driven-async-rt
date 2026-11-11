use crate::tasks::Task;

#[derive(Default)]
#[must_use]
pub(crate) enum TaskSlot {
    #[default]
    Empty,
    Idle(Task),
    Polling,
}
