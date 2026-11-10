use crate::tasks::Task;

#[derive(Default)]
pub(crate) enum TaskSlot {
    #[default]
    Empty,
    Idle(Task),
    Polling,
}
