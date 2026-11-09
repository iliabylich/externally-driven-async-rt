use crate::tasks::Task;

#[derive(Default)]
pub(super) enum TaskSlot {
    #[default]
    Empty,
    Idle(Task),
    Polling,
}
