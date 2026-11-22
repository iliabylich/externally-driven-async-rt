use crate::{const_default::ConstDefault, tasks::Task};

#[must_use]
pub(crate) enum TaskSlot {
    Empty,
    Idle(Task),
    Polling,
}

impl ConstDefault for TaskSlot {
    const DEFAULT: Self = Self::Empty;
}
