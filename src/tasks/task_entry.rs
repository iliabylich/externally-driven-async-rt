use core::cell::Cell;

use crate::{pool::Index, tasks::TaskSlot, waker_slot::WakerSlot};

pub(crate) struct TaskEntry {
    pub(super) task: Cell<TaskSlot>,
    pub(super) join_waiter: WakerSlot,
}

impl TaskEntry {
    fn peek<R>(&self, f: impl FnOnce(&TaskSlot) -> R) -> R {
        let slot = self.task.take();
        let result = f(&slot);
        self.task.set(slot);
        result
    }

    pub(super) fn has_task(&self) -> bool {
        self.peek(|slot| !matches!(slot, TaskSlot::Empty))
    }

    pub(super) fn is_idle(&self) -> bool {
        self.peek(|slot| matches!(slot, TaskSlot::Idle(_)))
    }
}

pub(crate) type TaskId = Index<TaskEntry>;
