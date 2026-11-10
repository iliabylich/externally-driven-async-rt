use core::{cell::Cell, task::Waker};

use crate::{pool::Index, tasks::TaskSlot, waker_slot::WakerSlot};

pub(crate) struct TaskEntry {
    task: Cell<TaskSlot>,
    join_waiter: WakerSlot,
}

impl TaskEntry {
    pub(crate) const fn new() -> Self {
        Self {
            task: Cell::new(TaskSlot::Empty),
            join_waiter: WakerSlot::new(),
        }
    }

    fn peek<R>(&self, f: impl FnOnce(&TaskSlot) -> R) -> R {
        let slot = self.task.take();
        let result = f(&slot);
        self.task.set(slot);
        result
    }

    pub(crate) fn has_task(&self) -> bool {
        self.peek(|slot| !matches!(slot, TaskSlot::Empty))
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.peek(|slot| matches!(slot, TaskSlot::Idle(_)))
    }

    pub(super) fn set_task(&self, slot: TaskSlot) {
        self.task.set(slot);
    }

    pub(super) fn replace_task(&self, slot: TaskSlot) -> TaskSlot {
        self.task.replace(slot)
    }

    pub(crate) fn set_join_waiter(&self, waker: &Waker) {
        self.join_waiter.store(waker);
    }

    pub(crate) fn take_join_waiter(&self) -> Option<Waker> {
        self.join_waiter.take()
    }
}

pub(crate) type TaskId = Index<TaskEntry>;
