use core::{
    cell::Cell,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

use crate::{
    pool::Index,
    tasks::{Aligned16, Task, TaskSlot},
    waker_slot::WakerSlot,
};

pub(crate) struct TaskEntry<const SIZE: usize> {
    task: Cell<TaskSlot>,
    join_waiter: WakerSlot,
    memory: Aligned16<SIZE>,
}

impl<const SIZE: usize> TaskEntry<SIZE> {
    pub(crate) const fn new() -> Self {
        Self {
            task: Cell::new(TaskSlot::Empty),
            join_waiter: WakerSlot::new(),
            memory: Aligned16::new(),
        }
    }

    pub(crate) fn memory(&self) -> NonNull<u8> {
        self.memory.as_ptr()
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

    pub(crate) fn occupy(&self, task: Task) {
        self.task.set(TaskSlot::Idle(task));
        self.join_waiter.take();
    }

    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut task = match self.task.replace(TaskSlot::Polling) {
            TaskSlot::Idle(task) => task,
            TaskSlot::Polling => panic!("a task can't be polled re-entrantly"),
            TaskSlot::Empty => unreachable!("an allocated task always has a future"),
        };
        let poll = task.poll(cx);
        self.task.set(TaskSlot::Idle(task));
        poll
    }

    pub(crate) fn drop_task(&self) {
        let TaskSlot::Idle(task) = self.task.replace(TaskSlot::Empty) else {
            unreachable!("only an idle task is dropped");
        };
        drop(task);
    }

    pub(crate) fn set_join_waiter(&self, waker: &Waker) {
        self.join_waiter.store(waker);
    }

    pub(crate) fn take_join_waiter(&self) -> Option<Waker> {
        self.join_waiter.take()
    }
}

pub(crate) type TaskId<const SIZE: usize> = Index<TaskEntry<SIZE>>;
