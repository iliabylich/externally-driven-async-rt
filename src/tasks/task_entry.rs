use core::{
    cell::Cell,
    future::Future,
    task::{Context, Poll, Waker},
};

use crate::{
    claimed::Claimed,
    const_default::ConstDefault,
    const_unreachable::const_unreachable,
    pool::VersionedIndex,
    tasks::{Task, TaskMemory, TaskSlot},
    waker_slot::WakerSlot,
};

#[must_use]
pub(crate) struct TaskEntry<const SIZE: usize> {
    task: Cell<TaskSlot>,
    join_waiter: WakerSlot,
    memory: TaskMemory<SIZE>,
}

impl<const SIZE: usize> TaskEntry<SIZE> {
    pub(crate) const fn new() -> Self {
        Self {
            task: Cell::new(TaskSlot::Empty),
            join_waiter: WakerSlot::new(),
            memory: TaskMemory::new(),
        }
    }

    pub(crate) fn write<'a, Fut>(&'static self, claimed: Claimed<'a>, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        let task = Task::write(&self.memory, claimed, future);
        self.occupy(task);
    }

    pub(crate) const fn has_task(&self) -> bool {
        let slot = self.task.replace(TaskSlot::DEFAULT);
        let has_task = !matches!(slot, TaskSlot::Empty);
        let empty = self.task.replace(slot);
        core::mem::forget(empty);
        has_task
    }

    fn occupy(&self, task: Task) {
        self.task.set(TaskSlot::Idle(task));
        self.join_waiter.take();
    }

    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Option<Poll<Task>> {
        let mut task = match self.task.replace(TaskSlot::Polling) {
            TaskSlot::Idle(task) => task,
            TaskSlot::Polling => const_unreachable!("a task can't be polled re-entrantly"),
            TaskSlot::Empty => {
                self.task.set(TaskSlot::Empty);
                return None;
            }
        };
        if task.poll(cx).is_pending() {
            self.task.set(TaskSlot::Idle(task));
            return Some(Poll::Pending);
        }
        self.task.set(TaskSlot::Empty);
        Some(Poll::Ready(task))
    }

    pub(crate) fn take_idle(&self) -> Option<Task> {
        match self.task.replace(TaskSlot::DEFAULT) {
            TaskSlot::Idle(task) => Some(task),
            slot => {
                self.task.set(slot);
                None
            }
        }
    }

    pub(crate) fn set_join_waiter(&self, waker: &Waker) {
        self.join_waiter.store(waker);
    }

    pub(crate) const fn take_join_waiter(&self) -> Option<Waker> {
        self.join_waiter.take()
    }
}

pub(crate) type TaskId<const SIZE: usize> = VersionedIndex<TaskEntry<SIZE>>;
