use core::{
    cell::Cell,
    future::Future,
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

    /// # Safety
    ///
    /// `self` must not move while it holds this task, and everything `Fut` borrows must outlive
    /// every poll and drop of the task.
    pub(crate) unsafe fn write<Fut>(&self, future: Fut)
    where
        Fut: Future<Output = ()>,
    {
        assert!(
            size_of::<Fut>() <= SIZE,
            "a task future of {} bytes doesn't fit TASK_SIZE ({SIZE} bytes)",
            size_of::<Fut>(),
        );
        assert!(
            align_of::<Fut>() <= 16,
            "a task future aligned to {} bytes doesn't fit (align at most 16)",
            align_of::<Fut>(),
        );
        assert!(!self.has_task(), "a reserved slot holds no task yet");
        // SAFETY: `Fut` fits the memory, which is aligned to 16 (both checked above). The memory
        // is unused: it holds no task (checked above), and only a `write` puts one there. It
        // stays valid and unused until the task is dropped: the task lives in `self`, which
        // doesn't move while holding it (guaranteed by the caller), and the next `write` asserts
        // the task is gone. Everything `Fut` borrows outlives every poll and drop of the task
        // (guaranteed by the caller).
        let task = unsafe { Task::write(self.memory.as_ptr(), future) };
        self.occupy(task);
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
