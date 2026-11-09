mod aligned16;
mod join_handle;
mod join_status;
mod root;
mod root_status;
mod task;
mod task_entry;
mod task_slot;
mod tasks_guard;
mod wake_queue;

use core::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    mem::MaybeUninit,
};

pub use self::join_handle::JoinHandle;
use self::{aligned16::Aligned16, task_slot::TaskSlot};
pub(crate) use self::{
    join_status::JoinStatus,
    root::Root,
    root_status::RootStatus,
    task::Task,
    task_entry::{TaskEntry, TaskId},
    tasks_guard::TasksGuard,
    wake_queue::WakeQueue,
};
use crate::{pool::ObjectPool, waker_slot::WakerSlot};

pub(crate) struct Tasks<
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
> {
    pool: ObjectPool<TaskEntry, MAX_TASKS_COUNT>,
    queue: WakeQueue<MAX_TASKS_COUNT>,
    slab: [Aligned16<MAX_TASK_SIZE>; MAX_TASKS_COUNT],
    root_region: Aligned16<MAX_ROOT_TASK_SIZE>,
    root: Cell<Root>,
}

impl<const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new(
                [const {
                    TaskEntry {
                        task: Cell::new(TaskSlot::Empty),
                        join_waiter: WakerSlot::new(),
                    }
                }; MAX_TASKS_COUNT],
            ),
            queue: WakeQueue::new(),
            slab: [const { Aligned16(UnsafeCell::new(MaybeUninit::uninit())) }; MAX_TASKS_COUNT],
            root_region: Aligned16(UnsafeCell::new(MaybeUninit::uninit())),
            root: Cell::new(Root::Unset),
        }
    }

    pub(crate) fn claim<'a>(
        &'static self,
    ) -> TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE> {
        self.queue.claim();
        TasksGuard {
            tasks: self,
            futures: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests;
