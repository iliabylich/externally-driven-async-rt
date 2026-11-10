mod aligned16;
mod join_handle;
mod join_status;
mod reserve_task;
mod root;
mod root_status;
mod task;
mod task_entry;
mod task_reservation;
mod task_slot;
mod tasks_guard;
mod wake_queue;

use core::cell::Cell;

use self::{aligned16::Aligned16, task_slot::TaskSlot};
pub use self::{
    join_handle::JoinHandle, reserve_task::ReserveTask, task_reservation::TaskReservation,
};
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
    slot_waiter: WakerSlot,
}

impl<const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new([const { TaskEntry::new() }; MAX_TASKS_COUNT]),
            queue: WakeQueue::new(),
            slab: [const { Aligned16::new() }; MAX_TASKS_COUNT],
            root_region: Aligned16::new(),
            root: Cell::new(Root::Unset),
            slot_waiter: WakerSlot::new(),
        }
    }

    pub(crate) fn claim<'a>(
        &'static self,
    ) -> TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE> {
        self.queue.claim();
        TasksGuard::new(self)
    }
}

#[cfg(test)]
mod tests;
