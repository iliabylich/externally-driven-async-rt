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

pub(crate) struct Tasks<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> {
    pool: ObjectPool<TaskEntry<TASK_SIZE>, TASKS>,
    queue: WakeQueue<TASKS>,
    root_region: Aligned16<ROOT_TASK_SIZE>,
    root: Cell<Root<TASK_SIZE>>,
    slot_waiter: WakerSlot,
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new([const { TaskEntry::new() }; TASKS]),
            queue: WakeQueue::new(),
            root_region: Aligned16::new(),
            root: Cell::new(Root::Unset),
            slot_waiter: WakerSlot::new(),
        }
    }
}

#[cfg(test)]
mod tests;
