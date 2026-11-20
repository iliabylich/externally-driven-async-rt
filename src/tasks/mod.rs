mod join_handle;
mod reserve_task;
mod root;
mod root_status;
mod slot_waiter_taken_error;
mod task;
mod task_entry;
mod task_memory;
mod task_reservation;
mod task_slot;
mod tasks_full_error;
mod wake_queue;

use core::{
    cell::Cell,
    task::{Poll, Waker},
};

pub use self::{
    join_handle::JoinHandle, reserve_task::ReserveTask,
    slot_waiter_taken_error::SlotWaiterTakenError, task_reservation::TaskReservation,
    tasks_full_error::TasksFullError,
};
pub(crate) use self::{
    root::Root,
    root_status::RootStatus,
    task::Task,
    task_entry::{TaskEntry, TaskId},
    wake_queue::WakeQueue,
};
use self::{task_memory::TaskMemory, task_slot::TaskSlot};
use crate::{claimed::Claimed, pool::ObjectPool, waker_slot::WakerSlot};

#[must_use]
pub(crate) struct Tasks<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> {
    pub(crate) pool: ObjectPool<TaskEntry<TASK_SIZE>, TASKS>,
    pub(crate) queue: WakeQueue<TASKS>,
    pub(crate) root_region: TaskMemory<ROOT_TASK_SIZE>,
    pub(crate) root: Cell<Root<TASK_SIZE>>,
    pub(crate) slot_waiter: WakerSlot,
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new([const { TaskEntry::new() }; TASKS]),
            queue: WakeQueue::new(),
            root_region: TaskMemory::new(),
            root: Cell::new(Root::Unset),
            slot_waiter: WakerSlot::new(),
        }
    }

    pub(crate) fn set_join_waiter(&self, id: TaskId<TASK_SIZE>, waker: &Waker) -> Poll<()> {
        if let Some(entry) = self.pool.get_versioned(id)
            && entry.has_task()
        {
            entry.set_join_waiter(waker);
            return Poll::Pending;
        }
        Poll::Ready(())
    }

    pub(crate) fn clear_join_waiter(&self, id: TaskId<TASK_SIZE>) {
        if let Some(entry) = self.pool.get_versioned(id) {
            entry.take_join_waiter();
        }
    }

    pub(crate) fn spawn<'a, Fut>(
        &'static self,
        claimed: Claimed<'a>,
        f: impl FnOnce() -> Fut,
    ) -> Result<TaskId<TASK_SIZE>, TasksFullError>
    where
        Fut: Future<Output = ()> + 'a,
    {
        let (id, entry) = self.pool.alloc().ok_or(TasksFullError)?;
        entry.write(claimed, f());
        self.queue.push(id.idx);
        Ok(id)
    }

    pub(crate) fn spawn_root<'a, Fut>(&'static self, claimed: Claimed<'a>, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        if !matches!(self.root.get(), Root::Unset) {
            unreachable!("the root is already spawned");
        }
        let Some((id, entry)) = self.pool.alloc() else {
            unreachable!("the root is the first task, the pool has room for it");
        };
        let task = Task::write(&self.root_region, claimed, future);
        entry.occupy(task);
        self.queue.push(id.idx);
        self.root.set(Root::Running(id));
    }

    pub(crate) fn spawn_reserved<'a, Fut>(
        &self,
        claimed: Claimed<'a>,
        id: TaskId<TASK_SIZE>,
        entry: &'static TaskEntry<TASK_SIZE>,
        future: Fut,
    ) where
        Fut: Future<Output = ()> + 'a,
    {
        entry.write(claimed, future);
        self.queue.push(id.idx);
    }

    pub(crate) fn release(&self, id: TaskId<TASK_SIZE>) {
        self.pool.free(id);
        self.slot_waiter.wake_by_ref();
    }

    fn free(&self, id: TaskId<TASK_SIZE>, entry: &TaskEntry<TASK_SIZE>) -> Option<Waker> {
        let join_waiter = entry.take_join_waiter();
        entry.drop_task();
        self.release(id);
        join_waiter
    }

    pub(crate) fn run_ready(&'static self) -> RootStatus {
        loop {
            let Some(slot) = self.queue.pop() else {
                return RootStatus::Running;
            };
            let (id, entry) = self.pool.get(slot);
            if !entry.has_task() {
                continue;
            }
            let waker = self.queue.waker(slot);
            let mut cx = core::task::Context::from_waker(&waker);
            if entry.poll(&mut cx).is_pending() {
                continue;
            }
            if let Some(join_waiter) = self.free(id, entry) {
                join_waiter.wake();
            }
            if let Root::Running(root) = self.root.get()
                && id == root
            {
                self.root.set(Root::Finished);
                return RootStatus::Finished;
            }
        }
    }

    pub(crate) fn drop_all(&self) {
        while let Some((id, entry)) = self.pool.iter().find(|(_, entry)| entry.is_idle()) {
            self.free(id, entry);
        }
    }
}

#[cfg(test)]
mod tests;
