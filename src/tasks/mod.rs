mod join_handle;
mod reserve_task;
mod root_status;
mod slot_waiter_taken_error;
mod task;
mod task_entry;
mod task_memory;
mod task_reservation;
mod task_slot;
mod tasks_full_error;
mod wake_queue;

use core::task::{Poll, Waker};

pub use self::{
    join_handle::JoinHandle, reserve_task::ReserveTask,
    slot_waiter_taken_error::SlotWaiterTakenError, task_reservation::TaskReservation,
    tasks_full_error::TasksFullError,
};
pub(crate) use self::{
    root_status::RootStatus,
    task::Task,
    task_entry::{TaskEntry, TaskId},
    wake_queue::{RootSlot, WakeQueue, Woken},
};
use self::{task_memory::TaskMemory, task_slot::TaskSlot};
use crate::{claimed::Claimed, data::pool::ObjectPool, waker_slot::WakerSlot};

#[must_use]
pub(crate) struct Tasks<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> {
    pub(crate) pool: ObjectPool<TaskEntry<TASK_SIZE>, TASKS>,
    pub(crate) queue: WakeQueue<TASKS>,
    pub(crate) root: TaskEntry<ROOT_TASK_SIZE>,
    pub(crate) slot_waiter: WakerSlot,
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new([const { TaskEntry::new() }; TASKS]),
            queue: WakeQueue::new(),
            root: TaskEntry::new(),
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

    pub(crate) fn spawn_root<'a, Fut>(&'static self, _: RootSlot, claimed: Claimed<'a>, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        self.root.write(claimed, future);
        self.queue.push_root();
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

    fn free(
        &self,
        id: TaskId<TASK_SIZE>,
        entry: &TaskEntry<TASK_SIZE>,
        task: Task,
    ) -> Option<Waker> {
        let join_waiter = entry.take_join_waiter();
        drop(task);
        self.release(id);
        join_waiter
    }

    pub(crate) fn run_ready(&'static self) -> RootStatus {
        loop {
            match self.queue.pop() {
                None => return RootStatus::Running,
                Some(Woken::Root) => {
                    if self.run_root() == RootStatus::Finished {
                        return RootStatus::Finished;
                    }
                }
                Some(Woken::Task(slot)) => self.run_task(slot),
            }
        }
    }

    fn run_root(&'static self) -> RootStatus {
        let waker = self.queue.root_waker();
        let mut cx = core::task::Context::from_waker(&waker);
        let Some(Poll::Ready(task)) = self.root.poll(&mut cx) else {
            return RootStatus::Running;
        };
        drop(task);
        RootStatus::Finished
    }

    fn run_task(&'static self, idx: u32) {
        let (id, entry) = self.pool.get(idx);
        let waker = self.queue.waker(idx);
        let mut cx = core::task::Context::from_waker(&waker);
        let Some(Poll::Ready(task)) = entry.poll(&mut cx) else {
            return;
        };
        if let Some(join_waiter) = self.free(id, entry, task) {
            join_waiter.wake();
        }
    }

    pub(crate) fn drop_all(&self) {
        if let Some(task) = self.root.take_idle() {
            drop(task);
        }
        while let Some((id, entry, task)) = self
            .pool
            .iter()
            .find_map(|(id, entry)| Some((id, entry, entry.take_idle()?)))
        {
            self.free(id, entry, task);
        }
    }
}

#[cfg(test)]
mod tests;
