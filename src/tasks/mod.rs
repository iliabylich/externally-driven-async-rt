mod root_status;
mod slot_waiter_taken_error;
mod task_entry;
mod task_reservation;
mod task_slot;
mod wake_queue;

use core::{
    mem::DropGuard,
    task::{Poll, Waker},
};

use self::task_slot::TaskSlot;
pub(crate) use self::{
    root_status::RootStatus,
    task_entry::{TaskEntry, TaskId},
    wake_queue::{RootSlot, WakeQueue, Woken},
};
pub use self::{slot_waiter_taken_error::SlotWaiterTakenError, task_reservation::TaskReservation};
use crate::{NotEnoughResources, claimed::Claimed, data::pool::ObjectPool, waker_slot::WakerSlot};

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

    fn clear_join_waiter(&self, id: TaskId<TASK_SIZE>) {
        if let Some(entry) = self.pool.get_versioned(id) {
            entry.take_join_waiter();
        }
    }

    pub(crate) async fn join(&'static self, id: TaskId<TASK_SIZE>) {
        let guard = DropGuard::new(self, |tasks: &Self| tasks.clear_join_waiter(id));
        core::future::poll_fn(|cx| self.set_join_waiter(id, cx.waker())).await;
        DropGuard::dismiss(guard);
    }

    pub(crate) async fn reserve<'a>(
        &'static self,
        claimed: Claimed<'a>,
    ) -> Result<TaskReservation<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>, SlotWaiterTakenError> {
        if let Some((id, entry)) = self.pool.alloc() {
            return Ok(TaskReservation::new(self, claimed, id, entry));
        }
        if self.slot_waiter.is_set() {
            return Err(SlotWaiterTakenError);
        }
        let guard = DropGuard::new(self, |tasks: &Self| {
            tasks.slot_waiter.take();
        });
        let (id, entry) = core::future::poll_fn(|cx| {
            if let Some(allocated) = self.pool.alloc() {
                return Poll::Ready(allocated);
            }
            self.slot_waiter.store(cx.waker());
            Poll::Pending
        })
        .await;
        drop(guard);
        Ok(TaskReservation::new(self, claimed, id, entry))
    }

    pub(crate) fn spawn<'a, Fut>(
        &'static self,
        claimed: Claimed<'a>,
        f: impl FnOnce() -> Fut,
    ) -> Result<TaskId<TASK_SIZE>, NotEnoughResources>
    where
        Fut: Future<Output = ()> + 'a,
    {
        let (id, entry) = self.pool.alloc().ok_or(NotEnoughResources::Tasks)?;
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

    fn free(&self, id: TaskId<TASK_SIZE>, entry: &TaskEntry<TASK_SIZE>) -> Option<Waker> {
        let join_waiter = entry.take_join_waiter();
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
                Some(Woken::Task(idx)) => self.run_task(idx),
            }
        }
    }

    fn run_root(&'static self) -> RootStatus {
        let waker = self.queue.root_waker();
        let mut cx = core::task::Context::from_waker(&waker);
        if self.root.poll(&mut cx) == Some(Poll::Ready(())) {
            return RootStatus::Finished;
        }
        RootStatus::Running
    }

    fn run_task(&'static self, idx: u32) {
        let (id, entry) = self.pool.get(idx);
        let waker = self.queue.waker(idx);
        let mut cx = core::task::Context::from_waker(&waker);
        if entry.poll(&mut cx) != Some(Poll::Ready(())) {
            return;
        }
        if let Some(join_waiter) = self.free(id, entry) {
            join_waiter.wake();
        }
    }

    pub(crate) fn drop_all(&self) {
        self.root.drop_idle();
        while let Some((id, entry)) = self.pool.iter().find(|(_, entry)| entry.drop_idle()) {
            self.free(id, entry);
        }
    }
}

#[cfg(test)]
mod tests;
