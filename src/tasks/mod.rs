mod slot_waiter_taken_error;
mod task_entry;
mod task_reservation;
mod task_slot;
mod wake_queue;

use core::{
    mem::DropGuard,
    task::{Poll, Waker},
};

pub(crate) use self::wake_queue::RootSlot;
pub use self::{slot_waiter_taken_error::SlotWaiterTakenError, task_reservation::TaskReservation};
use self::{
    task_entry::{TaskEntry, TaskId},
    task_slot::TaskSlot,
    wake_queue::{WakeQueue, Woken},
};
use crate::{NotEnoughResources, claimed::Claimed, data::pool::ObjectPool, waker_slot::WakerSlot};

#[must_use]
pub(crate) struct Tasks<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> {
    pool: ObjectPool<TaskEntry<TASK_SIZE>, TASKS>,
    queue: WakeQueue<TASKS>,
    root: TaskEntry<ROOT_TASK_SIZE>,
    slot_waiter: WakerSlot,
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

    pub(crate) fn claim(&self) -> Option<RootSlot> {
        self.queue.claim()
    }

    pub(crate) const fn has_root(&self) -> bool {
        self.root.has_task()
    }

    pub(crate) const fn has_woken(&self) -> bool {
        !self.queue.is_empty()
    }

    fn set_join_waiter(&self, id: TaskId<TASK_SIZE>, waker: &Waker) -> Poll<()> {
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

    async fn join(&'static self, id: TaskId<TASK_SIZE>) {
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
        future: Fut,
    ) -> Result<
        impl Future<Output = ()> + use<TASKS, TASK_SIZE, ROOT_TASK_SIZE, Fut>,
        NotEnoughResources,
    >
    where
        Fut: Future<Output = ()> + 'a,
    {
        let id = self.spawn_task(claimed, future)?;
        Ok(self.join(id))
    }

    fn spawn_task<'a, Fut>(
        &'static self,
        claimed: Claimed<'a>,
        future: Fut,
    ) -> Result<TaskId<TASK_SIZE>, NotEnoughResources>
    where
        Fut: Future<Output = ()> + 'a,
    {
        let (id, entry) = self.pool.alloc().ok_or(NotEnoughResources::Tasks)?;
        entry.write(claimed, future);
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

    fn spawn_reserved<'a, Fut>(
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

    fn release(&self, id: TaskId<TASK_SIZE>) {
        self.pool.free(id);
        self.slot_waiter.wake_by_ref();
    }

    fn free(&self, id: TaskId<TASK_SIZE>, entry: &TaskEntry<TASK_SIZE>) -> Option<Waker> {
        let join_waiter = entry.take_join_waiter();
        self.release(id);
        join_waiter
    }

    pub(crate) fn run_ready(&'static self) -> Poll<()> {
        loop {
            match self.queue.pop() {
                None => return Poll::Pending,
                Some(Woken::Root) => {
                    if self.run_root().is_ready() {
                        return Poll::Ready(());
                    }
                }
                Some(Woken::Task(idx)) => self.run_task(idx),
            }
        }
    }

    fn run_root(&'static self) -> Poll<()> {
        let waker = self.queue.root_waker();
        let mut cx = core::task::Context::from_waker(&waker);
        if self.root.poll(&mut cx) == Some(Poll::Ready(())) {
            return Poll::Ready(());
        }
        Poll::Pending
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
        self.root.drop_future();
        while let Some((id, entry)) = self.pool.iter().find(|(_, entry)| entry.drop_future()) {
            self.free(id, entry);
        }
    }
}

// SAFETY: `Tasks` never leaves the crate. In the event loop it lives in a `LoopStorage`, which is
// only used from its loop's thread, and tests only use a static `Tasks` from the single test that
// declares it. Its `WakeQueue` also asserts that every waker is woken on the owner thread.
unsafe impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Sync
    for Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
}

#[cfg(test)]
mod tests;
