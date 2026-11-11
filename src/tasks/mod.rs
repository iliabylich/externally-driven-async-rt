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
mod wake_queue;

use core::{cell::Cell, task::Waker};

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
    wake_queue::WakeQueue,
};
use crate::{pool::ObjectPool, waker_slot::WakerSlot};

pub(crate) struct Tasks<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> {
    pub(crate) pool: ObjectPool<TaskEntry<TASK_SIZE>, TASKS>,
    pub(crate) queue: WakeQueue<TASKS>,
    pub(crate) root_region: Aligned16<ROOT_TASK_SIZE>,
    pub(crate) root: Cell<Root<TASK_SIZE>>,
    pub(crate) slot_waiter: WakerSlot,
}

// SAFETY: `Tasks` is crate-private, so only the crate can reach it, and the crate uses each one
// from a single thread: a loop's tasks only through its `Context`, which `Context::claim` creates
// on the claiming thread, and the `JoinHandle`s it hands out, both `!Send`; a test's tasks only
// from that test. Wakers only touch the atomic `WakeQueue`, which asserts that thread.
unsafe impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Sync
    for Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
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

    pub(crate) fn root(&self) -> Root<TASK_SIZE> {
        self.root.get()
    }

    pub(crate) fn set_join_waiter(&self, id: TaskId<TASK_SIZE>, waker: &Waker) -> JoinStatus {
        match self.pool.get(id) {
            Some(entry) if entry.has_task() => {
                entry.set_join_waiter(waker);
                JoinStatus::Running
            }
            _ => JoinStatus::Finished,
        }
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.queue.is_empty()
    }

    /// # Safety
    ///
    /// Everything `Fut` borrows must outlive every poll and drop of its task.
    pub(crate) unsafe fn spawn<Fut>(&'static self, future: Fut) -> TaskId<TASK_SIZE>
    where
        Fut: Future<Output = ()>,
    {
        let Some((id, entry)) = self.reserve() else {
            panic!("more than {TASKS} concurrent tasks");
        };
        // SAFETY: `entry` lives in `self`, which is `'static`, so it never moves. Everything
        // `Fut` borrows outlives every poll and drop of its task (guaranteed by the caller).
        unsafe { entry.write(future) };
        self.queue.push(id.slot());
        id
    }

    /// # Safety
    ///
    /// Everything `Fut` borrows must outlive every poll and drop of its task.
    pub(crate) unsafe fn spawn_root<Fut>(&'static self, future: Fut)
    where
        Fut: Future<Output = ()>,
    {
        assert!(
            matches!(self.root.get(), Root::Unset),
            "the root is already spawned"
        );
        assert!(
            size_of::<Fut>() <= ROOT_TASK_SIZE,
            "the root future of {} bytes doesn't fit ROOT_TASK_SIZE \
             ({ROOT_TASK_SIZE} bytes)",
            size_of::<Fut>(),
        );
        assert!(
            align_of::<Fut>() <= 16,
            "the root future aligned to {} bytes doesn't fit (align at most 16)",
            align_of::<Fut>(),
        );
        let Some((id, entry)) = self.pool.alloc() else {
            unreachable!("the root is the first task, the pool has room for it");
        };
        // SAFETY: `Fut` fits the root region (asserted above), which lives in `self`, so it is
        // `'static` and never moves. The region is unused: the root is still `Unset` (asserted
        // above), and leaves that state below, so this is the only call that gets here.
        // Everything `Fut` borrows outlives every poll and drop of its task (guaranteed by the
        // caller).
        let task = unsafe { Task::write(self.root_region.as_ptr(), future) };
        entry.occupy(task);
        self.queue.push(id.slot());
        self.root.set(Root::Running(id));
    }

    /// # Safety
    ///
    /// Everything `Fut` borrows must outlive every poll and drop of its task.
    pub(crate) unsafe fn spawn_reserved<Fut>(
        &self,
        id: TaskId<TASK_SIZE>,
        entry: &'static TaskEntry<TASK_SIZE>,
        future: Fut,
    ) where
        Fut: Future<Output = ()>,
    {
        // SAFETY: `entry` is `'static`, so it never moves. Everything `Fut` borrows outlives every
        // poll and drop of its task (guaranteed by the caller).
        unsafe { entry.write(future) };
        self.queue.push(id.slot());
    }

    pub(crate) fn reserve(&self) -> Option<(TaskId<TASK_SIZE>, &TaskEntry<TASK_SIZE>)> {
        self.pool.alloc()
    }

    pub(crate) fn release(&self, id: TaskId<TASK_SIZE>) {
        self.pool.free(id);
        self.slot_waiter.wake_by_ref();
    }

    pub(crate) fn has_slot_waiter(&self) -> bool {
        self.slot_waiter.is_set()
    }

    pub(crate) fn set_slot_waiter(&self, waker: &Waker) {
        self.slot_waiter.store(waker);
    }

    pub(crate) fn clear_slot_waiter(&self) {
        self.slot_waiter.take();
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
            let Some((id, entry)) = self.pool.get_at(slot) else {
                continue;
            };
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
