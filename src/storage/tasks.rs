use core::{alloc::Layout, cell::Cell, future::Future, ptr::NonNull, task::Waker};

use crate::{
    capacities::Capacities,
    pool::{Index, ObjectPool},
    ready::ReadyQueue,
    root_region::RootRegion,
    storage::store_waker,
    task::{JoinStatus, OwnedTask, TaskPtr},
    task_slab::{TASK_ALIGN, TaskSlab},
    waker::new_waker,
};

#[derive(Default)]
pub(crate) struct TaskEntry {
    // `None` while the slot is reserved: while its future is written in (`insert`) or dropped
    // outside of the entry (`remove`), so a task spawned from the future's `Drop` can't take the
    // slot (and so its memory in the task slab).
    future: Cell<Option<OwnedTask>>,
    join_waiter: Cell<Option<Waker>>,
}

fn is_some<T>(cell: &Cell<Option<T>>) -> bool {
    let value = cell.take();
    let some = value.is_some();
    cell.set(value);
    some
}

pub(crate) type TaskId = Index<TaskEntry>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootStatus {
    Running,
    Finished,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Root {
    // Before `spawn_root`, the only state it accepts, so the root region never gets a second
    // future.
    Unset,
    Running(TaskId),
    Finished,
}

// Only shared references: the pool and the queue consist of `Cell`s, the slab and the root region
// are in `UnsafeCell`s.
pub(crate) struct Tasks {
    pool: &'static ObjectPool<TaskEntry>,
    ready: &'static ReadyQueue,
    slab: &'static TaskSlab,
    // The root's task slot still has a slab block, which stays unused.
    root_region: &'static RootRegion,
    root: Cell<Root>,
}

impl Tasks {
    /// # Safety
    ///
    /// As `ObjectPool::init`, `ReadyQueue::init`, `TaskSlab::init` and `RootRegion::init` for
    /// each pointer, with the memory valid (and otherwise unused) forever.
    pub(crate) unsafe fn init(
        pool_ptr: NonNull<u8>,
        ready_ptr: NonNull<u8>,
        slab_ptr: NonNull<u8>,
        root_region_ptr: NonNull<u8>,
        capacities: Capacities,
        root_layout: Layout,
    ) -> Self {
        // SAFETY: guaranteed by the caller.
        unsafe {
            Self {
                pool: ObjectPool::init(pool_ptr, capacities.max_tasks),
                ready: ReadyQueue::init(ready_ptr, capacities.max_tasks),
                slab: TaskSlab::init(slab_ptr, capacities.max_tasks, capacities.max_task_size),
                root_region: RootRegion::init(root_region_ptr, root_layout.size()),
                root: Cell::new(Root::Unset),
            }
        }
    }

    pub(crate) fn root(&self) -> Root {
        self.root.get()
    }

    #[cfg(test)]
    pub(crate) fn set_root(&self, root: Root) {
        self.root.set(root);
    }

    #[cfg(test)]
    pub(crate) fn ready(&self) -> &ReadyQueue {
        self.ready
    }

    /// Panics if the pool is full or the future doesn't fit the task slab's blocks.
    pub(crate) fn insert<Fut>(&self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'static,
    {
        let block_size = self.slab.block_size();
        let needed = Layout::new::<Fut>();
        if needed.size() > block_size || needed.align() > TASK_ALIGN {
            drop(future);
            panic!(
                "a task future of {} bytes (align {}) doesn't fit max_task_size ({} bytes after \
                 rounding up to a multiple of {TASK_ALIGN}, align at most {TASK_ALIGN})",
                needed.size(),
                needed.align(),
                block_size,
            );
        }
        let Some((id, entry)) = self.pool.alloc() else {
            drop(future);
            panic!("more than {} concurrent tasks", self.pool.capacity());
        };
        // SAFETY: `Fut` fits the slab's blocks (checked above). The slot's block is unused: the
        // slot was free, and a slot is only freed after its future was dropped. The slot stays
        // reserved until this future is dropped.
        let future = unsafe { OwnedTask::write(self.slab.block(id.slot()), future) };
        self.occupy(id, entry, future);
        id
    }

    /// Inserts the root future into its own region, queues it and makes it the root. Panics if
    /// called more than once.
    pub(crate) fn spawn_root<Fut>(&self, future: Fut)
    where
        Fut: Future<Output = ()> + 'static,
    {
        assert!(
            matches!(self.root.get(), Root::Unset),
            "the root is already spawned"
        );
        assert!(self.root_region.fits(Layout::new::<Fut>()));
        let Some((id, entry)) = self.pool.alloc() else {
            unreachable!("the root is the first task, the pool has room for it");
        };
        // SAFETY: `Fut` fits the root region (asserted above), and the region is unused: the root
        // is still `Unset` (asserted above), and leaves that state below, so this is the only
        // call that gets here.
        let future = unsafe { OwnedTask::write(self.root_region.memory(), future) };
        self.occupy(id, entry, future);
        self.ready.push(id.slot());
        self.root.set(Root::Running(id));
    }

    fn occupy(&self, id: TaskId, entry: &TaskEntry, future: OwnedTask) {
        entry.future.set(Some(future));
        entry.join_waiter.set(None);
        self.ready.track(id);
    }

    pub(crate) fn get(&self, id: TaskId) -> Option<TaskPtr> {
        let entry = self.pool.get(id)?;
        let future = entry.future.take()?;
        let ptr = future.ptr();
        entry.future.set(Some(future));
        Some(ptr)
    }

    /// Drops the task's future and frees its slot, returning its join waiter. The future is
    /// dropped while the slot is still reserved, since its `Drop` may use the storage.
    pub(crate) fn remove(&self, id: TaskId) -> Option<Option<Waker>> {
        let entry = self.pool.get(id)?;
        let future = entry.future.take()?;
        let join_waiter = entry.join_waiter.take();
        self.ready.untrack(id);
        drop(future);
        self.pool.free(id);
        Some(join_waiter)
    }

    pub(crate) fn set_join_waiter(&self, id: TaskId, waker: &Waker) -> JoinStatus {
        match self.pool.get(id) {
            Some(entry) if is_some(&entry.future) => {
                store_waker(&entry.join_waiter, waker);
                JoinStatus::Running
            }
            _ => JoinStatus::Finished,
        }
    }

    pub(crate) fn spawn<Fut>(&self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'static,
    {
        let id = self.insert(future);
        self.ready.push(id.slot());
        id
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Polls ready tasks until none is left or the root finishes, which marks it finished.
    pub(crate) fn run_ready(&self) -> RootStatus {
        loop {
            let Some(id) = self.ready.pop() else {
                return RootStatus::Running;
            };
            let Some(task) = self.get(id) else {
                continue;
            };
            // SAFETY: the ready queue lives in the storage's allocation, which outlives every
            // waker except deliberately leaked ones (documented as forbidden).
            let waker = unsafe { new_waker(self.ready, id) };
            let mut cx = core::task::Context::from_waker(&waker);
            // SAFETY: tasks are removed only right below (after the poll) and by `drop_all`,
            // which is never called from inside a task; `run_ready` isn't either, so the task
            // isn't polled re-entrantly.
            if unsafe { task.poll(&mut cx) }.is_pending() {
                continue;
            }
            let join_waiter = self
                .remove(id)
                .expect("a finished task is still in storage");
            if let Some(join_waiter) = join_waiter {
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

    /// Drops every task, so the pool holds nothing that needs dropping.
    pub(crate) fn drop_all(&self) {
        // Dropping a task may spawn new ones, they are dropped in later iterations.
        while let Some((id, _)) = self.pool.iter().next() {
            self.remove(id);
        }
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.pool.iter().count()
    }
}
