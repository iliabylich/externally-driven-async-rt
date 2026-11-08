use core::{alloc::Layout, cell::Cell, future::Future, ptr::NonNull, task::Waker};

use crate::{
    pool::{Index, ObjectPool},
    ready::ReadyQueue,
    root_region::RootRegion,
    storage::{
        layout::{offset_by, sequence},
        store_waker,
    },
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
    pub(crate) fn layout(max_tasks: usize, max_task_size: usize, root_layout: Layout) -> Layout {
        Self::plan(max_tasks, max_task_size, root_layout).0
    }

    // The layout and the offset of each part: the pool, the ready queue, the slab, the root region.
    fn plan(max_tasks: usize, max_task_size: usize, root_layout: Layout) -> (Layout, [usize; 4]) {
        sequence([
            ObjectPool::<TaskEntry>::layout(max_tasks),
            ReadyQueue::layout(max_tasks),
            TaskSlab::layout(max_tasks, max_task_size),
            root_layout,
        ])
    }

    /// # Safety
    ///
    /// `ptr` must be valid for writes of `Self::layout(max_tasks, max_task_size, root_layout)`
    /// bytes, aligned to it, and stay valid (and otherwise unused) forever.
    pub(crate) unsafe fn init(
        ptr: NonNull<u8>,
        max_tasks: usize,
        max_task_size: usize,
        root_layout: Layout,
    ) -> Self {
        let (_, offsets) = Self::plan(max_tasks, max_task_size, root_layout);
        let [pool_ptr, ready_ptr, slab_ptr, root_region_ptr] =
            offsets.map(|offset| offset_by(ptr, offset));
        // SAFETY: `plan` puts each part inside the layout the caller guarantees `ptr` is valid
        // for, aligned for it and not overlapping the others.
        unsafe {
            Self {
                pool: ObjectPool::init(pool_ptr, max_tasks),
                ready: ReadyQueue::init(ready_ptr, max_tasks),
                slab: TaskSlab::init(slab_ptr, max_tasks, max_task_size),
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

#[cfg(test)]
mod tests {
    use core::{alloc::Layout, mem::MaybeUninit, task::Waker};

    use crate::{
        storage::{Root, TaskId, Tasks},
        task::JoinStatus,
        test_utils::{TEST_CAPACITIES, aligned_in, flagged_task, stack_memory},
    };

    // No cleanup needed: the tests' futures don't allocate, and those whose drop is checked are
    // dropped explicitly.
    fn tasks_in(memory: &mut [MaybeUninit<u8>], max_tasks: usize) -> Tasks {
        let root_layout = Layout::new::<()>();
        sized_tasks_in(
            memory,
            max_tasks,
            TEST_CAPACITIES.max_task_size,
            root_layout,
        )
    }

    fn sized_tasks_in(
        memory: &mut [MaybeUninit<u8>],
        max_tasks: usize,
        max_task_size: usize,
        root_layout: Layout,
    ) -> Tasks {
        let ptr = aligned_in(memory, Tasks::layout(max_tasks, max_task_size, root_layout));
        // SAFETY: `ptr` is aligned and valid for the layout, and every test keeps `memory` alive
        // for as long as the returned tasks.
        unsafe { Tasks::init(ptr, max_tasks, max_task_size, root_layout) }
    }

    #[test]
    fn removing_a_task_drops_its_future_and_returns_its_join_waiter() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 2);
        static mut DROPPED: bool = false;
        let id = tasks.insert(flagged_task(&raw mut DROPPED));
        assert!(tasks.get(id).is_some());
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Running
        );
        let join_waiter = tasks.remove(id).unwrap();
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
        assert!(join_waiter.is_some());
        assert!(tasks.get(id).is_none());
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn finished_task_id_never_matches_a_reused_slot() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 1);
        let first = tasks.insert(async {});
        tasks.remove(first);
        let second = tasks.insert(async {});
        assert_ne!(first, second);
        assert!(tasks.get(first).is_none());
        assert_eq!(
            tasks.set_join_waiter(first, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent tasks")]
    fn full_task_pool_panics() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 1);
        tasks.insert(async {});
        tasks.insert(async {});
    }

    #[test]
    fn drop_all_drops_every_future() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 2);
        static mut DROPPED: bool = false;
        tasks.insert(flagged_task(&raw mut DROPPED));
        tasks.drop_all();
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
        assert_eq!(tasks.count(), 0);
    }

    struct SpawnOnDrop {
        // The test's `Tasks`, which outlive the task.
        tasks: *const Tasks,
        // Set once the task was inserted.
        id: *mut Option<TaskId>,
        spawned: *mut Option<TaskId>,
    }

    impl Drop for SpawnOnDrop {
        fn drop(&mut self) {
            // SAFETY: points to a `static mut` of the running test, which only that test's thread
            // uses, and no reference to it exists.
            let id = unsafe { *self.id }.unwrap();
            // SAFETY: the test's `Tasks` outlive the task, and are only accessed through shared
            // references.
            let tasks = unsafe { &*self.tasks };
            assert!(tasks.get(id).is_none());
            assert_eq!(
                tasks.set_join_waiter(id, Waker::noop()),
                JoinStatus::Finished
            );
            let spawned = tasks.insert(async {});
            // SAFETY: as above.
            unsafe { *self.spawned = Some(spawned) };
        }
    }

    #[test]
    fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 2);
        static mut ID: Option<TaskId> = None;
        static mut SPAWNED: Option<TaskId> = None;
        let guard = SpawnOnDrop {
            tasks: &raw const tasks,
            id: &raw mut ID,
            spawned: &raw mut SPAWNED,
        };
        let dropping = tasks.insert(async move {
            let _guard = guard;
        });
        // SAFETY: only this test's thread uses `ID`, and no reference to it exists.
        unsafe { ID = Some(dropping) };
        tasks.remove(dropping).unwrap();
        // SAFETY: only this test's thread uses `SPAWNED`, and no reference to it exists.
        let spawned = unsafe { SPAWNED }.unwrap();
        assert_ne!(spawned.slot(), dropping.slot());
        assert!(tasks.get(spawned).is_some());
    }

    #[test]
    fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 2);
        let ids = [1u8, 2].map(|byte| {
            let data = [byte; TEST_CAPACITIES.max_task_size - 16];
            tasks.insert(async move {
                assert!(data.iter().all(|b| *b == byte));
            })
        });
        let mut cx = core::task::Context::from_waker(Waker::noop());
        for id in ids {
            // SAFETY: the task is alive and not polled re-entrantly.
            assert!(unsafe { tasks.get(id).unwrap().poll(&mut cx) }.is_ready());
        }
    }

    #[test]
    #[should_panic(expected = "a task future of 8193 bytes (align 1) doesn't fit max_task_size")]
    fn a_too_big_future_panics() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 1);
        let data = [0u8; 8192];
        tasks.insert(async move {
            let _data = data;
        });
    }

    struct FixedFuture<T>(T);

    impl<T> Future for FixedFuture<T> {
        type Output = ();

        fn poll(
            self: core::pin::Pin<&mut Self>,
            _: &mut core::task::Context<'_>,
        ) -> core::task::Poll<()> {
            core::task::Poll::Ready(())
        }
    }

    #[repr(align(16))]
    struct Aligned16;

    #[test]
    fn max_task_size_is_rounded_up_to_whole_16_byte_blocks() {
        let mut memory = stack_memory();
        let tasks = sized_tasks_in(&mut memory, 2, 100, Layout::new::<()>());
        let ids = [
            tasks.insert(FixedFuture([1u8; 112])),
            tasks.insert(FixedFuture((Aligned16, [2u8; 96]))),
        ];
        let mut cx = core::task::Context::from_waker(Waker::noop());
        for id in ids {
            // SAFETY: the task is alive and not polled re-entrantly.
            assert!(unsafe { tasks.get(id).unwrap().poll(&mut cx) }.is_ready());
        }
    }

    #[test]
    #[should_panic(
        expected = "a task future of 113 bytes (align 1) doesn't fit max_task_size (112 bytes"
    )]
    fn a_future_bigger_than_the_rounded_up_block_panics() {
        let mut memory = stack_memory();
        let tasks = sized_tasks_in(&mut memory, 1, 100, Layout::new::<()>());
        tasks.insert(FixedFuture([0u8; 113]));
    }

    #[test]
    #[should_panic(expected = "(align 32) doesn't fit max_task_size")]
    fn an_overaligned_future_panics() {
        #[repr(align(32))]
        struct Aligned;
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 1);
        let aligned = Aligned;
        tasks.insert(async move {
            let _aligned = aligned;
        });
    }

    struct BigRoot {
        data: [u8; 9000],
        dropped: *mut bool,
    }

    impl Future for BigRoot {
        type Output = ();

        fn poll(
            self: core::pin::Pin<&mut Self>,
            _: &mut core::task::Context<'_>,
        ) -> core::task::Poll<()> {
            assert!(self.data.iter().all(|byte| *byte == 3));
            core::task::Poll::Pending
        }
    }

    impl Drop for BigRoot {
        fn drop(&mut self) {
            // SAFETY: points to a `static mut` of the running test, which only that test's thread
            // uses, and no reference to it exists.
            unsafe { *self.dropped = true };
        }
    }

    #[test]
    fn root_region_holds_a_root_bigger_than_a_slab_block() {
        static mut DROPPED: bool = false;
        let mut memory = stack_memory();
        let tasks = sized_tasks_in(
            &mut memory,
            2,
            TEST_CAPACITIES.max_task_size,
            Layout::new::<BigRoot>(),
        );
        let root = BigRoot {
            data: [3; 9000],
            dropped: &raw mut DROPPED,
        };
        tasks.spawn_root(root);
        let Root::Running(id) = tasks.root() else {
            panic!("spawning the root makes it running");
        };
        let sibling = tasks.insert(async {});
        assert_ne!(sibling.slot(), id.slot());
        let mut cx = core::task::Context::from_waker(Waker::noop());
        // SAFETY: the task is alive and not polled re-entrantly.
        assert!(unsafe { tasks.get(id).unwrap().poll(&mut cx) }.is_pending());
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(!unsafe { DROPPED });
        tasks.drop_all();
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
    }

    #[test]
    #[should_panic(expected = "the root is already spawned")]
    fn spawning_a_second_root_panics() {
        let mut memory = stack_memory();
        let tasks = tasks_in(&mut memory, 2);
        tasks.spawn_root(core::future::pending::<()>());
        tasks.spawn_root(core::future::pending::<()>());
    }
}
