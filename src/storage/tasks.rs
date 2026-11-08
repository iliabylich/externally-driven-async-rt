use core::{
    cell::{Cell, UnsafeCell},
    future::Future,
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::NonNull,
    task::Waker,
};

use crate::{
    current_thread_queue::CurrentThreadQueue,
    pool::{Index, ObjectPool},
    ready::ReadyQueue,
    storage::store_waker,
    task::{JoinStatus, OwnedTask, TaskPtr},
};

#[derive(Default)]
pub(crate) struct TaskEntry {
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

#[repr(C, align(16))]
struct Aligned16<const SIZE: usize>(UnsafeCell<MaybeUninit<[u8; SIZE]>>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootStatus {
    Running,
    Finished,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Root {
    Unset,
    Running(TaskId),
    Finished,
}

pub(crate) struct Tasks<
    'a,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
> {
    pool: ObjectPool<TaskEntry, MAX_TASKS_COUNT>,
    ready: ReadyQueue<MAX_TASKS_COUNT>,
    slab: [Aligned16<MAX_TASK_SIZE>; MAX_TASKS_COUNT],
    root_region: Aligned16<MAX_ROOT_TASK_SIZE>,
    root: Cell<Root>,
    futures: PhantomData<Cell<&'a ()>>,
}

impl<'a, const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    Tasks<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    pub(crate) fn init(place: &mut MaybeUninit<Self>) -> &Self {
        let ptr = place.as_mut_ptr();
        // SAFETY: `ptr` points to `place`, borrowed exclusively until the end; the fields are
        // disjoint, `&raw mut` creates no reference to them while they're uninitialized, and
        // `MaybeUninit<T>` has `T`'s layout.
        let (pool, ready, root, futures) = unsafe {
            (
                &mut *(&raw mut (*ptr).pool).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).ready).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).root).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).futures).cast::<MaybeUninit<_>>(),
            )
        };
        ObjectPool::<TaskEntry, MAX_TASKS_COUNT>::init(pool);
        ReadyQueue::<MAX_TASKS_COUNT>::init(ready);
        root.write(Cell::new(Root::Unset));
        futures.write(PhantomData::<Cell<&'a ()>>);
        // SAFETY: every field was written above, except the blocks, which are `MaybeUninit`.
        unsafe { place.assume_init_ref() }
    }

    pub(crate) fn root(&self) -> Root {
        self.root.get()
    }

    #[cfg(test)]
    pub(crate) fn set_root(&self, root: Root) {
        self.root.set(root);
    }

    #[cfg(test)]
    pub(crate) fn ready(&self) -> &ReadyQueue<MAX_TASKS_COUNT> {
        &self.ready
    }

    pub(crate) fn insert<Fut>(&self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'a,
    {
        assert!(
            size_of::<Fut>() <= MAX_TASK_SIZE,
            "a task future of {} bytes doesn't fit MAX_TASK_SIZE ({MAX_TASK_SIZE} bytes)",
            size_of::<Fut>(),
        );
        assert!(
            align_of::<Fut>() <= 16,
            "a task future aligned to {} bytes doesn't fit (align at most 16)",
            align_of::<Fut>(),
        );
        let Some((id, entry)) = self.pool.alloc() else {
            panic!("more than {MAX_TASKS_COUNT} concurrent tasks");
        };
        let memory = NonNull::from(&self.slab[id.slot() as usize].0).cast();
        // SAFETY: `Fut` fits the slab's blocks (checked above). The slot's block is unused: the
        // slot was free, and a slot is only freed after its future was dropped. The slot stays
        // reserved until this future is dropped. Futures are only polled and dropped through
        // `&self`, which can't outlive `'a`, and `Tasks` is invariant in `'a`, so `'a` can't be
        // shortened to accept futures that live less than the other ones.
        let future = unsafe { OwnedTask::write(memory, future) };
        self.occupy(id, entry, future);
        id
    }

    pub(crate) fn spawn_root<Fut>(&self, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        assert!(
            matches!(self.root.get(), Root::Unset),
            "the root is already spawned"
        );
        assert!(
            size_of::<Fut>() <= MAX_ROOT_TASK_SIZE,
            "the root future of {} bytes doesn't fit MAX_ROOT_TASK_SIZE \
             ({MAX_ROOT_TASK_SIZE} bytes)",
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
        // SAFETY: `Fut` fits the root region (asserted above), and the region is unused: the root
        // is still `Unset` (asserted above), and leaves that state below, so this is the only
        // call that gets here. `'a` holds as in `insert`.
        let future = unsafe { OwnedTask::write(NonNull::from(&self.root_region.0).cast(), future) };
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
        Fut: Future<Output = ()> + 'a,
    {
        let id = self.insert(future);
        self.ready.push(id.slot());
        id
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    pub(crate) fn run_ready(&self) -> RootStatus {
        loop {
            let Some(id) = self.ready.pop() else {
                return RootStatus::Running;
            };
            let Some(task) = self.get(id) else {
                continue;
            };
            // SAFETY: the ready queue lives in the loop's memory, which is either never freed
            // (`EventLoop::new`) or guaranteed by the caller of `EventLoop::new_in` to outlive
            // every waker that may be woken.
            let waker = unsafe {
                CurrentThreadQueue::<_, MAX_TASKS_COUNT>::waker(self.ready.link(id.slot()))
            };
            let mut cx = core::task::Context::from_waker(&waker);
            // SAFETY: tasks are removed only right below (after the poll) and by `drop_all`,
            // which is never called from inside a task; `run_ready` isn't either, so the task
            // isn't polled re-entrantly. `&self` can't outlive `'a`, which the task was written
            // for.
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

    pub(crate) fn drop_all(&self) {
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
    use core::{cell::Cell, mem::MaybeUninit, task::Waker};

    use crate::{
        storage::{Root, TaskId, Tasks},
        task::JoinStatus,
        test_utils::{DropFlag, flagged_task},
    };

    const MAX_TASK_SIZE: usize = 1024;

    type TestTasks<'a, const MAX_TASKS_COUNT: usize> = Tasks<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, 0>;

    #[test]
    fn removing_a_task_drops_its_future_and_returns_its_join_waiter() {
        let dropped = Cell::new(false);
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<2>::init(&mut place);
        let id = tasks.insert(flagged_task(&dropped));
        assert!(tasks.get(id).is_some());
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Running
        );
        let join_waiter = tasks.remove(id).unwrap();
        assert!(dropped.get());
        assert!(join_waiter.is_some());
        assert!(tasks.get(id).is_none());
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn finished_task_id_never_matches_a_reused_slot() {
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<1>::init(&mut place);
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
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<1>::init(&mut place);
        tasks.insert(async {});
        tasks.insert(async {});
    }

    #[test]
    fn drop_all_drops_every_future() {
        let dropped = Cell::new(false);
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<2>::init(&mut place);
        tasks.insert(flagged_task(&dropped));
        tasks.drop_all();
        assert!(dropped.get());
        assert_eq!(tasks.count(), 0);
    }

    struct SpawnOnDrop<'a> {
        tasks: &'a TestTasks<'a, 2>,
        id: &'a Cell<Option<TaskId>>,
        spawned: &'a Cell<Option<TaskId>>,
    }

    impl Drop for SpawnOnDrop<'_> {
        fn drop(&mut self) {
            let id = self.id.get().unwrap();
            let tasks = self.tasks;
            assert!(tasks.get(id).is_none());
            assert_eq!(
                tasks.set_join_waiter(id, Waker::noop()),
                JoinStatus::Finished
            );
            self.spawned.set(Some(tasks.insert(async {})));
        }
    }

    #[test]
    fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
        let id = Cell::new(None);
        let spawned = Cell::new(None);
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<2>::init(&mut place);
        let guard = SpawnOnDrop {
            tasks,
            id: &id,
            spawned: &spawned,
        };
        let dropping = tasks.insert(async move {
            let _guard = guard;
        });
        id.set(Some(dropping));
        tasks.remove(dropping).unwrap();
        let spawned = spawned.get().unwrap();
        assert_ne!(spawned.slot(), dropping.slot());
        assert!(tasks.get(spawned).is_some());
    }

    #[test]
    fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<2>::init(&mut place);
        let ids = [1u8, 2].map(|byte| {
            let data = [byte; MAX_TASK_SIZE - 16];
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
    #[should_panic(expected = "a task future of 8193 bytes doesn't fit MAX_TASK_SIZE")]
    fn a_too_big_future_panics() {
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<1>::init(&mut place);
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
    struct Align16;

    #[test]
    fn futures_up_to_task_size_fit() {
        let mut place = MaybeUninit::uninit();
        let tasks = Tasks::<2, 100, 0>::init(&mut place);
        let ids = [
            tasks.insert(FixedFuture([1u8; 100])),
            tasks.insert(FixedFuture((Align16, [2u8; 80]))),
        ];
        let mut cx = core::task::Context::from_waker(Waker::noop());
        for id in ids {
            // SAFETY: the task is alive and not polled re-entrantly.
            assert!(unsafe { tasks.get(id).unwrap().poll(&mut cx) }.is_ready());
        }
    }

    #[test]
    #[should_panic(expected = "a task future of 101 bytes doesn't fit MAX_TASK_SIZE (100 bytes)")]
    fn a_future_one_byte_bigger_than_task_size_panics() {
        let mut place = MaybeUninit::uninit();
        let tasks = Tasks::<1, 100, 0>::init(&mut place);
        tasks.insert(FixedFuture([0u8; 101]));
    }

    #[test]
    #[should_panic(expected = "a task future aligned to 32 bytes doesn't fit (align at most 16)")]
    fn an_overaligned_future_panics() {
        #[repr(align(32))]
        struct Aligned;
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<1>::init(&mut place);
        let aligned = Aligned;
        tasks.insert(async move {
            let _aligned = aligned;
        });
    }

    struct BigRoot<'a> {
        data: [u8; 9000],
        _flag: DropFlag<'a>,
    }

    impl Future for BigRoot<'_> {
        type Output = ();

        fn poll(
            self: core::pin::Pin<&mut Self>,
            _: &mut core::task::Context<'_>,
        ) -> core::task::Poll<()> {
            assert!(self.data.iter().all(|byte| *byte == 3));
            core::task::Poll::Pending
        }
    }

    #[test]
    fn root_region_holds_a_root_bigger_than_a_slab_block() {
        let dropped = Cell::new(false);
        let mut place = MaybeUninit::uninit();
        let tasks = Tasks::<2, MAX_TASK_SIZE, { size_of::<BigRoot>() }>::init(&mut place);
        let root = BigRoot {
            data: [3; 9000],
            _flag: DropFlag(&dropped),
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
        assert!(!dropped.get());
        tasks.drop_all();
        assert!(dropped.get());
    }

    #[test]
    #[should_panic(
        expected = "the root future of 1 bytes doesn't fit MAX_ROOT_TASK_SIZE (0 bytes)"
    )]
    fn a_too_big_root_panics() {
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<1>::init(&mut place);
        tasks.spawn_root(FixedFuture(0u8));
    }

    #[test]
    #[should_panic(expected = "the root is already spawned")]
    fn spawning_a_second_root_panics() {
        let mut place = MaybeUninit::uninit();
        let tasks = TestTasks::<2>::init(&mut place);
        tasks.spawn_root(core::future::pending::<()>());
        tasks.spawn_root(core::future::pending::<()>());
    }
}
