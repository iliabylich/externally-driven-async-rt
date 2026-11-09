use core::{
    cell::{Cell, UnsafeCell},
    future::Future,
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::NonNull,
    task::Waker,
};

use crate::{
    pool::{Index, ObjectPool},
    storage::store_waker,
    task::{JoinStatus, Task},
    wake_queue::WakeQueue,
};

#[derive(Default)]
enum TaskSlot {
    #[default]
    Empty,
    Idle(Task),
    Polling,
}

pub(crate) struct TaskEntry {
    task: Cell<TaskSlot>,
    join_waiter: Cell<Option<Waker>>,
}

impl TaskEntry {
    fn peek<R>(&self, f: impl FnOnce(&TaskSlot) -> R) -> R {
        let slot = self.task.take();
        let result = f(&slot);
        self.task.set(slot);
        result
    }

    fn has_task(&self) -> bool {
        self.peek(|slot| !matches!(slot, TaskSlot::Empty))
    }

    fn is_idle(&self) -> bool {
        self.peek(|slot| matches!(slot, TaskSlot::Idle(_)))
    }
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
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
> {
    pool: ObjectPool<TaskEntry, MAX_TASKS_COUNT>,
    queue: WakeQueue<MAX_TASKS_COUNT>,
    slab: [Aligned16<MAX_TASK_SIZE>; MAX_TASKS_COUNT],
    root_region: Aligned16<MAX_ROOT_TASK_SIZE>,
    root: Cell<Root>,
}

impl<const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new(
                [const {
                    TaskEntry {
                        task: Cell::new(TaskSlot::Empty),
                        join_waiter: Cell::new(None),
                    }
                }; MAX_TASKS_COUNT],
            ),
            queue: WakeQueue::new(),
            slab: [const { Aligned16(UnsafeCell::new(MaybeUninit::uninit())) }; MAX_TASKS_COUNT],
            root_region: Aligned16(UnsafeCell::new(MaybeUninit::uninit())),
            root: Cell::new(Root::Unset),
        }
    }

    pub(crate) fn claim<'a>(
        &'static self,
    ) -> TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE> {
        self.queue.claim();
        TasksGuard {
            tasks: self,
            futures: PhantomData,
        }
    }
}

pub(crate) struct TasksGuard<
    'a,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
> {
    tasks: &'static Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
    futures: PhantomData<(Cell<&'a ()>, *const ())>,
}

impl<'a, const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    pub(crate) fn root(self) -> Root {
        self.tasks.root.get()
    }

    #[cfg(test)]
    pub(crate) fn set_root(self, root: Root) {
        self.tasks.root.set(root);
    }

    #[cfg(test)]
    pub(crate) fn queue(self) -> &'static WakeQueue<MAX_TASKS_COUNT> {
        &self.tasks.queue
    }

    #[cfg(test)]
    pub(crate) fn contains(self, id: TaskId) -> bool {
        self.tasks.pool.get(id).is_some_and(TaskEntry::has_task)
    }

    pub(crate) fn set_join_waiter(self, id: TaskId, waker: &Waker) -> JoinStatus {
        match self.tasks.pool.get(id) {
            Some(entry) if entry.has_task() => {
                store_waker(&entry.join_waiter, waker);
                JoinStatus::Running
            }
            _ => JoinStatus::Finished,
        }
    }

    pub(crate) fn has_ready(self) -> bool {
        !self.tasks.queue.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn count(self) -> usize {
        self.tasks.pool.iter().count()
    }

    pub(crate) fn insert<Fut>(self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'a,
    {
        let tasks = self.tasks;
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
        let Some((id, entry)) = tasks.pool.alloc() else {
            panic!("more than {MAX_TASKS_COUNT} concurrent tasks");
        };
        let memory = NonNull::from(&tasks.slab[id.slot() as usize].0).cast();
        // SAFETY: `Fut` fits the slab's blocks (checked above). The slot's block is unused: the
        // slot was free, and a slot is only freed after its future was dropped. The slot stays
        // reserved until this future is dropped. `Tasks` keeps its futures in private fields
        // that only `TasksGuard` touches, and `Tasks::claim` creates a guard once per `Tasks`
        // (copies share its invariant `'a`), so every future in it was written through a guard
        // with this same `'a` (which `Fut` outlives), and is polled and dropped only through
        // one, before `'a` ends.
        let future = unsafe { Task::write(memory, future) };
        self.occupy(id, entry, future);
        id
    }

    pub(crate) fn spawn<Fut>(self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'a,
    {
        let id = self.insert(future);
        self.tasks.queue.push(id.slot());
        id
    }

    pub(crate) fn spawn_root<Fut>(self, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        let tasks = self.tasks;
        assert!(
            matches!(tasks.root.get(), Root::Unset),
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
        let Some((id, entry)) = tasks.pool.alloc() else {
            unreachable!("the root is the first task, the pool has room for it");
        };
        // SAFETY: `Fut` fits the root region (asserted above), and the region is unused: the root
        // is still `Unset` (asserted above), and leaves that state below, so this is the only
        // call that gets here. `Fut` outlives its use as in `insert`.
        let future = unsafe { Task::write(NonNull::from(&tasks.root_region.0).cast(), future) };
        self.occupy(id, entry, future);
        tasks.queue.push(id.slot());
        tasks.root.set(Root::Running(id));
    }

    fn occupy(self, id: TaskId, entry: &TaskEntry, future: Task) {
        entry.task.set(TaskSlot::Idle(future));
        entry.join_waiter.set(None);
        self.tasks.queue.track(id);
    }

    pub(crate) fn remove(self, id: TaskId) -> Option<Option<Waker>> {
        let entry = self.tasks.pool.get(id)?;
        let task = match entry.task.take() {
            TaskSlot::Idle(task) => task,
            TaskSlot::Polling => {
                entry.task.set(TaskSlot::Polling);
                return None;
            }
            TaskSlot::Empty => return None,
        };
        Some(self.free(id, entry, task))
    }

    fn free(self, id: TaskId, entry: &TaskEntry, task: Task) -> Option<Waker> {
        entry.task.set(TaskSlot::Empty);
        let join_waiter = entry.join_waiter.take();
        self.tasks.queue.untrack(id);
        drop(task);
        self.tasks.pool.free(id);
        join_waiter
    }

    pub(crate) fn run_ready(self) -> RootStatus {
        let tasks = self.tasks;
        loop {
            let Some(id) = tasks.queue.pop() else {
                return RootStatus::Running;
            };
            let Some(entry) = tasks.pool.get(id) else {
                continue;
            };
            let mut task = match entry.task.replace(TaskSlot::Polling) {
                TaskSlot::Idle(task) => task,
                TaskSlot::Polling => panic!("a task can't be polled re-entrantly"),
                TaskSlot::Empty => unreachable!("an allocated task always has a future"),
            };
            let waker = tasks.queue.waker(id.slot());
            let mut cx = core::task::Context::from_waker(&waker);
            if task.poll(&mut cx).is_pending() {
                entry.task.set(TaskSlot::Idle(task));
                continue;
            }
            if let Some(join_waiter) = self.free(id, entry, task) {
                join_waiter.wake();
            }
            if let Root::Running(root) = tasks.root.get()
                && id == root
            {
                tasks.root.set(Root::Finished);
                return RootStatus::Finished;
            }
        }
    }

    pub(crate) fn drop_all(self) {
        while let Some((id, _)) = self.tasks.pool.iter().find(|(_, entry)| entry.is_idle()) {
            self.remove(id);
        }
    }

    #[cfg(test)]
    pub(crate) fn waker(self, id: TaskId) -> Waker {
        self.tasks.queue.waker(id.slot())
    }
}

impl<const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    Clone for TasksGuard<'_, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize> Copy
    for TasksGuard<'_, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
}

#[cfg(test)]
mod tests {
    use core::{cell::Cell, task::Waker};

    use crate::{
        LoopStorage,
        storage::{LoopStorageGuard, Root, RootStatus, TaskId},
        task::JoinStatus,
        test_utils::{DropFlag, flagged_task},
    };

    const MAX_TASK_SIZE: usize = 1024;

    type TasksStorage<const MAX_TASKS_COUNT: usize> =
        LoopStorage<(), MAX_TASKS_COUNT, MAX_TASK_SIZE, 0, 1, 1, ()>;

    #[test]
    fn removing_a_task_drops_its_future_and_returns_its_join_waiter() {
        let dropped = Cell::new(false);
        let storage = {
            static STORAGE: TasksStorage<2> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        let tasks = &storage.tasks;
        let id = storage.tasks.insert(flagged_task(&dropped));
        assert!(tasks.contains(id));
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Running
        );
        let join_waiter = storage.tasks.remove(id).unwrap();
        assert!(dropped.get());
        assert!(join_waiter.is_some());
        assert!(!tasks.contains(id));
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn finished_task_id_never_matches_a_reused_slot() {
        let storage = {
            static STORAGE: TasksStorage<1> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        let tasks = &storage.tasks;
        let first = storage.tasks.insert(async {});
        storage.tasks.remove(first);
        let second = storage.tasks.insert(async {});
        assert_ne!(first, second);
        assert!(!tasks.contains(first));
        assert_eq!(
            tasks.set_join_waiter(first, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent tasks")]
    fn full_task_pool_panics() {
        let storage = {
            static STORAGE: TasksStorage<1> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        storage.tasks.insert(async {});
        storage.tasks.insert(async {});
    }

    #[test]
    fn drop_all_drops_every_future() {
        let dropped = Cell::new(false);
        let storage = {
            static STORAGE: TasksStorage<2> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        let tasks = &storage.tasks;
        storage.tasks.insert(flagged_task(&dropped));
        storage.tasks.drop_all();
        assert!(dropped.get());
        assert_eq!(tasks.count(), 0);
    }

    struct SpawnOnDrop<'a> {
        storage: LoopStorageGuard<'a, (), 2, MAX_TASK_SIZE, 0, 1, 1, ()>,
        id: &'a Cell<Option<TaskId>>,
        spawned: &'a Cell<Option<TaskId>>,
    }

    impl Drop for SpawnOnDrop<'_> {
        fn drop(&mut self) {
            let id = self.id.get().unwrap();
            let storage = self.storage;
            let tasks = &storage.tasks;
            assert!(!tasks.contains(id));
            assert_eq!(
                tasks.set_join_waiter(id, Waker::noop()),
                JoinStatus::Finished
            );
            self.spawned.set(Some(storage.tasks.insert(async {})));
        }
    }

    #[test]
    fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
        let id = Cell::new(None);
        let spawned = Cell::new(None);
        let storage = {
            static STORAGE: TasksStorage<2> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        let tasks = &storage.tasks;
        let guard = SpawnOnDrop {
            storage,
            id: &id,
            spawned: &spawned,
        };
        let dropping = storage.tasks.insert(async move {
            let _guard = guard;
        });
        id.set(Some(dropping));
        storage.tasks.remove(dropping).unwrap();
        let spawned = spawned.get().unwrap();
        assert_ne!(spawned.slot(), dropping.slot());
        assert!(tasks.contains(spawned));
    }

    #[test]
    fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
        let storage = {
            static STORAGE: TasksStorage<2> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        let tasks = &storage.tasks;
        let ids = [1u8, 2].map(|byte| {
            let data = [byte; MAX_TASK_SIZE - 16];
            storage.tasks.spawn(async move {
                assert!(data.iter().all(|b| *b == byte));
            })
        });
        assert_eq!(storage.tasks.run_ready(), RootStatus::Running);
        assert!(ids.iter().all(|id| !tasks.contains(*id)));
    }

    #[test]
    #[should_panic(expected = "a task future of 8193 bytes doesn't fit MAX_TASK_SIZE")]
    fn a_too_big_future_panics() {
        let storage = {
            static STORAGE: TasksStorage<1> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        let data = [0u8; 8192];
        storage.tasks.insert(async move {
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
        let storage = {
            static STORAGE: LoopStorage<(), 2, 100, 0, 1, 1, ()> = LoopStorage::new();
            STORAGE.claim((), ())
        };
        let tasks = &storage.tasks;
        let ids = [
            storage.tasks.spawn(FixedFuture([1u8; 100])),
            storage.tasks.spawn(FixedFuture((Align16, [2u8; 80]))),
        ];
        assert_eq!(storage.tasks.run_ready(), RootStatus::Running);
        assert!(ids.iter().all(|id| !tasks.contains(*id)));
    }

    #[test]
    #[should_panic(expected = "a task future of 101 bytes doesn't fit MAX_TASK_SIZE (100 bytes)")]
    fn a_future_one_byte_bigger_than_task_size_panics() {
        let storage = {
            static STORAGE: LoopStorage<(), 1, 100, 0, 1, 1, ()> = LoopStorage::new();
            STORAGE.claim((), ())
        };
        storage.tasks.insert(FixedFuture([0u8; 101]));
    }

    #[test]
    #[should_panic(expected = "a task future aligned to 32 bytes doesn't fit (align at most 16)")]
    fn an_overaligned_future_panics() {
        #[repr(align(32))]
        struct Aligned;
        let storage = {
            static STORAGE: TasksStorage<1> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        let aligned = Aligned;
        storage.tasks.insert(async move {
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
        let storage = {
            static STORAGE: LoopStorage<
                (),
                2,
                MAX_TASK_SIZE,
                { size_of::<BigRoot<'static>>() },
                1,
                1,
                (),
            > = LoopStorage::new();
            STORAGE.claim((), ())
        };
        let tasks = &storage.tasks;
        let root = BigRoot {
            data: [3; 9000],
            _flag: DropFlag(&dropped),
        };
        storage.tasks.spawn_root(root);
        let Root::Running(id) = tasks.root() else {
            panic!("spawning the root makes it running");
        };
        let sibling = storage.tasks.insert(async {});
        assert_ne!(sibling.slot(), id.slot());
        assert_eq!(storage.tasks.run_ready(), RootStatus::Running);
        assert!(tasks.contains(id));
        assert!(!dropped.get());
        storage.tasks.drop_all();
        assert!(dropped.get());
    }

    #[test]
    #[should_panic(
        expected = "the root future of 1 bytes doesn't fit MAX_ROOT_TASK_SIZE (0 bytes)"
    )]
    fn a_too_big_root_panics() {
        let storage = {
            static STORAGE: TasksStorage<1> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        storage.tasks.spawn_root(FixedFuture(0u8));
    }

    #[test]
    #[should_panic(expected = "the root is already spawned")]
    fn spawning_a_second_root_panics() {
        let storage = {
            static STORAGE: TasksStorage<2> = TasksStorage::new();
            STORAGE.claim((), ())
        };
        storage.tasks.spawn_root(core::future::pending::<()>());
        storage.tasks.spawn_root(core::future::pending::<()>());
    }
}
