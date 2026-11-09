use core::{cell::Cell, future::Future, marker::PhantomData, ptr::NonNull, task::Waker};

use crate::tasks::{JoinStatus, Root, RootStatus, Task, TaskEntry, TaskId, TaskSlot, Tasks};

pub(crate) struct TasksGuard<
    'a,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
> {
    pub(super) tasks: &'static Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
    pub(super) futures: PhantomData<(Cell<&'a ()>, *const ())>,
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
    pub(crate) fn contains(self, id: TaskId) -> bool {
        self.tasks.pool.get(id).is_some_and(TaskEntry::has_task)
    }

    pub(crate) fn set_join_waiter(self, id: TaskId, waker: &Waker) -> JoinStatus {
        match self.tasks.pool.get(id) {
            Some(entry) if entry.has_task() => {
                entry.join_waiter.store(waker);
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
        let Some(id) = self.reserve() else {
            panic!("more than {MAX_TASKS_COUNT} concurrent tasks");
        };
        self.write(id, future);
        id
    }

    pub(crate) fn reserve(self) -> Option<TaskId> {
        self.tasks.pool.alloc().map(|(id, _)| id)
    }

    pub(crate) fn release(self, id: TaskId) {
        self.tasks.pool.free(id);
        self.tasks.slot_waiter.wake_by_ref();
    }

    pub(crate) fn has_slot_waiter(self) -> bool {
        self.tasks.slot_waiter.is_set()
    }

    pub(crate) fn set_slot_waiter(self, waker: &Waker) {
        self.tasks.slot_waiter.store(waker);
    }

    pub(crate) fn clear_slot_waiter(self) {
        self.tasks.slot_waiter.take();
    }

    pub(crate) fn spawn_reserved<Fut>(self, id: TaskId, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        self.write(id, future);
        self.tasks.queue.push(id.slot());
    }

    fn write<Fut>(self, id: TaskId, future: Fut)
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
        let tasks = self.tasks;
        let entry = tasks
            .pool
            .get(id)
            .expect("a task is written into a reserved slot");
        assert!(!entry.has_task(), "a reserved slot holds no task yet");
        let memory = NonNull::from(&tasks.slab[id.slot() as usize].0).cast();
        // SAFETY: `Fut` fits the slab's blocks (checked above). The slot's block is unused: the
        // slot is reserved and holds no task (checked above), and a slot is only freed after its
        // future was dropped. The slot stays reserved until this future is dropped. `Tasks` keeps
        // its futures in fields private to the `tasks` module, where only `TasksGuard` touches
        // them, and `Tasks::claim` creates a guard once per `Tasks` (copies share its invariant
        // `'a`), so every future in it was written through a guard with this same `'a` (which
        // `Fut` outlives), and is polled and dropped only through one, before `'a` ends.
        let future = unsafe { Task::write(memory, future) };
        self.occupy(id, entry, future);
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
        entry.join_waiter.take();
        self.tasks.queue.track(id);
    }

    fn free(self, id: TaskId, entry: &TaskEntry, task: Task) -> Option<Waker> {
        entry.task.set(TaskSlot::Empty);
        let join_waiter = entry.join_waiter.take();
        self.tasks.queue.untrack(id);
        drop(task);
        self.release(id);
        join_waiter
    }

    pub(crate) fn run_ready(self) -> RootStatus {
        let tasks = self.tasks;
        loop {
            let Some(id) = tasks.queue.pop() else {
                return RootStatus::Running;
            };
            let entry = tasks
                .pool
                .get(id)
                .expect("the queue only yields tracked tasks");
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
        while let Some((id, entry)) = self.tasks.pool.iter().find(|(_, entry)| entry.is_idle()) {
            let TaskSlot::Idle(task) = entry.task.take() else {
                unreachable!("`is_idle` checked the slot");
            };
            self.free(id, entry, task);
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
