use core::{
    cell::{Cell, RefCell},
    future::Future,
    marker::PhantomData,
    task::Waker,
    time::Duration,
};

use crate::{
    io::{IoIndex, IoWaiters},
    selector::{EVENTS_PER_CALL, Selector, SelectorEvent},
    state::State,
    storage::LoopStorage,
    tasks::{
        JoinHandle, JoinStatus, ReserveTask, Root, RootStatus, Task, TaskEntry, TaskId, Tasks,
    },
    time::{Instant, Sleep, Timeout, Timers},
};

/// A handle of the event loop that you are currently in.
///
/// Used to spawn tasks, sleep, dispatch events to the state and start IO operations.
pub struct Context<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    pub(crate) timers: &'static Timers<TIMERS>,
    pub(crate) io_waiters: &'static IoWaiters<FDS>,
    pub(crate) selector: &'a Selector,
    pub(crate) state: &'a RefCell<S>,
    futures: PhantomData<(Cell<&'a ()>, *const ())>,
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    pub(crate) fn claim(
        storage: &'static LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        selector: &'a Selector,
        state: &'a RefCell<S>,
    ) -> Self {
        storage.tasks.queue.claim();
        Self {
            tasks: &storage.tasks,
            timers: &storage.timers,
            io_waiters: &storage.io_waiters,
            selector,
            state,
            futures: PhantomData,
        }
    }

    /// Dispatches given `event` on a state that was given to [`EventLoop`](crate::EventLoop).
    pub fn dispatch(&self, event: S::Event) {
        self.state.borrow_mut().apply(event);
    }

    /// Spawns a task with a given async closure.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. all `TASKS` task slots are taken
    /// 2. if the future is bigger than `TASK_SIZE` bytes
    /// 3. if it is aligned to more than 16 bytes
    ///
    /// Use [`reserve_task`](Self::reserve_task) to wait for a free slot instead.
    pub fn spawn<F, Fut>(
        &self,
        f: F,
    ) -> JoinHandle<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let id = self.spawn_future(f(*self));
        JoinHandle::new(*self, id)
    }

    /// Reserves a task slot.
    ///
    /// This method returns a future that is resolved once a slot in the internal
    /// pool of tasks is available.
    ///
    /// The future stays pending until some other task finishes and frees a slot.
    pub fn reserve_task(
        &self,
    ) -> ReserveTask<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS> {
        ReserveTask::new(*self)
    }

    /// Sleeps for `duration`.
    pub fn sleep(&self, duration: Duration) -> Sleep<'_, TIMERS> {
        Sleep::new(self.timers, Instant::now().saturating_add(duration))
    }

    /// Runs `future` with a timeout of `duration`, resolves to `None` in case of a timeout.
    pub fn timeout<F: Future>(&self, duration: Duration, future: F) -> Timeout<'_, F, TIMERS> {
        Timeout::new(future, self.sleep(duration))
    }

    pub(crate) fn root(self) -> Root<TASK_SIZE> {
        self.tasks.root.get()
    }

    #[cfg(test)]
    pub(crate) fn set_root(self, root: Root<TASK_SIZE>) {
        self.tasks.root.set(root);
    }

    #[cfg(test)]
    pub(crate) fn contains(self, id: TaskId<TASK_SIZE>) -> bool {
        self.tasks.pool.get(id).is_some_and(TaskEntry::has_task)
    }

    pub(crate) fn set_join_waiter(self, id: TaskId<TASK_SIZE>, waker: &Waker) -> JoinStatus {
        match self.tasks.pool.get(id) {
            Some(entry) if entry.has_task() => {
                entry.set_join_waiter(waker);
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

    pub(crate) fn insert<Fut>(self, future: Fut) -> TaskId<TASK_SIZE>
    where
        Fut: Future<Output = ()> + 'a,
    {
        let Some((id, entry)) = self.reserve() else {
            panic!("more than {TASKS} concurrent tasks");
        };
        self.write(entry, future);
        id
    }

    pub(crate) fn reserve(self) -> Option<(TaskId<TASK_SIZE>, &'static TaskEntry<TASK_SIZE>)> {
        self.tasks.pool.alloc()
    }

    pub(crate) fn release(self, id: TaskId<TASK_SIZE>) {
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

    pub(crate) fn spawn_reserved<Fut>(
        self,
        id: TaskId<TASK_SIZE>,
        entry: &TaskEntry<TASK_SIZE>,
        future: Fut,
    ) where
        Fut: Future<Output = ()> + 'a,
    {
        self.write(entry, future);
        self.tasks.queue.push(id.slot());
    }

    fn write<Fut>(self, entry: &TaskEntry<TASK_SIZE>, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        assert!(
            size_of::<Fut>() <= TASK_SIZE,
            "a task future of {} bytes doesn't fit TASK_SIZE ({TASK_SIZE} bytes)",
            size_of::<Fut>(),
        );
        assert!(
            align_of::<Fut>() <= 16,
            "a task future aligned to {} bytes doesn't fit (align at most 16)",
            align_of::<Fut>(),
        );
        assert!(!entry.has_task(), "a reserved slot holds no task yet");
        // SAFETY: `Fut` fits the entry's memory (checked above). The entry's memory is unused:
        // the slot is reserved and holds no task (checked above), and a slot is only freed after
        // its future was dropped. Entries live in a `'static` pool and never move. The slot stays
        // reserved until this future is dropped. Only `Context` touches the futures in `Tasks`,
        // and `Context::claim` creates a context once per storage (it claims the `WakeQueue`,
        // which panics the second time; copies share the context's invariant `'a`), so every
        // future in it was written through a context with this same `'a` (which `Fut`
        // outlives), and is polled and dropped only through one, before `'a` ends.
        let future = unsafe { Task::write(entry.memory(), future) };
        entry.occupy(future);
    }

    pub(crate) fn spawn_future<Fut>(self, future: Fut) -> TaskId<TASK_SIZE>
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
        let Some((id, entry)) = tasks.pool.alloc() else {
            unreachable!("the root is the first task, the pool has room for it");
        };
        // SAFETY: `Fut` fits the root region (asserted above), and the region is unused: the root
        // is still `Unset` (asserted above), and leaves that state below, so this is the only
        // call that gets here. `Fut` outlives its use as in `insert`.
        let future = unsafe { Task::write(tasks.root_region.as_ptr(), future) };
        entry.occupy(future);
        tasks.queue.push(id.slot());
        tasks.root.set(Root::Running(id));
    }

    fn free(self, id: TaskId<TASK_SIZE>, entry: &TaskEntry<TASK_SIZE>) -> Option<Waker> {
        let join_waiter = entry.take_join_waiter();
        entry.drop_task();
        self.release(id);
        join_waiter
    }

    pub(crate) fn run_ready(self) -> RootStatus {
        let tasks = self.tasks;
        loop {
            let Some(slot) = tasks.queue.pop() else {
                return RootStatus::Running;
            };
            let Some((id, entry)) = tasks.pool.get_at(slot) else {
                continue;
            };
            if !entry.has_task() {
                continue;
            }
            let waker = tasks.queue.waker(slot);
            let mut cx = core::task::Context::from_waker(&waker);
            if entry.poll(&mut cx).is_pending() {
                continue;
            }
            if let Some(join_waiter) = self.free(id, entry) {
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
            self.free(id, entry);
        }
    }

    #[cfg(test)]
    pub(crate) fn waker(self, id: TaskId<TASK_SIZE>) -> Waker {
        self.tasks.queue.waker(id.slot())
    }

    pub(crate) fn clear(self) {
        self.drop_all();
        self.timers.clear();
        self.io_waiters.clear();
    }

    pub(crate) fn process_selector_events(self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector.poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Io { token, ready } => {
                        self.io_waiters.wake_ready(IoIndex::from_bits(token), ready);
                    }
                    SelectorEvent::Timer => {
                        let now = Instant::now();
                        while let Some(waker) = self.timers.take_expired(now) {
                            waker.wake();
                        }
                    }
                }
            }
            if !full {
                return Ok(());
            }
        }
    }

    pub(crate) fn flush(self) -> rustix::io::Result<()> {
        match self.timers.next_deadline() {
            Some(deadline) => self.selector.arm_timer(deadline),
            None => self.selector.disarm_timer(),
        }
        self.selector.flush(self.io_waiters.pending_changes())?;
        self.io_waiters.mark_flushed();
        Ok(())
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Clone for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Copy for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Context")
    }
}
