use core::{
    alloc::Layout, cell::Cell, future::Future, mem::MaybeUninit, ptr::NonNull, task::Waker,
};

use rustix::fd::{AsRawFd, BorrowedFd, RawFd};

use crate::{
    capacities::Capacities,
    instant::Instant,
    pool::{Index, ObjectPool},
    ready::ReadyQueue,
    selectors::{Direction, EVENTS_PER_CALL, IoAction, IoChange, Selector, SelectorEvent, Token},
    task::{JoinStatus, OwnedTask, TaskPtr},
    waker::new_waker,
};

// Pool values are `Cell`s (see `ObjectPool`); a vacant slot holds a stale or default value.
#[derive(Default)]
pub(crate) struct Timer {
    deadline: Cell<Instant>,
    // Insertion order, so timers with equal deadlines expire in the order they were created.
    seq: Cell<u64>,
    waker: Cell<Option<Waker>>,
}

pub(crate) type TimerIndex = Index<Timer>;

pub(crate) struct IoWaiter {
    fd: Cell<RawFd>,
    direction: Cell<Direction>,
    waker: Cell<Option<Waker>>,
    // Whether the kernel filter is currently armed; the desired state is `is_waiting()`.
    armed: Cell<bool>,
}

impl Default for IoWaiter {
    fn default() -> Self {
        Self {
            fd: Cell::new(-1),
            direction: Cell::new(Direction::Read),
            waker: Cell::new(None),
            armed: Cell::new(false),
        }
    }
}

impl IoWaiter {
    fn is_waiting(&self) -> bool {
        is_some(&self.waker)
    }

    fn pending_change(&self, index: IoIndex) -> Option<IoChange> {
        let action = match (self.is_waiting(), self.armed.get()) {
            (true, false) => IoAction::Arm,
            (false, true) => IoAction::Disarm,
            (true, true) | (false, false) => return None,
        };
        Some(IoChange {
            token: Token(index.to_bits()),
            fd: self.fd.get(),
            direction: self.direction.get(),
            action,
        })
    }
}

pub(crate) type IoIndex = Index<IoWaiter>;

#[derive(Default)]
pub(crate) struct TaskEntry {
    // `None` while the slot is reserved: while its future is written in (`insert`) or dropped
    // outside of the entry (`remove_task`), so a task spawned from the future's `Drop` can't take
    // the slot (and so its memory in the task slab).
    future: Cell<Option<OwnedTask>>,
    join_waiter: Cell<Option<Waker>>,
}

fn is_some<T>(cell: &Cell<Option<T>>) -> bool {
    let value = cell.take();
    let some = value.is_some();
    cell.set(value);
    some
}

// `clone_from`, so storing a waker that wakes the same task as the stored one doesn't clone it.
fn store_waker(cell: &Cell<Option<Waker>>, waker: &Waker) {
    let stored = match cell.take() {
        Some(mut stored) => {
            stored.clone_from(waker);
            stored
        }
        None => waker.clone(),
    };
    cell.set(Some(stored));
}

pub(crate) type TaskId = Index<TaskEntry>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootStatus {
    Running,
    Finished,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Root {
    Running(TaskId),
    Finished,
}

// At the start of the runtime's single allocation: the inline values, and a pointer to every
// other part of the allocation (the state, each pool, the ready queue).
struct StorageHeader<Sel> {
    state: NonNull<u8>,
    selector: Sel,
    root: Cell<Root>,
    next_timer_seq: Cell<u64>,
    // `'static`: they live as long as the storage, which nothing that can reach the header
    // outlives. Only shared references: the pools and the queue consist of `Cell`s.
    timers: &'static ObjectPool<Timer>,
    io_waiters: &'static ObjectPool<IoWaiter>,
    tasks: &'static ObjectPool<TaskEntry>,
    ready: &'static ReadyQueue,
    // `max_tasks` blocks of `task_stride` bytes (`max_task_size` rounded up to `TASK_ALIGN`), one
    // per task slot, each holding its task's future, aligned to `TASK_ALIGN`.
    task_slab: NonNull<u8>,
    task_stride: usize,
    // The root future's own region, laid out exactly for it (any size and alignment), so
    // `max_task_size` only has to fit spawned tasks. The root's slot still has a slab block, which
    // stays unused.
    root_future: NonNull<u8>,
    root_layout: Layout,
}

// The alignment of every task slab block: the largest standard alignment on the supported targets
// (`malloc`'s guarantee), which every future has unless it holds an over-aligned type.
const TASK_ALIGN: usize = 16;

enum TaskMemory {
    Slab,
    Root,
}

// The layout of the runtime's single allocation and the offset of each part in it, laid out one
// after another like the fields of a C struct, the header first.
struct StorageLayout {
    layout: Layout,
    state: usize,
    timers: usize,
    io_waiters: usize,
    tasks: usize,
    ready: usize,
    task_slab: usize,
    root_future: usize,
    task_stride: usize,
}

impl StorageLayout {
    fn new<Sel, S>(capacities: Capacities, root_layout: Layout) -> Self {
        let extend = |layout: Layout, next: Layout| layout.extend(next).expect("too large");
        let task_stride = capacities.max_task_size.next_multiple_of(TASK_ALIGN);
        let task_slab = task_stride
            .checked_mul(capacities.max_tasks)
            .and_then(|size| Layout::from_size_align(size, TASK_ALIGN).ok())
            .expect("too large");
        let layout = Layout::new::<StorageHeader<Sel>>();
        let (layout, state) = extend(layout, Layout::new::<S>());
        let (layout, timers) = extend(layout, ObjectPool::<Timer>::layout(capacities.max_timers));
        let (layout, io_waiters) =
            extend(layout, ObjectPool::<IoWaiter>::layout(capacities.max_io));
        let (layout, tasks) = extend(
            layout,
            ObjectPool::<TaskEntry>::layout(capacities.max_tasks),
        );
        let (layout, ready) = extend(layout, ReadyQueue::layout(capacities.max_tasks));
        let (layout, task_slab) = extend(layout, task_slab);
        let (layout, root_future) = extend(layout, root_layout);
        Self {
            layout: layout.pad_to_align(),
            state,
            timers,
            io_waiters,
            tasks,
            ready,
            task_slab,
            root_future,
            task_stride,
        }
    }
}

// The runtime: one block of caller-provided memory that never moves, reached through this pointer to its start (copied
// into the `EventLoop` and every `Context`). Objects inside are addressed by typed indices.
pub(crate) struct Storage<Sel = Selector>(NonNull<StorageHeader<Sel>>);

impl<Sel> Clone for Storage<Sel> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Sel> Copy for Storage<Sel> {}

impl<Sel> Storage<Sel> {
    /// The size of the memory `new` needs: the storage's layout plus slack for aligning its start,
    /// so memory of this size works wherever it starts.
    pub(crate) fn required_size<S>(capacities: Capacities, root_layout: Layout) -> usize {
        let layout = StorageLayout::new::<Sel, S>(capacities, root_layout).layout;
        layout.size() + (layout.align() - 1)
    }

    /// Builds the storage inside `memory`, at its first suitably aligned address. Panics if
    /// `memory` is smaller than needed (`required_size` is always enough).
    ///
    /// # Safety
    ///
    /// `memory` must be valid for reads and writes and not be used by anything else until
    /// `destroy` returns.
    pub(crate) unsafe fn new<S>(
        memory: NonNull<[MaybeUninit<u8>]>,
        selector: Sel,
        state: S,
        capacities: Capacities,
        root_layout: Layout,
    ) -> Self {
        let parts = StorageLayout::new::<Sel, S>(capacities, root_layout);
        let start = memory
            .cast::<u8>()
            .as_ptr()
            .align_offset(parts.layout.align());
        if start.saturating_add(parts.layout.size()) > memory.len() {
            panic!(
                "the EventLoop memory is too small: {} bytes, {} needed",
                memory.len(),
                Self::required_size::<S>(capacities, root_layout),
            );
        }
        // SAFETY: `start` is within `memory`, checked right above.
        let base = unsafe { memory.cast::<u8>().byte_add(start) };
        // SAFETY: `base` is aligned to the layout and `memory` holds the whole layout from there
        // (checked above), valid and unused (guaranteed by the caller). `StorageLayout` placed
        // every part inside the layout, aligned for it and not overlapping the others; each is
        // written exactly once, the header last. The parts live until `destroy`, which nothing
        // holding the header outlives, and are only accessed through shared references (and by
        // `destroy`), hence `'static`.
        unsafe {
            let state_ptr = base.byte_add(parts.state);
            state_ptr.cast::<S>().write(state);
            let header = StorageHeader {
                state: state_ptr,
                selector,
                root: Cell::new(Root::Finished),
                next_timer_seq: Cell::new(0),
                timers: ObjectPool::init(base.byte_add(parts.timers), capacities.max_timers)
                    .as_ref(),
                io_waiters: ObjectPool::init(base.byte_add(parts.io_waiters), capacities.max_io)
                    .as_ref(),
                tasks: ObjectPool::init(base.byte_add(parts.tasks), capacities.max_tasks).as_ref(),
                ready: ReadyQueue::init(base.byte_add(parts.ready), capacities.max_tasks).as_ref(),
                task_slab: base.byte_add(parts.task_slab),
                task_stride: parts.task_stride,
                root_future: base.byte_add(parts.root_future),
                root_layout,
            };
            base.cast::<StorageHeader<Sel>>().write(header);
        }
        Self(base.cast())
    }

    fn header(&self) -> &StorageHeader<Sel> {
        // SAFETY: the header is initialized in `new` and lives until `destroy`, which no
        // reference into the storage outlives; it is only ever accessed through `&`.
        unsafe { self.0.as_ref() }
    }

    /// # Safety
    ///
    /// `S` must be the state type the storage was created with.
    pub(crate) unsafe fn state<S>(&self) -> NonNull<S> {
        self.header().state.cast()
    }

    /// Drops every task, the state, the pools and the header. The memory isn't freed: it's the
    /// caller's.
    ///
    /// # Safety
    ///
    /// Called exactly once, with the state type the storage was created with, while no task is
    /// being polled; the storage isn't used afterwards.
    pub(crate) unsafe fn destroy<S>(self) {
        // Task futures hold `Context`s pointing at the storage and may use them in their `Drop`
        // impls, so they go first, while everything is intact.
        self.drop_all_tasks();
        let header = self.header();
        let pools = (
            self.writable(header.timers),
            self.writable(header.io_waiters),
            self.writable(header.tasks),
        );
        // SAFETY: no task (so no `Context` and no reference into the storage) is left; every part
        // was initialized in `new` and is dropped exactly once here (the ready queue holds only
        // `Copy` data, the task slab and root region no live future), the header last. The
        // header's references to the pools are never used again.
        unsafe {
            core::ptr::drop_in_place(self.state::<S>().as_ptr());
            core::ptr::drop_in_place(pools.0);
            core::ptr::drop_in_place(pools.1);
            core::ptr::drop_in_place(pools.2);
            core::ptr::drop_in_place(self.0.as_ptr());
        }
    }

    // `pool` with the provenance of the allocation's base: one derived from a shared reference
    // mustn't be used to drop it.
    fn writable<T>(&self, pool: &ObjectPool<T>) -> *mut ObjectPool<T> {
        let addr = NonNull::from(pool).cast::<u8>().addr();
        ObjectPool::raw(self.0.cast::<u8>().with_addr(addr), pool.capacity())
    }

    pub(crate) fn root(&self) -> Root {
        self.header().root.get()
    }

    pub(crate) fn set_root(&self, root: Root) {
        self.header().root.set(root);
    }

    pub(crate) fn ready(&self) -> &ReadyQueue {
        self.header().ready
    }

    fn tasks(&self) -> &ObjectPool<TaskEntry> {
        self.header().tasks
    }

    fn timers(&self) -> &ObjectPool<Timer> {
        self.header().timers
    }

    fn io_waiters(&self) -> &ObjectPool<IoWaiter> {
        self.header().io_waiters
    }

    pub(crate) fn task_capacity(&self) -> usize {
        self.tasks().capacity()
    }

    /// On a full pool, the future is handed back. Panics if the future doesn't fit the task slab's
    /// blocks.
    pub(crate) fn insert_task<Fut>(&self, future: Fut) -> Result<TaskId, Fut>
    where
        Fut: Future<Output = ()> + 'static,
    {
        let header = self.header();
        let needed = Layout::new::<Fut>();
        if needed.size() > header.task_stride || needed.align() > TASK_ALIGN {
            drop(future);
            panic!(
                "a task future of {} bytes (align {}) doesn't fit max_task_size ({} bytes after \
                 rounding up to a multiple of {TASK_ALIGN}, align at most {TASK_ALIGN})",
                needed.size(),
                needed.align(),
                header.task_stride,
            );
        }
        // SAFETY: `Fut` fits the slab's blocks (checked above).
        unsafe { self.insert(future, TaskMemory::Slab) }
    }

    /// Inserts the root future into its own region, queues it and makes it the root.
    ///
    /// # Safety
    ///
    /// Called at most once, so the root region holds no other future.
    pub(crate) unsafe fn spawn_root<Fut>(&self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'static,
    {
        assert_eq!(Layout::new::<Fut>(), self.header().root_layout);
        // SAFETY: `Fut` has exactly the root region's layout (asserted above), and the caller
        // guarantees the region is unused.
        let Ok(id) = (unsafe { self.insert(future, TaskMemory::Root) }) else {
            unreachable!("the root is the first task, the pool has room for it");
        };
        self.ready().push(id.slot());
        self.set_root(Root::Running(id));
        id
    }

    /// # Safety
    ///
    /// `Fut` must fit `memory`: the slab's blocks (size at most `task_stride`, align at most
    /// `TASK_ALIGN`), or
    /// exactly the root region's layout, which must not hold another future.
    unsafe fn insert<Fut>(&self, future: Fut, memory: TaskMemory) -> Result<TaskId, Fut>
    where
        Fut: Future<Output = ()> + 'static,
    {
        let header = self.header();
        let Some((id, entry)) = self.tasks().alloc() else {
            return Err(future);
        };
        let memory = match memory {
            // SAFETY: `slot < max_tasks_count`, so the block lies inside the slab.
            TaskMemory::Slab => unsafe {
                header
                    .task_slab
                    .byte_add(id.slot() as usize * header.task_stride)
            },
            TaskMemory::Root => header.root_future,
        };
        // SAFETY: blocks are `task_stride` (a multiple of `TASK_ALIGN`) apart from a base aligned
        // to `TASK_ALIGN`, and the
        // root region is laid out for exactly `root_layout`, so `memory` is aligned and big enough
        // for `Fut` (guaranteed by the caller). It's unused: a slab block belongs to the slot,
        // which was free, and a slot is only freed after its future was dropped; the root region
        // is used once (guaranteed by the caller). The slot stays reserved until this future is
        // dropped.
        let future = unsafe { OwnedTask::write(memory, future) };
        entry.future.set(Some(future));
        entry.join_waiter.set(None);
        self.ready().track(id);
        Ok(id)
    }

    pub(crate) fn task(&self, id: TaskId) -> Option<TaskPtr> {
        let entry = self.tasks().get(id)?;
        let future = entry.future.take()?;
        let ptr = future.ptr();
        entry.future.set(Some(future));
        Some(ptr)
    }

    /// Drops the task's future and frees its slot, returning its join waiter. The future is
    /// dropped while the slot is still reserved, since its `Drop` may use the storage.
    pub(crate) fn remove_task(&self, id: TaskId) -> Option<Option<Waker>> {
        let entry = self.tasks().get(id)?;
        let future = entry.future.take()?;
        let join_waiter = entry.join_waiter.take();
        self.ready().untrack(id);
        drop(future);
        self.tasks().free(id);
        Some(join_waiter)
    }

    pub(crate) fn set_join_waiter(&self, id: TaskId, waker: &Waker) -> JoinStatus {
        match self.tasks().get(id) {
            Some(entry) if is_some(&entry.future) => {
                store_waker(&entry.join_waiter, waker);
                JoinStatus::Running
            }
            _ => JoinStatus::Finished,
        }
    }

    pub(crate) fn alloc_timer(&self, deadline: Instant, waker: Waker) -> TimerIndex {
        let Some((index, timer)) = self.timers().alloc() else {
            panic!("more than {} concurrent sleeps", self.timers().capacity());
        };
        let seq = self.header().next_timer_seq.get();
        self.header().next_timer_seq.set(seq + 1);
        timer.deadline.set(deadline);
        timer.seq.set(seq);
        timer.waker.set(Some(waker));
        index
    }

    pub(crate) fn set_timer_waker(&self, index: TimerIndex, waker: &Waker) {
        let timer = self
            .timers()
            .get(index)
            .expect("a pending sleep's timer is in storage");
        store_waker(&timer.waker, waker);
    }

    pub(crate) fn release_timer(&self, index: TimerIndex) {
        if let Some(timer) = self.timers().get(index) {
            timer.waker.take();
            self.timers().free(index);
        }
    }

    pub(crate) fn next_timer_deadline(&self) -> Option<Instant> {
        self.timers()
            .iter()
            .map(|(_, timer)| timer.deadline.get())
            .min()
    }

    pub(crate) fn take_expired_timer(&self, now: Instant) -> Option<Waker> {
        let (index, _) = self
            .timers()
            .iter()
            .filter(|(_, timer)| timer.deadline.get() <= now)
            .min_by_key(|(_, timer)| (timer.deadline.get(), timer.seq.get()))?;
        let waker = self.timers().get(index)?.waker.take();
        self.timers().free(index);
        waker
    }

    pub(crate) fn alloc_io(&self, fd: RawFd, direction: Direction) -> IoIndex {
        let Some((index, io)) = self.io_waiters().alloc() else {
            panic!(
                "more than {} concurrent IO operations",
                self.io_waiters().capacity()
            );
        };
        io.fd.set(fd);
        io.direction.set(direction);
        io.waker.set(None);
        io.armed.set(false);
        index
    }

    #[cfg(test)]
    pub(crate) fn io_count(&self) -> usize {
        self.io_waiters().iter().count()
    }

    pub(crate) fn spawn<Fut>(&self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'static,
    {
        let result = self.insert_task(future);
        match result {
            Ok(id) => {
                self.ready().push(id.slot());
                id
            }
            Err(future) => {
                drop(future);
                panic!("more than {} concurrent tasks", self.task_capacity());
            }
        }
    }

    pub(crate) fn register_io(&self, fd: BorrowedFd<'_>, direction: Direction) -> IoIndex {
        self.alloc_io(fd.as_raw_fd(), direction)
    }

    pub(crate) fn set_io_waker(&self, index: IoIndex, waker: &Waker) {
        let io = self.io_waiters().get(index).expect("IO is not registered");
        store_waker(&io.waker, waker);
    }

    pub(crate) fn remove_io_waker(&self, index: IoIndex) {
        if let Some(io) = self.io_waiters().get(index) {
            io.waker.take();
        }
    }

    pub(crate) fn has_io_waker(&self, index: IoIndex) -> bool {
        self.io_waiters()
            .get(index)
            .is_some_and(IoWaiter::is_waiting)
    }

    fn take_io_waker(&self, token: Token, direction: Direction) -> Option<Waker> {
        let io = self.io_waiters().get(IoIndex::from_bits(token.0))?;
        if io.direction.get() != direction {
            return None;
        }
        // The filter is one-shot: delivering the event disarmed it.
        io.armed.set(false);
        io.waker.take()
    }

    pub(crate) fn has_ready_tasks(&self) -> bool {
        !self.ready().is_empty()
    }

    pub(crate) fn run_ready_tasks(&self, root: TaskId) -> RootStatus {
        loop {
            let Some(id) = self.ready().pop() else {
                return RootStatus::Running;
            };
            let Some(task) = self.task(id) else {
                continue;
            };
            // SAFETY: the ready queue lives in the storage's allocation, which outlives every
            // waker except deliberately leaked ones (documented as forbidden).
            let waker = unsafe { new_waker(self.ready(), id) };
            let mut cx = core::task::Context::from_waker(&waker);
            // SAFETY: tasks are removed only right below (after the poll) and by
            // `drop_all_tasks`, which is never called from inside a task; `run_ready_tasks`
            // isn't either, so the task isn't polled re-entrantly.
            if unsafe { task.poll(&mut cx) }.is_pending() {
                continue;
            }
            let join_waiter = self
                .remove_task(id)
                .expect("a finished task is still in storage");
            if let Some(join_waiter) = join_waiter {
                join_waiter.wake();
            }
            if id == root {
                return RootStatus::Finished;
            }
        }
    }

    pub(crate) fn drop_all_tasks(&self) {
        // Dropping a task may spawn new ones, they are dropped in later iterations.
        while let Some((id, _)) = self.tasks().iter().next() {
            self.remove_task(id);
        }
    }
}

impl Storage<Selector> {
    pub(crate) fn selector(&self) -> &Selector {
        &self.header().selector
    }

    /// Must be called while the fd is still open (see `Selector::apply_now`).
    pub(crate) fn deregister_io(&self, index: IoIndex) -> rustix::io::Result<()> {
        let Some(io) = self.io_waiters().get(index) else {
            return Ok(());
        };
        io.waker.take();
        let armed = io.armed.get();
        let change = IoChange {
            token: Token(index.to_bits()),
            fd: io.fd.get(),
            direction: io.direction.get(),
            action: IoAction::Disarm,
        };
        self.io_waiters().free(index);
        if !armed {
            return Ok(());
        }
        self.selector().apply_now(change)
    }

    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector().poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Readable(token) => {
                        if let Some(waker) = self.take_io_waker(token, Direction::Read) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Writable(token) => {
                        if let Some(waker) = self.take_io_waker(token, Direction::Write) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Timer => {
                        let now = Instant::now();
                        while let Some(waker) = self.take_expired_timer(now) {
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

    pub(crate) fn flush(&self) -> rustix::io::Result<()> {
        match self.next_timer_deadline() {
            Some(deadline) => self.selector().arm_timer(deadline),
            None => self.selector().disarm_timer(),
        }
        let changes = self
            .io_waiters()
            .iter()
            .filter_map(|(index, io)| io.pending_change(index));
        self.selector().flush(changes)?;
        for (_, io) in self.io_waiters().iter() {
            io.armed.set(io.is_waiting());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
