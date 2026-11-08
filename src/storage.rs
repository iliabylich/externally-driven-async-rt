use core::alloc::Layout;
use core::cell::{Cell, Ref, RefCell, RefMut};
use core::future::Future;
use core::mem::MaybeUninit;
use core::ptr::{self, NonNull};
use core::task::Waker;

use rustix::fd::{AsRawFd, BorrowedFd, RawFd};

use crate::capacities::Capacities;
use crate::instant::Instant;
use crate::pool::{Index, ObjectPool};
use crate::ready::ReadyQueue;
use crate::selectors::{
    Direction, EVENTS_PER_CALL, IoAction, IoChange, Selector, SelectorEvent, Token,
};
use crate::task::{JoinStatus, OwnedTask, TaskPtr};
use crate::waker::new_waker;

pub(crate) struct Timer {
    pub(crate) deadline: Instant,
    pub(crate) waker: Waker,
    // Insertion order, so timers with equal deadlines expire in the order they were created.
    seq: u64,
}

pub(crate) type TimerIndex = Index<Timer>;

pub(crate) struct IoWaiter {
    pub(crate) fd: RawFd,
    pub(crate) direction: Direction,
    pub(crate) waker: Option<Waker>,
    // Whether the kernel filter is currently armed; the desired state is `waker.is_some()`. A
    // `Cell`, so `flush` can update it while iterating the pool through a shared borrow.
    pub(crate) armed: Cell<bool>,
}

impl IoWaiter {
    fn pending_change(&self, index: IoIndex) -> Option<IoChange> {
        let action = match (self.waker.is_some(), self.armed.get()) {
            (true, false) => IoAction::Arm,
            (false, true) => IoAction::Disarm,
            (true, true) | (false, false) => return None,
        };
        Some(IoChange {
            token: Token(index.to_bits()),
            fd: self.fd,
            direction: self.direction,
            action,
        })
    }
}

pub(crate) type IoIndex = Index<IoWaiter>;

pub(crate) enum TaskEntry {
    Running {
        future: OwnedTask,
        join_waiter: Option<Waker>,
    },
    // No live future: the slot (and so its memory in the task slab) stays reserved while its
    // future is written in (`insert_task`) or dropped outside any borrow (`remove_task`), so a
    // task spawned from the future's `Drop` can't take it.
    Reserved,
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
// other part of the allocation (the state, each pool, the ready queue). The pointers are derived
// from the allocation's base, so they're valid for the whole allocation, not just the header.
struct StorageHeader<Sel> {
    state: NonNull<u8>,
    selector: Sel,
    root: Cell<Root>,
    next_timer_seq: Cell<u64>,
    // `&mut` to a pool only exists while its `RefCell` is borrowed.
    timers: RefCell<NonNull<ObjectPool<Timer>>>,
    io_waiters: RefCell<NonNull<ObjectPool<IoWaiter>>>,
    tasks: RefCell<NonNull<ObjectPool<TaskEntry>>>,
    ready: NonNull<ReadyQueue>,
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
        // written exactly once, the header last.
        unsafe {
            let state_ptr = base.byte_add(parts.state);
            state_ptr.cast::<S>().write(state);
            let header = StorageHeader {
                state: state_ptr,
                selector,
                root: Cell::new(Root::Finished),
                next_timer_seq: Cell::new(0),
                timers: RefCell::new(ObjectPool::init(
                    base.byte_add(parts.timers),
                    capacities.max_timers,
                )),
                io_waiters: RefCell::new(ObjectPool::init(
                    base.byte_add(parts.io_waiters),
                    capacities.max_io,
                )),
                tasks: RefCell::new(ObjectPool::init(
                    base.byte_add(parts.tasks),
                    capacities.max_tasks,
                )),
                ready: ReadyQueue::init(base.byte_add(parts.ready), capacities.max_tasks),
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
        let parts = (
            *header.timers.borrow(),
            *header.io_waiters.borrow(),
            *header.tasks.borrow(),
        );
        // SAFETY: no task (so no `Context` and no reference into the storage) is left; every part
        // was initialized in `new` and is dropped exactly once here (the ready queue holds only
        // `Copy` data, the task slab and root region no live future), the header last.
        unsafe {
            ptr::drop_in_place(self.state::<S>().as_ptr());
            ptr::drop_in_place(parts.0.as_ptr());
            ptr::drop_in_place(parts.1.as_ptr());
            ptr::drop_in_place(parts.2.as_ptr());
            ptr::drop_in_place(self.0.as_ptr());
        }
    }

    pub(crate) fn root(&self) -> Root {
        self.header().root.get()
    }

    pub(crate) fn set_root(&self, root: Root) {
        self.header().root.set(root);
    }

    pub(crate) fn ready(&self) -> &ReadyQueue {
        // SAFETY: initialized in `new`, alive until `destroy`; it is only ever accessed through
        // shared references (it consists of `Cell`s).
        unsafe { self.header().ready.as_ref() }
    }

    fn timers(&self) -> RefMut<'_, ObjectPool<Timer>> {
        // SAFETY: initialized in `new`, alive until `destroy`; the borrow makes this the only
        // reference to the pool.
        RefMut::map(self.header().timers.borrow_mut(), |pool| unsafe {
            pool.as_mut()
        })
    }

    fn timers_ref(&self) -> Ref<'_, ObjectPool<Timer>> {
        // SAFETY: as in `timers`, and the shared borrow rules out a mutable reference.
        Ref::map(self.header().timers.borrow(), |pool| unsafe {
            pool.as_ref()
        })
    }

    fn io_waiters(&self) -> RefMut<'_, ObjectPool<IoWaiter>> {
        // SAFETY: as in `timers`.
        RefMut::map(self.header().io_waiters.borrow_mut(), |pool| unsafe {
            pool.as_mut()
        })
    }

    fn io_waiters_ref(&self) -> Ref<'_, ObjectPool<IoWaiter>> {
        // SAFETY: as in `timers_ref`.
        Ref::map(self.header().io_waiters.borrow(), |pool| unsafe {
            pool.as_ref()
        })
    }

    fn tasks(&self) -> RefMut<'_, ObjectPool<TaskEntry>> {
        // SAFETY: as in `timers`.
        RefMut::map(self.header().tasks.borrow_mut(), |pool| unsafe {
            pool.as_mut()
        })
    }

    fn tasks_ref(&self) -> Ref<'_, ObjectPool<TaskEntry>> {
        // SAFETY: as in `timers_ref`.
        Ref::map(self.header().tasks.borrow(), |pool| unsafe {
            pool.as_ref()
        })
    }

    pub(crate) fn task_capacity(&self) -> usize {
        self.tasks_ref().capacity()
    }

    /// On a full pool, the future is handed back, so the caller can drop it outside the pool
    /// borrow. Panics if the future doesn't fit the task slab's blocks.
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
        let mut tasks = self.tasks();
        let Ok(id) = tasks.alloc(TaskEntry::Reserved) else {
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
        *tasks.get_mut(id).expect("the slot was just allocated") = TaskEntry::Running {
            future,
            join_waiter: None,
        };
        drop(tasks);
        self.ready().track(id);
        Ok(id)
    }

    pub(crate) fn task(&self, id: TaskId) -> Option<TaskPtr> {
        match self.tasks_ref().get(id)? {
            TaskEntry::Running { future, .. } => Some(future.ptr()),
            TaskEntry::Reserved => None,
        }
    }

    /// Drops the task's future and frees its slot, returning its join waiter. The future is
    /// dropped outside the pool borrow, since its `Drop` may use the storage.
    pub(crate) fn remove_task(&self, id: TaskId) -> Option<Option<Waker>> {
        let entry = core::mem::replace(self.tasks().get_mut(id)?, TaskEntry::Reserved);
        let TaskEntry::Running {
            future,
            join_waiter,
        } = entry
        else {
            return None;
        };
        self.ready().untrack(id);
        drop(future);
        self.tasks().free(id);
        Some(join_waiter)
    }

    pub(crate) fn set_join_waiter(&self, id: TaskId, waker: &Waker) -> JoinStatus {
        let mut tasks = self.tasks();
        let Some(TaskEntry::Running { join_waiter, .. }) = tasks.get_mut(id) else {
            return JoinStatus::Finished;
        };
        match join_waiter {
            Some(stored) => stored.clone_from(waker),
            None => *join_waiter = Some(waker.clone()),
        }
        JoinStatus::Running
    }

    pub(crate) fn alloc_timer(&self, deadline: Instant, waker: Waker) -> TimerIndex {
        let seq = self.header().next_timer_seq.get();
        let timer = Timer {
            deadline,
            waker,
            seq,
        };
        let mut timers = self.timers();
        let Ok(index) = timers.alloc(timer) else {
            panic!("more than {} concurrent sleeps", timers.capacity());
        };
        self.header().next_timer_seq.set(seq + 1);
        index
    }

    pub(crate) fn get_timer_mut(&self, index: TimerIndex) -> Option<RefMut<'_, Timer>> {
        RefMut::filter_map(self.timers(), |timers| timers.get_mut(index)).ok()
    }

    pub(crate) fn release_timer(&self, index: TimerIndex) -> Option<Timer> {
        self.timers().free(index)
    }

    pub(crate) fn next_timer_deadline(&self) -> Option<Instant> {
        self.timers_ref()
            .iter()
            .map(|(_, timer)| timer.deadline)
            .min()
    }

    pub(crate) fn take_expired_timer(&self, now: Instant) -> Option<Waker> {
        let mut timers = self.timers();
        let (index, _) = timers
            .iter()
            .filter(|(_, timer)| timer.deadline <= now)
            .min_by_key(|(_, timer)| (timer.deadline, timer.seq))?;
        timers.free(index).map(|timer| timer.waker)
    }

    pub(crate) fn alloc_io(&self, fd: RawFd, direction: Direction) -> IoIndex {
        let waiter = IoWaiter {
            fd,
            direction,
            waker: None,
            armed: Cell::new(false),
        };
        let mut io_waiters = self.io_waiters();
        let Ok(index) = io_waiters.alloc(waiter) else {
            panic!(
                "more than {} concurrent IO operations",
                io_waiters.capacity()
            );
        };
        index
    }

    pub(crate) fn get_io(&self, index: IoIndex) -> Option<Ref<'_, IoWaiter>> {
        Ref::filter_map(self.io_waiters_ref(), |io_waiters| io_waiters.get(index)).ok()
    }

    pub(crate) fn get_io_mut(&self, index: IoIndex) -> Option<RefMut<'_, IoWaiter>> {
        RefMut::filter_map(self.io_waiters(), |io_waiters| io_waiters.get_mut(index)).ok()
    }

    pub(crate) fn release_io(&self, index: IoIndex) -> Option<IoWaiter> {
        self.io_waiters().free(index)
    }

    #[cfg(test)]
    pub(crate) fn io_count(&self) -> usize {
        self.io_waiters_ref().iter().count()
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
                // Dropped outside the pool borrow: its `Drop` may use the storage.
                drop(future);
                panic!("more than {} concurrent tasks", self.task_capacity());
            }
        }
    }

    pub(crate) fn register_io(&self, fd: BorrowedFd<'_>, direction: Direction) -> IoIndex {
        self.alloc_io(fd.as_raw_fd(), direction)
    }

    pub(crate) fn set_io_waker(&self, index: IoIndex, waker: &Waker) {
        let mut io = self.get_io_mut(index).expect("IO is not registered");
        io.waker = Some(waker.clone());
    }

    pub(crate) fn remove_io_waker(&self, index: IoIndex) {
        if let Some(mut io) = self.get_io_mut(index) {
            io.waker = None;
        }
    }

    pub(crate) fn has_io_waker(&self, index: IoIndex) -> bool {
        self.get_io(index).is_some_and(|io| io.waker.is_some())
    }

    fn take_io_waker(&self, token: Token, direction: Direction) -> Option<Waker> {
        let mut io = self.get_io_mut(IoIndex::from_bits(token.0))?;
        if io.direction != direction {
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
        loop {
            // Not `while let`: its scrutinee's borrow would live across `remove_task`.
            let Some((id, _)) = self.tasks_ref().iter().next() else {
                return;
            };
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
        let Some(io) = self.release_io(index) else {
            return Ok(());
        };
        if !io.armed.get() {
            return Ok(());
        }
        self.selector().apply_now(IoChange {
            token: Token(index.to_bits()),
            fd: io.fd,
            direction: io.direction,
            action: IoAction::Disarm,
        })
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
        let io_waiters = self.io_waiters_ref();
        let changes = io_waiters
            .iter()
            .filter_map(|(index, io)| io.pending_change(index));
        self.selector().flush(changes)?;
        for (_, io) in io_waiters.iter() {
            io.armed.set(io.waker.is_some());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::JoinStatus;
    use crate::test_utils::{TEST_CAPACITIES, TestStorage, assert_yields, stack_memory};
    use core::cell::Cell;
    use core::task::{RawWaker, RawWakerVTable};
    use core::time::Duration;

    // Does nothing; told apart by its data pointer, `id` (`Waker::data`).
    fn waker(id: usize) -> Waker {
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(|data| RawWaker::new(data, &VTABLE), |_| {}, |_| {}, |_| {});
        // SAFETY: the vtable never touches the data pointer, and its functions do nothing.
        unsafe { Waker::new(ptr::without_provenance(id), &VTABLE) }
    }

    // Does nothing but count its drops (clones included) in `drops`.
    fn counting_waker(drops: &Cell<u32>) -> Waker {
        static VTABLE: RawWakerVTable = RawWakerVTable::new(
            |data| RawWaker::new(data, &VTABLE),
            |_| {},
            |_| {},
            |data| {
                // SAFETY: the data is the `Cell` from `counting_waker`, which every test keeps
                // alive for as long as its wakers.
                let drops = unsafe { &*data.cast::<Cell<u32>>() };
                drops.set(drops.get() + 1);
            },
        );
        // SAFETY: the vtable only reads the data as a `&Cell<u32>`, and `drops` outlives the
        // waker (see above).
        unsafe { Waker::new(ptr::from_ref(drops).cast(), &VTABLE) }
    }

    #[track_caller]
    fn assert_expires(storage: Storage<()>, now: Instant, ids: impl IntoIterator<Item = usize>) {
        let expired = core::iter::from_fn(|| storage.take_expired_timer(now));
        assert_yields(expired.map(|waker| waker.data().addr()), ids);
    }

    #[track_caller]
    fn assert_pending_changes(storage: Storage<()>, expected: impl IntoIterator<Item = IoChange>) {
        let io_waiters = storage.io_waiters_ref();
        let pending = io_waiters
            .iter()
            .filter_map(|(index, io)| io.pending_change(index));
        assert_yields(pending, expected);
    }

    fn noop_waker() -> Waker {
        Waker::noop().clone()
    }

    #[test]
    fn alloc_get_release() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 2, 1);
        let storage = backing.storage();
        let index = storage.alloc_timer(now, noop_waker());
        assert_eq!(storage.get_timer_mut(index).unwrap().deadline, now);

        let later = now + Duration::from_secs(1);
        storage.get_timer_mut(index).unwrap().deadline = later;
        assert_eq!(storage.release_timer(index).unwrap().deadline, later);
        assert!(storage.get_timer_mut(index).is_none());
        assert!(storage.release_timer(index).is_none());
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent sleeps")]
    fn timer_capacity_is_bounded() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let storage = backing.storage();
        storage.alloc_timer(now, noop_waker());
        storage.alloc_timer(now, noop_waker());
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        let earliest = Instant::now();
        let now = earliest + Duration::from_millis(1);
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 8, 1);
        let storage = backing.storage();
        storage.alloc_timer(now + Duration::from_millis(5), waker(3));
        storage.alloc_timer(now, waker(2));
        storage.alloc_timer(now + Duration::from_millis(5), waker(4));
        storage.alloc_timer(now + Duration::from_millis(10), waker(5));
        storage.alloc_timer(earliest, waker(1));

        assert_expires(storage, now + Duration::from_millis(5), [1, 2, 3, 4]);
        assert_expires(storage, now + Duration::from_millis(9), []);
        assert_eq!(
            storage.next_timer_deadline(),
            Some(now + Duration::from_millis(10))
        );
    }

    #[test]
    fn equal_deadlines_expire_in_insertion_order_across_slot_reuse() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 2, 1);
        let storage = backing.storage();
        let first = storage.alloc_timer(now, noop_waker());
        storage.alloc_timer(now, waker(1));
        storage.release_timer(first);
        storage.alloc_timer(now, waker(2));
        assert_expires(storage, now, [1, 2]);
    }

    #[test]
    fn released_timer_is_not_woken() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let storage = backing.storage();
        let index = storage.alloc_timer(now, waker(1));
        storage.release_timer(index);
        assert_expires(storage, now, []);
        assert_eq!(storage.next_timer_deadline(), None);
    }

    #[test]
    fn replaced_waker_is_the_one_woken() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let storage = backing.storage();
        let index = storage.alloc_timer(now, waker(1));
        storage.get_timer_mut(index).unwrap().waker = waker(2);
        assert_expires(storage, now, [2]);
    }

    #[test]
    fn io_alloc_get_release() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let storage = backing.storage();
        let index = storage.alloc_io(3, Direction::Write);
        assert_eq!(storage.get_io(index).unwrap().direction, Direction::Write);
        storage.get_io_mut(index).unwrap().waker = Some(noop_waker());
        assert!(storage.get_io(index).unwrap().waker.is_some());
        assert!(storage.release_io(index).is_some());
        assert!(storage.get_io(index).is_none());
        let reused = storage.alloc_io(3, Direction::Read);
        assert_ne!(reused, index);
    }

    #[test]
    fn io_changes_follow_the_stored_waker() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 2);
        let storage = backing.storage();
        let reader = storage.alloc_io(3, Direction::Read);
        let writer = storage.alloc_io(4, Direction::Write);
        assert_pending_changes(storage, []);

        let change = |action| IoChange {
            token: Token(reader.to_bits()),
            fd: 3,
            direction: Direction::Read,
            action,
        };
        storage.get_io_mut(reader).unwrap().waker = Some(noop_waker());
        assert_pending_changes(storage, [change(IoAction::Arm)]);
        storage.get_io(reader).unwrap().armed.set(true);
        assert_pending_changes(storage, []);

        storage.get_io_mut(reader).unwrap().waker = None;
        assert_pending_changes(storage, [change(IoAction::Disarm)]);
        assert_eq!(IoIndex::from_bits(reader.to_bits()), reader);
        assert!(storage.get_io(writer).is_some());
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent IO operations")]
    fn io_capacity_is_bounded() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let storage = backing.storage();
        storage.alloc_io(3, Direction::Read);
        storage.alloc_io(3, Direction::Write);
    }

    #[test]
    fn dropping_the_storage_drops_live_wakers() {
        let drops = Cell::new(0);
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 2, 2);
        let storage = backing.storage();
        storage.alloc_timer(Instant::now(), counting_waker(&drops));
        let io = storage.alloc_io(3, Direction::Read);
        storage.get_io_mut(io).unwrap().waker = Some(counting_waker(&drops));
        assert_eq!(drops.get(), 0);
        drop(backing);
        assert_eq!(drops.get(), 2);
    }

    #[test]
    #[should_panic(expected = "object pool capacity must not be zero")]
    fn zero_capacity_is_rejected() {
        let mut memory = stack_memory();
        TestStorage::new(&mut memory, 1, 1, 0);
    }

    #[test]
    fn next_timer_deadline_is_the_earliest_one() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 4, 1);
        let storage = backing.storage();
        assert_eq!(storage.next_timer_deadline(), None);
        let later = now + Duration::from_millis(30);
        let earlier = now + Duration::from_millis(10);
        storage.alloc_timer(later, noop_waker());
        let earliest = storage.alloc_timer(earlier, noop_waker());
        storage.alloc_timer(now + Duration::from_millis(20), noop_waker());
        assert_eq!(storage.next_timer_deadline(), Some(earlier));
        storage.release_timer(earliest);
        assert_eq!(
            storage.next_timer_deadline(),
            Some(now + Duration::from_millis(20))
        );
    }

    struct DropFlag(*mut bool);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            // SAFETY: points to a `static mut` of the running test, which only that test's thread
            // uses, and no reference to it exists.
            unsafe { *self.0 = true };
        }
    }

    fn flagged_task(dropped: *mut bool) -> impl Future<Output = ()> {
        let flag = DropFlag(dropped);
        async move {
            let _flag = flag;
        }
    }

    #[test]
    fn removing_a_task_drops_its_future_and_returns_its_join_waiter() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 2, 1, 1);
        let storage = backing.storage();
        static mut DROPPED: bool = false;
        let id = storage
            .insert_task(flagged_task(&raw mut DROPPED))
            .ok()
            .unwrap();
        assert!(storage.task(id).is_some());
        assert_eq!(
            storage.set_join_waiter(id, Waker::noop()),
            JoinStatus::Running
        );
        let join_waiter = storage.remove_task(id).unwrap();
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
        assert!(join_waiter.is_some());
        assert!(storage.task(id).is_none());
        assert_eq!(
            storage.set_join_waiter(id, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn finished_task_id_never_matches_a_reused_slot() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let storage = backing.storage();
        let first = storage.insert_task(async {}).ok().unwrap();
        storage.remove_task(first);
        let second = storage.insert_task(async {}).ok().unwrap();
        assert_ne!(first, second);
        assert!(storage.task(first).is_none());
        assert_eq!(
            storage.set_join_waiter(first, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn full_task_pool_hands_the_future_back() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let storage = backing.storage();
        storage.insert_task(async {}).ok().unwrap();
        static mut DROPPED: bool = false;
        let rejected = storage
            .insert_task(flagged_task(&raw mut DROPPED))
            .err()
            .unwrap();
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(!unsafe { DROPPED });
        drop(rejected);
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
        assert_eq!(storage.task_capacity(), 1);
    }

    #[test]
    fn drop_all_tasks_and_drop_free_the_futures() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 2, 1, 1);
        let storage = backing.storage();
        static mut DROPPED: bool = false;
        static mut LEFT_OVER: bool = false;
        storage
            .insert_task(flagged_task(&raw mut DROPPED))
            .ok()
            .unwrap();
        storage.drop_all_tasks();
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
        assert!(storage.tasks_ref().iter().next().is_none());

        storage
            .insert_task(flagged_task(&raw mut LEFT_OVER))
            .ok()
            .unwrap();
        drop(backing);
        // SAFETY: only this test's thread uses `LEFT_OVER`, and no reference to it exists.
        assert!(unsafe { LEFT_OVER });
    }

    struct SpawnOnDrop {
        storage: Storage<()>,
        // Set once the task was inserted.
        id: *mut Option<TaskId>,
        spawned: *mut Option<TaskId>,
    }

    impl Drop for SpawnOnDrop {
        fn drop(&mut self) {
            // SAFETY: points to a `static mut` of the running test, which only that test's thread
            // uses, and no reference to it exists.
            let id = unsafe { *self.id }.unwrap();
            assert!(self.storage.task(id).is_none());
            assert_eq!(
                self.storage.set_join_waiter(id, Waker::noop()),
                JoinStatus::Finished
            );
            let spawned = self.storage.insert_task(async {}).ok().unwrap();
            // SAFETY: as above.
            unsafe { *self.spawned = Some(spawned) };
        }
    }

    #[test]
    fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 2, 1, 1);
        let storage = backing.storage();
        static mut ID: Option<TaskId> = None;
        static mut SPAWNED: Option<TaskId> = None;
        let guard = SpawnOnDrop {
            storage,
            id: &raw mut ID,
            spawned: &raw mut SPAWNED,
        };
        let dropping = storage
            .insert_task(async move {
                let _guard = guard;
            })
            .ok()
            .unwrap();
        // SAFETY: only this test's thread uses `ID`, and no reference to it exists.
        unsafe { ID = Some(dropping) };
        storage.remove_task(dropping).unwrap();
        // SAFETY: only this test's thread uses `SPAWNED`, and no reference to it exists.
        let spawned = unsafe { SPAWNED }.unwrap();
        assert_ne!(spawned.slot(), dropping.slot());
        assert!(storage.task(spawned).is_some());
    }

    #[test]
    fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 2, 1, 1);
        let storage = backing.storage();
        let ids = [1u8, 2].map(|byte| {
            let data = [byte; TEST_CAPACITIES.max_task_size - 16];
            storage
                .insert_task(async move {
                    assert!(data.iter().all(|b| *b == byte));
                })
                .ok()
                .unwrap()
        });
        let mut cx = core::task::Context::from_waker(Waker::noop());
        for id in ids {
            // SAFETY: the task is alive and not polled re-entrantly.
            assert!(unsafe { storage.task(id).unwrap().poll(&mut cx) }.is_ready());
        }
    }

    #[test]
    #[should_panic(expected = "a task future of 8193 bytes (align 1) doesn't fit max_task_size")]
    fn a_too_big_future_panics() {
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let data = [0u8; 8192];
        let _ = backing.storage().insert_task(async move {
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

    fn small_tasks(memory: &mut [MaybeUninit<u8>], max_tasks: usize) -> TestStorage<'_> {
        TestStorage::with_capacities(
            memory,
            Capacities {
                max_tasks,
                max_task_size: 100,
                max_timers: 1,
                max_io: 1,
            },
        )
    }

    #[test]
    fn max_task_size_is_rounded_up_to_whole_16_byte_blocks() {
        let mut memory = stack_memory();
        let backing = small_tasks(&mut memory, 2);
        let storage = backing.storage();
        let ids = [
            storage.insert_task(FixedFuture([1u8; 112])).ok().unwrap(),
            storage
                .insert_task(FixedFuture((Aligned16, [2u8; 96])))
                .ok()
                .unwrap(),
        ];
        let mut cx = core::task::Context::from_waker(Waker::noop());
        for id in ids {
            // SAFETY: the task is alive and not polled re-entrantly.
            assert!(unsafe { storage.task(id).unwrap().poll(&mut cx) }.is_ready());
        }
    }

    #[test]
    #[should_panic(
        expected = "a task future of 113 bytes (align 1) doesn't fit max_task_size (112 bytes"
    )]
    fn a_future_bigger_than_the_rounded_up_block_panics() {
        let mut memory = stack_memory();
        let backing = small_tasks(&mut memory, 1);
        let _ = backing.storage().insert_task(FixedFuture([0u8; 113]));
    }

    #[test]
    #[should_panic(expected = "(align 32) doesn't fit max_task_size")]
    fn an_overaligned_future_panics() {
        #[repr(align(32))]
        struct Aligned;
        let mut memory = stack_memory();
        let backing = TestStorage::new(&mut memory, 1, 1, 1);
        let aligned = Aligned;
        let _ = backing.storage().insert_task(async move {
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
        let capacities = Capacities {
            max_tasks: 2,
            max_timers: 1,
            max_io: 1,
            ..TEST_CAPACITIES
        };
        let root_layout = Layout::new::<BigRoot>();
        let mut memory = stack_memory();
        // SAFETY: `memory` is only used by this storage, and outlives it.
        let storage =
            unsafe { Storage::new(NonNull::from(&mut memory), (), (), capacities, root_layout) };
        let root = BigRoot {
            data: [3; 9000],
            dropped: &raw mut DROPPED,
        };
        // SAFETY: the only root of this storage.
        let id = unsafe { storage.spawn_root(root) };
        let sibling = storage.insert_task(async {}).ok().unwrap();
        assert_ne!(sibling.slot(), id.slot());
        let mut cx = core::task::Context::from_waker(Waker::noop());
        // SAFETY: the task is alive and not polled re-entrantly.
        assert!(unsafe { storage.task(id).unwrap().poll(&mut cx) }.is_pending());
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(!unsafe { DROPPED });
        // SAFETY: created with the state `()`, destroyed once, no task is being polled.
        unsafe { storage.destroy::<()>() };
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
    }

    const SMALL: Capacities = Capacities {
        max_tasks: 1,
        max_task_size: 16,
        max_timers: 1,
        max_io: 1,
    };

    // `len` bytes of `buffer` starting at the address that wastes the most bytes on alignment.
    fn worst_aligned(buffer: &mut [MaybeUninit<u8>], len: usize) -> NonNull<[MaybeUninit<u8>]> {
        let align = StorageLayout::new::<(), ()>(SMALL, Layout::new::<()>())
            .layout
            .align();
        let start = (0..align)
            .find(|start| buffer[*start..].as_ptr().addr() % align == 1)
            .unwrap();
        NonNull::from(&mut buffer[start..start + len])
    }

    #[test]
    fn required_size_is_enough_at_the_worst_aligned_start() {
        let root_layout = Layout::new::<()>();
        let size = Storage::<()>::required_size::<()>(SMALL, root_layout);
        let mut buffer = stack_memory();
        let memory = worst_aligned(&mut buffer, size);
        // SAFETY: `memory` is only used by this storage, and outlives it.
        let storage = unsafe { Storage::new(memory, (), (), SMALL, root_layout) };
        let id = storage.insert_task(async {}).ok().unwrap();
        assert!(storage.remove_task(id).is_some());
        // SAFETY: created with the state `()`, destroyed once, no task is being polled.
        unsafe { storage.destroy::<()>() };
    }

    #[test]
    #[should_panic(expected = "the EventLoop memory is too small")]
    fn one_byte_less_than_required_panics() {
        let root_layout = Layout::new::<()>();
        let size = Storage::<()>::required_size::<()>(SMALL, root_layout);
        let mut buffer = stack_memory();
        let memory = worst_aligned(&mut buffer, size - 1);
        // SAFETY: `memory` is only used by this storage (which panics before using it).
        let _ = unsafe { Storage::new(memory, (), (), SMALL, root_layout) };
    }

    #[test]
    fn state_region_is_aligned_usable_and_dropped() {
        #[repr(align(64))]
        struct AlignedState<'a> {
            value: u32,
            dropped: &'a Cell<bool>,
        }
        impl Drop for AlignedState<'_> {
            fn drop(&mut self) {
                self.dropped.set(true);
            }
        }
        let dropped = Cell::new(false);
        let capacities = Capacities {
            max_tasks: 1,
            max_timers: 1,
            max_io: 1,
            ..TEST_CAPACITIES
        };
        let root_layout = Layout::new::<()>();
        let mut memory = stack_memory();
        let state = AlignedState {
            value: 7,
            dropped: &dropped,
        };
        // SAFETY: `memory` is only used by this storage, and outlives it.
        let storage = unsafe {
            Storage::new(
                NonNull::from(&mut memory),
                (),
                state,
                capacities,
                root_layout,
            )
        };
        // SAFETY: created with `AlignedState`; nothing else references the state.
        let state = unsafe { storage.state::<AlignedState>().as_mut() };
        assert_eq!((state as *const AlignedState).addr() % 64, 0);
        assert_eq!(state.value, 7);
        state.value = 8;
        // SAFETY: as above.
        assert_eq!(unsafe { storage.state::<AlignedState>().as_ref() }.value, 8);
        assert!(!dropped.get());
        // SAFETY: created with `AlignedState`, destroyed once, no task is being polled.
        unsafe { storage.destroy::<AlignedState>() };
        assert!(dropped.get());
    }
}
