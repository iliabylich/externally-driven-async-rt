use alloc::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use core::cell::{Cell, Ref, RefCell, RefMut};
use core::ptr::{self, NonNull};
use core::task::Waker;
use std::time::Instant;

use rustix::fd::{AsRawFd, BorrowedFd, RawFd};

use crate::pool::{Index, ObjectPool};
use crate::ready::ReadyQueue;
use crate::selectors::{Direction, IoAction, IoChange, Selector, SelectorEvent, Token};
use crate::task::{JoinStatus, OwnedTask, TaskFuture, TaskPtr};
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
    // Whether the kernel filter is currently armed; the desired state is `waker.is_some()`.
    pub(crate) armed: bool,
}

pub(crate) type IoIndex = Index<IoWaiter>;

pub(crate) struct TaskEntry {
    future: OwnedTask,
    join_waiter: Option<Waker>,
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
    layout: Layout,
    state: NonNull<u8>,
    selector: Sel,
    root: Cell<Root>,
    next_timer_seq: Cell<u64>,
    // `&mut` to a pool only exists while its `RefCell` is borrowed.
    timers: RefCell<NonNull<ObjectPool<Timer>>>,
    io_waiters: RefCell<NonNull<ObjectPool<IoWaiter>>>,
    tasks: RefCell<NonNull<ObjectPool<TaskEntry>>>,
    ready: NonNull<ReadyQueue>,
}

// The runtime: one allocation that never moves, reached through this pointer to its start (copied
// into the `EventLoop` and every `Context`). Objects inside are addressed by typed indices.
pub(crate) struct Storage<Sel = Selector>(NonNull<StorageHeader<Sel>>);

impl<Sel> Clone for Storage<Sel> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Sel> Copy for Storage<Sel> {}

impl<Sel> Storage<Sel> {
    pub(crate) fn new<S>(
        selector: Sel,
        state: S,
        max_tasks_count: usize,
        max_timers_count: usize,
        max_io_count: usize,
    ) -> Self {
        let extend = |layout: Layout, next: Layout| layout.extend(next).expect("too large");
        let layout = Layout::new::<StorageHeader<Sel>>();
        let (layout, state_offset) = extend(layout, Layout::new::<S>());
        let (layout, timers_offset) = extend(layout, ObjectPool::<Timer>::layout(max_timers_count));
        let (layout, io_offset) = extend(layout, ObjectPool::<IoWaiter>::layout(max_io_count));
        let (layout, tasks_offset) =
            extend(layout, ObjectPool::<TaskEntry>::layout(max_tasks_count));
        let (layout, ready_offset) = extend(layout, ReadyQueue::layout(max_tasks_count));
        let layout = layout.pad_to_align();
        // SAFETY: `layout` isn't zero-sized, it holds at least the header.
        let base =
            NonNull::new(unsafe { alloc(layout) }).unwrap_or_else(|| handle_alloc_error(layout));
        // SAFETY: `Layout::extend` placed every part inside the fresh allocation, aligned for it
        // and not overlapping the others; each is written exactly once, the header last.
        unsafe {
            let state_ptr = base.byte_add(state_offset);
            state_ptr.cast::<S>().write(state);
            let header = StorageHeader {
                layout,
                state: state_ptr,
                selector,
                root: Cell::new(Root::Finished),
                next_timer_seq: Cell::new(0),
                timers: RefCell::new(ObjectPool::init(
                    base.byte_add(timers_offset),
                    max_timers_count,
                )),
                io_waiters: RefCell::new(ObjectPool::init(base.byte_add(io_offset), max_io_count)),
                tasks: RefCell::new(ObjectPool::init(
                    base.byte_add(tasks_offset),
                    max_tasks_count,
                )),
                ready: ReadyQueue::init(base.byte_add(ready_offset), max_tasks_count),
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

    /// Drops every task, the state, the pools and the header, and frees the allocation.
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
        let layout = header.layout;
        let parts = (
            *header.timers.borrow(),
            *header.io_waiters.borrow(),
            *header.tasks.borrow(),
        );
        // SAFETY: no task (so no `Context` and no reference into the storage) is left; every part
        // was initialized in `new` and is dropped exactly once here (the ready queue holds only
        // `Copy` data), the header last, then the allocation is freed with its layout.
        unsafe {
            ptr::drop_in_place(self.state::<S>().as_ptr());
            ptr::drop_in_place(parts.0.as_ptr());
            ptr::drop_in_place(parts.1.as_ptr());
            ptr::drop_in_place(parts.2.as_ptr());
            ptr::drop_in_place(self.0.as_ptr());
            dealloc(self.0.cast().as_ptr(), layout);
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

    pub(crate) fn insert_task(&self, future: TaskFuture) -> Result<TaskId, TaskFuture> {
        let entry = TaskEntry {
            future: OwnedTask::new(future),
            join_waiter: None,
        };
        let id = self
            .tasks()
            .alloc(entry)
            .map_err(|entry| entry.future.into_future())?;
        self.ready().track(id);
        Ok(id)
    }

    pub(crate) fn task(&self, id: TaskId) -> Option<TaskPtr> {
        Some(self.tasks_ref().get(id)?.future.ptr())
    }

    pub(crate) fn remove_task(&self, id: TaskId) -> Option<(TaskFuture, Option<Waker>)> {
        let entry = self.tasks().free(id)?;
        self.ready().untrack(id);
        Some((entry.future.into_future(), entry.join_waiter))
    }

    pub(crate) fn set_join_waiter(&self, id: TaskId, waker: &Waker) -> JoinStatus {
        let mut tasks = self.tasks();
        let Some(entry) = tasks.get_mut(id) else {
            return JoinStatus::Finished;
        };
        match &mut entry.join_waiter {
            Some(stored) => stored.clone_from(waker),
            None => entry.join_waiter = Some(waker.clone()),
        }
        JoinStatus::Running
    }

    pub(crate) fn take_all_tasks(&self, out: &mut Vec<TaskFuture>) {
        let ids: Vec<TaskId> = self.tasks_ref().iter().map(|(id, _)| id).collect();
        let mut tasks = self.tasks();
        for id in ids {
            let entry = tasks.free(id).expect("an iterated task is in the pool");
            self.ready().untrack(id);
            out.push(entry.future.into_future());
        }
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

    pub(crate) fn expire_timers(&self, now: Instant, out: &mut Vec<Waker>) {
        let mut timers = self.timers();
        let mut due: Vec<(Instant, u64, TimerIndex)> = timers
            .iter()
            .filter(|(_, timer)| timer.deadline <= now)
            .map(|(index, timer)| (timer.deadline, timer.seq, index))
            .collect();
        due.sort_unstable_by_key(|(deadline, seq, _)| (*deadline, *seq));
        for (_, _, index) in due {
            let timer = timers.free(index).expect("a due timer is in the pool");
            out.push(timer.waker);
        }
    }

    pub(crate) fn alloc_io(&self, fd: RawFd, direction: Direction) -> IoIndex {
        let waiter = IoWaiter {
            fd,
            direction,
            waker: None,
            armed: false,
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

    pub(crate) fn io_changes(&self, out: &mut Vec<IoChange>) {
        for (index, io) in self.io_waiters_ref().iter() {
            let desired = io.waker.is_some();
            if desired == io.armed {
                continue;
            }
            let action = if desired {
                IoAction::Arm
            } else {
                IoAction::Disarm
            };
            out.push(IoChange {
                token: Token(index.to_bits()),
                fd: io.fd,
                direction: io.direction,
                action,
            });
        }
    }

    pub(crate) fn mark_io_applied(&self, changes: &[IoChange]) {
        let mut io_waiters = self.io_waiters();
        for change in changes {
            if let Some(io) = io_waiters.get_mut(IoIndex::from_bits(change.token.0)) {
                io.armed = change.action == IoAction::Arm;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn io_count(&self) -> usize {
        self.io_waiters_ref().iter().count()
    }

    pub(crate) fn spawn(&self, future: TaskFuture) -> TaskId {
        let result = self.insert_task(future);
        match result {
            Ok(id) => {
                self.ready().push(id);
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
        io.armed = false;
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
            let (future, join_waiter) = self
                .remove_task(id)
                .expect("a finished task is still in storage");
            drop(future);
            if let Some(join_waiter) = join_waiter {
                join_waiter.wake();
            }
            if id == root {
                return RootStatus::Finished;
            }
        }
    }

    pub(crate) fn drop_all_tasks(&self) {
        let mut futures = Vec::new();
        loop {
            self.take_all_tasks(&mut futures);
            if futures.is_empty() {
                return;
            }
            // Dropping may spawn new tasks, the next iteration drops them as well.
            futures.clear();
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
        if !io.armed {
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
        let mut events = Vec::new();
        self.selector().poll_nonblocking(&mut events)?;
        let mut wakers = Vec::new();
        for event in events {
            match event {
                SelectorEvent::Readable(token) => {
                    wakers.extend(self.take_io_waker(token, Direction::Read));
                }
                SelectorEvent::Writable(token) => {
                    wakers.extend(self.take_io_waker(token, Direction::Write));
                }
                SelectorEvent::Timer => self.expire_timers(Instant::now(), &mut wakers),
            }
        }
        wakers.into_iter().for_each(Waker::wake);
        Ok(())
    }

    pub(crate) fn flush(&self) -> rustix::io::Result<()> {
        match self.next_timer_deadline() {
            Some(deadline) => self.selector().arm_timer(deadline),
            None => self.selector().disarm_timer(),
        }
        let mut io_changes = Vec::new();
        self.io_changes(&mut io_changes);
        self.selector().flush(&io_changes)?;
        self.mark_io_applied(&io_changes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::JoinStatus;
    use crate::test_utils::TestStorage;
    use alloc::sync::Arc;
    use core::time::Duration;
    use std::sync::Mutex;
    use std::task::Wake;

    #[derive(Default)]
    struct Log(Mutex<Vec<u32>>);

    struct Recorder {
        id: u32,
        log: Arc<Log>,
    }

    impl Wake for Recorder {
        fn wake(self: Arc<Self>) {
            self.log.0.lock().unwrap().push(self.id);
        }
    }

    fn waker(log: &Arc<Log>, id: u32) -> Waker {
        Waker::from(Arc::new(Recorder {
            id,
            log: log.clone(),
        }))
    }

    fn wake_all(wakers: Vec<Waker>, log: &Arc<Log>) -> Vec<u32> {
        wakers.into_iter().for_each(Waker::wake);
        core::mem::take(&mut *log.0.lock().unwrap())
    }

    fn noop_waker() -> Waker {
        Waker::noop().clone()
    }

    #[test]
    fn alloc_get_release() {
        let now = Instant::now();
        let backing = TestStorage::new(1, 2, 1);
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
        let backing = TestStorage::new(1, 1, 1);
        let storage = backing.storage();
        storage.alloc_timer(now, noop_waker());
        storage.alloc_timer(now, noop_waker());
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let backing = TestStorage::new(1, 8, 1);
        let storage = backing.storage();
        storage.alloc_timer(now + Duration::from_millis(5), waker(&log, 3));
        storage.alloc_timer(now, waker(&log, 2));
        storage.alloc_timer(now + Duration::from_millis(5), waker(&log, 4));
        storage.alloc_timer(now + Duration::from_millis(10), waker(&log, 5));
        storage.alloc_timer(now - Duration::from_millis(1), waker(&log, 1));

        let mut out = Vec::new();
        storage.expire_timers(now + Duration::from_millis(5), &mut out);
        assert_eq!(wake_all(out, &log), vec![1, 2, 3, 4]);

        let mut out = Vec::new();
        storage.expire_timers(now + Duration::from_millis(9), &mut out);
        assert!(out.is_empty());
        assert_eq!(
            storage.next_timer_deadline(),
            Some(now + Duration::from_millis(10))
        );
    }

    #[test]
    fn equal_deadlines_expire_in_insertion_order_across_slot_reuse() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let backing = TestStorage::new(1, 2, 1);
        let storage = backing.storage();
        let first = storage.alloc_timer(now, noop_waker());
        storage.alloc_timer(now, waker(&log, 1));
        storage.release_timer(first);
        storage.alloc_timer(now, waker(&log, 2));
        let mut out = Vec::new();
        storage.expire_timers(now, &mut out);
        assert_eq!(wake_all(out, &log), vec![1, 2]);
    }

    #[test]
    fn released_timer_is_not_woken() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let backing = TestStorage::new(1, 1, 1);
        let storage = backing.storage();
        let index = storage.alloc_timer(now, waker(&log, 1));
        storage.release_timer(index);
        let mut out = Vec::new();
        storage.expire_timers(now, &mut out);
        assert!(out.is_empty());
        assert_eq!(storage.next_timer_deadline(), None);
    }

    #[test]
    fn replaced_waker_is_the_one_woken() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let backing = TestStorage::new(1, 1, 1);
        let storage = backing.storage();
        let index = storage.alloc_timer(now, waker(&log, 1));
        storage.get_timer_mut(index).unwrap().waker = waker(&log, 2);
        let mut out = Vec::new();
        storage.expire_timers(now, &mut out);
        assert_eq!(wake_all(out, &log), vec![2]);
    }

    #[test]
    fn io_alloc_get_release() {
        let backing = TestStorage::new(1, 1, 1);
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
        let backing = TestStorage::new(1, 1, 2);
        let storage = backing.storage();
        let reader = storage.alloc_io(3, Direction::Read);
        let writer = storage.alloc_io(4, Direction::Write);
        let mut changes = Vec::new();
        storage.io_changes(&mut changes);
        assert!(changes.is_empty());

        storage.get_io_mut(reader).unwrap().waker = Some(noop_waker());
        storage.io_changes(&mut changes);
        assert_eq!(
            changes,
            vec![IoChange {
                token: Token(reader.to_bits()),
                fd: 3,
                direction: Direction::Read,
                action: IoAction::Arm,
            }]
        );
        storage.mark_io_applied(&changes);
        changes.clear();
        storage.io_changes(&mut changes);
        assert!(changes.is_empty());

        storage.get_io_mut(reader).unwrap().waker = None;
        storage.io_changes(&mut changes);
        assert_eq!(changes[0].action, IoAction::Disarm);
        assert_eq!(IoIndex::from_bits(changes[0].token.0), reader);
        assert!(storage.get_io(writer).is_some());
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent IO operations")]
    fn io_capacity_is_bounded() {
        let backing = TestStorage::new(1, 1, 1);
        let storage = backing.storage();
        storage.alloc_io(3, Direction::Read);
        storage.alloc_io(3, Direction::Write);
    }

    #[test]
    fn dropping_the_storage_drops_live_wakers() {
        let log = Arc::new(Log::default());
        let backing = TestStorage::new(1, 2, 2);
        let storage = backing.storage();
        storage.alloc_timer(Instant::now(), waker(&log, 1));
        let io = storage.alloc_io(3, Direction::Read);
        storage.get_io_mut(io).unwrap().waker = Some(waker(&log, 2));
        assert_eq!(Arc::strong_count(&log), 3);
        drop(backing);
        assert_eq!(Arc::strong_count(&log), 1);
    }

    #[test]
    #[should_panic(expected = "object pool capacity must not be zero")]
    fn zero_capacity_is_rejected() {
        TestStorage::new(1, 1, 0);
    }

    #[test]
    fn next_timer_deadline_is_the_earliest_one() {
        let now = Instant::now();
        let backing = TestStorage::new(1, 4, 1);
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

    struct DropFlag(std::rc::Rc<core::cell::Cell<bool>>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    fn flagged_task() -> (TaskFuture, std::rc::Rc<core::cell::Cell<bool>>) {
        let dropped = std::rc::Rc::new(core::cell::Cell::new(false));
        let flag = DropFlag(dropped.clone());
        let future: TaskFuture = Box::pin(async move {
            let _flag = flag;
        });
        (future, dropped)
    }

    #[test]
    fn removing_a_task_returns_its_future_and_join_waiter() {
        let backing = TestStorage::new(2, 1, 1);
        let storage = backing.storage();
        let id = storage.insert_task(Box::pin(async {})).ok().unwrap();
        assert!(storage.task(id).is_some());
        assert_eq!(
            storage.set_join_waiter(id, Waker::noop()),
            JoinStatus::Running
        );
        let (_, join_waiter) = storage.remove_task(id).unwrap();
        assert!(join_waiter.is_some());
        assert!(storage.task(id).is_none());
        assert_eq!(
            storage.set_join_waiter(id, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn finished_task_id_never_matches_a_reused_slot() {
        let backing = TestStorage::new(1, 1, 1);
        let storage = backing.storage();
        let first = storage.insert_task(Box::pin(async {})).ok().unwrap();
        storage.remove_task(first);
        let second = storage.insert_task(Box::pin(async {})).ok().unwrap();
        assert_ne!(first, second);
        assert!(storage.task(first).is_none());
        assert_eq!(
            storage.set_join_waiter(first, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn full_task_pool_hands_the_future_back() {
        let backing = TestStorage::new(1, 1, 1);
        let storage = backing.storage();
        storage.insert_task(Box::pin(async {})).ok().unwrap();
        let (future, dropped) = flagged_task();
        let rejected = storage.insert_task(future).err().unwrap();
        assert!(!dropped.get());
        drop(rejected);
        assert!(dropped.get());
        assert_eq!(storage.task_capacity(), 1);
    }

    #[test]
    fn take_all_tasks_and_drop_free_the_futures() {
        let backing = TestStorage::new(2, 1, 1);
        let storage = backing.storage();
        let (future, taken) = flagged_task();
        storage.insert_task(future).ok().unwrap();
        let mut out = Vec::new();
        storage.take_all_tasks(&mut out);
        assert_eq!(out.len(), 1);
        assert!(!taken.get());
        drop(out);
        assert!(taken.get());

        let (future, left_over) = flagged_task();
        storage.insert_task(future).ok().unwrap();
        drop(backing);
        assert!(left_over.get());
    }

    #[test]
    fn state_region_is_aligned_usable_and_dropped() {
        #[repr(align(64))]
        struct AlignedState {
            value: u32,
            dropped: std::rc::Rc<core::cell::Cell<bool>>,
        }
        impl Drop for AlignedState {
            fn drop(&mut self) {
                self.dropped.set(true);
            }
        }
        let dropped = std::rc::Rc::new(core::cell::Cell::new(false));
        let storage = Storage::new(
            (),
            AlignedState {
                value: 7,
                dropped: dropped.clone(),
            },
            1,
            1,
            1,
        );
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
