use alloc::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use core::ptr::{self, NonNull};
use core::task::Waker;

use rustix::fd::RawFd;
use std::time::Instant;

use crate::pool::{Index, ObjectPool};
use crate::ready::ReadyQueue;
use crate::selectors::{Direction, IoAction, IoChange, Token};
use crate::task::{JoinStatus, OwnedTask, TaskFuture, TaskPtr};

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

pub(crate) fn io_token(index: IoIndex) -> Token {
    Token(index.to_bits())
}

pub(crate) fn io_index(token: Token) -> IoIndex {
    IoIndex::from_bits(token.0)
}

pub(crate) struct TaskEntry {
    future: OwnedTask,
    join_waiter: Option<Waker>,
}

pub(crate) type TaskId = Index<TaskEntry>;

// One heap chunk holding every pool back to back, laid out like a C struct (`Layout::extend`);
// each pool (free list included) lives entirely inside it.
pub(crate) struct Storage {
    chunk: NonNull<u8>,
    layout: Layout,
    timers: NonNull<ObjectPool<Timer>>,
    io_waiters: NonNull<ObjectPool<IoWaiter>>,
    tasks: NonNull<ObjectPool<TaskEntry>>,
    ready: NonNull<ReadyQueue>,
    next_timer_seq: u64,
}

impl Storage {
    pub(crate) fn new(
        max_tasks_count: usize,
        max_timers_count: usize,
        max_io_count: usize,
    ) -> Self {
        let (layout, io_offset) = ObjectPool::<Timer>::layout(max_timers_count)
            .extend(ObjectPool::<IoWaiter>::layout(max_io_count))
            .expect("storage is too large");
        let (layout, tasks_offset) = layout
            .extend(ObjectPool::<TaskEntry>::layout(max_tasks_count))
            .expect("storage is too large");
        let (layout, ready_offset) = layout
            .extend(ReadyQueue::layout(max_tasks_count))
            .expect("storage is too large");
        let layout = layout.pad_to_align();
        // SAFETY: `layout` isn't zero-sized: every pool holds at least its free-list head.
        let chunk =
            NonNull::new(unsafe { alloc(layout) }).unwrap_or_else(|| handle_alloc_error(layout));
        // SAFETY: `Layout::extend` placed each pool at an offset inside the allocation, aligned
        // for it and not overlapping the others; the chunk lives until `drop`.
        let (timers, io_waiters, tasks, ready) = unsafe {
            (
                ObjectPool::init(chunk, max_timers_count),
                ObjectPool::init(chunk.byte_add(io_offset), max_io_count),
                ObjectPool::init(chunk.byte_add(tasks_offset), max_tasks_count),
                ReadyQueue::init(chunk.byte_add(ready_offset), max_tasks_count),
            )
        };
        Self {
            chunk,
            layout,
            timers,
            io_waiters,
            tasks,
            ready,
            next_timer_seq: 0,
        }
    }

    fn timers(&mut self) -> &mut ObjectPool<Timer> {
        // SAFETY: the pool is initialized in `new` and lives until `drop`; `&mut self` makes
        // this the only reference to it.
        unsafe { self.timers.as_mut() }
    }

    fn timers_ref(&self) -> &ObjectPool<Timer> {
        // SAFETY: as in `timers`, and `&self` rules out a mutable reference.
        unsafe { self.timers.as_ref() }
    }

    fn io_waiters(&mut self) -> &mut ObjectPool<IoWaiter> {
        // SAFETY: as in `timers`.
        unsafe { self.io_waiters.as_mut() }
    }

    fn io_waiters_ref(&self) -> &ObjectPool<IoWaiter> {
        // SAFETY: as in `timers_ref`.
        unsafe { self.io_waiters.as_ref() }
    }

    fn tasks(&mut self) -> &mut ObjectPool<TaskEntry> {
        // SAFETY: as in `timers`.
        unsafe { self.tasks.as_mut() }
    }

    fn tasks_ref(&self) -> &ObjectPool<TaskEntry> {
        // SAFETY: as in `timers_ref`.
        unsafe { self.tasks.as_ref() }
    }

    pub(crate) fn ready(&self) -> &ReadyQueue {
        // SAFETY: the queue is initialized in `new` and lives until `drop`; it is only ever
        // accessed through shared references (it consists of `Cell`s).
        unsafe { self.ready.as_ref() }
    }

    pub(crate) fn task_capacity(&self) -> usize {
        self.tasks_ref().capacity()
    }

    pub(crate) fn insert_task(&mut self, future: TaskFuture) -> Result<TaskId, TaskFuture> {
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

    pub(crate) fn remove_task(&mut self, id: TaskId) -> Option<(TaskFuture, Option<Waker>)> {
        let entry = self.tasks().free(id)?;
        self.ready().untrack(id);
        Some((entry.future.into_future(), entry.join_waiter))
    }

    pub(crate) fn set_join_waiter(&mut self, id: TaskId, waker: &Waker) -> JoinStatus {
        let Some(entry) = self.tasks().get_mut(id) else {
            return JoinStatus::Finished;
        };
        match &mut entry.join_waiter {
            Some(stored) => stored.clone_from(waker),
            None => entry.join_waiter = Some(waker.clone()),
        }
        JoinStatus::Running
    }

    pub(crate) fn take_all_tasks(&mut self, out: &mut Vec<TaskFuture>) {
        let ids: Vec<TaskId> = self.tasks_ref().iter().map(|(id, _)| id).collect();
        for id in ids {
            let entry = self
                .tasks()
                .free(id)
                .expect("an iterated task is in the pool");
            self.ready().untrack(id);
            out.push(entry.future.into_future());
        }
    }

    pub(crate) fn alloc_timer(&mut self, deadline: Instant, waker: Waker) -> TimerIndex {
        let timer = Timer {
            deadline,
            waker,
            seq: self.next_timer_seq,
        };
        let timers = self.timers();
        let Ok(index) = timers.alloc(timer) else {
            panic!("more than {} concurrent sleeps", timers.capacity());
        };
        self.next_timer_seq += 1;
        index
    }

    pub(crate) fn get_timer_mut(&mut self, index: TimerIndex) -> Option<&mut Timer> {
        self.timers().get_mut(index)
    }

    pub(crate) fn release_timer(&mut self, index: TimerIndex) -> Option<Timer> {
        self.timers().free(index)
    }

    pub(crate) fn next_timer_deadline(&self) -> Option<Instant> {
        self.timers_ref()
            .iter()
            .map(|(_, timer)| timer.deadline)
            .min()
    }

    pub(crate) fn expire_timers(&mut self, now: Instant, out: &mut Vec<Waker>) {
        let timers = self.timers();
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

    pub(crate) fn alloc_io(&mut self, fd: RawFd, direction: Direction) -> IoIndex {
        let io_waiters = self.io_waiters();
        let waiter = IoWaiter {
            fd,
            direction,
            waker: None,
            armed: false,
        };
        let Ok(index) = io_waiters.alloc(waiter) else {
            panic!(
                "more than {} concurrent IO operations",
                io_waiters.capacity()
            );
        };
        index
    }

    pub(crate) fn get_io(&self, index: IoIndex) -> Option<&IoWaiter> {
        self.io_waiters_ref().get(index)
    }

    pub(crate) fn get_io_mut(&mut self, index: IoIndex) -> Option<&mut IoWaiter> {
        self.io_waiters().get_mut(index)
    }

    pub(crate) fn release_io(&mut self, index: IoIndex) -> Option<IoWaiter> {
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
                token: io_token(index),
                fd: io.fd,
                direction: io.direction,
                action,
            });
        }
    }

    pub(crate) fn mark_io_applied(&mut self, changes: &[IoChange]) {
        for change in changes {
            if let Some(io) = self.io_waiters().get_mut(io_index(change.token)) {
                io.armed = change.action == IoAction::Arm;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn io_count(&self) -> usize {
        self.io_waiters_ref().iter().count()
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        // SAFETY: both pools are initialized (see `new`) and dropped exactly once here; the
        // chunk is then freed with the layout it was allocated with.
        unsafe {
            ptr::drop_in_place(self.timers.as_ptr());
            ptr::drop_in_place(self.io_waiters.as_ptr());
            ptr::drop_in_place(self.tasks.as_ptr());
            dealloc(self.chunk.as_ptr(), self.layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::JoinStatus;
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
        let mut storage = Storage::new(1, 2, 1);
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
        let mut storage = Storage::new(1, 1, 1);
        storage.alloc_timer(now, noop_waker());
        storage.alloc_timer(now, noop_waker());
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let mut storage = Storage::new(1, 8, 1);
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
        let mut storage = Storage::new(1, 2, 1);
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
        let mut storage = Storage::new(1, 1, 1);
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
        let mut storage = Storage::new(1, 1, 1);
        let index = storage.alloc_timer(now, waker(&log, 1));
        storage.get_timer_mut(index).unwrap().waker = waker(&log, 2);
        let mut out = Vec::new();
        storage.expire_timers(now, &mut out);
        assert_eq!(wake_all(out, &log), vec![2]);
    }

    #[test]
    fn io_alloc_get_release() {
        let mut storage = Storage::new(1, 1, 1);
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
        let mut storage = Storage::new(1, 1, 2);
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
                token: io_token(reader),
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
        assert_eq!(io_index(changes[0].token), reader);
        assert!(storage.get_io(writer).is_some());
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent IO operations")]
    fn io_capacity_is_bounded() {
        let mut storage = Storage::new(1, 1, 1);
        storage.alloc_io(3, Direction::Read);
        storage.alloc_io(3, Direction::Write);
    }

    #[test]
    fn dropping_the_storage_drops_live_wakers() {
        let log = Arc::new(Log::default());
        let mut storage = Storage::new(1, 2, 2);
        storage.alloc_timer(Instant::now(), waker(&log, 1));
        let io = storage.alloc_io(3, Direction::Read);
        storage.get_io_mut(io).unwrap().waker = Some(waker(&log, 2));
        assert_eq!(Arc::strong_count(&log), 3);
        drop(storage);
        assert_eq!(Arc::strong_count(&log), 1);
    }

    #[test]
    #[should_panic(expected = "object pool capacity must not be zero")]
    fn zero_capacity_is_rejected() {
        Storage::new(1, 1, 0);
    }

    #[test]
    fn next_timer_deadline_is_the_earliest_one() {
        let now = Instant::now();
        let mut storage = Storage::new(1, 4, 1);
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
        let mut storage = Storage::new(2, 1, 1);
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
        let mut storage = Storage::new(1, 1, 1);
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
        let mut storage = Storage::new(1, 1, 1);
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
        let mut storage = Storage::new(2, 1, 1);
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
        drop(storage);
        assert!(left_over.get());
    }
}
