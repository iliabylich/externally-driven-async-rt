use alloc::collections::VecDeque;
use core::cell::RefCell;
use core::mem;
use core::ptr::NonNull;
use core::task::Waker;
use std::collections::HashMap;
use std::time::Instant;

use rustix::fd::BorrowedFd;

use crate::selectors::{Interest, Selector, SelectorEvent, Token};
use crate::task::{TaskFuture, TaskId, Tasks};
use crate::timers::Timers;
use crate::waker::{ReadyQueue, new_waker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootStatus {
    Running,
    Finished,
}

#[derive(Default)]
struct IoWaiters {
    reader: Option<Waker>,
    writer: Option<Waker>,
}

impl IoWaiters {
    fn slot(&mut self, direction: Direction) -> &mut Option<Waker> {
        match direction {
            Direction::Read => &mut self.reader,
            Direction::Write => &mut self.writer,
        }
    }

    fn interest(&self) -> Interest {
        Interest {
            read: self.reader.is_some(),
            write: self.writer.is_some(),
        }
    }
}

pub(crate) struct Inner<E> {
    selector: Selector,
    timers: RefCell<Timers>,
    tasks: RefCell<Tasks>,
    ready: ReadyQueue,
    events: RefCell<VecDeque<E>>,
    io: RefCell<HashMap<Token, IoWaiters>>,
}

impl<E> Inner<E> {
    fn new() -> rustix::io::Result<Self> {
        Ok(Self {
            selector: Selector::new()?,
            timers: RefCell::default(),
            tasks: RefCell::default(),
            ready: ReadyQueue::default(),
            events: RefCell::default(),
            io: RefCell::default(),
        })
    }

    pub(crate) fn spawn(&self, future: TaskFuture) -> TaskId {
        let id = self.tasks.borrow_mut().insert(future);
        self.ready.borrow_mut().push(id);
        id
    }

    pub(crate) fn register_io(&self, fd: BorrowedFd<'_>) -> Token {
        let token = self.selector.register(fd);
        self.io.borrow_mut().insert(token, IoWaiters::default());
        token
    }

    pub(crate) fn deregister_io(&self, token: Token) -> rustix::io::Result<()> {
        self.io.borrow_mut().remove(&token);
        self.selector.deregister(token)
    }

    pub(crate) fn set_io_waker(&self, token: Token, direction: Direction, waker: &Waker) {
        let mut io = self.io.borrow_mut();
        let waiters = io.get_mut(&token).expect("IO token is not registered");
        *waiters.slot(direction) = Some(waker.clone());
        self.selector.set_interest(token, waiters.interest());
    }

    pub(crate) fn remove_io_waker(&self, token: Token, direction: Direction) {
        let mut io = self.io.borrow_mut();
        let Some(waiters) = io.get_mut(&token) else {
            return;
        };
        *waiters.slot(direction) = None;
        self.selector.set_interest(token, waiters.interest());
    }

    fn take_io_waker(&self, token: Token, direction: Direction) -> Option<Waker> {
        let mut io = self.io.borrow_mut();
        let waiters = io.get_mut(&token)?;
        let waker = waiters.slot(direction).take();
        self.selector.set_interest(token, waiters.interest());
        waker
    }

    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = Vec::new();
        self.selector.poll_nonblocking(&mut events)?;
        let mut wakers = Vec::new();
        for event in events {
            match event {
                SelectorEvent::Readable(token) => {
                    wakers.extend(self.take_io_waker(token, Direction::Read));
                }
                SelectorEvent::Writable(token) => {
                    wakers.extend(self.take_io_waker(token, Direction::Write));
                }
                SelectorEvent::Timer => {
                    self.timers.borrow_mut().expire(Instant::now(), &mut wakers);
                }
            }
        }
        wakers.into_iter().for_each(Waker::wake);
        Ok(())
    }

    pub(crate) fn flush(&self) -> rustix::io::Result<()> {
        match self.timers.borrow().next_deadline() {
            Some(deadline) => self.selector.arm_timer(deadline),
            None => self.selector.disarm_timer(),
        }
        self.selector.flush()
    }

    pub(crate) fn run_ready_tasks(&self, root: TaskId) -> RootStatus {
        loop {
            let ready = mem::take(&mut *self.ready.borrow_mut());
            if ready.is_empty() {
                return RootStatus::Running;
            }
            for id in ready {
                let Some(task) = self.tasks.borrow().future(id) else {
                    continue;
                };
                // SAFETY: the ready queue is a field of `Inner`, which outlives every waker
                // except deliberately leaked ones (documented as forbidden).
                let waker = unsafe { new_waker(&self.ready, id) };
                let mut cx = core::task::Context::from_waker(&waker);
                // SAFETY: tasks are removed only right below (after the poll) and by
                // `drop_all_tasks`, which is never called from inside a task; `run_ready_tasks`
                // isn't either, so the task isn't polled re-entrantly.
                if unsafe { task.poll(&mut cx) }.is_pending() {
                    continue;
                }
                let (future, join_waiter) = self
                    .tasks
                    .borrow_mut()
                    .remove(id)
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
    }

    pub(crate) fn drop_all_tasks(&self) {
        let mut futures = Vec::new();
        loop {
            self.tasks.borrow_mut().take_all(&mut futures);
            if futures.is_empty() {
                return;
            }
            // Dropping may spawn new tasks, the next iteration drops them as well.
            futures.clear();
        }
    }
}

pub(crate) struct OwnedInner<E>(NonNull<Inner<E>>);

impl<E> OwnedInner<E> {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        Ok(Self(NonNull::from(Box::leak(Box::new(Inner::new()?)))))
    }

    pub(crate) fn get(&self) -> &Inner<E> {
        // SAFETY: the pointer comes from `Box::leak` in `new` and is freed only in `drop`;
        // only shared references to `Inner` are ever created.
        unsafe { self.0.as_ref() }
    }

    pub(crate) fn as_ptr(&self) -> *const Inner<E> {
        self.0.as_ptr()
    }
}

impl<E> Drop for OwnedInner<E> {
    fn drop(&mut self) {
        self.get().drop_all_tasks();
        // SAFETY: the pointer comes from `Box::leak` in `new`. Every `Context` lives inside a
        // task future and all of them were just dropped, so no reference to `Inner` is left.
        drop(unsafe { Box::from_raw(self.0.as_ptr()) });
    }
}

#[cfg(test)]
mod tests;
