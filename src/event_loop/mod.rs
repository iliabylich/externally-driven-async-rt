use core::cell::{Cell, RefCell, UnsafeCell};
use core::fmt;
use core::marker::PhantomPinned;
use core::pin::Pin;
use core::ptr;
use core::task::Waker;
use std::time::Instant;

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use crate::context::{Context, task_future};
use crate::selectors::{Direction, IoAction, IoChange, Selector, SelectorEvent, Token};
use crate::state::State;
use crate::storage::{IoIndex, Storage, TaskId, io_index, io_token};
use crate::task::TaskFuture;
use crate::waker::new_waker;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Io(rustix::io::Errno),
    RootFinished,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "event loop IO error: {errno}"),
            Self::RootFinished => f.write_str("the root future has finished"),
        }
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(errno) => Some(errno),
            Self::RootFinished => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootStatus {
    Running,
    Finished,
}

#[derive(Debug, Clone, Copy)]
enum Root {
    Running(TaskId),
    Finished,
}

pub struct EventLoop<S: State> {
    selector: Selector,
    storage: RefCell<Storage>,
    root: Cell<Root>,
    // Accessed mutably only by `dispatch` (inside a task, while the caller of `new`, the drain or
    // `drop` has exclusive access to the loop) and by `state_mut` (`Pin<&mut Self>`), so they
    // never overlap; the `UnsafeCell` keeps the shared `&EventLoop`s created by tasks and wakers
    // from conflicting with a live `&mut S`.
    state: UnsafeCell<S>,
    // Tasks and wakers point into the loop, so it must never move, and `&mut EventLoop` must not
    // assert uniqueness while they hold shared references created from those pointers.
    _pinned: PhantomPinned,
}

impl<S: State> EventLoop<S> {
    pub fn new<F>(
        state: S,
        max_tasks_count: usize,
        max_timers_count: usize,
        max_io_count: usize,
        f: F,
    ) -> Result<Pin<Box<Self>>, Error>
    where
        F: AsyncFnOnce(&Context<S>) + 'static,
    {
        let event_loop = Box::pin(Self {
            selector: Selector::new().map_err(Error::Io)?,
            storage: RefCell::new(Storage::new(
                max_tasks_count,
                max_timers_count,
                max_io_count,
            )),
            root: Cell::new(Root::Finished),
            state: UnsafeCell::new(state),
            _pinned: PhantomPinned,
        });
        let ptr: *const Self = &*event_loop;
        // SAFETY: the future is stored in this loop's task storage right away.
        let root = event_loop.spawn(unsafe { task_future(ptr, f) });
        event_loop.root.set(Root::Running(root));
        match event_loop.run_ready_tasks(root) {
            RootStatus::Running => {}
            RootStatus::Finished => return Err(Error::RootFinished),
        }
        event_loop.finish_drain(RootStatus::Running)?;
        Ok(event_loop)
    }

    pub fn drain_events_without_blocking(self: Pin<&mut Self>) -> Result<(), Error> {
        let this = self.into_ref().get_ref();
        let Root::Running(root) = this.root.get() else {
            return Err(Error::RootFinished);
        };
        let status = loop {
            this.process_selector_events().map_err(Error::Io)?;
            if !this.has_ready_tasks() {
                break RootStatus::Running;
            }
            if this.run_ready_tasks(root) == RootStatus::Finished {
                break RootStatus::Finished;
            }
        };
        this.finish_drain(status)
    }

    pub fn state_mut(self: Pin<&mut Self>) -> &mut S {
        // SAFETY: the state is not structurally pinned (it's never pointed to), so handing out
        // `&mut S` doesn't move the loop.
        unsafe { self.get_unchecked_mut() }.state.get_mut()
    }

    fn finish_drain(&self, status: RootStatus) -> Result<(), Error> {
        if status == RootStatus::Finished {
            self.root.set(Root::Finished);
            self.drop_all_tasks();
        }
        self.flush().map_err(Error::Io)?;
        match status {
            RootStatus::Running => Ok(()),
            RootStatus::Finished => Err(Error::RootFinished),
        }
    }

    pub(crate) fn dispatch(&self, event: S::Event) {
        // SAFETY: `dispatch` is only reachable through a `Context`, so it runs inside a task
        // (polled, or dropped), and tasks only run while the caller of `new`,
        // `drain_events_without_blocking` or `drop` has exclusive access to the loop: no
        // `state_mut` reference can be live. `apply` gets no `Context` (and `S`, its events and
        // everything they drop are `'static`, so they can't hold one), so it can't re-enter
        // `dispatch`: this is the only reference to the state.
        unsafe { &mut *self.state.get() }.apply(event);
    }

    pub(crate) fn spawn(&self, future: TaskFuture) -> TaskId {
        let result = self.storage.borrow_mut().insert_task(future);
        match result {
            Ok(id) => {
                self.storage.borrow().ready().push(id);
                id
            }
            Err(future) => {
                // Dropped outside the storage borrow: its `Drop` may use the loop.
                drop(future);
                let capacity = self.storage.borrow().task_capacity();
                panic!("more than {capacity} concurrent tasks");
            }
        }
    }

    pub(crate) fn storage(&self) -> &RefCell<Storage> {
        &self.storage
    }

    pub(crate) fn register_io(&self, fd: BorrowedFd<'_>, direction: Direction) -> IoIndex {
        self.storage
            .borrow_mut()
            .alloc_io(fd.as_raw_fd(), direction)
    }

    /// Must be called while the fd is still open (see `Selector::apply_now`).
    pub(crate) fn deregister_io(&self, index: IoIndex) -> rustix::io::Result<()> {
        let Some(io) = self.storage.borrow_mut().release_io(index) else {
            return Ok(());
        };
        if !io.armed {
            return Ok(());
        }
        self.selector.apply_now(IoChange {
            token: io_token(index),
            fd: io.fd,
            direction: io.direction,
            action: IoAction::Disarm,
        })
    }

    pub(crate) fn set_io_waker(&self, index: IoIndex, waker: &Waker) {
        let mut storage = self.storage.borrow_mut();
        let io = storage.get_io_mut(index).expect("IO is not registered");
        io.waker = Some(waker.clone());
    }

    pub(crate) fn remove_io_waker(&self, index: IoIndex) {
        if let Some(io) = self.storage.borrow_mut().get_io_mut(index) {
            io.waker = None;
        }
    }

    pub(crate) fn has_io_waker(&self, index: IoIndex) -> bool {
        self.storage
            .borrow()
            .get_io(index)
            .is_some_and(|io| io.waker.is_some())
    }

    #[cfg(test)]
    pub(crate) fn io_registration_count(&self) -> usize {
        self.storage.borrow().io_count()
    }

    fn take_io_waker(&self, token: Token, direction: Direction) -> Option<Waker> {
        let mut storage = self.storage.borrow_mut();
        let io = storage.get_io_mut(io_index(token))?;
        if io.direction != direction {
            return None;
        }
        // The filter is one-shot: delivering the event disarmed it.
        io.armed = false;
        io.waker.take()
    }

    fn process_selector_events(&self) -> rustix::io::Result<()> {
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
                    self.storage
                        .borrow_mut()
                        .expire_timers(Instant::now(), &mut wakers);
                }
            }
        }
        wakers.into_iter().for_each(Waker::wake);
        Ok(())
    }

    fn flush(&self) -> rustix::io::Result<()> {
        let mut io_changes = Vec::new();
        let storage = self.storage.borrow();
        match storage.next_timer_deadline() {
            Some(deadline) => self.selector.arm_timer(deadline),
            None => self.selector.disarm_timer(),
        }
        storage.io_changes(&mut io_changes);
        drop(storage);
        self.selector.flush(&io_changes)?;
        self.storage.borrow_mut().mark_io_applied(&io_changes);
        Ok(())
    }

    fn has_ready_tasks(&self) -> bool {
        !self.storage.borrow().ready().is_empty()
    }

    fn run_ready_tasks(&self, root: TaskId) -> RootStatus {
        loop {
            let Some(id) = self.storage.borrow().ready().pop() else {
                return RootStatus::Running;
            };
            let storage = self.storage.borrow();
            let Some(task) = storage.task(id) else {
                continue;
            };
            let ready = ptr::from_ref(storage.ready());
            drop(storage);
            // SAFETY: the ready queue lives in the storage of the pinned `EventLoop`, which
            // outlives every waker except deliberately leaked ones (documented as forbidden).
            let waker = unsafe { new_waker(ready, id) };
            let mut cx = core::task::Context::from_waker(&waker);
            // SAFETY: tasks are removed only right below (after the poll) and by
            // `drop_all_tasks`, which is never called from inside a task; `run_ready_tasks`
            // isn't either, so the task isn't polled re-entrantly.
            if unsafe { task.poll(&mut cx) }.is_pending() {
                continue;
            }
            let (future, join_waiter) = self
                .storage
                .borrow_mut()
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

    fn drop_all_tasks(&self) {
        let mut futures = Vec::new();
        loop {
            self.storage.borrow_mut().take_all_tasks(&mut futures);
            if futures.is_empty() {
                return;
            }
            // Dropping may spawn new tasks, the next iteration drops them as well.
            futures.clear();
        }
    }
}

impl<S: State> Drop for EventLoop<S> {
    fn drop(&mut self) {
        // Task futures hold `Context`s pointing at the loop and may use them in their `Drop`
        // impls, so they go first, while every field is still intact.
        self.drop_all_tasks();
    }
}

impl<S: State> AsFd for EventLoop<S> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.selector.as_fd()
    }
}

impl<S: State> AsRawFd for EventLoop<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

#[cfg(test)]
mod tests;
