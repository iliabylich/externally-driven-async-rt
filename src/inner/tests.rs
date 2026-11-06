use super::*;
use core::cell::Cell;
use core::future::{Future, pending, poll_fn};
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::rc::Rc;

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fd::{AsFd, OwnedFd};
use rustix::net::{AddressFamily, SocketFlags, SocketType, socketpair};

fn inner() -> OwnedInner<()> {
    OwnedInner::new().unwrap()
}

fn counting(polls: &Rc<Cell<u32>>) -> TaskFuture {
    let polls = polls.clone();
    Box::pin(poll_fn(move |_| {
        polls.set(polls.get() + 1);
        Poll::<()>::Pending
    }))
}

fn parked_task(inner: &Inner<()>) -> (TaskId, Waker) {
    let id = inner.tasks.borrow_mut().insert(Box::pin(pending()));
    // SAFETY: `inner` outlives the waker.
    let waker = unsafe { new_waker(&inner.ready, id) };
    (id, waker)
}

fn pair() -> (OwnedFd, OwnedFd) {
    let (a, b) = socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::empty(),
        None,
    )
    .unwrap();
    rustix::io::ioctl_fionbio(&a, true).unwrap();
    rustix::io::ioctl_fionbio(&b, true).unwrap();
    (a, b)
}

fn kqueue_is_readable(inner: &Inner<()>, timeout_ms: i64) -> bool {
    let mut fds = [PollFd::new(&inner.selector, PollFlags::IN)];
    let timeout = Timespec {
        tv_sec: 0,
        tv_nsec: timeout_ms * 1_000_000,
    };
    poll(&mut fds, Some(&timeout)).unwrap() == 1
}

#[test]
fn finished_task_is_removed() {
    let inner = inner();
    let inner = inner.get();
    let (root, _) = parked_task(inner);
    let id = inner.spawn(Box::pin(async {}));
    assert_eq!(inner.run_ready_tasks(root), RootStatus::Running);
    assert!(inner.tasks.borrow().future(id).is_none());
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    let inner = inner();
    let inner = inner.get();
    let (root, _) = parked_task(inner);
    let polls = Rc::new(Cell::new(0));
    let counter = polls.clone();
    inner.spawn(Box::pin(poll_fn(move |cx| {
        counter.set(counter.get() + 1);
        if counter.get() < 3 {
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    })));
    inner.run_ready_tasks(root);
    assert_eq!(polls.get(), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    let owned = inner();
    let inner = owned.get();
    let (root, _) = parked_task(inner);
    let ptr = owned.as_ptr();
    let polls = Rc::new(Cell::new(0));
    let child = counting(&polls);
    inner.spawn(Box::pin(async move {
        // SAFETY: tasks are dropped before `Inner` is freed.
        unsafe { &*ptr }.spawn(child);
    }));
    inner.run_ready_tasks(root);
    assert_eq!(polls.get(), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    let inner = inner();
    let inner = inner.get();
    let (root, _) = parked_task(inner);
    let polls = Rc::new(Cell::new(0));
    let waiter = inner.spawn(counting(&polls));
    let task = inner.spawn(Box::pin(async {}));
    // SAFETY: `inner` outlives the waker.
    let waker = unsafe { new_waker(&inner.ready, waiter) };
    inner.tasks.borrow_mut().set_join_waiter(task, &waker);
    inner.run_ready_tasks(root);
    assert_eq!(polls.get(), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    let inner = inner();
    let inner = inner.get();
    let root = inner.spawn(Box::pin(async {}));
    let polls = Rc::new(Cell::new(0));
    inner.spawn(counting(&polls));
    assert_eq!(inner.run_ready_tasks(root), RootStatus::Finished);
    assert_eq!(polls.get(), 0);
}

struct SpawnOnDrop {
    inner: *const Inner<()>,
    spawned_dropped: Rc<Cell<bool>>,
}

impl Future for SpawnOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for SpawnOnDrop {
    fn drop(&mut self) {
        let flag = self.spawned_dropped.clone();
        let guard = DropGuard(flag);
        // SAFETY: tasks are dropped before `Inner` is freed.
        unsafe { &*self.inner }.spawn(Box::pin(async move {
            let _guard = guard;
        }));
    }
}

struct DropGuard(Rc<Cell<bool>>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn dropping_inner_drops_tasks_spawned_from_drop() {
    let owned = inner();
    let spawned_dropped = Rc::new(Cell::new(false));
    owned.get().spawn(Box::pin(SpawnOnDrop {
        inner: owned.as_ptr(),
        spawned_dropped: spawned_dropped.clone(),
    }));
    drop(owned);
    assert!(spawned_dropped.get());
}

#[test]
fn readable_event_wakes_the_reader_once() {
    let inner = inner();
    let inner = inner.get();
    let (a, b) = pair();
    let (task, waker) = parked_task(inner);
    let token = inner.register_io(a.as_fd());
    inner.set_io_waker(token, Direction::Read, &waker);
    inner.flush().unwrap();
    rustix::io::write(&b, b"x").unwrap();
    inner.process_selector_events().unwrap();
    assert_eq!(mem::take(&mut *inner.ready.borrow_mut()), vec![task]);

    rustix::io::write(&b, b"x").unwrap();
    inner.flush().unwrap();
    assert!(!kqueue_is_readable(inner, 0));
}

#[test]
fn removed_io_waker_arms_nothing() {
    let inner = inner();
    let inner = inner.get();
    let (a, b) = pair();
    let (_, waker) = parked_task(inner);
    let token = inner.register_io(a.as_fd());
    inner.set_io_waker(token, Direction::Read, &waker);
    inner.remove_io_waker(token, Direction::Read);
    rustix::io::write(&b, b"x").unwrap();
    inner.flush().unwrap();
    assert!(!kqueue_is_readable(inner, 0));
}

#[test]
fn deregistered_io_produces_no_events() {
    let inner = inner();
    let inner = inner.get();
    let (a, b) = pair();
    let (_, waker) = parked_task(inner);
    let token = inner.register_io(a.as_fd());
    inner.set_io_waker(token, Direction::Read, &waker);
    inner.flush().unwrap();
    inner.deregister_io(token).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!kqueue_is_readable(inner, 0));
}

#[test]
fn expired_timer_wakes_its_task() {
    let inner = inner();
    let inner = inner.get();
    let (task, waker) = parked_task(inner);
    let (_, later_waker) = parked_task(inner);
    let now = Instant::now();
    inner
        .timers
        .borrow_mut()
        .insert(now + Duration::from_millis(5), waker);
    inner
        .timers
        .borrow_mut()
        .insert(now + Duration::from_secs(60), later_waker);
    inner.flush().unwrap();
    assert!(kqueue_is_readable(inner, 500));
    inner.process_selector_events().unwrap();
    assert_eq!(*inner.ready.borrow(), vec![task]);
}
