use super::*;
use core::cell::Cell;
use core::future::{Future, pending};
use core::pin::Pin;
use core::task::Poll;
use std::rc::Rc;

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fd::OwnedFd;
use rustix::net::{AddressFamily, SocketFlags, SocketType, socketpair};

use crate::inner::Direction;
use crate::selectors::Token;

#[derive(Default)]
struct Log(Vec<u32>);

impl State for Log {
    type Event = u32;

    fn apply(&mut self, event: u32) {
        self.0.push(event);
    }
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

fn is_readable(fd: impl AsFd, timeout_ms: i64) -> bool {
    let mut fds = [PollFd::new(&fd, PollFlags::IN)];
    let timeout = Timespec {
        tv_sec: 0,
        tv_nsec: timeout_ms * 1_000_000,
    };
    poll(&mut fds, Some(&timeout)).unwrap() == 1
}

// Stand-in for real IO futures (step 11): resolves once after `fd` becomes readable.
struct Readable<'a> {
    ctx: &'a Context<u32>,
    fd: BorrowedFd<'a>,
    token: Option<Token>,
}

fn readable<'a>(ctx: &'a Context<u32>, fd: &'a OwnedFd) -> Readable<'a> {
    Readable {
        ctx,
        fd: fd.as_fd(),
        token: None,
    }
}

impl Future for Readable<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        if self.token.is_some() {
            return Poll::Ready(());
        }
        let inner = self.ctx.inner();
        let token = inner.register_io(self.fd);
        inner.set_io_waker(token, Direction::Read, cx.waker());
        self.token = Some(token);
        Poll::Pending
    }
}

impl Drop for Readable<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token {
            self.ctx.inner().deregister_io(token).unwrap();
        }
    }
}

fn wake_by_writing(event_loop: &impl AsFd, peer: &OwnedFd) {
    rustix::io::write(peer, b"x").unwrap();
    assert!(is_readable(event_loop, 1000));
}

#[test]
fn root_finishing_during_new_is_an_error() {
    let result = EventLoop::new(Log::default(), async |ctx| ctx.dispatch(1));
    assert_eq!(result.err(), Some(Error::RootFinished));
}

#[test]
fn events_dispatched_during_new_are_applied() {
    let event_loop = EventLoop::new(Log::default(), async |ctx| {
        ctx.dispatch(1);
        ctx.dispatch(2);
        pending::<()>().await;
    });
    assert_eq!(event_loop.unwrap().state_mut().0, vec![1, 2]);
}

#[test]
fn idle_loop_is_not_readable_and_drains_to_nothing() {
    let mut event_loop = EventLoop::new(Log::default(), async |_| pending().await).unwrap();
    assert!(!is_readable(&event_loop, 0));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert!(event_loop.state_mut().0.is_empty());
}

#[test]
fn root_finishing_inside_a_drain() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        ctx.dispatch(1);
        readable(ctx, &a).await;
        ctx.dispatch(2);
        ctx.dispatch(3);
    })
    .unwrap();
    assert_eq!(event_loop.state_mut().0, vec![1]);

    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().0, vec![1, 2, 3]);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert!(!is_readable(&event_loop, 0));
}

#[test]
fn root_progresses_across_drains() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        let mut i = 0;
        loop {
            readable(ctx, &a).await;
            let mut buf = [0; 16];
            rustix::io::read(&a, &mut buf).unwrap();
            ctx.dispatch(i);
            i += 1;
        }
    })
    .unwrap();
    for i in 0..3 {
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().0.pop(), Some(i));
        assert!(!is_readable(&event_loop, 0));
    }
}

#[test]
fn moving_the_loop_after_new_keeps_working() {
    let (a, b) = pair();
    let event_loop = EventLoop::new(Log::default(), async move |ctx| {
        readable(ctx, &a).await;
        ctx.dispatch(1);
    })
    .unwrap();
    let mut boxed = Box::new(event_loop);
    wake_by_writing(&*boxed, &b);
    assert_eq!(
        boxed.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(boxed.state_mut().0, vec![1]);
}

#[test]
fn two_loops_on_one_thread_are_independent() {
    let (a1, b1) = pair();
    let (a2, _b2) = pair();
    let mut first = EventLoop::new(Log::default(), async move |ctx| {
        readable(ctx, &a1).await;
        ctx.dispatch(1);
    })
    .unwrap();
    let mut second = EventLoop::new(Log::default(), async move |ctx| {
        readable(ctx, &a2).await;
        ctx.dispatch(2);
    })
    .unwrap();

    wake_by_writing(&first, &b1);
    assert!(!is_readable(&second, 0));
    assert_eq!(second.drain_events_without_blocking(), Ok(()));
    assert_eq!(
        first.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(first.state_mut().0, vec![1]);
    assert!(second.state_mut().0.is_empty());
}

#[test]
#[should_panic(expected = "boom in new")]
fn panic_propagates_out_of_new() {
    let _ = EventLoop::new(Log::default(), async |_| panic!("boom in new"));
}

#[test]
#[should_panic(expected = "boom in drain")]
fn panic_propagates_out_of_drain() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        readable(ctx, &a).await;
        panic!("boom in drain");
    })
    .unwrap();
    wake_by_writing(&event_loop, &b);
    let _ = event_loop.drain_events_without_blocking();
}

struct PanickingState;

impl State for PanickingState {
    type Event = ();

    fn apply(&mut self, (): ()) {
        panic!("boom in apply");
    }
}

#[test]
#[should_panic(expected = "boom in apply")]
fn panic_in_apply_propagates() {
    let _ = EventLoop::new(PanickingState, async |ctx| {
        ctx.dispatch(());
        pending::<()>().await;
    });
}

struct DispatchOnDrop<'a>(&'a Context<u32>, u32);

impl Drop for DispatchOnDrop<'_> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}

#[test]
fn spawned_task_runs_in_the_same_drain() {
    let event_loop = EventLoop::new(Log::default(), async |ctx| {
        ctx.spawn(async |ctx| ctx.dispatch(2));
        ctx.dispatch(1);
        pending::<()>().await;
    });
    assert_eq!(event_loop.unwrap().state_mut().0, vec![1, 2]);
}

#[test]
fn joining_a_running_task_waits_for_it() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        ctx.spawn(async move |ctx| {
            readable(ctx, &a).await;
            ctx.dispatch(1);
        })
        .await;
        ctx.dispatch(2);
    })
    .unwrap();
    assert!(event_loop.state_mut().0.is_empty());
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().0, vec![1, 2]);
}

#[test]
fn joining_a_finished_task_resolves_immediately() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        let handle = ctx.spawn(async |ctx| ctx.dispatch(1));
        readable(ctx, &a).await;
        handle.await;
        ctx.dispatch(2);
    })
    .unwrap();
    assert_eq!(event_loop.state_mut().0, vec![1]);
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().0, vec![1, 2]);
}

#[test]
fn dropped_handle_detaches_the_task() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        ctx.spawn(async move |ctx| {
            readable(ctx, &a).await;
            ctx.dispatch(1);
        });
        pending::<()>().await;
    })
    .unwrap();
    wake_by_writing(&event_loop, &b);
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_eq!(event_loop.state_mut().0, vec![1]);
}

#[test]
fn spawned_task_can_spawn_and_join() {
    let event_loop = EventLoop::new(Log::default(), async |ctx| {
        ctx.spawn(async |ctx| {
            ctx.spawn(async |ctx| ctx.dispatch(1)).await;
            ctx.dispatch(2);
        })
        .await;
        ctx.dispatch(3);
        pending::<()>().await;
    });
    assert_eq!(event_loop.unwrap().state_mut().0, vec![1, 2, 3]);
}

#[test]
fn tasks_are_dropped_when_the_root_finishes() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        ctx.spawn(async |ctx| {
            let _guard = DispatchOnDrop(ctx, 10);
            pending::<()>().await;
        });
        ctx.spawn(async |ctx| {
            let _guard = DispatchOnDrop(ctx, 20);
            pending::<()>().await;
        });
        readable(ctx, &a).await;
        ctx.dispatch(1);
    })
    .unwrap();
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    let log = &mut event_loop.state_mut().0;
    assert_eq!(log[0], 1);
    log[1..].sort();
    assert_eq!(log[1..], [10, 20]);
}

struct SpawnOnDrop<'a>(&'a Context<u32>);

impl Drop for SpawnOnDrop<'_> {
    fn drop(&mut self) {
        self.0.spawn(async |ctx| ctx.dispatch(100));
        self.0.dispatch(3);
    }
}

#[test]
fn spawning_and_dispatching_from_drop_during_teardown() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
        ctx.spawn(async |ctx| {
            let _guard = SpawnOnDrop(ctx);
            pending::<()>().await;
        });
        readable(ctx, &a).await;
        ctx.dispatch(1);
    })
    .unwrap();
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().0, vec![1, 3]);
}

struct Flag(Rc<Cell<bool>>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn dropping_the_loop_drops_its_tasks() {
    let dropped = Rc::new(Cell::new(false));
    let flag = Flag(dropped.clone());
    let event_loop = EventLoop::new(Log::default(), async move |ctx| {
        ctx.spawn(async move |_| {
            let _flag = flag;
            pending::<()>().await;
        });
        pending::<()>().await;
    })
    .unwrap();
    assert!(!dropped.get());
    drop(event_loop);
    assert!(dropped.get());
}
