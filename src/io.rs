use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use rustix::fd::BorrowedFd;

use crate::selectors::Direction;
use crate::storage::IoIndex;
use crate::storage::Storage;

// Runs a non-blocking syscall until it doesn't return `EAGAIN`, waiting for `$direction`
// readiness of `$fd` in between. Must be used inside an `async` block.
macro_rules! io_loop {
    ($ctx:expr, $fd:expr, $direction:expr, $syscall:expr) => {{
        let mut registration = $crate::io::IoRegistration::new($ctx.storage(), $fd, $direction);
        loop {
            match $syscall {
                Err(rustix::io::Errno::INTR) => {}
                Err(rustix::io::Errno::AGAIN) => registration.wait().await,
                result => break result,
            }
        }
    }};
}

pub(crate) use io_loop;

pub(crate) struct IoRegistration<'a> {
    storage: &'a Storage,
    index: IoIndex,
}

impl<'a> IoRegistration<'a> {
    pub(crate) fn new(storage: &'a Storage, fd: BorrowedFd<'a>, direction: Direction) -> Self {
        Self {
            storage,
            index: storage.register_io(fd, direction),
        }
    }

    pub(crate) fn wait(&mut self) -> Wait<'_, 'a> {
        Wait {
            registration: self,
            waiting: false,
        }
    }
}

impl Drop for IoRegistration<'_> {
    fn drop(&mut self) {
        // On failure the slot is released anyway, so a filter left armed can only produce one
        // ignored event (its index no longer matches) until the fd is closed.
        let _ = self.storage.deregister_io(self.index);
    }
}

pub(crate) struct Wait<'r, 'a> {
    registration: &'r mut IoRegistration<'a>,
    waiting: bool,
}

impl Future for Wait<'_, '_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let IoRegistration { storage, index } = *this.registration;
        if this.waiting && !storage.has_io_waker(index) {
            this.waiting = false;
            return Poll::Ready(());
        }
        storage.set_io_waker(index, cx.waker());
        this.waiting = true;
        Poll::Pending
    }
}

impl Drop for Wait<'_, '_> {
    fn drop(&mut self) {
        if self.waiting {
            let IoRegistration { storage, index } = *self.registration;
            storage.remove_io_waker(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::{pending, poll_fn};
    use core::time::Duration;

    use rustix::fd::{AsFd, OwnedFd};
    use rustix::io::Errno;

    use super::*;
    use crate::EventLoop;
    use crate::test_utils::{
        First, Log, TEST_MAX_IO, TEST_MAX_TASKS, TEST_MAX_TIMERS, TestContext, Winner, is_readable,
        pair, run_until_root_finishes,
    };

    async fn read_some(ctx: &TestContext, fd: &OwnedFd) -> rustix::io::Result<usize> {
        let mut buf = [0; 64];
        let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), Direction::Read);
        loop {
            match rustix::io::read(fd, &mut buf) {
                Err(Errno::AGAIN) => registration.wait().await,
                result => return result,
            }
        }
    }

    async fn write_some(ctx: &TestContext, fd: &OwnedFd) -> rustix::io::Result<usize> {
        let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), Direction::Write);
        loop {
            match rustix::io::write(fd, b"x") {
                Err(Errno::AGAIN) => registration.wait().await,
                result => return result,
            }
        }
    }

    fn fill_send_buffer(fd: &OwnedFd) {
        while rustix::io::write(fd, &[0; 4096]).is_ok() {}
    }

    fn drain_receive_buffer(fd: &OwnedFd) {
        let mut buf = [0; 4096];
        while rustix::io::read(fd, &mut buf).is_ok() {}
    }

    fn registrations(event_loop: &EventLoop<Log>) -> usize {
        event_loop.storage().io_count()
    }

    #[test]
    fn read_waits_until_the_peer_writes() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let n = read_some(ctx, &a).await.unwrap();
                ctx.dispatch(n as u32);
                pending::<()>().await;
            },
        )
        .unwrap();
        assert_eq!(registrations(&event_loop), 1);
        assert!(!is_readable(&event_loop, 0));

        rustix::io::write(&b, b"abc").unwrap();
        assert!(is_readable(&event_loop, 1000));
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().0, vec![3]);
        assert_eq!(registrations(&event_loop), 0);
    }

    #[test]
    fn write_waits_on_a_full_buffer() {
        let (a, b) = pair();
        fill_send_buffer(&a);
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let n = write_some(ctx, &a).await.unwrap();
                ctx.dispatch(n as u32);
            },
        )
        .unwrap();
        assert!(event_loop.state_mut().0.is_empty());
        assert!(!is_readable(&event_loop, 0));

        drain_receive_buffer(&b);
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().0, vec![1]);
    }

    #[test]
    fn ready_fd_arms_nothing() {
        let (a, b) = pair();
        rustix::io::write(&b, b"abc").unwrap();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let n = read_some(ctx, &a).await.unwrap();
                ctx.dispatch(n as u32);
                pending::<()>().await;
            },
        )
        .unwrap();
        assert_eq!(event_loop.state_mut().0, vec![3]);
        assert_eq!(registrations(&event_loop), 0);
        rustix::io::write(&b, b"abc").unwrap();
        assert!(!is_readable(&event_loop, 0));
    }

    #[test]
    fn dropping_a_pending_read_deregisters() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let read = Box::pin(read_some(ctx, &a));
                let winner = First(read, ctx.sleep(Duration::from_millis(10))).await;
                assert_eq!(winner, Winner::B);
                ctx.dispatch(1);
                pending::<()>().await;
            },
        )
        .unwrap();
        assert_eq!(registrations(&event_loop), 1);
        while event_loop.state_mut().0.is_empty() {
            assert!(is_readable(&event_loop, 2000));
            assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        }
        assert_eq!(registrations(&event_loop), 0);
        rustix::io::write(&b, b"abc").unwrap();
        assert!(!is_readable(&event_loop, 0));
    }

    #[test]
    fn read_survives_a_spurious_wake() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let read = Box::pin(read_some(ctx, &a));
                let mut woken = false;
                let wake_once = poll_fn(move |cx| {
                    if !woken {
                        woken = true;
                        cx.waker().wake_by_ref();
                    }
                    Poll::<()>::Pending
                });
                let winner = First(read, wake_once).await;
                assert_eq!(winner, Winner::A);
                ctx.dispatch(1);
                pending::<()>().await;
            },
        )
        .unwrap();
        assert!(event_loop.state_mut().0.is_empty());
        assert_eq!(registrations(&event_loop), 1);

        rustix::io::write(&b, b"abc").unwrap();
        assert!(is_readable(&event_loop, 1000));
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().0, vec![1]);
    }

    #[test]
    fn closed_and_reused_fd_number_does_not_affect_the_new_socket() {
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async |ctx| {
                let (a, _b) = pair();
                let read = Box::pin(read_some(ctx, &a));
                let winner = First(read, ctx.sleep(Duration::from_millis(5))).await;
                assert_eq!(winner, Winner::B);
                drop(a);

                let (c, d) = pair();
                ctx.spawn(async move |ctx| {
                    ctx.sleep(Duration::from_millis(5)).await;
                    rustix::io::write(&d, b"abc").unwrap();
                });
                let n = read_some(ctx, &c).await.unwrap();
                ctx.dispatch(n as u32);
            },
        )
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().0, vec![3]);
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent IO operations")]
    fn exceeding_max_io_count_panics() {
        let (a, _b) = pair();
        let (c, _d) = pair();
        let _ = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            1,
            async move |ctx| {
                First(Box::pin(read_some(ctx, &a)), Box::pin(read_some(ctx, &c))).await;
            },
        );
    }

    #[test]
    fn sequential_io_reuses_the_slot() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            1,
            async move |ctx| {
                for _ in 0..3 {
                    let len = read_some(ctx, &a).await.unwrap();
                    ctx.dispatch(len as u32);
                }
                pending::<()>().await;
            },
        )
        .unwrap();
        for i in 1..=3 {
            rustix::io::write(&b, &vec![0; i]).unwrap();
            while event_loop.state_mut().0.len() < i {
                assert!(is_readable(&event_loop, 2000));
                assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
            }
        }
        assert_eq!(event_loop.state_mut().0, vec![1, 2, 3]);
        assert_eq!(event_loop.storage().io_count(), 0);
    }
}
