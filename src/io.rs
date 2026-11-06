use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use rustix::fd::BorrowedFd;

use crate::inner::{Direction, Inner};
use crate::selectors::Token;

// Runs a non-blocking syscall until it doesn't return `EAGAIN`, waiting for `$direction`
// readiness of `$fd` in between. Must be used inside an `async` block.
macro_rules! io_loop {
    ($ctx:expr, $fd:expr, $direction:expr, $syscall:expr) => {{
        let mut registration = $crate::io::IoRegistration::new($ctx.inner(), $fd);
        loop {
            match $syscall {
                Err(rustix::io::Errno::INTR) => {}
                Err(rustix::io::Errno::AGAIN) => registration.wait($direction).await,
                result => break result,
            }
        }
    }};
}

pub(crate) use io_loop;

pub(crate) struct IoRegistration<'a, E> {
    inner: &'a Inner<E>,
    token: Token,
}

impl<'a, E> IoRegistration<'a, E> {
    pub(crate) fn new(inner: &'a Inner<E>, fd: BorrowedFd<'a>) -> Self {
        Self {
            inner,
            token: inner.register_io(fd),
        }
    }

    pub(crate) fn wait(&mut self, direction: Direction) -> Wait<'_, 'a, E> {
        Wait {
            registration: self,
            direction,
            waiting: false,
        }
    }
}

impl<E> Drop for IoRegistration<'_, E> {
    fn drop(&mut self) {
        // On failure the token is forgotten anyway, so a filter left armed can only produce
        // one ignored event per direction until the fd is closed.
        let _ = self.inner.deregister_io(self.token);
    }
}

pub(crate) struct Wait<'r, 'a, E> {
    registration: &'r mut IoRegistration<'a, E>,
    direction: Direction,
    waiting: bool,
}

impl<E> Future for Wait<'_, '_, E> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let IoRegistration { inner, token } = *this.registration;
        if this.waiting && !inner.has_io_waker(token, this.direction) {
            this.waiting = false;
            return Poll::Ready(());
        }
        inner.set_io_waker(token, this.direction, cx.waker());
        this.waiting = true;
        Poll::Pending
    }
}

impl<E> Drop for Wait<'_, '_, E> {
    fn drop(&mut self) {
        if self.waiting {
            let IoRegistration { inner, token } = *self.registration;
            inner.remove_io_waker(token, self.direction);
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
    use crate::test_utils::{First, Log, Winner, is_readable, pair, run_until_root_finishes};
    use crate::{Context, EventLoop};

    async fn read_some(ctx: &Context<u32>, fd: &OwnedFd) -> rustix::io::Result<usize> {
        let mut buf = [0; 64];
        let mut registration = IoRegistration::new(ctx.inner(), fd.as_fd());
        loop {
            match rustix::io::read(fd, &mut buf) {
                Err(Errno::AGAIN) => registration.wait(Direction::Read).await,
                result => return result,
            }
        }
    }

    async fn write_some(ctx: &Context<u32>, fd: &OwnedFd) -> rustix::io::Result<usize> {
        let mut registration = IoRegistration::new(ctx.inner(), fd.as_fd());
        loop {
            match rustix::io::write(fd, b"x") {
                Err(Errno::AGAIN) => registration.wait(Direction::Write).await,
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
        event_loop.inner().io_registration_count()
    }

    #[test]
    fn read_waits_until_the_peer_writes() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let n = read_some(ctx, &a).await.unwrap();
            ctx.dispatch(n as u32);
            pending::<()>().await;
        })
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
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let n = write_some(ctx, &a).await.unwrap();
            ctx.dispatch(n as u32);
        })
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
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let n = read_some(ctx, &a).await.unwrap();
            ctx.dispatch(n as u32);
            pending::<()>().await;
        })
        .unwrap();
        assert_eq!(event_loop.state_mut().0, vec![3]);
        assert_eq!(registrations(&event_loop), 0);
        rustix::io::write(&b, b"abc").unwrap();
        assert!(!is_readable(&event_loop, 0));
    }

    #[test]
    fn dropping_a_pending_read_deregisters() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let read = Box::pin(read_some(ctx, &a));
            let winner = First(read, ctx.sleep(Duration::from_millis(10))).await;
            assert_eq!(winner, Winner::B);
            ctx.dispatch(1);
            pending::<()>().await;
        })
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
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
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
        })
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
        let mut event_loop = EventLoop::new(Log::default(), async |ctx| {
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
        })
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().0, vec![3]);
    }
}
