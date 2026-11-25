use core::{cell::Cell, mem::DropGuard, task::Poll};

use rustix::io::Errno;

use crate::{
    selector::{Direction, Directions},
    waker_slot::WakerSlot,
};

#[must_use]
pub(crate) struct IoWaiter {
    ready: Cell<Directions>,
    reader: WakerSlot,
    writer: WakerSlot,
}

impl IoWaiter {
    pub(super) const fn new() -> Self {
        Self {
            ready: Cell::new(Directions::Empty),
            reader: WakerSlot::new(),
            writer: WakerSlot::new(),
        }
    }

    pub(super) fn reset(&self) {
        self.ready.set(Directions::Both);
    }

    pub(super) fn wake(&self, ready: Directions) {
        for direction in [Direction::Read, Direction::Write] {
            if !ready.contains(direction) {
                continue;
            }
            self.ready.set(self.ready.get().with(direction));
            if let Some(waker) = self.waker(direction).take() {
                waker.wake();
            }
        }
    }

    pub(crate) fn clear(&self, direction: Direction) {
        self.ready.set(self.ready.get().without(direction));
    }

    pub(crate) async fn ready(&self, direction: Direction) {
        let waker = self.waker(direction);
        let _guard = DropGuard::new(waker, |waker: &WakerSlot| drop(waker.take()));
        core::future::poll_fn(|cx| {
            if self.ready.get().contains(direction) {
                return Poll::Ready(());
            }
            waker.store(cx.waker());
            Poll::Pending
        })
        .await;
    }

    pub(crate) async fn retry<T>(
        &self,
        direction: Direction,
        mut op: impl FnMut() -> rustix::io::Result<T>,
    ) -> rustix::io::Result<T> {
        loop {
            self.ready(direction).await;
            match op() {
                Err(Errno::INTR) => {}
                Err(Errno::AGAIN) => self.clear(direction),
                result => return result,
            }
        }
    }

    pub(super) fn drop_wakers(&self) {
        self.reader.take();
        self.writer.take();
    }

    const fn waker(&self, direction: Direction) -> &WakerSlot {
        match direction {
            Direction::Read => &self.reader,
            Direction::Write => &self.writer,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{
        cell::Cell,
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::io::Errno;

    use super::IoWaiter;
    use crate::{
        selector::{Direction, Directions},
        testing::flag_waker,
    };

    fn registered() -> IoWaiter {
        let io = IoWaiter::new();
        io.reset();
        io
    }

    #[test]
    fn a_registration_starts_ready_in_both_directions() {
        let io = registered();
        let mut cx = Context::from_waker(Waker::noop());
        for direction in [Direction::Read, Direction::Write] {
            assert_eq!(pin!(io.ready(direction)).poll(&mut cx), Poll::Ready(()));
        }
    }

    #[test]
    fn a_cleared_direction_waits_for_its_own_edge() {
        let io = registered();
        let woken = Cell::new(false);
        let waker = flag_waker(&woken);
        let mut cx = Context::from_waker(&waker);
        io.clear(Direction::Read);
        let mut ready = pin!(io.ready(Direction::Read));
        assert_eq!(ready.as_mut().poll(&mut cx), Poll::Pending);

        io.wake(Directions::Write);
        assert!(!woken.get());
        assert_eq!(ready.as_mut().poll(&mut cx), Poll::Pending);

        io.wake(Directions::Read);
        assert!(woken.get());
        assert_eq!(ready.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn retry_clears_on_again_and_runs_again_after_the_edge() {
        let io = registered();
        let attempts = Cell::new(0);
        let mut cx = Context::from_waker(Waker::noop());
        let mut retry = pin!(io.retry(Direction::Read, || {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                return Err(Errno::AGAIN);
            }
            Ok(5)
        }));
        assert_eq!(retry.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(retry.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(attempts.get(), 1);

        io.wake(Directions::Read);
        assert_eq!(retry.as_mut().poll(&mut cx), Poll::Ready(Ok(5)));
        assert_eq!(attempts.get(), 2);
    }

    #[test]
    fn retry_repeats_an_interrupted_call_right_away() {
        let io = registered();
        let attempts = Cell::new(0);
        let mut cx = Context::from_waker(Waker::noop());
        let mut retry = pin!(io.retry(Direction::Write, || {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                return Err(Errno::INTR);
            }
            Ok(1)
        }));
        assert_eq!(retry.as_mut().poll(&mut cx), Poll::Ready(Ok(1)));
        assert_eq!(attempts.get(), 2);
    }

    #[test]
    fn dropping_a_pending_wait_forgets_its_waker() {
        let io = registered();
        let woken = Cell::new(false);
        let waker = flag_waker(&woken);
        io.clear(Direction::Read);
        {
            let mut ready = pin!(io.ready(Direction::Read));
            assert_eq!(
                ready.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Pending
            );
        }
        io.wake(Directions::Read);
        assert!(!woken.get());
    }
}
