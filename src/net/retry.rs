use core::{
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use rustix::{
    fd::{AsFd, OwnedFd},
    io::Errno,
};

use crate::{
    context::Context,
    io::{IoIndex, IoRegistration},
    selector::Direction,
    state::State,
};

#[must_use]
pub(crate) struct Retry<
    'a,
    'ctx,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    F,
> {
    ctx: &'a Context<'ctx, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    fd: &'a OwnedFd,
    cached: &'a Cell<Option<IoIndex>>,
    direction: Direction,
    op: F,
    registration: Option<IoRegistration<'a, FDS>>,
    waiting: bool,
}

impl<
    'a,
    'ctx,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    F,
> Retry<'a, 'ctx, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, F>
{
    pub(crate) const fn new(
        ctx: &'a Context<'ctx, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        fd: &'a OwnedFd,
        cached: &'a Cell<Option<IoIndex>>,
        direction: Direction,
        op: F,
    ) -> Self {
        Self {
            ctx,
            fd,
            cached,
            direction,
            op,
            registration: None,
            waiting: false,
        }
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    T,
    F: FnMut(&OwnedFd) -> rustix::io::Result<T> + Unpin,
> Future for Retry<'_, '_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, F>
{
    type Output = rustix::io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let registration = this.registration.get_or_insert_with(|| {
            IoRegistration::new(this.ctx, this.fd.as_fd(), this.cached, this.direction)
        });
        loop {
            if this.waiting && registration.poll_wait(&mut this.waiting, cx).is_pending() {
                return Poll::Pending;
            }
            match (this.op)(this.fd) {
                Err(Errno::INTR) => {}
                Err(Errno::AGAIN) => {
                    if registration.poll_wait(&mut this.waiting, cx).is_pending() {
                        return Poll::Pending;
                    }
                }
                result => return Poll::Ready(result),
            }
        }
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    F,
> Drop for Retry<'_, '_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, F>
{
    fn drop(&mut self) {
        if let Some(registration) = &self.registration {
            registration.cancel_wait(self.waiting);
        }
    }
}
