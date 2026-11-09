use core::{
    future::Future,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use rustix::{
    fd::{AsFd, OwnedFd},
    io::Errno,
};

use crate::{context::Context, io::IoRegistration, selector::Direction, state::State};

pub(super) struct Retry<
    'a,
    'ctx,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    F,
> {
    ctx: &'a Context<
        'ctx,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    fd: &'a OwnedFd,
    direction: Direction,
    op: F,
    registration: Option<IoRegistration<'a, MAX_IO_COUNT>>,
    waiting: bool,
}

impl<
    'a,
    'ctx,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    F,
>
    Retry<
        'a,
        'ctx,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        F,
    >
{
    pub(super) fn new(
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        fd: &'a OwnedFd,
        direction: Direction,
        op: F,
    ) -> Self {
        Self {
            ctx,
            fd,
            direction,
            op,
            registration: None,
            waiting: false,
        }
    }
}

impl<
    'a,
    'ctx,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    T,
    F: FnMut(&OwnedFd) -> rustix::io::Result<T> + Unpin,
> Future
    for Retry<
        'a,
        'ctx,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        F,
    >
{
    type Output = rustix::io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let registration = this.registration.get_or_insert_with(|| {
            IoRegistration::new(this.ctx.storage(), this.fd.as_fd(), this.direction)
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
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    F,
> Drop
    for Retry<
        '_,
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        F,
    >
{
    fn drop(&mut self) {
        if let Some(registration) = &self.registration {
            registration.cancel_wait(self.waiting);
        }
    }
}
