use core::future::Future;

use crate::{Context, State};

/// A stream that can be read from asynchronously.
pub trait AsyncRead {
    /// Reads into a given buffer, returns the number of read bytes.
    ///
    /// Returns `0` if the stream is closed (or if the buffer is empty).
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    fn read<
        'a,
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        ctx: &'a Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        buf: &'a mut [u8],
    ) -> impl Future<Output = rustix::io::Result<usize>>;
}
