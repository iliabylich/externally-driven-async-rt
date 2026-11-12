use core::future::Future;

use crate::{Context, State};

/// A stream that can be written to asynchronously.
pub trait AsyncWrite {
    /// The error returned by [`write`](Self::write) and [`flush`](Self::flush).
    type Error;

    /// Writes from a given buffer, returns the number of written bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    ///
    /// # Cancel safety
    ///
    /// Implementations in this crate are cancel safe.
    fn write<
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
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>>;

    /// Makes sure that everything accepted by [`write`](Self::write) is sent.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    ///
    /// # Cancel safety
    ///
    /// Implementations in this crate are cancel safe.
    fn flush<
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
    ) -> impl Future<Output = Result<(), Self::Error>>;
}
