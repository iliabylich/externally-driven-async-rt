use core::fmt::Debug;

use crate::Context;

pub(crate) struct DispatchOnDrop<
    'c,
    'a,
    E: Copy + Debug + 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
>(
    pub(crate) &'c Context<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    pub(crate) E,
);

impl<
    E: Copy + Debug + 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Drop for DispatchOnDrop<'_, '_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn drop(&mut self) {
        self.0.dispatch(self.1).unwrap();
    }
}
