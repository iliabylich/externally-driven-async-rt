use crate::{EventLoop, testing::FdTestingExt};

pub(crate) trait EventLoopTestingExt<E> {
    fn run_until_events<const N: usize>(&mut self) -> [E; N];
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> EventLoopTestingExt<E>
    for EventLoop<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn run_until_events<const N: usize>(&mut self) -> [E; N] {
        core::array::from_fn(|_| {
            loop {
                if let Some(event) = self.events().next() {
                    return event;
                }
                assert!(
                    self.wait_readable_blocking(2000),
                    "the loop never became ready"
                );
                assert_eq!(self.drain_events_without_blocking(), Ok(()));
            }
        })
    }
}
