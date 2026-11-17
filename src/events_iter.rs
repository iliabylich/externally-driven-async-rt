use crate::event_queue::EventQueue;

/// An iterator that pops events pushed by [`Context::dispatch`](crate::Context::dispatch).
///
/// Returned by [`EventLoop::events`](crate::EventLoop::events).
#[must_use]
pub struct EventsIter<'a, E, const EVENTS: usize> {
    queue: &'a EventQueue<E, EVENTS>,
}

impl<'a, E, const EVENTS: usize> EventsIter<'a, E, EVENTS> {
    pub(crate) const fn new(queue: &'a EventQueue<E, EVENTS>) -> Self {
        Self { queue }
    }
}

impl<E, const EVENTS: usize> Iterator for EventsIter<'_, E, EVENTS> {
    type Item = E;

    fn next(&mut self) -> Option<E> {
        self.queue.pop()
    }
}

impl<E, const EVENTS: usize> core::fmt::Debug for EventsIter<'_, E, EVENTS> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("EventsIter")
    }
}
