use core::task::Waker;

use crate::{storage::TaskId, wake_queue::WakeQueue};

#[derive(Clone, Copy)]
pub(crate) struct ReadyQueue<const N: usize>(&'static WakeQueue<N>);

impl<const N: usize> ReadyQueue<N> {
    pub(crate) fn new(queue: &'static WakeQueue<N>) -> Self {
        queue.claim();
        Self(queue)
    }

    pub(crate) fn waker(self, slot: u32) -> Waker {
        self.0.waker(slot)
    }

    pub(crate) fn track(&self, id: TaskId) {
        self.0.set_generation(id.slot(), Some(id.generation()));
    }

    pub(crate) fn untrack(&self, id: TaskId) {
        if self.0.generation(id.slot()) == Some(id.generation()) {
            self.0.set_generation(id.slot(), None);
        }
    }

    pub(crate) fn push(&self, slot: u32) {
        if self.0.generation(slot).is_some() {
            self.0.push(slot);
        }
    }

    pub(crate) fn pop(&self) -> Option<TaskId> {
        loop {
            let slot = self.0.pop()?;
            if let Some(generation) = self.0.generation(slot) {
                return Some(TaskId::from_parts(slot, generation));
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::{WakeQueue, ready::ReadyQueue, storage::TaskId, test_utils::assert_yields};

    fn tracked<const N: usize>(queue: ReadyQueue<N>, slot: u32, generation: u32) -> TaskId {
        let id = TaskId::from_parts(slot, generation);
        queue.track(id);
        id
    }

    fn drain<const N: usize>(queue: ReadyQueue<N>) -> impl Iterator<Item = TaskId> {
        core::iter::from_fn(move || queue.pop())
    }

    #[test]
    fn pushing_an_empty_slot_is_ignored() {
        let queue = ReadyQueue::new({
            static QUEUE: WakeQueue<1> = WakeQueue::new();
            &QUEUE
        });
        queue.push(0);
        assert!(queue.is_empty());
        let old = tracked(queue, 0, 0);
        queue.untrack(old);
        queue.push(0);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_reused_slot_pops_its_new_task() {
        let queue = ReadyQueue::new({
            static QUEUE: WakeQueue<1> = WakeQueue::new();
            &QUEUE
        });
        let old = tracked(queue, 0, 0);
        queue.untrack(old);
        let new = tracked(queue, 0, 1);
        queue.push(old.slot());
        assert_yields(drain(queue), [new]);
    }

    #[test]
    fn a_task_finished_while_queued_is_skipped() {
        let queue = ReadyQueue::new({
            static QUEUE: WakeQueue<2> = WakeQueue::new();
            &QUEUE
        });
        let a = tracked(queue, 0, 0);
        let b = tracked(queue, 1, 0);
        queue.push(a.slot());
        queue.push(b.slot());
        queue.untrack(a);
        assert_yields(drain(queue), [b]);
        assert!(queue.is_empty());
    }

    #[test]
    fn untracking_a_newer_task_is_ignored() {
        let queue = ReadyQueue::new({
            static QUEUE: WakeQueue<1> = WakeQueue::new();
            &QUEUE
        });
        let old = TaskId::from_parts(0, 0);
        let new = tracked(queue, 0, 1);
        queue.untrack(old);
        queue.push(new.slot());
        assert_yields(drain(queue), [new]);
    }
}
