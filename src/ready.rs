use alloc::alloc::Layout;
use core::cell::Cell;
use core::ptr::{self, NonNull};

use crate::storage::TaskId;

struct Link {
    // The generation of the task currently in this slot, so a wake of a finished task (whose
    // slot may already hold a new one) is ignored.
    generation: Cell<u32>,
    queued: Cell<bool>,
    next: Cell<Option<u32>>,
}

// FIFO of ready tasks threaded through one link per task slot: bounded by the task capacity,
// a task is queued at most once, and push/pop are O(1). Only `Cell`s, so wakers can use it
// through a shared reference at any time, independently of the `RefCell` around the storage.
// `repr(C)` so `layout` can compute the size and `init` can place it in raw memory.
#[repr(C)]
pub(crate) struct ReadyQueue {
    head: Cell<Option<u32>>,
    tail: Cell<Option<u32>>,
    links: [Link],
}

impl ReadyQueue {
    pub(crate) fn layout(len: usize) -> Layout {
        let end = Layout::new::<Cell<Option<u32>>>();
        let links = Layout::array::<Link>(len).expect("ready queue is too large");
        let (layout, _) = end.extend(end).expect("ready queue is too large");
        let (layout, _) = layout.extend(links).expect("ready queue is too large");
        layout.pad_to_align()
    }

    /// Writes an empty queue with `len` links at `ptr`.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for writes of `Self::layout(len)` bytes, aligned to it, and stay valid
    /// (and otherwise unused) for as long as the returned queue is used.
    pub(crate) unsafe fn init(ptr: NonNull<u8>, len: usize) -> NonNull<Self> {
        let queue = ptr::slice_from_raw_parts_mut(ptr.as_ptr().cast::<Link>(), len) as *mut Self;
        // SAFETY: the caller guarantees `ptr` is valid for `Self::layout(len)`, which (`repr(C)`)
        // covers every field; every link is written without reading the uninitialized memory.
        unsafe {
            (&raw mut (*queue).head).write(Cell::new(None));
            (&raw mut (*queue).tail).write(Cell::new(None));
            let links = (&raw mut (*queue).links).cast::<Link>();
            for slot in 0..len {
                links.add(slot).write(Link {
                    generation: Cell::new(0),
                    queued: Cell::new(false),
                    next: Cell::new(None),
                });
            }
            NonNull::new_unchecked(queue)
        }
    }

    pub(crate) fn track(&self, id: TaskId) {
        self.links[id.slot() as usize]
            .generation
            .set(id.generation());
    }

    pub(crate) fn untrack(&self, id: TaskId) {
        let link = &self.links[id.slot() as usize];
        if link.generation.get() == id.generation() {
            link.generation.set(id.generation().wrapping_add(1));
        }
    }

    pub(crate) fn push(&self, id: TaskId) {
        let Some(link) = self.links.get(id.slot() as usize) else {
            return;
        };
        if link.generation.get() != id.generation() || link.queued.get() {
            return;
        }
        link.queued.set(true);
        link.next.set(None);
        match self.tail.get() {
            Some(tail) => self.links[tail as usize].next.set(Some(id.slot())),
            None => self.head.set(Some(id.slot())),
        }
        self.tail.set(Some(id.slot()));
    }

    pub(crate) fn pop(&self) -> Option<TaskId> {
        let slot = self.head.get()?;
        let link = &self.links[slot as usize];
        self.head.set(link.next.get());
        if self.head.get().is_none() {
            self.tail.set(None);
        }
        link.queued.set(false);
        link.next.set(None);
        Some(TaskId::from_parts(slot, link.generation.get()))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.head.get().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(len: usize) -> Box<ReadyQueue> {
        let layout = ReadyQueue::layout(len);
        // SAFETY: `layout` isn't zero-sized (it always holds `head` and `tail`).
        let ptr = NonNull::new(unsafe { alloc::alloc::alloc(layout) }).unwrap();
        // SAFETY: `ptr` is a fresh allocation of `layout`. `Box` frees it with the layout of the
        // value, which equals `layout` (checked by Miri).
        unsafe { Box::from_raw(ReadyQueue::init(ptr, len).as_ptr()) }
    }

    fn tracked(queue: &ReadyQueue, slot: u32, generation: u32) -> TaskId {
        let id = TaskId::from_parts(slot, generation);
        queue.track(id);
        id
    }

    fn drain(queue: &ReadyQueue) -> Vec<TaskId> {
        core::iter::from_fn(|| queue.pop()).collect()
    }

    #[test]
    fn layout_matches_the_layout_of_the_value() {
        let queue = boxed(5);
        assert_eq!(Layout::for_value(&*queue), ReadyQueue::layout(5));
    }

    #[test]
    fn pops_in_push_order() {
        let queue = boxed(4);
        let ids: Vec<_> = [2, 0, 3].map(|slot| tracked(&queue, slot, 0)).into();
        for id in &ids {
            queue.push(*id);
        }
        assert!(!queue.is_empty());
        assert_eq!(drain(&queue), ids);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_queued_task_is_pushed_once() {
        let queue = boxed(2);
        let a = tracked(&queue, 0, 0);
        let b = tracked(&queue, 1, 0);
        queue.push(a);
        queue.push(b);
        queue.push(a);
        assert_eq!(drain(&queue), vec![a, b]);
        queue.push(a);
        assert_eq!(drain(&queue), vec![a]);
    }

    #[test]
    fn re_push_during_draining_goes_to_the_back() {
        let queue = boxed(2);
        let a = tracked(&queue, 0, 0);
        let b = tracked(&queue, 1, 0);
        queue.push(a);
        queue.push(b);
        assert_eq!(queue.pop(), Some(a));
        queue.push(a);
        assert_eq!(drain(&queue), vec![b, a]);
    }

    #[test]
    fn wakes_of_finished_tasks_are_ignored() {
        let queue = boxed(1);
        let old = tracked(&queue, 0, 0);
        queue.untrack(old);
        queue.push(old);
        assert!(queue.is_empty());

        let new = tracked(&queue, 0, 1);
        queue.push(old);
        assert!(queue.is_empty());
        queue.push(new);
        assert_eq!(drain(&queue), vec![new]);
    }

    #[test]
    fn untracking_a_newer_task_is_ignored() {
        let queue = boxed(1);
        let old = TaskId::from_parts(0, 0);
        let new = tracked(&queue, 0, 1);
        queue.untrack(old);
        queue.push(new);
        assert_eq!(drain(&queue), vec![new]);
    }
}
