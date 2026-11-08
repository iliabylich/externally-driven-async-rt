use core::{alloc::Layout, cell::Cell, mem::MaybeUninit, ptr::NonNull};

use crate::{storage::TaskId, thread_id::ThreadId};

// A waker is a pointer to its task's link, so the link knows its queue and slot (both never
// written after `init`).
pub(crate) struct Link {
    queue: NonNull<ReadyQueue>,
    slot: u32,
    // The generation of the task in this slot, `None` while the slot is empty. Wakers only know
    // the slot: waking an empty slot is ignored, and a stale wake of a reused slot queues its new
    // task (a spurious poll, which is harmless).
    generation: Cell<Option<u32>>,
    queued: Cell<bool>,
    next: Cell<Option<u32>>,
}

// FIFO of ready tasks threaded through one link per task slot: bounded by the task capacity,
// a task is queued at most once, and push/pop are O(1). Only `Cell`s, so wakers can use it
// through a shared reference at any time.
// `repr(C)` so `layout` can compute the size and `init` can place it in raw memory.
#[repr(C)]
pub(crate) struct ReadyQueue {
    // The thread that created the queue, the only one wakers may push from. Never written after
    // `init`, so it can be read from any thread (see `thread_of`).
    thread: ThreadId,
    head: Cell<Option<u32>>,
    tail: Cell<Option<u32>>,
    links: [Link],
}

impl ReadyQueue {
    pub(crate) fn layout(len: usize) -> Layout {
        let thread = Layout::new::<ThreadId>();
        let end = Layout::new::<Cell<Option<u32>>>();
        let links = Layout::array::<Link>(len).expect("ready queue is too large");
        let (layout, _) = thread.extend(end).expect("ready queue is too large");
        let (layout, _) = layout.extend(end).expect("ready queue is too large");
        let (layout, _) = layout.extend(links).expect("ready queue is too large");
        layout.pad_to_align()
    }

    /// Writes an empty queue with `len` links at `ptr`, owned by the current thread.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for writes of `Self::layout(len)` bytes, aligned to it, and stay valid
    /// (and otherwise unused) for `'a`.
    pub(crate) unsafe fn init<'a>(ptr: NonNull<u8>, len: usize) -> &'a Self {
        let queue =
            core::ptr::slice_from_raw_parts_mut(ptr.as_ptr().cast::<Link>(), len) as *mut Self;
        // SAFETY: the caller guarantees `ptr` is valid for `Self::layout(len)`, which (`repr(C)`)
        // covers every field, disjointly. `&raw mut` creates no reference to the uninitialized
        // queue, and the fields are borrowed as `MaybeUninit`, which needs no initialized
        // contents. `queue` comes from the non-null `ptr`.
        let (thread, head, tail, links, queue_ptr) = unsafe {
            (
                &mut *(&raw mut (*queue).thread).cast::<MaybeUninit<ThreadId>>(),
                &mut *(&raw mut (*queue).head).cast::<MaybeUninit<Cell<Option<u32>>>>(),
                &mut *(&raw mut (*queue).tail).cast::<MaybeUninit<Cell<Option<u32>>>>(),
                &mut *(&raw mut (*queue).links as *mut [MaybeUninit<Link>]),
                NonNull::new_unchecked(queue),
            )
        };
        thread.write(ThreadId::current());
        head.write(Cell::new(None));
        tail.write(Cell::new(None));
        for (slot, place) in links.iter_mut().enumerate() {
            place.write(Link {
                queue: queue_ptr,
                slot: slot as u32,
                generation: Cell::new(None),
                queued: Cell::new(false),
                next: Cell::new(None),
            });
        }
        // SAFETY: every field was written above, and the memory stays valid for `'a` (guaranteed
        // by the caller).
        unsafe { queue_ptr.as_ref() }
    }

    pub(crate) fn link(&self, slot: u32) -> NonNull<Link> {
        NonNull::from(&self.links[slot as usize])
    }

    /// The thread of `link`'s queue. Unlike the `&self` methods, this may be called from any
    /// thread.
    ///
    /// # Safety
    ///
    /// `link` must come from `Self::link` of a queue that is still alive.
    pub(crate) unsafe fn thread_of(link: NonNull<Link>) -> ThreadId {
        // SAFETY: the caller guarantees the queue is alive. Only `queue` and `thread` are read,
        // through raw pointers (no reference to the `Cell`s that the owning thread may be
        // mutating), and neither is written after `init`, so this doesn't race.
        unsafe { (*(*link.as_ptr()).queue.as_ptr()).thread }
    }

    /// # Safety
    ///
    /// `link` must come from `Self::link` of a queue that is still alive, and this must run on
    /// the queue's thread (see `thread_of`).
    pub(crate) unsafe fn push_link(link: NonNull<Link>) {
        // SAFETY: the caller guarantees the queue is alive and that this runs on its thread, so
        // nothing accesses it concurrently.
        let link = unsafe { link.as_ref() };
        // SAFETY: as above, `queue` points to the live queue that contains `link`.
        unsafe { link.queue.as_ref() }.push(link.slot);
    }

    pub(crate) fn track(&self, id: TaskId) {
        self.links[id.slot() as usize]
            .generation
            .set(Some(id.generation()));
    }

    pub(crate) fn untrack(&self, id: TaskId) {
        let link = &self.links[id.slot() as usize];
        if link.generation.get() == Some(id.generation()) {
            link.generation.set(None);
        }
    }

    pub(crate) fn push(&self, slot: u32) {
        let link = &self.links[slot as usize];
        if link.generation.get().is_none() || link.queued.get() {
            return;
        }
        link.queued.set(true);
        link.next.set(None);
        match self.tail.get() {
            Some(tail) => self.links[tail as usize].next.set(Some(slot)),
            None => self.head.set(Some(slot)),
        }
        self.tail.set(Some(slot));
    }

    pub(crate) fn pop(&self) -> Option<TaskId> {
        loop {
            let slot = self.head.get()?;
            let link = &self.links[slot as usize];
            self.head.set(link.next.get());
            if self.head.get().is_none() {
                self.tail.set(None);
            }
            link.queued.set(false);
            link.next.set(None);
            // A task that finished while queued leaves its slot queued but empty.
            if let Some(generation) = link.generation.get() {
                return Some(TaskId::from_parts(slot, generation));
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.head.get().is_none()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ready::ReadyQueue,
        storage::TaskId,
        test_utils::{aligned_in, assert_yields, stack_memory},
    };
    use core::{alloc::Layout, mem::MaybeUninit};

    // The queue needs no drop (it's all `Cell`s of `Copy` values).
    fn queue_in(memory: &mut [MaybeUninit<u8>], len: usize) -> &ReadyQueue {
        let ptr = aligned_in(memory, ReadyQueue::layout(len));
        // SAFETY: `ptr` is aligned and valid for the layout, and `memory` stays borrowed for as
        // long as the returned queue.
        unsafe { ReadyQueue::init(ptr, len) }
    }

    fn tracked(queue: &ReadyQueue, slot: u32, generation: u32) -> TaskId {
        let id = TaskId::from_parts(slot, generation);
        queue.track(id);
        id
    }

    fn drain(queue: &ReadyQueue) -> impl Iterator<Item = TaskId> + '_ {
        core::iter::from_fn(|| queue.pop())
    }

    #[test]
    fn layout_matches_the_layout_of_the_value() {
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 5);
        assert_eq!(Layout::for_value(queue), ReadyQueue::layout(5));
    }

    #[test]
    fn pops_in_push_order() {
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 4);
        let ids = [2, 0, 3].map(|slot| tracked(queue, slot, 0));
        for id in ids {
            queue.push(id.slot());
        }
        assert!(!queue.is_empty());
        assert_yields(drain(queue), ids);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_queued_task_is_pushed_once() {
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 2);
        let a = tracked(queue, 0, 0);
        let b = tracked(queue, 1, 0);
        queue.push(a.slot());
        queue.push(b.slot());
        queue.push(a.slot());
        assert_yields(drain(queue), [a, b]);
        queue.push(a.slot());
        assert_yields(drain(queue), [a]);
    }

    #[test]
    fn re_push_during_draining_goes_to_the_back() {
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 2);
        let a = tracked(queue, 0, 0);
        let b = tracked(queue, 1, 0);
        queue.push(a.slot());
        queue.push(b.slot());
        assert_eq!(queue.pop(), Some(a));
        queue.push(a.slot());
        assert_yields(drain(queue), [b, a]);
    }

    #[test]
    fn pushing_an_empty_slot_is_ignored() {
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 1);
        queue.push(0);
        assert!(queue.is_empty());
        let old = tracked(queue, 0, 0);
        queue.untrack(old);
        queue.push(0);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_reused_slot_pops_its_new_task() {
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 1);
        let old = tracked(queue, 0, 0);
        queue.untrack(old);
        let new = tracked(queue, 0, 1);
        queue.push(old.slot());
        assert_yields(drain(queue), [new]);
    }

    #[test]
    fn a_task_finished_while_queued_is_skipped() {
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 2);
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
        let mut memory = stack_memory();
        let queue = queue_in(&mut memory, 1);
        let old = TaskId::from_parts(0, 0);
        let new = tracked(queue, 0, 1);
        queue.untrack(old);
        queue.push(new.slot());
        assert_yields(drain(queue), [new]);
    }
}
