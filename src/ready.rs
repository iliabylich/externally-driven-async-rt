use core::{cell::Cell, mem::MaybeUninit, ptr::NonNull};

use crate::{storage::TaskId, thread_id::ThreadId};

pub(crate) struct Link<const N: usize> {
    queue: NonNull<ReadyQueue<N>>,
    slot: u32,
    generation: Cell<Option<u32>>,
    queued: Cell<bool>,
    next: Cell<Option<u32>>,
}

pub(crate) struct ReadyQueue<const N: usize> {
    thread: ThreadId,
    head: Cell<Option<u32>>,
    tail: Cell<Option<u32>>,
    links: [Link<N>; N],
}

impl<const N: usize> ReadyQueue<N> {
    pub(crate) fn init(place: &mut MaybeUninit<Self>) -> &Self {
        let ptr = place.as_mut_ptr();
        // SAFETY: from the reference `place`.
        let queue = unsafe { NonNull::new_unchecked(ptr) };
        // SAFETY: `ptr` points to `place`, borrowed exclusively until the end; the fields are
        // disjoint, `&raw mut` creates no reference to them while they're uninitialized, and
        // `MaybeUninit<T>` has `T`'s layout.
        let (thread, head, tail, links) = unsafe {
            (
                &mut *(&raw mut (*ptr).thread).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).head).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).tail).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).links).cast::<[MaybeUninit<_>; N]>(),
            )
        };
        thread.write(ThreadId::current());
        head.write(Cell::new(None::<u32>));
        tail.write(Cell::new(None::<u32>));
        for (slot, link) in links.iter_mut().enumerate() {
            link.write(Link {
                queue,
                slot: slot as u32,
                generation: Cell::new(None),
                queued: Cell::new(false),
                next: Cell::new(None),
            });
        }
        // SAFETY: every field was written above. Derived from `ptr`, like the links' `queue`.
        unsafe { queue.as_ref() }
    }

    pub(crate) fn link(&self, slot: u32) -> NonNull<Link<N>> {
        NonNull::from(&self.links[slot as usize])
    }

    /// # Safety
    ///
    /// `link` must come from `Self::link` of a queue that is still alive.
    pub(crate) unsafe fn thread_of(link: NonNull<Link<N>>) -> ThreadId {
        // SAFETY: the caller guarantees the queue is alive. Only `queue` and `thread` are read,
        // through raw pointers (no reference to the `Cell`s that the owning thread may be
        // mutating), and neither is written after `init`, so this doesn't race.
        unsafe { (*(*link.as_ptr()).queue.as_ptr()).thread }
    }

    /// # Safety
    ///
    /// `link` must come from `Self::link` of a queue that is still alive, and this must run on
    /// the queue's thread (see `thread_of`).
    pub(crate) unsafe fn push_link(link: NonNull<Link<N>>) {
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
    use crate::{ready::ReadyQueue, storage::TaskId, test_utils::assert_yields};
    use core::mem::MaybeUninit;

    fn tracked<const N: usize>(queue: &ReadyQueue<N>, slot: u32, generation: u32) -> TaskId {
        let id = TaskId::from_parts(slot, generation);
        queue.track(id);
        id
    }

    fn drain<const N: usize>(queue: &ReadyQueue<N>) -> impl Iterator<Item = TaskId> + '_ {
        core::iter::from_fn(|| queue.pop())
    }

    #[test]
    fn pops_in_push_order() {
        let mut place = MaybeUninit::uninit();
        let queue = ReadyQueue::<4>::init(&mut place);
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
        let mut place = MaybeUninit::uninit();
        let queue = ReadyQueue::<2>::init(&mut place);
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
        let mut place = MaybeUninit::uninit();
        let queue = ReadyQueue::<2>::init(&mut place);
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
        let mut place = MaybeUninit::uninit();
        let queue = ReadyQueue::<1>::init(&mut place);
        queue.push(0);
        assert!(queue.is_empty());
        let old = tracked(queue, 0, 0);
        queue.untrack(old);
        queue.push(0);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_reused_slot_pops_its_new_task() {
        let mut place = MaybeUninit::uninit();
        let queue = ReadyQueue::<1>::init(&mut place);
        let old = tracked(queue, 0, 0);
        queue.untrack(old);
        let new = tracked(queue, 0, 1);
        queue.push(old.slot());
        assert_yields(drain(queue), [new]);
    }

    #[test]
    fn a_task_finished_while_queued_is_skipped() {
        let mut place = MaybeUninit::uninit();
        let queue = ReadyQueue::<2>::init(&mut place);
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
        let mut place = MaybeUninit::uninit();
        let queue = ReadyQueue::<1>::init(&mut place);
        let old = TaskId::from_parts(0, 0);
        let new = tracked(queue, 0, 1);
        queue.untrack(old);
        queue.push(new.slot());
        assert_yields(drain(queue), [new]);
    }
}
