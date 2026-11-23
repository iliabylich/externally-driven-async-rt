use core::{
    cell::{Cell, UnsafeCell},
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

use crate::{claimed::Claimed, data::pool::VersionedIndex, tasks::TaskSlot, waker_slot::WakerSlot};

#[repr(C, align(16))]
#[must_use]
pub(super) struct TaskEntry<const SIZE: usize> {
    bytes: UnsafeCell<MaybeUninit<[u8; SIZE]>>,
    task: Cell<TaskSlot>,
    join_waiter: WakerSlot,
}

impl<const SIZE: usize> TaskEntry<SIZE> {
    pub(super) const fn new() -> Self {
        Self {
            bytes: UnsafeCell::new(MaybeUninit::uninit()),
            task: Cell::new(TaskSlot::Empty),
            join_waiter: WakerSlot::new(),
        }
    }

    pub(super) fn write<'a, Fut>(&'static self, _: Claimed<'a>, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        const {
            assert!(
                size_of::<Fut>() <= SIZE,
                "a task future doesn't fit TASK_SIZE or ROOT_TASK_SIZE"
            );
            assert!(
                align_of::<Fut>() <= 16,
                "a task future is aligned to more than 16 bytes"
            );
        }
        if !matches!(self.task.get(), TaskSlot::Empty) {
            unreachable!("the task entry is already occupied");
        }
        let memory = NonNull::from_ref(&self.bytes).cast::<Fut>();
        let task: NonNull<dyn Future<Output = ()> + 'a> = memory;
        // SAFETY: `Fut` fits `bytes`, which come first in an entry aligned to 16 (both asserted
        // above). The entry is `'static`, so `bytes` stay valid and never move, and the slot is
        // `Empty` (checked above), so nothing else uses them. Erasing the trait object's lifetime
        // (the two pointer types differ only in it, so have the same layout) is fine because the
        // future is only polled and dropped by this entry, and `Fut: 'a` with the `Claimed<'a>`
        // guarantees that happens before `'a` ends.
        let future = unsafe {
            memory.write(future);
            core::mem::transmute::<
                NonNull<dyn Future<Output = ()> + 'a>,
                NonNull<dyn Future<Output = ()>>,
            >(task)
        };
        self.task.set(TaskSlot::Idle(future));
        self.join_waiter.take();
    }

    pub(super) const fn has_task(&self) -> bool {
        matches!(self.task.get(), TaskSlot::Idle(_) | TaskSlot::Polling)
    }

    pub(super) fn poll(&self, cx: &mut Context<'_>) -> Option<Poll<()>> {
        let mut future = match self.task.replace(TaskSlot::Polling) {
            TaskSlot::Idle(future) => future,
            TaskSlot::Polling => unreachable!("a task can't be polled re-entrantly"),
            TaskSlot::Dropping => unreachable!("a task can't be polled while it drops"),
            TaskSlot::Empty => {
                self.task.set(TaskSlot::Empty);
                return None;
            }
        };
        // SAFETY: the future was written by `write` into `bytes` of a `'static` entry, so it never
        // moves and pinning holds. The slot is `Polling` until the future is put back or dropped,
        // so nothing else accesses it.
        if unsafe { Pin::new_unchecked(future.as_mut()) }
            .poll(cx)
            .is_pending()
        {
            self.task.set(TaskSlot::Idle(future));
            return Some(Poll::Pending);
        }
        self.task.set(TaskSlot::Dropping);
        // SAFETY: the future was written by `write`. The slot is `Dropping` while it drops, so it
        // is dropped exactly once and nothing reuses `bytes` meanwhile.
        unsafe { core::ptr::drop_in_place(future.as_ptr()) };
        self.task.set(TaskSlot::Empty);
        Some(Poll::Ready(()))
    }

    pub(super) fn drop_idle(&self) -> bool {
        let TaskSlot::Idle(future) = self.task.get() else {
            return false;
        };
        self.task.set(TaskSlot::Dropping);
        // SAFETY: the future was written by `write` and the slot was `Idle`, so it is alive and
        // not being polled. The slot is `Dropping` while it drops, so it is dropped exactly once
        // and nothing reuses `bytes` meanwhile.
        unsafe { core::ptr::drop_in_place(future.as_ptr()) };
        self.task.set(TaskSlot::Empty);
        true
    }

    pub(super) fn set_join_waiter(&self, waker: &Waker) {
        self.join_waiter.store(waker);
    }

    pub(super) const fn take_join_waiter(&self) -> Option<Waker> {
        self.join_waiter.take()
    }
}

pub(super) type TaskId<const SIZE: usize> = VersionedIndex<TaskEntry<SIZE>>;

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicBool, Ordering::Relaxed};

    use crate::testing::{FlagOnDrop, TasksStorage, claimed_forever};

    type Storage = TasksStorage<1, 0, 64>;

    #[test]
    fn dropping_drops_the_future_in_place() {
        static STORAGE: Storage = Storage::new();
        static DROPPED: AtomicBool = AtomicBool::new(false);
        let root = &STORAGE.tasks.root;
        root.write(claimed_forever(), FlagOnDrop::new(&DROPPED));
        assert!(root.drop_idle());
        assert!(DROPPED.load(Relaxed));
    }

    #[test]
    fn dropping_frees_the_entry_for_the_next_write() {
        static STORAGE: Storage = Storage::new();
        let root = &STORAGE.tasks.root;
        root.write(claimed_forever(), async {});
        assert!(root.drop_idle());
        root.write(claimed_forever(), async {});
        assert!(root.drop_idle());
    }
}
