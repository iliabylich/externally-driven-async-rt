use core::{
    cell::Cell,
    future::Future,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use crate::{claimed::Claimed, tasks::TaskMemory};

#[must_use]
pub(crate) struct Task {
    future: NonNull<dyn Future<Output = ()>>,
    occupied: &'static Cell<bool>,
}

impl Task {
    pub(crate) fn write<'a, const SIZE: usize, Fut>(
        memory: &'static TaskMemory<SIZE>,
        _: Claimed<'a>,
        future: Fut,
    ) -> Self
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
        let occupied = memory.occupied();
        if occupied.replace(true) {
            unreachable!("the task memory is already occupied");
        }
        let memory = memory.as_ptr().cast::<Fut>();
        let task: NonNull<dyn Future<Output = ()> + 'a> = memory;
        // SAFETY: `Fut` fits the memory, which is aligned to 16 (both asserted above). The memory
        // is `'static`, so it stays valid, and it was unoccupied (checked above) and stays
        // occupied until the returned `Task` drops the future, so nothing else uses it. Erasing
        // the trait object's lifetime (the two pointer types differ only in it, so have the same
        // layout) is fine because the future is only polled and dropped through the `Task`, and
        // `Fut: 'a` with the `Claimed<'a>` guarantees that happens before `'a` ends.
        let future = unsafe {
            memory.write(future);
            core::mem::transmute::<
                NonNull<dyn Future<Output = ()> + 'a>,
                NonNull<dyn Future<Output = ()>>,
            >(task)
        };
        Self { future, occupied }
    }

    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the future was written by `write` and stays valid until `self` is dropped,
        // as is everything it borrows. Only `self` moves, never the future, so pinning holds, and
        // `&mut self` rules out any other access.
        unsafe { Pin::new_unchecked(self.future.as_mut()) }.poll(cx)
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        // SAFETY: the future was written by `write`, its memory is still valid and occupied by
        // it, and `drop` runs at most once.
        unsafe { core::ptr::drop_in_place(self.future.as_ptr()) };
        self.occupied.set(false);
    }
}

#[cfg(test)]
mod tests {
    use core::{
        sync::atomic::{AtomicBool, Ordering::Relaxed},
        task::{Context, Poll, Waker},
    };

    use crate::testing::{FlagOnDrop, TasksStorage, claimed_forever};

    type Storage = TasksStorage<1, 0, 64>;

    #[test]
    fn polls_in_place_while_the_owner_moves() {
        static STORAGE: Storage = Storage::new();
        let root = &STORAGE.tasks.root;
        let mut polled_once = false;
        root.write(
            claimed_forever(),
            core::future::poll_fn(move |_| {
                if polled_once {
                    Poll::Ready(())
                } else {
                    polled_once = true;
                    Poll::Pending
                }
            }),
        );
        let mut task = root.take_idle().unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(task.poll(&mut cx), Poll::Pending);
        let mut moved = [task];
        assert_eq!(moved[0].poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn dropping_drops_the_future_in_place() {
        static STORAGE: Storage = Storage::new();
        static DROPPED: AtomicBool = AtomicBool::new(false);
        let root = &STORAGE.tasks.root;
        root.write(claimed_forever(), FlagOnDrop::new(&DROPPED));
        drop(root.take_idle().unwrap());
        assert!(DROPPED.load(Relaxed));
    }

    #[test]
    fn dropping_frees_the_memory_for_the_next_write() {
        static STORAGE: Storage = Storage::new();
        let root = &STORAGE.tasks.root;
        root.write(claimed_forever(), async {});
        drop(root.take_idle().unwrap());
        root.write(claimed_forever(), async {});
        drop(root.take_idle().unwrap());
    }
}
