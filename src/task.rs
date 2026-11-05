use core::future::Future;
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll, Waker};
use std::collections::HashMap;

pub(crate) type TaskFuture = Pin<Box<dyn Future<Output = ()>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TaskId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinStatus {
    Running,
    Finished,
}

#[derive(Clone, Copy)]
pub(crate) struct TaskPtr(NonNull<dyn Future<Output = ()>>);

impl TaskPtr {
    /// # Safety
    ///
    /// The task must not be removed from `Tasks` (or `Tasks` dropped) while this runs, and the
    /// same task must not be polled re-entrantly.
    pub(crate) unsafe fn poll(self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the pointer comes from `Box::into_raw` in `Tasks::insert` and stays valid
        // until `Tasks` frees it, which the caller rules out for the duration of the poll; the
        // future is never moved, so pinning holds; no other reference to it exists because it
        // is only ever accessed here, never re-entrantly.
        unsafe { Pin::new_unchecked(&mut *self.0.as_ptr()).poll(cx) }
    }
}

struct TaskEntry {
    future: TaskPtr,
    join_waiter: Option<Waker>,
}

#[derive(Default)]
pub(crate) struct Tasks {
    map: HashMap<TaskId, TaskEntry>,
    next_id: u64,
}

impl Tasks {
    pub(crate) fn insert(&mut self, future: TaskFuture) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id += 1;
        // SAFETY: the future is never moved out of its allocation; it is only polled in place
        // through `TaskPtr` and finally dropped via `Box::from_raw`.
        let future = Box::leak(unsafe { Pin::into_inner_unchecked(future) });
        self.map.insert(
            id,
            TaskEntry {
                future: TaskPtr(NonNull::from(future)),
                join_waiter: None,
            },
        );
        id
    }

    pub(crate) fn future(&self, id: TaskId) -> Option<TaskPtr> {
        Some(self.map.get(&id)?.future)
    }

    pub(crate) fn remove(&mut self, id: TaskId) -> Option<(TaskFuture, Option<Waker>)> {
        let entry = self.map.remove(&id)?;
        Some((into_box(entry.future), entry.join_waiter))
    }

    pub(crate) fn set_join_waiter(&mut self, id: TaskId, waker: &Waker) -> JoinStatus {
        let Some(entry) = self.map.get_mut(&id) else {
            return JoinStatus::Finished;
        };
        match &mut entry.join_waiter {
            Some(stored) => stored.clone_from(waker),
            None => entry.join_waiter = Some(waker.clone()),
        }
        JoinStatus::Running
    }

    pub(crate) fn take_all(&mut self, out: &mut Vec<TaskFuture>) {
        out.extend(self.map.drain().map(|(_, entry)| into_box(entry.future)));
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        let mut futures = Vec::new();
        self.take_all(&mut futures);
    }
}

fn into_box(future: TaskPtr) -> TaskFuture {
    // SAFETY: `future` was created by `Box::leak` in `Tasks::insert` and has just been removed
    // from the map, so this is the only owner left.
    unsafe { Box::into_pin(Box::from_raw(future.0.as_ptr())) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;
    use std::rc::Rc;

    struct DropFlag(Rc<Cell<bool>>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    fn future() -> TaskFuture {
        Box::pin(async {})
    }

    fn poll(tasks: &Tasks, id: TaskId) -> Poll<()> {
        let mut cx = Context::from_waker(Waker::noop());
        // SAFETY: the task stays in `tasks` for the duration of the poll.
        unsafe { tasks.future(id).unwrap().poll(&mut cx) }
    }

    #[test]
    fn polls_in_place_across_map_growth() {
        let mut tasks = Tasks::default();
        let mut polled_once = false;
        let id = tasks.insert(Box::pin(core::future::poll_fn(move |_| {
            if polled_once {
                Poll::Ready(())
            } else {
                polled_once = true;
                Poll::Pending
            }
        })));
        assert_eq!(poll(&tasks, id), Poll::Pending);
        for _ in 0..1000 {
            tasks.insert(future());
        }
        assert_eq!(poll(&tasks, id), Poll::Ready(()));
    }

    #[test]
    fn remove_returns_the_future_and_join_waiter() {
        let mut tasks = Tasks::default();
        let id = tasks.insert(future());
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Running
        );
        let (_, join_waiter) = tasks.remove(id).unwrap();
        assert!(join_waiter.is_some());
        assert!(tasks.future(id).is_none());
        assert_eq!(
            tasks.set_join_waiter(id, Waker::noop()),
            JoinStatus::Finished
        );
    }

    #[test]
    fn ids_are_never_reused() {
        let mut tasks = Tasks::default();
        let first = tasks.insert(future());
        tasks.remove(first);
        let second = tasks.insert(future());
        assert_ne!(first, second);
    }

    #[test]
    fn take_all_and_drop_free_the_futures() {
        let dropped = Rc::new(Cell::new(false));
        let flag = DropFlag(dropped.clone());
        let mut tasks = Tasks::default();
        tasks.insert(Box::pin(async move {
            let _flag = flag;
        }));
        let mut out = Vec::new();
        tasks.take_all(&mut out);
        assert!(!dropped.get());
        drop(out);
        assert!(dropped.get());

        let dropped = Rc::new(Cell::new(false));
        let flag = DropFlag(dropped.clone());
        let mut tasks = Tasks::default();
        tasks.insert(Box::pin(async move {
            let _flag = flag;
        }));
        drop(tasks);
        assert!(dropped.get());
    }
}
