use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use crate::inner::Inner;
use crate::task::{JoinStatus, TaskFuture, TaskId, Tasks};

pub struct Context<E> {
    inner: *const Inner<E>,
}

impl<E> Context<E> {
    pub(crate) fn inner(&self) -> &Inner<E> {
        // SAFETY: a `Context` only exists inside its task's future (see `task_future`), and all
        // task futures are dropped before `Inner` is freed (`OwnedInner::drop`).
        unsafe { &*self.inner }
    }

    pub fn dispatch(&self, event: E) {
        self.inner().dispatch(event);
    }

    pub fn spawn<F>(&self, f: F) -> JoinHandle<'_>
    where
        E: 'static,
        F: AsyncFnOnce(&Context<E>) + 'static,
    {
        let inner = self.inner();
        // SAFETY: the future is stored in this `Inner`'s task storage right away.
        let id = inner.spawn(unsafe { task_future(self.inner, f) });
        JoinHandle {
            tasks: inner.tasks(),
            id,
        }
    }
}

pub struct JoinHandle<'ctx> {
    tasks: &'ctx RefCell<Tasks>,
    id: TaskId,
}

impl Future for JoinHandle<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        match self.tasks.borrow_mut().set_join_waiter(self.id, cx.waker()) {
            JoinStatus::Running => Poll::Pending,
            JoinStatus::Finished => Poll::Ready(()),
        }
    }
}

/// # Safety
///
/// The returned future must be stored in `inner`'s task storage (and so dropped before `inner`
/// is freed).
pub(crate) unsafe fn task_future<E, F>(inner: *const Inner<E>, f: F) -> TaskFuture
where
    E: 'static,
    F: AsyncFnOnce(&Context<E>) + 'static,
{
    Box::pin(async move {
        let ctx = Context { inner };
        f(&ctx).await;
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inner::OwnedInner;

    #[test]
    fn task_dispatches_through_its_context() {
        let owned = OwnedInner::<u32>::new().unwrap();
        let inner = owned.get();
        // SAFETY: spawned into `inner`.
        let future = unsafe {
            task_future(owned.as_ptr(), async |ctx| {
                ctx.dispatch(1);
                ctx.dispatch(2);
            })
        };
        let root = inner.spawn(future);
        inner.run_ready_tasks(root);
        assert_eq!(inner.pop_event(), Some(1));
        assert_eq!(inner.pop_event(), Some(2));
        assert_eq!(inner.pop_event(), None);
    }
}
