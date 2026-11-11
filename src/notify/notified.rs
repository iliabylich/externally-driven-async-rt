use core::{future::Future, pin::Pin, task::Poll};

use crate::notify::Notification;

/// A future that resolves once [`EventLoop::notify`](crate::EventLoop::notify) has been called.
///
/// Notifications don't queue up: any number of `notify()` calls made before the future is polled
/// resolve it only once.
///
/// Only one task can wait for a notification at a time.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Notified<'ctx> {
    notification: &'ctx Notification,
    registered: bool,
}

impl<'ctx> Notified<'ctx> {
    pub(crate) const fn new(notification: &'ctx Notification) -> Self {
        Self {
            notification,
            registered: false,
        }
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        let poll = self.notification.poll(cx.waker());
        self.registered = poll.is_pending();
        poll
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        if self.registered {
            self.notification.forget_waker();
        }
    }
}

impl core::fmt::Debug for Notified<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Notified")
    }
}
