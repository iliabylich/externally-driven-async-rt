use core::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    task::{Context, Poll},
};

pub(crate) struct FlagOnDrop<'a>(&'a AtomicBool);

impl<'a> FlagOnDrop<'a> {
    pub(crate) fn new(flag: &'a AtomicBool) -> Self {
        Self(flag)
    }
}

impl Drop for FlagOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Relaxed);
    }
}

impl Future for FlagOnDrop<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
