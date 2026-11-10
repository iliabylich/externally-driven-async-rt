use core::{
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) struct FlagOnDrop<'a>(&'a Cell<bool>);

impl<'a> FlagOnDrop<'a> {
    pub(crate) fn new(flag: &'a Cell<bool>) -> Self {
        Self(flag)
    }
}

impl Drop for FlagOnDrop<'_> {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

impl Future for FlagOnDrop<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
