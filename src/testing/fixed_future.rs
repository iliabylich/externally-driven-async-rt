use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) struct FixedFuture<T>(T);

impl<T> FixedFuture<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> Future for FixedFuture<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
