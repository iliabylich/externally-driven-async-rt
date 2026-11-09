use core::future::Future;

pub(crate) struct FixedFuture<T>(pub(crate) T);

impl<T> Future for FixedFuture<T> {
    type Output = ();

    fn poll(
        self: core::pin::Pin<&mut Self>,
        _: &mut core::task::Context<'_>,
    ) -> core::task::Poll<()> {
        core::task::Poll::Ready(())
    }
}
