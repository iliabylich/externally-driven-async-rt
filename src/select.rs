use core::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

/// An output of [`select`](crate::select).
#[derive(Debug, PartialEq, Eq)]
pub enum Either<L, R> {
    /// The left future has completed first.
    Left(L),
    /// The right future has completed first.
    Right(R),
}

/// Polls two futures concurrently, resolves the one that completes first.
///
/// If both complete at the same time, the left one wins, the other one is dropped.
///
/// # Cancel safety
///
/// This function is cancel safe if both `left` and `right` are.
pub async fn select<L: Future, R: Future>(left: L, right: R) -> Either<L::Output, R::Output> {
    let mut left = pin!(left);
    let mut right = pin!(right);
    poll_fn(|cx| {
        if let Poll::Ready(output) = left.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(output));
        }
        if let Poll::Ready(output) = right.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(output));
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use crate::{Either, select};
    use core::{
        future::{Future, pending, ready},
        pin::pin,
        task::{Context, Poll, Waker},
    };

    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn test_left() {
        assert_eq!(
            poll_once(select(ready(1), pending::<()>())),
            Poll::Ready(Either::Left(1))
        );
    }

    #[test]
    fn test_right() {
        assert_eq!(
            poll_once(select(pending::<()>(), ready(2))),
            Poll::Ready(Either::Right(2))
        );
    }

    #[test]
    fn test_left_wins_when_both_ready() {
        assert_eq!(
            poll_once(select(ready(1), ready(2))),
            Poll::Ready(Either::Left(1))
        );
    }

    #[test]
    fn test_pending() {
        assert_eq!(
            poll_once(select(pending::<()>(), pending::<()>())),
            Poll::Pending
        );
    }
}
