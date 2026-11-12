use core::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

/// An output of [`select2`](crate::select2).
#[derive(Debug, PartialEq, Eq)]
pub enum Either2<L, R> {
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
pub async fn select2<L: Future, R: Future>(left: L, right: R) -> Either2<L::Output, R::Output> {
    let mut left = pin!(left);
    let mut right = pin!(right);
    poll_fn(|cx| {
        if let Poll::Ready(output) = left.as_mut().poll(cx) {
            return Poll::Ready(Either2::Left(output));
        }
        if let Poll::Ready(output) = right.as_mut().poll(cx) {
            return Poll::Ready(Either2::Right(output));
        }
        Poll::Pending
    })
    .await
}

/// An output of [`select3`](crate::select3).
#[derive(Debug, PartialEq, Eq)]
pub enum Either3<A, B, C> {
    /// The first future has completed first.
    First(A),
    /// The second future has completed first.
    Second(B),
    /// The third future has completed first.
    Third(C),
}

/// Polls three futures concurrently, resolves the one that completes first.
///
/// If several complete at the same time, the earliest one in the argument list wins,
/// the others are dropped.
///
/// # Cancel safety
///
/// This function is cancel safe if `first`, `second` and `third` are.
pub async fn select3<A: Future, B: Future, C: Future>(
    first: A,
    second: B,
    third: C,
) -> Either3<A::Output, B::Output, C::Output> {
    let mut first = pin!(first);
    let mut second = pin!(second);
    let mut third = pin!(third);
    poll_fn(|cx| {
        if let Poll::Ready(output) = first.as_mut().poll(cx) {
            return Poll::Ready(Either3::First(output));
        }
        if let Poll::Ready(output) = second.as_mut().poll(cx) {
            return Poll::Ready(Either3::Second(output));
        }
        if let Poll::Ready(output) = third.as_mut().poll(cx) {
            return Poll::Ready(Either3::Third(output));
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use crate::{Either2, Either3, select2, select3};
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
            poll_once(select2(ready(1), pending::<()>())),
            Poll::Ready(Either2::Left(1))
        );
    }

    #[test]
    fn test_right() {
        assert_eq!(
            poll_once(select2(pending::<()>(), ready(2))),
            Poll::Ready(Either2::Right(2))
        );
    }

    #[test]
    fn test_left_wins_when_both_ready() {
        assert_eq!(
            poll_once(select2(ready(1), ready(2))),
            Poll::Ready(Either2::Left(1))
        );
    }

    #[test]
    fn test_pending() {
        assert_eq!(
            poll_once(select2(pending::<()>(), pending::<()>())),
            Poll::Pending
        );
    }

    #[test]
    fn test3_first() {
        assert_eq!(
            poll_once(select3(ready(1), pending::<()>(), pending::<()>())),
            Poll::Ready(Either3::First(1))
        );
    }

    #[test]
    fn test3_second() {
        assert_eq!(
            poll_once(select3(pending::<()>(), ready(2), pending::<()>())),
            Poll::Ready(Either3::Second(2))
        );
    }

    #[test]
    fn test3_third() {
        assert_eq!(
            poll_once(select3(pending::<()>(), pending::<()>(), ready(3))),
            Poll::Ready(Either3::Third(3))
        );
    }

    #[test]
    fn test3_first_wins_when_all_ready() {
        assert_eq!(
            poll_once(select3(ready(1), ready(2), ready(3))),
            Poll::Ready(Either3::First(1))
        );
    }

    #[test]
    fn test3_pending() {
        assert_eq!(
            poll_once(select3(pending::<()>(), pending::<()>(), pending::<()>())),
            Poll::Pending
        );
    }
}
