use core::future::Future;

use crate::Context;

use super::AsyncRead;

/// Helper methods on top of [`AsyncRead`], implemented for every [`AsyncRead`].
pub trait AsyncReadExt: AsyncRead {
    /// Fills the whole given buffer, calling [`AsyncRead::read`] until it's done.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// 1. reading fails
    /// 2. or if the stream is closed before the buffer is filled.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe: an unknown number of bytes may already be read into `buf`.
    fn read_exact<
        'a,
        E: 'static,
        const EVENTS: usize,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        ctx: &'a Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        mut buf: &'a mut [u8],
    ) -> impl Future<Output = Result<(), ReadExactError<Self::Error>>> {
        async move {
            while !buf.is_empty() {
                let len = match self.read(ctx, buf).await {
                    Ok(0) => return Err(ReadExactError::UnexpectedEof),
                    Ok(len) => len,
                    Err(err) => return Err(ReadExactError::Io(err)),
                };
                let Some(rest) = core::mem::take(&mut buf).get_mut(len..) else {
                    unreachable!("read() reported more read bytes than the buffer has");
                };
                buf = rest;
            }
            Ok(())
        }
    }
}

impl<T: AsyncRead + ?Sized> AsyncReadExt for T {}

/// An error returned from [`AsyncReadExt::read_exact`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadExactError<E> {
    /// Reading failed.
    Io(E),
    /// The stream was closed before the buffer was filled.
    UnexpectedEof,
}

impl<E: core::fmt::Display> core::fmt::Display for ReadExactError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "read failed: {err}"),
            Self::UnexpectedEof => f.write_str("stream closed before the buffer was filled"),
        }
    }
}

impl<E: core::error::Error> core::error::Error for ReadExactError<E> {}

#[cfg(test)]
mod tests {
    use core::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::io::Errno;

    use super::{AsyncRead, AsyncReadExt, ReadExactError};

    use crate::{LoopStorage, Selector};

    type Storage = LoopStorage<(), 1, 1, 0, 0, 1, 1>;

    struct Scripted {
        data: &'static [u8],
        max_per_call: usize,
        error: Option<Errno>,
        yielding: bool,
        calls: usize,
    }

    impl Scripted {
        const fn new(data: &'static [u8], max_per_call: usize, error: Option<Errno>) -> Self {
            Self {
                data,
                max_per_call,
                error,
                yielding: false,
                calls: 0,
            }
        }

        const fn yielding(mut self) -> Self {
            self.yielding = true;
            self
        }

        fn read_now(&mut self, buf: &mut [u8]) -> rustix::io::Result<usize> {
            if let Some(errno) = self.error {
                return Err(errno);
            }
            let len = buf.len().min(self.max_per_call).min(self.data.len());
            buf[..len].copy_from_slice(&self.data[..len]);
            self.data = &self.data[len..];
            Ok(len)
        }
    }

    impl AsyncRead for Scripted {
        type Error = Errno;

        fn read<
            'a,
            E: 'static,
            const EVENTS: usize,
            const TASKS: usize,
            const TASK_SIZE: usize,
            const ROOT_TASK_SIZE: usize,
            const TIMERS: usize,
            const FDS: usize,
        >(
            &'a mut self,
            _ctx: &'a crate::Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
            buf: &'a mut [u8],
        ) -> impl Future<Output = rustix::io::Result<usize>> {
            self.calls += 1;
            let mut pending = self.yielding;
            core::future::poll_fn(move |cx| {
                if pending {
                    pending = false;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(self.read_now(buf))
            })
        }
    }

    fn read_exact(
        storage: &'static Storage,
        reader: &mut Scripted,
        buf: &mut [u8],
    ) -> Poll<Result<(), ReadExactError<Errno>>> {
        let selector = Selector::new().unwrap();
        let ctx = storage.claim(&selector).unwrap().0;
        let mut cx = Context::from_waker(Waker::noop());
        pin!(reader.read_exact(&ctx, buf)).poll(&mut cx)
    }

    #[test]
    fn read_exact_retries_partial_reads() {
        static STORAGE: Storage = Storage::new();
        let mut reader = Scripted::new(b"hello world!", 3, None);
        let mut buf = [0; 11];
        assert_eq!(
            read_exact(&STORAGE, &mut reader, &mut buf),
            Poll::Ready(Ok(()))
        );
        assert_eq!(&buf, b"hello world");
        assert_eq!(reader.calls, 4);
        assert_eq!(reader.data, b"!");
    }

    #[test]
    fn read_exact_into_nothing_never_reads() {
        static STORAGE: Storage = Storage::new();
        let mut reader = Scripted::new(b"hello", 3, None);
        assert_eq!(
            read_exact(&STORAGE, &mut reader, &mut []),
            Poll::Ready(Ok(()))
        );
        assert_eq!(reader.calls, 0);
    }

    #[test]
    fn read_exact_fails_when_the_stream_ends_early() {
        static STORAGE: Storage = Storage::new();
        let mut reader = Scripted::new(b"hel", 2, None);
        let mut buf = [0; 5];
        assert_eq!(
            read_exact(&STORAGE, &mut reader, &mut buf),
            Poll::Ready(Err(ReadExactError::UnexpectedEof))
        );
        assert_eq!(&buf[..3], b"hel");
        assert_eq!(reader.calls, 3);
    }

    #[test]
    fn read_exact_returns_syscall_errors() {
        static STORAGE: Storage = Storage::new();
        let mut reader = Scripted::new(b"hello", 3, Some(Errno::CONNRESET));
        let mut buf = [0; 5];
        assert_eq!(
            read_exact(&STORAGE, &mut reader, &mut buf),
            Poll::Ready(Err(ReadExactError::Io(Errno::CONNRESET)))
        );
        assert_eq!(reader.calls, 1);
    }

    #[test]
    fn read_exact_resumes_after_a_pending_read() {
        static STORAGE: Storage = Storage::new();
        let selector = Selector::new().unwrap();
        let ctx = STORAGE.claim(&selector).unwrap().0;
        let mut reader = Scripted::new(b"hello world", 6, None).yielding();
        let mut buf = [0; 11];
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut read = pin!(reader.read_exact(&ctx, &mut buf));
            assert_eq!(read.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(read.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(read.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        }
        assert_eq!(&buf, b"hello world");
        assert_eq!(reader.calls, 2);
    }
}
