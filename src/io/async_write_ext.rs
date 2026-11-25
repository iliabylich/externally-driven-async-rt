use core::future::Future;

use super::AsyncWrite;

/// Helper methods on top of [`AsyncWrite`], implemented for every [`AsyncWrite`].
pub trait AsyncWriteExt: AsyncWrite {
    /// Writes the whole given buffer, calling [`AsyncWrite::write`] until it's done.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails or if the stream accepts zero bytes.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe: an unknown number of bytes from `buf` may already be written.
    fn write_all<'a>(
        &'a mut self,
        mut buf: &'a [u8],
    ) -> impl Future<Output = Result<(), WriteAllError<Self::Error>>> {
        async move {
            while !buf.is_empty() {
                let len = match self.write(buf).await {
                    Ok(0) => return Err(WriteAllError::WriteZero),
                    Ok(len) => len,
                    Err(err) => return Err(WriteAllError::Io(err)),
                };
                let Some(rest) = buf.get(len..) else {
                    unreachable!("write() reported {len} written bytes out of {}", buf.len());
                };
                buf = rest;
            }
            Ok(())
        }
    }
}

impl<T: AsyncWrite + ?Sized> AsyncWriteExt for T {}

/// An error returned from [`AsyncWriteExt::write_all`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAllError<E> {
    /// Writing failed.
    Io(E),
    /// The stream accepted zero bytes.
    WriteZero,
}

impl<E: core::fmt::Display> core::fmt::Display for WriteAllError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "write failed: {err}"),
            Self::WriteZero => f.write_str("stream accepted zero bytes"),
        }
    }
}

impl<E: core::error::Error> core::error::Error for WriteAllError<E> {}

#[cfg(test)]
mod tests {
    use core::{
        future::{Future, ready},
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::io::Errno;

    use super::{AsyncWrite, AsyncWriteExt, WriteAllError};

    struct Scripted {
        max_per_call: usize,
        error: Option<Errno>,
        written: [u8; 32],
        len: usize,
        yielding: bool,
        calls: usize,
    }

    impl Scripted {
        const fn new(max_per_call: usize, error: Option<Errno>) -> Self {
            Self {
                max_per_call,
                error,
                written: [0; 32],
                len: 0,
                yielding: false,
                calls: 0,
            }
        }

        const fn yielding(mut self) -> Self {
            self.yielding = true;
            self
        }

        fn written(&self) -> &[u8] {
            &self.written[..self.len]
        }

        fn write_now(&mut self, buf: &[u8]) -> rustix::io::Result<usize> {
            if let Some(errno) = self.error {
                return Err(errno);
            }
            let len = buf.len().min(self.max_per_call);
            self.written[self.len..self.len + len].copy_from_slice(&buf[..len]);
            self.len += len;
            Ok(len)
        }
    }

    impl AsyncWrite for Scripted {
        type Error = Errno;

        fn write<'a>(
            &'a mut self,
            buf: &'a [u8],
        ) -> impl Future<Output = rustix::io::Result<usize>> {
            self.calls += 1;
            let mut pending = self.yielding;
            core::future::poll_fn(move |cx| {
                if pending {
                    pending = false;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(self.write_now(buf))
            })
        }

        fn flush(&mut self) -> impl Future<Output = rustix::io::Result<()>> {
            ready(Ok(()))
        }
    }

    fn write_all(writer: &mut Scripted, buf: &[u8]) -> Poll<Result<(), WriteAllError<Errno>>> {
        let mut cx = Context::from_waker(Waker::noop());
        pin!(writer.write_all(buf)).poll(&mut cx)
    }

    #[test]
    fn write_all_retries_partial_writes() {
        let mut writer = Scripted::new(3, None);
        assert_eq!(write_all(&mut writer, b"hello world"), Poll::Ready(Ok(())));
        assert_eq!(writer.written(), b"hello world");
        assert_eq!(writer.calls, 4);
    }

    #[test]
    fn write_all_of_nothing_never_writes() {
        let mut writer = Scripted::new(3, None);
        assert_eq!(write_all(&mut writer, b""), Poll::Ready(Ok(())));
        assert_eq!(writer.calls, 0);
    }

    #[test]
    fn write_all_fails_when_zero_bytes_are_accepted() {
        let mut writer = Scripted::new(0, None);
        assert_eq!(
            write_all(&mut writer, b"hello"),
            Poll::Ready(Err(WriteAllError::WriteZero))
        );
        assert_eq!(writer.calls, 1);
    }

    #[test]
    fn write_all_returns_syscall_errors() {
        let mut writer = Scripted::new(3, Some(Errno::PIPE));
        assert_eq!(
            write_all(&mut writer, b"hello"),
            Poll::Ready(Err(WriteAllError::Io(Errno::PIPE)))
        );
        assert_eq!(writer.calls, 1);
    }

    #[test]
    fn write_all_resumes_after_a_pending_write() {
        let mut writer = Scripted::new(6, None).yielding();
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut write = pin!(writer.write_all(b"hello world"));
            assert_eq!(write.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(write.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(write.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        }
        assert_eq!(writer.written(), b"hello world");
        assert_eq!(writer.calls, 2);
    }
}
