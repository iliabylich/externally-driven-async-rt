use super::AsyncWrite;

/// Writes frames of exactly `N` bytes to an [`AsyncWrite`].
///
/// A frame is accepted as a whole and a partially written frame is kept inside, so unlike
/// [`AsyncWriteExt::write_all`](crate::io::AsyncWriteExt::write_all) writing a frame can be
/// cancelled and resumed.
///
/// A frame that is not fully written yet is lost when this value is dropped, call
/// [`flush`](Self::flush) to avoid it.
#[must_use]
pub struct FrameWriter<W, const N: usize> {
    inner: W,
    buf: [u8; N],
    written: usize,
}

impl<W: AsyncWrite, const N: usize> FrameWriter<W, N> {
    /// Wraps a given writer.
    pub const fn new(inner: W) -> Self {
        Self {
            inner,
            buf: [0; N],
            written: N,
        }
    }

    /// Finishes writing the previous frame and accepts `frame` as the next one.
    ///
    /// `frame` is written by the next call to [`write_all`](Self::write_all) or
    /// [`flush`](Self::flush).
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails or if the stream accepts zero bytes.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn write_all(&mut self, frame: &[u8; N]) -> Result<(), FrameWriterError<W::Error>> {
        self.flush().await?;
        self.buf = *frame;
        self.written = 0;
        Ok(())
    }

    /// Finishes writing the last accepted frame and flushes the wrapped writer.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails or if the stream accepts zero bytes.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn flush(&mut self) -> Result<(), FrameWriterError<W::Error>> {
        while self.written < N {
            let Some(rest) = self.buf.get(self.written..) else {
                unreachable!("written is below N");
            };
            let len = match self.inner.write(rest).await {
                Ok(0) => return Err(FrameWriterError::WriteZero),
                Ok(len) => len,
                Err(err) => return Err(FrameWriterError::Io(err)),
            };
            let Some(written) = self
                .written
                .checked_add(len)
                .filter(|&written| written <= N)
            else {
                unreachable!("write() reported more written bytes than the buffer has");
            };
            self.written = written;
        }
        self.inner.flush().await.map_err(FrameWriterError::Io)
    }
}

/// An error returned from [`FrameWriter::write_all`] and [`FrameWriter::flush`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameWriterError<E> {
    /// Writing failed.
    Io(E),
    /// The stream accepted zero bytes.
    WriteZero,
}

impl<E: core::fmt::Display> core::fmt::Display for FrameWriterError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "write failed: {err}"),
            Self::WriteZero => f.write_str("stream accepted zero bytes"),
        }
    }
}

impl<E: core::error::Error> core::error::Error for FrameWriterError<E> {}

#[cfg(test)]
mod tests {
    use core::{
        future::{Future, poll_fn},
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::io::Errno;

    use super::{AsyncWrite, FrameWriter, FrameWriterError};

    struct Sink {
        max_per_call: usize,
        room: usize,
        error: Option<Errno>,
        written: [u8; 16],
        len: usize,
        flushed: usize,
    }

    impl Sink {
        const fn new(max_per_call: usize, room: usize, error: Option<Errno>) -> Self {
            Self {
                max_per_call,
                room,
                error,
                written: [0; 16],
                len: 0,
                flushed: 0,
            }
        }

        fn written(&self) -> &[u8] {
            &self.written[..self.len]
        }
    }

    impl AsyncWrite for Sink {
        type Error = Errno;

        fn write<'a>(
            &'a mut self,
            buf: &'a [u8],
        ) -> impl Future<Output = rustix::io::Result<usize>> {
            poll_fn(move |_| {
                if let Some(errno) = self.error {
                    return Poll::Ready(Err(errno));
                }
                if self.room == 0 {
                    return Poll::Pending;
                }
                let len = buf.len().min(self.max_per_call).min(self.room);
                self.written[self.len..self.len + len].copy_from_slice(&buf[..len]);
                self.len += len;
                self.room -= len;
                Poll::Ready(Ok(len))
            })
        }

        fn flush(&mut self) -> impl Future<Output = rustix::io::Result<()>> {
            self.flushed = self.len;
            core::future::ready(Ok(()))
        }
    }

    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn writes_frames_on_the_next_write_and_flush() {
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(2, 16, None));

        assert_eq!(poll_once(writer.write_all(b"hello")), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"");
        assert_eq!(poll_once(writer.write_all(b"world")), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"hello");
        assert_eq!(writer.inner.flushed, 5);
        assert_eq!(poll_once(writer.flush()), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"helloworld");
        assert_eq!(writer.inner.flushed, 10);
    }

    #[test]
    fn resumes_a_cancelled_write() {
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(2, 3, None));

        assert_eq!(poll_once(writer.write_all(b"hello")), Poll::Ready(Ok(())));
        assert_eq!(poll_once(writer.write_all(b"world")), Poll::Pending);
        assert_eq!(writer.inner.written(), b"hel");

        writer.inner.room = 16;
        assert_eq!(poll_once(writer.flush()), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"hello");
        assert_eq!(poll_once(writer.write_all(b"world")), Poll::Ready(Ok(())));
        assert_eq!(poll_once(writer.flush()), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"helloworld");
    }

    #[test]
    fn fails_when_zero_bytes_are_accepted() {
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(0, 16, None));

        assert_eq!(poll_once(writer.write_all(b"hello")), Poll::Ready(Ok(())));
        assert_eq!(
            poll_once(writer.flush()),
            Poll::Ready(Err(FrameWriterError::WriteZero))
        );
    }

    #[test]
    fn returns_syscall_errors() {
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(2, 16, Some(Errno::PIPE)));

        assert_eq!(poll_once(writer.write_all(b"hello")), Poll::Ready(Ok(())));
        assert_eq!(
            poll_once(writer.flush()),
            Poll::Ready(Err(FrameWriterError::Io(Errno::PIPE)))
        );
    }
}
