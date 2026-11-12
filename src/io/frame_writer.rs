use rustix::io::Errno;

use crate::{Context, State, io::AsyncWrite};

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
    /// Returns an error if a syscall fails or if the stream accepts zero bytes.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn write_all<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &mut self,
        ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        frame: &[u8; N],
    ) -> Result<(), FrameWriterError> {
        self.flush(ctx).await?;
        self.buf = *frame;
        self.written = 0;
        Ok(())
    }

    /// Finishes writing the last accepted frame and flushes the wrapped writer.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails or if the stream accepts zero bytes.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn flush<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &mut self,
        ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) -> Result<(), FrameWriterError> {
        while self.written < N {
            let Some(rest) = self.buf.get(self.written..) else {
                unreachable!("written is below N");
            };
            let len = match self.inner.write(ctx, rest).await {
                Ok(0) => return Err(FrameWriterError::WriteZero),
                Ok(len) => len,
                Err(errno) => return Err(FrameWriterError::Io(errno)),
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
        self.inner.flush(ctx).await.map_err(FrameWriterError::Io)
    }
}

impl<W, const N: usize> core::fmt::Debug for FrameWriter<W, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("FrameWriter")
    }
}

/// An error returned from [`FrameWriter::write_all`] and [`FrameWriter::flush`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameWriterError {
    /// A syscall failed.
    Io(Errno),
    /// The stream accepted zero bytes.
    WriteZero,
}

impl core::fmt::Display for FrameWriterError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "write failed: {errno}"),
            Self::WriteZero => f.write_str("stream accepted zero bytes"),
        }
    }
}

impl core::error::Error for FrameWriterError {}

#[cfg(test)]
mod tests {
    use core::{
        cell::RefCell,
        future::{Future, poll_fn},
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::io::Errno;

    use crate::{
        Selector, State,
        io::{AsyncWrite, FrameWriter, FrameWriterError},
        testing::{Log, TestContext, TestStorage},
    };

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
        fn write<
            'a,
            S: State,
            const TASKS: usize,
            const TASK_SIZE: usize,
            const ROOT_TASK_SIZE: usize,
            const TIMERS: usize,
            const FDS: usize,
        >(
            &'a mut self,
            _ctx: &'a crate::Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
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

        fn flush<
            'a,
            S: State,
            const TASKS: usize,
            const TASK_SIZE: usize,
            const ROOT_TASK_SIZE: usize,
            const TIMERS: usize,
            const FDS: usize,
        >(
            &'a mut self,
            _ctx: &'a crate::Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        ) -> impl Future<Output = rustix::io::Result<()>> {
            self.flushed = self.len;
            core::future::ready(Ok(()))
        }
    }

    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn writes_frames_on_the_next_write_and_flush() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(2, 16, None));

        assert_eq!(
            poll_once(writer.write_all(&ctx, b"hello")),
            Poll::Ready(Ok(()))
        );
        assert_eq!(writer.inner.written(), b"");
        assert_eq!(
            poll_once(writer.write_all(&ctx, b"world")),
            Poll::Ready(Ok(()))
        );
        assert_eq!(writer.inner.written(), b"hello");
        assert_eq!(writer.inner.flushed, 5);
        assert_eq!(poll_once(writer.flush(&ctx)), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"helloworld");
        assert_eq!(writer.inner.flushed, 10);
    }

    #[test]
    fn resumes_a_cancelled_write() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(2, 3, None));

        assert_eq!(
            poll_once(writer.write_all(&ctx, b"hello")),
            Poll::Ready(Ok(()))
        );
        assert_eq!(poll_once(writer.write_all(&ctx, b"world")), Poll::Pending);
        assert_eq!(writer.inner.written(), b"hel");

        writer.inner.room = 16;
        assert_eq!(poll_once(writer.flush(&ctx)), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"hello");
        assert_eq!(
            poll_once(writer.write_all(&ctx, b"world")),
            Poll::Ready(Ok(()))
        );
        assert_eq!(poll_once(writer.flush(&ctx)), Poll::Ready(Ok(())));
        assert_eq!(writer.inner.written(), b"helloworld");
    }

    #[test]
    fn fails_when_zero_bytes_are_accepted() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(0, 16, None));

        assert_eq!(
            poll_once(writer.write_all(&ctx, b"hello")),
            Poll::Ready(Ok(()))
        );
        assert_eq!(
            poll_once(writer.flush(&ctx)),
            Poll::Ready(Err(FrameWriterError::WriteZero))
        );
    }

    #[test]
    fn returns_syscall_errors() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut writer = FrameWriter::<_, 5>::new(Sink::new(2, 16, Some(Errno::PIPE)));

        assert_eq!(
            poll_once(writer.write_all(&ctx, b"hello")),
            Poll::Ready(Ok(()))
        );
        assert_eq!(
            poll_once(writer.flush(&ctx)),
            Poll::Ready(Err(FrameWriterError::Io(Errno::PIPE)))
        );
    }
}
