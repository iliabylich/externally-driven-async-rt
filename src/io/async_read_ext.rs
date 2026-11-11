use core::future::Future;

use rustix::io::Errno;

use crate::{Context, State, io::AsyncRead};

/// Helper methods on top of [`AsyncRead`], implemented for every [`AsyncRead`].
pub trait AsyncReadExt: AsyncRead {
    /// Fills the whole given buffer, calling [`AsyncRead::read`] until it's done.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// 1. a syscall fails
    /// 2. or if the stream is closed before the buffer is filled.
    fn read_exact<
        'a,
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        ctx: &'a Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        mut buf: &'a mut [u8],
    ) -> impl Future<Output = Result<(), ReadExactError>> {
        async move {
            while !buf.is_empty() {
                let len = match self.read(ctx, buf).await {
                    Ok(0) => return Err(ReadExactError::UnexpectedEof),
                    Ok(len) => len,
                    Err(errno) => return Err(ReadExactError::Io(errno)),
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
pub enum ReadExactError {
    /// A syscall failed.
    Io(Errno),
    /// The stream was closed before the buffer was filled.
    UnexpectedEof,
}

impl core::fmt::Display for ReadExactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "read failed: {errno}"),
            Self::UnexpectedEof => f.write_str("stream closed before the buffer was filled"),
        }
    }
}

impl core::error::Error for ReadExactError {}

#[cfg(test)]
mod tests {
    use core::{
        cell::RefCell,
        future::{Future, pending, ready},
        net::SocketAddr,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::{
        io::Errno,
        net::{AddressFamily, SocketType},
    };

    use crate::{
        Selector, State,
        io::{AsyncRead, AsyncReadExt, ReadExactError},
        net::TcpStream,
        testing::{
            EventLoopTestingExt, Log, TestContext, TestLoop, TestStorage, is_readable, raw_accept,
        },
    };

    struct Scripted {
        data: &'static [u8],
        max_per_call: usize,
        error: Option<Errno>,
        calls: usize,
    }

    impl Scripted {
        const fn new(data: &'static [u8], max_per_call: usize, error: Option<Errno>) -> Self {
            Self {
                data,
                max_per_call,
                error,
                calls: 0,
            }
        }
    }

    impl AsyncRead for Scripted {
        fn read<
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
            buf: &'a mut [u8],
        ) -> impl Future<Output = rustix::io::Result<usize>> {
            self.calls += 1;
            if let Some(errno) = self.error {
                return ready(Err(errno));
            }
            let len = buf.len().min(self.max_per_call).min(self.data.len());
            buf[..len].copy_from_slice(&self.data[..len]);
            self.data = &self.data[len..];
            ready(Ok(len))
        }
    }

    fn read_exact(
        storage: &'static TestStorage,
        reader: &mut Scripted,
        buf: &mut [u8],
    ) -> Poll<Result<(), ReadExactError>> {
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = storage.claim(&selector, &state);
        let mut cx = Context::from_waker(Waker::noop());
        pin!(reader.read_exact(&ctx, buf)).poll(&mut cx)
    }

    #[test]
    fn read_exact_retries_partial_reads() {
        let mut reader = Scripted::new(b"hello world!", 3, None);
        let mut buf = [0; 11];
        assert_eq!(
            read_exact(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut reader,
                &mut buf
            ),
            Poll::Ready(Ok(()))
        );
        assert_eq!(&buf, b"hello world");
        assert_eq!(reader.calls, 4);
        assert_eq!(reader.data, b"!");
    }

    #[test]
    fn read_exact_into_nothing_never_reads() {
        let mut reader = Scripted::new(b"hello", 3, None);
        assert_eq!(
            read_exact(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut reader,
                &mut []
            ),
            Poll::Ready(Ok(()))
        );
        assert_eq!(reader.calls, 0);
    }

    #[test]
    fn read_exact_fails_when_the_stream_ends_early() {
        let mut reader = Scripted::new(b"hel", 2, None);
        let mut buf = [0; 5];
        assert_eq!(
            read_exact(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut reader,
                &mut buf
            ),
            Poll::Ready(Err(ReadExactError::UnexpectedEof))
        );
        assert_eq!(&buf[..3], b"hel");
        assert_eq!(reader.calls, 3);
    }

    #[test]
    fn read_exact_returns_syscall_errors() {
        let mut reader = Scripted::new(b"hello", 3, Some(Errno::CONNRESET));
        let mut buf = [0; 5];
        assert_eq!(
            read_exact(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut reader,
                &mut buf
            ),
            Poll::Ready(Err(ReadExactError::Io(Errno::CONNRESET)))
        );
        assert_eq!(reader.calls, 1);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn read_exact_waits_for_the_rest_from_a_socket() {
        let listener = crate::testing::raw_socket(AddressFamily::INET, SocketType::STREAM);
        let localhost = SocketAddr::from(([127, 0, 0, 1], 0));
        rustix::net::bind(&listener, &localhost).unwrap();
        rustix::net::listen(&listener, 1).unwrap();
        let addr: SocketAddr = rustix::net::getsockname(&listener)
            .unwrap()
            .try_into()
            .unwrap();

        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async move |ctx| {
                let mut stream = TcpStream::connect(&ctx, addr).await.unwrap();
                ctx.dispatch(1);
                let mut buf = [0; 11];
                stream.read_exact(&ctx, &mut buf).await.unwrap();
                assert_eq!(&buf, b"hello world");
                ctx.dispatch(2);
                pending::<()>().await;
            },
        )
        .unwrap();

        let peer = raw_accept(&listener);
        event_loop.run_until_events(1);

        rustix::io::write(&peer, b"hello ").unwrap();
        assert!(is_readable(&event_loop, 1000));
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert!(!is_readable(&event_loop, 0));
        assert_eq!(event_loop.state_mut().events(), [1]);

        rustix::io::write(&peer, b"world").unwrap();
        event_loop.run_until_events(2);
        assert_eq!(event_loop.state_mut().events(), [1, 2]);
    }
}
