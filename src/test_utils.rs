use core::alloc::Layout;
use core::future::Future;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::Poll;
use core::time::Duration;

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fd::{AsFd, OwnedFd};
use rustix::net::{AddressFamily, SocketFlags, SocketType, socketpair};

use crate::io::IoRegistration;
use crate::ready::ReadyQueue;
use crate::selectors::Direction;
use crate::storage::Storage;
use crate::{Capacities, Context, Error, EventLoop, State};

pub(crate) const TEST_CAPACITIES: Capacities = Capacities {
    max_tasks: 16,
    max_task_size: 1024,
    max_timers: 16,
    max_io: 8,
};

pub(crate) type TestContext = Context<Log>;

#[derive(Default)]
pub(crate) struct Log {
    events: [u32; 32],
    len: usize,
}

impl Log {
    pub(crate) fn events(&self) -> &[u32] {
        &self.events[..self.len]
    }

    pub(crate) fn events_mut(&mut self) -> &mut [u32] {
        &mut self.events[..self.len]
    }
}

impl State for Log {
    type Event = u32;

    fn apply(&mut self, event: u32) {
        assert!(self.len < self.events.len(), "the test Log is full");
        self.events[self.len] = event;
        self.len += 1;
    }
}

pub(crate) fn pair() -> (OwnedFd, OwnedFd) {
    let (a, b) = socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::empty(),
        None,
    )
    .unwrap();
    rustix::io::ioctl_fionbio(&a, true).unwrap();
    rustix::io::ioctl_fionbio(&b, true).unwrap();
    (a, b)
}

pub(crate) fn is_readable(fd: impl AsFd, timeout_ms: i64) -> bool {
    is_ready(fd, PollFlags::IN, timeout_ms)
}

pub(crate) fn is_writable(fd: impl AsFd, timeout_ms: i64) -> bool {
    is_ready(fd, PollFlags::OUT, timeout_ms)
}

fn is_ready(fd: impl AsFd, flags: PollFlags, timeout_ms: i64) -> bool {
    let mut fds = [PollFd::new(&fd, flags)];
    let timeout = Timespec {
        tv_sec: timeout_ms / 1000,
        tv_nsec: timeout_ms % 1000 * 1_000_000,
    };
    poll(&mut fds, Some(&timeout)).unwrap() == 1
}

// A blocking sleep: `poll` on no fds.
pub(crate) fn sleep(duration: Duration) {
    let timeout = Timespec {
        tv_sec: duration.as_secs() as i64,
        tv_nsec: i64::from(duration.subsec_nanos()),
    };
    assert_eq!(poll(&mut [], Some(&timeout)).unwrap(), 0);
}

pub(crate) async fn readable(ctx: &TestContext, fd: &OwnedFd) {
    let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), Direction::Read);
    while !is_readable(fd, 0) {
        registration.wait().await;
    }
}

// Like `assert_eq!` on two collections, without collecting: reports the first item that differs.
#[track_caller]
pub(crate) fn assert_yields<T: PartialEq + core::fmt::Debug>(
    actual: impl IntoIterator<Item = T>,
    expected: impl IntoIterator<Item = T>,
) {
    let (mut actual, mut expected) = (actual.into_iter(), expected.into_iter());
    for i in 0.. {
        match (actual.next(), expected.next()) {
            (None, None) => return,
            (actual, expected) => assert_eq!(actual, expected, "item {i}"),
        }
    }
}

pub(crate) fn wake_by_writing(event_loop: &impl AsFd, peer: &OwnedFd) {
    rustix::io::write(peer, b"x").unwrap();
    assert!(is_readable(event_loop, 1000));
}

pub(crate) fn run_until_root_finishes<S: State>(event_loop: &mut EventLoop<S>) {
    loop {
        assert!(
            is_readable(&*event_loop, 2000),
            "the loop never became ready"
        );
        match event_loop.drain_events_without_blocking() {
            Ok(()) => {}
            Err(Error::RootFinished) => return,
            Err(err) => panic!("{err}"),
        }
    }
}

// Raw nonblocking rustix sockets for the other end of socket tests: an implementation independent
// of ours.
pub(crate) fn raw_socket(family: AddressFamily, ty: SocketType) -> OwnedFd {
    let fd = rustix::net::socket(family, ty, None).unwrap();
    rustix::io::ioctl_fionbio(&fd, true).unwrap();
    fd
}

pub(crate) fn raw_accept(listener: &OwnedFd) -> OwnedFd {
    assert!(is_readable(listener, 2000), "nobody connected");
    let fd = rustix::net::accept(listener).unwrap();
    rustix::io::ioctl_fionbio(&fd, true).unwrap();
    fd
}

// For a peer outside the loop: drains the loop until `peer` has something to read. Waits on both
// at once: data the loop wrote may reach the peer later (loopback delivery can be deferred).
pub(crate) fn drain_until_readable<S: State>(event_loop: &mut EventLoop<S>, peer: impl AsFd) {
    loop {
        let mut fds = [
            PollFd::new(&peer, PollFlags::IN),
            PollFd::new(&*event_loop, PollFlags::IN),
        ];
        let timeout = Timespec {
            tv_sec: 2,
            tv_nsec: 0,
        };
        assert_ne!(
            poll(&mut fds, Some(&timeout)).unwrap(),
            0,
            "neither the peer nor the loop became ready"
        );
        if !fds[0].revents().is_empty() {
            return;
        }
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    }
}

// For a nonblocking peer outside the loop: fills `buf` from `peer`, draining the loop meanwhile.
pub(crate) fn peer_read_exact<S: State>(
    event_loop: &mut EventLoop<S>,
    peer: impl AsFd,
    buf: &mut [u8],
) {
    let mut filled = 0;
    while filled < buf.len() {
        drain_until_readable(event_loop, &peer);
        let len = rustix::io::read(&peer, &mut buf[filled..]).unwrap();
        assert_ne!(len, 0, "the peer's socket was closed early");
        filled += len;
    }
}

pub(crate) fn has_timers<S: State>(event_loop: &EventLoop<S>) -> bool {
    event_loop.storage().next_timer_deadline().is_some()
}

// Resolves with whichever future completes first, dropping the other one.
pub(crate) struct First<A, B>(pub(crate) A, pub(crate) B);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Winner {
    A,
    B,
}

impl<A: Future + Unpin, B: Future + Unpin> Future for First<A, B> {
    type Output = Winner;

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Winner> {
        if Pin::new(&mut self.0).poll(cx).is_ready() {
            return Poll::Ready(Winner::A);
        }
        if Pin::new(&mut self.1).poll(cx).is_ready() {
            return Poll::Ready(Winner::B);
        }
        Poll::Pending
    }
}

pub(crate) fn run_until_events(event_loop: &mut EventLoop<Log>, count: usize) {
    while event_loop.state_mut().events().len() < count {
        assert!(
            is_readable(&*event_loop, 2000),
            "the loop never became ready"
        );
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    }
}

pub(crate) const TEST_MEMORY: usize = 64 * 1024;

// `'static` memory for an `EventLoop`. Each expansion has its own `static mut`, so it must only
// be expanded directly in a `#[test]` function (not in a helper or a loop): then it runs once, on
// that test's thread.
macro_rules! test_memory_within_current_test {
    () => {{
        static mut MEMORY: [core::mem::MaybeUninit<u8>; $crate::test_utils::TEST_MEMORY] =
            [core::mem::MaybeUninit::uninit(); $crate::test_utils::TEST_MEMORY];
        // SAFETY: expanded directly in a test, so this runs once, on that test's thread: this is
        // the only reference to `MEMORY` ever created.
        unsafe { &mut *&raw mut MEMORY }
    }};
}
pub(crate) use test_memory_within_current_test;

// An `EventLoop` in its call site's own `test_memory_within_current_test!`, so the same rule
// applies: expand it directly in a `#[test]` function.
macro_rules! test_loop {
    ($($arg:expr),+ $(,)?) => {
        $crate::test_utils::test_loop_in($crate::test_utils::test_memory_within_current_test!(), $($arg),+)
    };
}
pub(crate) use test_loop;

// An `EventLoop` in exactly `required_size` bytes of `memory`.
pub(crate) fn test_loop_in<S, F>(
    memory: &'static mut [MaybeUninit<u8>],
    state: S,
    capacities: Capacities,
    f: F,
) -> Result<EventLoop<S>, Error>
where
    S: State,
    F: AsyncFnOnce(&Context<S>) + 'static,
{
    let size = EventLoop::<S>::required_size(capacities, &f);
    assert!(
        size <= memory.len(),
        "TEST_MEMORY is too small: {size} bytes needed"
    );
    let (memory, _) = memory.split_at_mut(size);
    EventLoop::new(memory, state, capacities, f)
}

pub(crate) const STORAGE_MEMORY: usize = 16 * 1024;

pub(crate) fn stack_memory() -> [MaybeUninit<u8>; STORAGE_MEMORY] {
    [MaybeUninit::uninit(); STORAGE_MEMORY]
}

// The first address in `memory` aligned for `layout`, with room for it.
pub(crate) fn aligned_in(memory: &mut [MaybeUninit<u8>], layout: Layout) -> NonNull<u8> {
    let start = memory.as_ptr().align_offset(layout.align());
    assert!(
        start + layout.size() <= memory.len(),
        "STORAGE_MEMORY is too small"
    );
    // The whole tail, not just its first byte: the pointer must be valid for all of `layout`.
    NonNull::from(&mut memory[start..]).cast()
}

// A `Storage` without a selector (so no kqueue: usable under Miri) in borrowed memory, destroyed
// on drop. It starts one byte into the memory, so the storage has to align its start.
pub(crate) struct TestStorage<'m> {
    storage: Storage<()>,
    _memory: PhantomData<&'m mut [MaybeUninit<u8>]>,
}

impl<'m> TestStorage<'m> {
    pub(crate) fn new(
        memory: &'m mut [MaybeUninit<u8>],
        max_tasks_count: usize,
        max_timers_count: usize,
        max_io_count: usize,
    ) -> Self {
        Self::with_capacities(
            memory,
            Capacities {
                max_tasks: max_tasks_count,
                max_task_size: TEST_CAPACITIES.max_task_size,
                max_timers: max_timers_count,
                max_io: max_io_count,
            },
        )
    }

    pub(crate) fn with_capacities(
        memory: &'m mut [MaybeUninit<u8>],
        capacities: Capacities,
    ) -> Self {
        let root_layout = Layout::new::<()>();
        // SAFETY: `memory` is borrowed for as long as the storage exists (it's destroyed on drop).
        let storage = unsafe {
            Storage::new(
                NonNull::from(&mut memory[1..]),
                (),
                (),
                capacities,
                root_layout,
            )
        };
        Self {
            storage,
            _memory: PhantomData,
        }
    }

    pub(crate) fn storage(&self) -> Storage<()> {
        self.storage
    }

    pub(crate) fn ready(&self) -> &ReadyQueue {
        self.storage.ready()
    }
}

impl Drop for TestStorage<'_> {
    fn drop(&mut self) {
        // SAFETY: created with the state `()`, destroyed once, with no task being polled.
        unsafe { self.storage.destroy::<()>() };
    }
}
