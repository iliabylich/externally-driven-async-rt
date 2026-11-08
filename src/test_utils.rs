use core::{
    cell::Cell,
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    task::{Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::{AsFd, OwnedFd},
    net::{AddressFamily, SocketFlags, SocketType},
};

use crate::{Context, Error, EventLoop, State, io::IoRegistration, selectors::Direction};

pub(crate) const TEST_MAX_TASKS_COUNT: usize = 16;
pub(crate) const TEST_MAX_TASK_SIZE: usize = 1024;
pub(crate) const TEST_MAX_ROOT_TASK_SIZE: usize = 2048;
pub(crate) const TEST_MAX_TIMERS_COUNT: usize = 16;
pub(crate) const TEST_MAX_IO_COUNT: usize = 8;

pub(crate) type TestLoop<S> = EventLoop<
    S,
    TEST_MAX_TASKS_COUNT,
    TEST_MAX_TASK_SIZE,
    TEST_MAX_ROOT_TASK_SIZE,
    TEST_MAX_TIMERS_COUNT,
    TEST_MAX_IO_COUNT,
>;
pub(crate) type TestContext<S = Log> = Context<
    S,
    TEST_MAX_TASKS_COUNT,
    TEST_MAX_TASK_SIZE,
    TEST_MAX_ROOT_TASK_SIZE,
    TEST_MAX_TIMERS_COUNT,
    TEST_MAX_IO_COUNT,
>;

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

pub(crate) fn socketpair() -> (OwnedFd, OwnedFd) {
    let (a, b) = rustix::net::socketpair(
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

pub(crate) fn sleep(duration: Duration) {
    let timeout = Timespec {
        tv_sec: duration.as_secs() as i64,
        tv_nsec: i64::from(duration.subsec_nanos()),
    };
    assert_eq!(poll(&mut [], Some(&timeout)).unwrap(), 0);
}

pub(crate) async fn wait_readable<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    ctx: &Context<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    fd: &OwnedFd,
) {
    let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), Direction::Read);
    while !is_readable(fd, 0) {
        registration.wait().await;
    }
}

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

pub(crate) fn waker(id: usize) -> Waker {
    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(|data| RawWaker::new(data, &VTABLE), |_| {}, |_| {}, |_| {});
    // SAFETY: the vtable never touches the data pointer, and its functions do nothing.
    unsafe { Waker::new(core::ptr::without_provenance(id), &VTABLE) }
}

pub(crate) fn counting_waker(drops: &Cell<u32>) -> Waker {
    static VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| RawWaker::new(data, &VTABLE),
        |_| {},
        |_| {},
        |data| {
            // SAFETY: the data is the `Cell` from `counting_waker`, which every test keeps
            // alive for as long as its wakers.
            let drops = unsafe { &*data.cast::<Cell<u32>>() };
            drops.set(drops.get() + 1);
        },
    );
    // SAFETY: the vtable only reads the data as a `&Cell<u32>`, and `drops` outlives the
    // waker (see above).
    unsafe { Waker::new(core::ptr::from_ref(drops).cast(), &VTABLE) }
}

pub(crate) struct DropFlag(pub(crate) *mut bool);

impl Drop for DropFlag {
    fn drop(&mut self) {
        // SAFETY: points to a `static mut` of the running test, which only that test's thread
        // uses, and no reference to it exists.
        unsafe { *self.0 = true };
    }
}

pub(crate) fn flagged_task(dropped: *mut bool) -> impl Future<Output = ()> {
    let flag = DropFlag(dropped);
    async move {
        let _flag = flag;
    }
}

pub(crate) fn wake_by_writing(event_loop: &impl AsFd, peer: &OwnedFd) {
    rustix::io::write(peer, b"x").unwrap();
    assert!(is_readable(event_loop, 1000));
}

pub(crate) fn run_until_root_finishes<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    event_loop: &mut EventLoop<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
) {
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

pub(crate) fn drain_until_readable<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    event_loop: &mut EventLoop<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    peer: impl AsFd,
) {
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

pub(crate) fn peer_read_exact<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    event_loop: &mut EventLoop<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
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

pub(crate) fn has_timers<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    event_loop: &EventLoop<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
) -> bool {
    event_loop.storage.timers.next_deadline().is_some()
}

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

pub(crate) fn run_until_events<
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    event_loop: &mut EventLoop<
        Log,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    count: usize,
) {
    while event_loop.state_mut().events().len() < count {
        assert!(
            is_readable(&*event_loop, 2000),
            "the loop never became ready"
        );
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    }
}

pub(crate) const TEST_MEMORY: usize = 64 * 1024;

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

macro_rules! test_loop {
    ([$($capacity:expr),+ $(,)?], $state:expr, $f:expr $(,)?) => {
        $crate::test_utils::test_loop_in::<_, $({ $capacity }),+, _>(
            $crate::test_utils::test_memory_within_current_test!(),
            $state,
            $f,
        )
    };
    ($state:expr, $f:expr $(,)?) => {
        $crate::test_utils::test_loop!(
            [
                $crate::test_utils::TEST_MAX_TASKS_COUNT,
                $crate::test_utils::TEST_MAX_TASK_SIZE,
                $crate::test_utils::TEST_MAX_ROOT_TASK_SIZE,
                $crate::test_utils::TEST_MAX_TIMERS_COUNT,
                $crate::test_utils::TEST_MAX_IO_COUNT,
            ],
            $state,
            $f,
        )
    };
}
pub(crate) use test_loop;

pub(crate) fn test_loop_in<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    F,
>(
    memory: &'static mut [MaybeUninit<u8>; TEST_MEMORY],
    state: S,
    f: F,
) -> Result<
    EventLoop<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    Error,
>
where
    F: AsyncFnOnce(
            &Context<
                S,
                MAX_TASKS_COUNT,
                MAX_TASK_SIZE,
                MAX_ROOT_TASK_SIZE,
                MAX_TIMERS_COUNT,
                MAX_IO_COUNT,
            >,
        ) + 'static,
{
    EventLoop::new(memory, state, f)
}
