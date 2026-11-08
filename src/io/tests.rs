use core::{
    future::{pending, poll_fn},
    mem::MaybeUninit,
    pin::pin,
    task::Poll,
    time::Duration,
};

use rustix::{
    fd::{AsFd, OwnedFd},
    io::Errno,
};

use crate::{
    Context, EventLoop, State,
    io::IoRegistration,
    selectors::Direction,
    test_utils::{
        First, Log, TEST_MAX_ROOT_TASK_SIZE, TEST_MAX_TASK_SIZE, TEST_MAX_TASKS_COUNT,
        TEST_MAX_TIMERS_COUNT, TEST_MEMORY, TestContext, TestLoop, TestQueue, Winner, is_readable,
        run_until_root_finishes, socketpair,
    },
};

async fn read_some<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    ctx: &Context<
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    fd: &OwnedFd,
) -> rustix::io::Result<usize> {
    let mut buf = [0; 64];
    let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), Direction::Read);
    loop {
        match rustix::io::read(fd, &mut buf) {
            Err(Errno::AGAIN) => registration.wait().await,
            result => return result,
        }
    }
}

async fn write_some(ctx: &TestContext<'_>, fd: &OwnedFd) -> rustix::io::Result<usize> {
    let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), Direction::Write);
    loop {
        match rustix::io::write(fd, b"x") {
            Err(Errno::AGAIN) => registration.wait().await,
            result => return result,
        }
    }
}

fn fill_send_buffer(fd: &OwnedFd) {
    while rustix::io::write(fd, &[0; 4096]).is_ok() {}
}

fn drain_receive_buffer(fd: &OwnedFd) {
    let mut buf = [0; 4096];
    while rustix::io::read(fd, &mut buf).is_ok() {}
}

fn registrations(event_loop: &TestLoop<Log>) -> usize {
    event_loop.storage.io_waiters.count()
}

#[test]
fn read_waits_until_the_peer_writes() {
    let (a, b) = socketpair();
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let mut event_loop = TestLoop::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async move |ctx| {
            let n = read_some(ctx, &a).await.unwrap();
            ctx.dispatch(n as u32);
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(registrations(&event_loop), 1);
    assert!(!is_readable(&event_loop, 0));

    rustix::io::write(&b, b"abc").unwrap();
    assert!(is_readable(&event_loop, 1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_eq!(event_loop.state_mut().events(), [3]);
    assert_eq!(registrations(&event_loop), 0);
}

#[test]
fn write_waits_on_a_full_buffer() {
    let (a, b) = socketpair();
    fill_send_buffer(&a);
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let mut event_loop = TestLoop::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async move |ctx| {
            let n = write_some(ctx, &a).await.unwrap();
            ctx.dispatch(n as u32);
        },
    )
    .unwrap();
    assert!(event_loop.state_mut().events().is_empty());
    assert!(!is_readable(&event_loop, 0));

    drain_receive_buffer(&b);
    run_until_root_finishes(&mut event_loop);
    assert_eq!(event_loop.state_mut().events(), [1]);
}

#[test]
fn ready_fd_arms_nothing() {
    let (a, b) = socketpair();
    rustix::io::write(&b, b"abc").unwrap();
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let mut event_loop = TestLoop::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async move |ctx| {
            let n = read_some(ctx, &a).await.unwrap();
            ctx.dispatch(n as u32);
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(event_loop.state_mut().events(), [3]);
    assert_eq!(registrations(&event_loop), 0);
    rustix::io::write(&b, b"abc").unwrap();
    assert!(!is_readable(&event_loop, 0));
}

#[test]
fn dropping_a_pending_read_deregisters() {
    let (a, b) = socketpair();
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let mut event_loop = TestLoop::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async move |ctx| {
            let winner = First(
                pin!(read_some(ctx, &a)),
                ctx.sleep(Duration::from_millis(10)),
            )
            .await;
            assert_eq!(winner, Winner::B);
            ctx.dispatch(1);
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(registrations(&event_loop), 1);
    while event_loop.state_mut().events().is_empty() {
        assert!(is_readable(&event_loop, 2000));
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    }
    assert_eq!(registrations(&event_loop), 0);
    rustix::io::write(&b, b"abc").unwrap();
    assert!(!is_readable(&event_loop, 0));
}

#[test]
fn read_survives_a_spurious_wake() {
    let (a, b) = socketpair();
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let mut event_loop = TestLoop::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async move |ctx| {
            let mut woken = false;
            let wake_once = poll_fn(move |cx| {
                if !woken {
                    woken = true;
                    cx.waker().wake_by_ref();
                }
                Poll::<()>::Pending
            });
            let winner = First(pin!(read_some(ctx, &a)), wake_once).await;
            assert_eq!(winner, Winner::A);
            ctx.dispatch(1);
            pending::<()>().await;
        },
    )
    .unwrap();
    assert!(event_loop.state_mut().events().is_empty());
    assert_eq!(registrations(&event_loop), 1);

    rustix::io::write(&b, b"abc").unwrap();
    assert!(is_readable(&event_loop, 1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_eq!(event_loop.state_mut().events(), [1]);
}

#[test]
fn closed_and_reused_fd_number_does_not_affect_the_new_socket() {
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let mut event_loop = TestLoop::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async |ctx| {
            let (a, _b) = socketpair();
            let winner = First(
                pin!(read_some(ctx, &a)),
                ctx.sleep(Duration::from_millis(5)),
            )
            .await;
            assert_eq!(winner, Winner::B);
            drop(a);

            let (c, d) = socketpair();
            ctx.spawn(async move |ctx| {
                ctx.sleep(Duration::from_millis(5)).await;
                rustix::io::write(&d, b"abc").unwrap();
            });
            let n = read_some(ctx, &c).await.unwrap();
            ctx.dispatch(n as u32);
        },
    )
    .unwrap();
    run_until_root_finishes(&mut event_loop);
    assert_eq!(event_loop.state_mut().events(), [3]);
}

#[test]
#[should_panic(expected = "more than 1 concurrent IO operations")]
fn exceeding_max_io_count_panics() {
    let (a, _b) = socketpair();
    let (c, _d) = socketpair();
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let _ = EventLoop::<
        '_,
        _,
        TEST_MAX_TASKS_COUNT,
        TEST_MAX_TASK_SIZE,
        TEST_MAX_ROOT_TASK_SIZE,
        TEST_MAX_TIMERS_COUNT,
        1,
    >::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async move |ctx| {
            First(pin!(read_some(ctx, &a)), pin!(read_some(ctx, &c))).await;
        },
    );
}

#[test]
fn sequential_io_reuses_the_slot() {
    let (a, b) = socketpair();
    let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
    let mut event_loop = EventLoop::<
        '_,
        _,
        TEST_MAX_TASKS_COUNT,
        TEST_MAX_TASK_SIZE,
        TEST_MAX_ROOT_TASK_SIZE,
        TEST_MAX_TIMERS_COUNT,
        1,
    >::new(
        {
            static QUEUE: TestQueue = TestQueue::new();
            &QUEUE
        },
        &mut memory,
        Log::default(),
        async move |ctx| {
            for _ in 0..3 {
                let len = read_some(ctx, &a).await.unwrap();
                ctx.dispatch(len as u32);
            }
            pending::<()>().await;
        },
    )
    .unwrap();
    for i in 1..=3 {
        rustix::io::write(&b, &[0; 3][..i]).unwrap();
        while event_loop.state_mut().events().len() < i {
            assert!(is_readable(&event_loop, 2000));
            assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        }
    }
    assert_eq!(event_loop.state_mut().events(), [1, 2, 3]);
    assert_eq!(event_loop.storage.io_waiters.count(), 0);
}
