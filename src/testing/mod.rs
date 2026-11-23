mod assert_yields;
mod dispatch_on_drop;
mod event_loop_testing_ext;
mod fd_testing_ext;
mod fixed_future;
mod flag_on_drop;
mod poll;
mod sockets;
mod spawn;
mod wakers;

use crate::{Context, EventLoop, LoopStorage};

pub(crate) use self::{
    assert_yields::assert_yields,
    dispatch_on_drop::DispatchOnDrop,
    event_loop_testing_ext::EventLoopTestingExt,
    fd_testing_ext::FdTestingExt,
    fixed_future::FixedFuture,
    flag_on_drop::FlagOnDrop,
    poll::{fake_instant, wake_expired_timers},
    sockets::{raw_accept, raw_socket, socketpair},
    spawn::{claimed_forever, spawn_reserved_static, spawn_root_static, spawn_static},
    wakers::counting_waker,
};

pub(crate) const TEST_EVENTS: usize = 16;
pub(crate) const TEST_TASKS: usize = 16;
pub(crate) const TEST_TASK_SIZE: usize = 1024;
const TEST_ROOT_TASK_SIZE: usize = 2048;
pub(crate) const TEST_TIMERS: usize = 16;
pub(crate) const TEST_FDS: usize = 8;

pub(crate) type TestStorage<E = u32> = LoopStorage<
    E,
    TEST_EVENTS,
    TEST_TASKS,
    TEST_TASK_SIZE,
    TEST_ROOT_TASK_SIZE,
    TEST_TIMERS,
    TEST_FDS,
>;

pub(crate) type TasksStorage<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
> = LoopStorage<(), 1, TASKS, TASK_SIZE, ROOT_TASK_SIZE, 1, 1>;

pub(crate) type TestLoop<'a, E = u32> = EventLoop<
    'a,
    E,
    TEST_EVENTS,
    TEST_TASKS,
    TEST_TASK_SIZE,
    TEST_ROOT_TASK_SIZE,
    TEST_TIMERS,
    TEST_FDS,
>;
pub(crate) type TestContext<'a, E = u32> = Context<
    'a,
    E,
    TEST_EVENTS,
    TEST_TASKS,
    TEST_TASK_SIZE,
    TEST_ROOT_TASK_SIZE,
    TEST_TIMERS,
    TEST_FDS,
>;
