mod assert_yields;
mod dispatch_on_drop;
mod event_loop_testing_ext;
mod fixed_future;
mod flag_on_drop;
mod log;
mod poll;
mod sockets;
mod wakers;

use crate::{Context, EventLoop, LoopStorage};

pub(crate) use self::{
    assert_yields::assert_yields,
    dispatch_on_drop::DispatchOnDrop,
    event_loop_testing_ext::EventLoopTestingExt,
    fixed_future::FixedFuture,
    flag_on_drop::FlagOnDrop,
    log::Log,
    poll::{is_readable, is_writable, sleep_blocking, wait_readable},
    sockets::{raw_accept, raw_socket, socketpair},
    wakers::counting_waker,
};

pub(crate) const TEST_MAX_TASKS_COUNT: usize = 16;
pub(crate) const TEST_MAX_TASK_SIZE: usize = 1024;
pub(crate) const TEST_MAX_ROOT_TASK_SIZE: usize = 2048;
pub(crate) const TEST_MAX_TIMERS_COUNT: usize = 16;
pub(crate) const TEST_MAX_IO_COUNT: usize = 8;

pub(crate) type TestStorage<S = Log> = LoopStorage<
    S,
    TEST_MAX_TASKS_COUNT,
    TEST_MAX_TASK_SIZE,
    TEST_MAX_ROOT_TASK_SIZE,
    TEST_MAX_TIMERS_COUNT,
    TEST_MAX_IO_COUNT,
>;

pub(crate) type TestLoop<'a, S> = EventLoop<
    'a,
    S,
    TEST_MAX_TASKS_COUNT,
    TEST_MAX_TASK_SIZE,
    TEST_MAX_ROOT_TASK_SIZE,
    TEST_MAX_TIMERS_COUNT,
    TEST_MAX_IO_COUNT,
>;
pub(crate) type TestContext<'a, S = Log> = Context<
    'a,
    S,
    TEST_MAX_TASKS_COUNT,
    TEST_MAX_TASK_SIZE,
    TEST_MAX_ROOT_TASK_SIZE,
    TEST_MAX_TIMERS_COUNT,
    TEST_MAX_IO_COUNT,
>;
