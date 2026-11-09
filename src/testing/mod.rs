mod assert_yields;
mod dispatch_on_drop;
mod driving;
mod drop_flag;
mod fixed_future;
mod log;
mod poll;
mod sockets;
mod wakers;

use crate::{Context, EventLoop, LoopStorage};

pub(crate) use self::{
    assert_yields::assert_yields,
    dispatch_on_drop::DispatchOnDrop,
    driving::{
        drain_until_readable, peer_read_exact, run_until_events, run_until_root_finishes,
        wake_by_writing,
    },
    drop_flag::{DropFlag, flagged_task},
    fixed_future::FixedFuture,
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
