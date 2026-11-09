use crate::{context::Context, net::socket::Socket, state::State};

pub struct ReadHalf {
    socket: Socket,
}

impl ReadHalf {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }

    pub fn read<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a mut self,
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &'a mut [u8],
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.read(ctx, buf)
    }
}
