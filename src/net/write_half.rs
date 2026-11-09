use rustix::net::Shutdown;

use crate::{context::Context, net::socket::Socket, state::State};

pub struct WriteHalf {
    socket: Socket,
}

impl WriteHalf {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }

    pub async fn write<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &mut self,
        ctx: &Context<
            '_,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &[u8],
    ) -> rustix::io::Result<usize> {
        self.socket.write(ctx, buf).await
    }

    pub fn shutdown(&self) -> rustix::io::Result<()> {
        self.socket.shutdown(Shutdown::Write)
    }
}

impl Drop for WriteHalf {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
