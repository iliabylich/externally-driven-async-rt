use core::{mem::MaybeUninit, net::SocketAddr, time::Duration};

use externally_driven_async_rt::{
    Context, Error, EventLoop, State,
    net::{TcpListener, TcpStream},
};

use rustix::{
    event::{PollFd, PollFlags, poll},
    fd::BorrowedFd,
    io::Errno,
};

const MAX_TASKS_COUNT: usize = 1024;
const MAX_TASK_SIZE: usize = 1024;
const MAX_ROOT_TASK_SIZE: usize = 4096;
const MAX_TIMERS_COUNT: usize = 4;
const MAX_IO_COUNT: usize = 1024;

type ServerLoop<'a> = EventLoop<
    'a,
    Server,
    MAX_TASKS_COUNT,
    MAX_TASK_SIZE,
    MAX_ROOT_TASK_SIZE,
    MAX_TIMERS_COUNT,
    MAX_IO_COUNT,
>;
type ServerContext<'a> = Context<
    'a,
    Server,
    MAX_TASKS_COUNT,
    MAX_TASK_SIZE,
    MAX_ROOT_TASK_SIZE,
    MAX_TIMERS_COUNT,
    MAX_IO_COUNT,
>;

const QSIZE: usize = ServerLoop::QUEUE_REQUIRED_SIZE;
const MEMSIZE: usize = ServerLoop::REQUIRED_SIZE;

#[derive(Default)]
struct Server {
    active: usize,
    total: usize,
    bytes: u64,
    log: Vec<String>,
}

enum Event {
    Listening(SocketAddr),
    Connected(SocketAddr),
    Echoed {
        peer: SocketAddr,
        len: usize,
    },
    Disconnected {
        peer: SocketAddr,
        error: Option<Errno>,
    },
    AcceptFailed(Errno),
    Tick,
}

impl State for Server {
    type Event = Event;

    fn apply(&mut self, event: Event) {
        let line = match event {
            Event::Listening(addr) => format!("listening on {addr}"),
            Event::Connected(peer) => {
                self.active += 1;
                self.total += 1;
                format!("{peer} connected")
            }
            Event::Echoed { peer, len } => {
                self.bytes += len as u64;
                format!("{peer}: echoed {len} bytes")
            }
            Event::Disconnected { peer, error } => {
                self.active -= 1;
                match error {
                    Some(errno) => format!("{peer} disconnected: {errno}"),
                    None => format!("{peer} disconnected"),
                }
            }
            Event::AcceptFailed(errno) => format!("accept failed: {errno}"),
            Event::Tick => self.stats(),
        };
        self.log.push(line);
    }
}

impl Server {
    fn stats(&self) -> String {
        format!(
            "{} active, {} total connections, {} bytes echoed",
            self.active, self.total, self.bytes
        )
    }
}

async fn accept_loop(ctx: &ServerContext<'_>, addr: SocketAddr) {
    let mut listener = TcpListener::bind(addr).unwrap();
    ctx.dispatch(Event::Listening(listener.local_addr().unwrap()));
    ctx.spawn(async |ctx| {
        loop {
            ctx.sleep(Duration::from_secs(10)).await;
            ctx.dispatch(Event::Tick);
        }
    });
    loop {
        match listener.accept(ctx).await {
            Ok((stream, peer)) => {
                ctx.dispatch(Event::Connected(peer));
                ctx.spawn(async move |ctx| {
                    let error = echo(ctx, stream, peer).await.err();
                    ctx.dispatch(Event::Disconnected { peer, error });
                });
            }
            Err(errno) => {
                ctx.dispatch(Event::AcceptFailed(errno));
                ctx.sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn echo(
    ctx: &ServerContext<'_>,
    mut stream: TcpStream,
    peer: SocketAddr,
) -> rustix::io::Result<()> {
    let mut buf = [0; 512];
    loop {
        let len = stream.read(ctx, &mut buf).await?;
        if len == 0 {
            return Ok(());
        }
        let mut written = 0;
        while written < len {
            written += stream.write(ctx, &buf[written..len]).await?;
        }
        ctx.dispatch(Event::Echoed { peer, len });
    }
}

enum Command {
    Stats,
    Quit,
    Unknown(String),
}

fn read_commands(
    stdin: BorrowedFd<'_>,
    pending: &mut Vec<u8>,
    commands: &mut Vec<Command>,
) -> rustix::io::Result<()> {
    let mut buf = [0; 1024];
    let len = rustix::io::read(stdin, &mut buf)?;
    if len == 0 {
        commands.push(Command::Quit);
        return Ok(());
    }
    pending.extend_from_slice(&buf[..len]);
    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
        let line: Vec<u8> = pending.drain(..=newline).collect();
        commands.push(match String::from_utf8_lossy(&line).trim() {
            "stats" => Command::Stats,
            "quit" => Command::Quit,
            other => Command::Unknown(other.to_owned()),
        });
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = match std::env::args().nth(1) {
        Some(arg) => arg.parse()?,
        None => "127.0.0.1:7878".parse()?,
    };
    let queue: Box<[_; QSIZE]> = Box::new_uninit_slice(QSIZE).try_into().unwrap();
    let queue = Box::into_raw(queue);
    let mut memory = [MaybeUninit::uninit(); MEMSIZE];
    // SAFETY: `queue` comes from `Box::into_raw`, and is only freed below, after `run` returns.
    let result = run(addr, unsafe { &mut *queue }, &mut memory);
    // SAFETY: `run` has dropped the loop, and with it every task and the state, the only holders
    // of its wakers (nothing sends one to another thread or a global), so nothing can wake into
    // the queue anymore.
    drop(unsafe { Box::from_raw(queue) });
    result
}

fn run(
    addr: SocketAddr,
    queue: &'static mut [MaybeUninit<u8>; QSIZE],
    memory: &mut [MaybeUninit<u8>; MEMSIZE],
) -> Result<(), Box<dyn std::error::Error>> {
    let root = async move |ctx: &ServerContext<'_>| {
        accept_loop(ctx, addr).await;
    };
    let mut event_loop = ServerLoop::new(queue, memory, Server::default(), root)?;
    for line in core::mem::take(&mut event_loop.state_mut().log) {
        println!("{line}");
    }
    println!("commands: stats, quit");
    // SAFETY: fd 0 is stdin, which stays open for the whole process.
    let stdin = unsafe { BorrowedFd::borrow_raw(0) };
    let mut pending = Vec::new();
    let mut commands = Vec::new();

    loop {
        let (loop_ready, stdin_ready) = {
            let mut fds = [
                PollFd::new(&event_loop, PollFlags::IN),
                PollFd::new(&stdin, PollFlags::IN),
            ];
            match poll(&mut fds, None) {
                Ok(_) => {}
                Err(Errno::INTR) => continue,
                Err(errno) => return Err(errno.to_string().into()),
            }
            (!fds[0].revents().is_empty(), !fds[1].revents().is_empty())
        };

        if loop_ready {
            let result = event_loop.drain_events_without_blocking();
            for line in core::mem::take(&mut event_loop.state_mut().log) {
                println!("{line}");
            }
            match result {
                Ok(()) => {}
                Err(Error::RootFinished) => return Ok(()),
                Err(err) => return Err(err.into()),
            }
        }

        if stdin_ready {
            match read_commands(stdin, &mut pending, &mut commands) {
                Ok(()) | Err(Errno::INTR) => {}
                Err(errno) => return Err(errno.to_string().into()),
            }
            for command in commands.drain(..) {
                match command {
                    Command::Stats => println!("{}", event_loop.state_mut().stats()),
                    Command::Quit => return Ok(()),
                    Command::Unknown(command) => println!("unknown command: {command:?}"),
                }
            }
        }
    }
}
