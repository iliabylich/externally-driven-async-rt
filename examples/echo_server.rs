#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::indexing_slicing
)]

use core::{net::SocketAddr, time::Duration};
use std::os::fd::AsFd;

use externally_driven_async_rt::{
    Context, Error, EventLoop, LoopStorage, Selector,
    io::{AsyncRead, AsyncWriteExt, WriteAllError},
    net::{TcpListener, TcpStream},
};

use rustix::{
    event::{PollFd, PollFlags, poll},
    fd::BorrowedFd,
    io::Errno,
};

const EVENTS: usize = 256;
const TASKS: usize = 5;
const TASK_SIZE: usize = 1024;
const ROOT_TASK_SIZE: usize = 4096;
const TIMERS: usize = 4;
const FDS: usize = 5;

type ServerLoop<'a> = EventLoop<'a, Event, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;
type ServerContext<'a> = Context<'a, Event, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;

type ServerStorage = LoopStorage<Event, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;

#[derive(Default)]
struct Server {
    active: usize,
    total: usize,
    bytes: u64,
}

#[derive(Clone, Copy)]
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

impl Server {
    fn on_event(&mut self, event: Event) {
        match event {
            Event::Listening(addr) => println!("listening on {addr}"),
            Event::Connected(peer) => {
                self.active += 1;
                self.total += 1;
                println!("{peer} connected");
            }
            Event::Echoed { peer, len } => {
                self.bytes += len as u64;
                println!("{peer}: echoed {len} bytes");
            }
            Event::Disconnected { peer, error } => {
                self.active -= 1;
                match error {
                    Some(errno) => println!("{peer} disconnected: {errno}"),
                    None => println!("{peer} disconnected"),
                }
            }
            Event::AcceptFailed(errno) => println!("accept failed: {errno}"),
            Event::Tick => self.print_stats(),
        }
    }

    fn print_stats(&self) {
        println!(
            "{} active, {} total connections, {} bytes echoed",
            self.active, self.total, self.bytes
        );
    }
}

async fn accept_loop(ctx: &ServerContext<'_>, addr: SocketAddr) {
    let mut listener = TcpListener::bind(addr).unwrap();
    ctx.dispatch(Event::Listening(listener.local_addr().unwrap()));
    let _ = ctx.spawn(async |ctx| {
        loop {
            ctx.sleep(Duration::from_secs(10)).await;
            ctx.dispatch(Event::Tick);
        }
    });
    loop {
        match listener.accept(ctx).await {
            Ok((stream, peer)) => {
                ctx.dispatch(Event::Connected(peer));
                let _ = ctx.spawn(async move |ctx| {
                    let error = echo(&ctx, stream, peer).await.err();
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
        match stream.write_all(ctx, &buf[..len]).await {
            Ok(()) => {}
            Err(WriteAllError::Io(errno)) => return Err(errno),
            Err(WriteAllError::WriteZero) => return Ok(()),
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
    let root = async move |ctx: ServerContext<'_>| {
        accept_loop(&ctx, addr).await;
    };
    static STORAGE: ServerStorage = ServerStorage::new();
    let mut selector = Selector::new()?;
    let mut server = Server::default();
    let mut event_loop = ServerLoop::new(&STORAGE, &mut selector, root)?;
    for event in event_loop.events() {
        server.on_event(event);
    }
    println!("commands: stats, quit");
    let stdin = std::io::stdin();
    let stdin = stdin.as_fd();
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
            for event in event_loop.events() {
                server.on_event(event);
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
                    Command::Stats => server.print_stats(),
                    Command::Quit => return Ok(()),
                    Command::Unknown(command) => println!("unknown command: {command:?}"),
                }
            }
        }
    }
}
