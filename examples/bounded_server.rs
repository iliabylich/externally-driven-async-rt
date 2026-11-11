#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]

use core::{cell::RefCell, net::SocketAddr, time::Duration};

use externally_driven_async_rt::{
    Context, Error, EventLoop, LoopStorage, Selector, State,
    net::{TcpListener, TcpStream},
};

use rustix::{
    event::{PollFd, PollFlags, poll},
    io::Errno,
};

const MAX_CLIENTS: usize = 3;
const MAX_REQUEST_LEN: usize = 64;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

const TASKS: usize = MAX_CLIENTS + 2;
const TASK_SIZE: usize = 1024;
const ROOT_TASK_SIZE: usize = 1024;
const TIMERS: usize = MAX_CLIENTS + 2;
const FDS: usize = MAX_CLIENTS + 1;

type ServerLoop<'a> = EventLoop<'a, Server, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;
type ServerContext<'a> = Context<'a, Server, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;
type ServerStorage = LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;

#[derive(Default)]
struct Server {
    active: usize,
    served: usize,
    kicked: usize,
    log: Vec<String>,
}

enum Event {
    Listening(SocketAddr),
    Connected(SocketAddr),
    Served {
        peer: SocketAddr,
        len: usize,
    },
    TimedOut(SocketAddr),
    RequestTooLong(SocketAddr),
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
            Event::Listening(addr) => format!(
                "listening on {addr}, at most {MAX_CLIENTS} clients, \
                 {REQUEST_TIMEOUT:?} per request"
            ),
            Event::Connected(peer) => {
                self.active += 1;
                format!("{peer} connected")
            }
            Event::Served { peer, len } => {
                self.served += 1;
                format!("{peer}: served {len} bytes")
            }
            Event::TimedOut(peer) => {
                self.active -= 1;
                self.kicked += 1;
                format!("{peer} kicked out: no full exchange within {REQUEST_TIMEOUT:?}")
            }
            Event::RequestTooLong(peer) => {
                self.active -= 1;
                self.kicked += 1;
                format!("{peer} kicked out: request longer than {MAX_REQUEST_LEN} bytes")
            }
            Event::Disconnected { peer, error } => {
                self.active -= 1;
                match error {
                    Some(errno) => format!("{peer} disconnected: {errno}"),
                    None => format!("{peer} disconnected"),
                }
            }
            Event::AcceptFailed(errno) => format!("accept failed: {errno}"),
            Event::Tick => format!(
                "{} active, {} requests served, {} clients kicked out",
                self.active, self.served, self.kicked
            ),
        };
        self.log.push(line);
    }
}

enum Exchange {
    Served(usize),
    TooLong,
    Closed,
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
        let task = ctx.reserve_task().await;
        let (stream, peer) = loop {
            match listener.accept(ctx).await {
                Ok(client) => break client,
                Err(errno) => {
                    ctx.dispatch(Event::AcceptFailed(errno));
                    ctx.sleep(Duration::from_millis(100)).await;
                }
            }
        };
        ctx.dispatch(Event::Connected(peer));
        task.spawn(async move |ctx| {
            let event = serve(&ctx, stream, peer).await;
            ctx.dispatch(event);
        });
    }
}

async fn serve(ctx: &ServerContext<'_>, mut stream: TcpStream, peer: SocketAddr) -> Event {
    let mut buf = [0; MAX_REQUEST_LEN];
    let mut len = 0;
    loop {
        let exchange = exchange(ctx, &mut stream, &mut buf, &mut len);
        match ctx.timeout(REQUEST_TIMEOUT, exchange).await {
            Some(Ok(Exchange::Served(len))) => ctx.dispatch(Event::Served { peer, len }),
            Some(Ok(Exchange::TooLong)) => return Event::RequestTooLong(peer),
            Some(Ok(Exchange::Closed)) => return Event::Disconnected { peer, error: None },
            Some(Err(errno)) => {
                return Event::Disconnected {
                    peer,
                    error: Some(errno),
                };
            }
            None => return Event::TimedOut(peer),
        }
    }
}

async fn exchange(
    ctx: &ServerContext<'_>,
    stream: &mut TcpStream,
    buf: &mut [u8; MAX_REQUEST_LEN],
    len: &mut usize,
) -> rustix::io::Result<Exchange> {
    let end = loop {
        if let Some(newline) = buf[..*len].iter().position(|byte| *byte == b'\n') {
            break newline + 1;
        }
        if *len == buf.len() {
            return Ok(Exchange::TooLong);
        }
        let read = stream.read(ctx, &mut buf[*len..]).await?;
        if read == 0 {
            return Ok(Exchange::Closed);
        }
        *len += read;
    };
    let mut written = 0;
    while written < end {
        written += stream.write(ctx, &buf[written..end]).await?;
    }
    buf.copy_within(end..*len, 0);
    *len -= end;
    Ok(Exchange::Served(end))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = match std::env::args().nth(1) {
        Some(arg) => arg.parse()?,
        None => "127.0.0.1:7879".parse()?,
    };
    let root = async move |ctx: ServerContext<'_>| {
        accept_loop(&ctx, addr).await;
    };
    static STORAGE: ServerStorage = ServerStorage::new();
    let mut selector = Selector::new()?;
    let mut state = RefCell::new(Server::default());
    let mut event_loop = ServerLoop::new(&STORAGE, &mut selector, &mut state, root)?;
    loop {
        for line in core::mem::take(&mut event_loop.state_mut().log) {
            println!("{line}");
        }
        match poll(&mut [PollFd::new(&event_loop, PollFlags::IN)], None) {
            Ok(_) | Err(Errno::INTR) => {}
            Err(errno) => return Err(errno.to_string().into()),
        }
        match event_loop.drain_events_without_blocking() {
            Ok(()) => {}
            Err(Error::RootFinished) => return Ok(()),
            Err(err) => return Err(err.into()),
        }
    }
}
