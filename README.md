## `externally-driven-async-rt`

What is this:

1. an async runtime
2. that is `no_std`
3. and is driven by an external event loop

Why would I need it? Well, the most typical example is building a single shared async library that must be embedded into an external event loop, e.g.

1. `g_source_add_unix_fd` from GIO/GTK
2. `QSocketNotifier` from Qt
3. `MessageQueue.OnFileDescriptorEventListener` on Android
4. `DispatchSource.makeReadSource` on macOS

The whole concept of the library is to define an async closure, wrap it with the runtime provided by this crate, and then pass its file descriptor (because it's epoll/kqueue under the hood) to the outer loop. From there you can subscribe to this epollfd/kqueuefd, and once it's readable you can "poke" the runtime from the outside and make it process accumulated events.

### Where to look

+ [`EventLoop`] - the main type
    + takes an async closure in [`EventLoop::new`]
    + borrows user-defined state (see [`State`]) for its whole lifetime
    + implements [`rustix::fd::AsFd`]
    + can be driven with [`EventLoop::drain_events_without_blocking`]
    + the state can be accessed between drains with [`EventLoop::state_mut`]
+ [`LoopStorage`] - the storage
    + must be static
    + can be used by only one [`EventLoop`] ever, creating a second one with the same storage panics, even after the first one is dropped
+ [`Selector`]
    + a `cfg(target_os)`-selected selector, epoll or kqueue
    + create it with [`Selector::new`] and pass it to [`EventLoop::new`], it must outlive the [`EventLoop`]
    + it's fully managed by [`EventLoop`], you don't need to call anything else on it
+ [`Context`] - a shared handle to interact with the event loop
    + provides methods like [`Context::spawn`], [`Context::sleep`]
    + is an argument of most network related operations like [`net::TcpStream::connect`]
+ [`State`] trait - a way to inject your own state management
    + define your own state and event types, make it implement [`State`] by specifying how it consumes events
    + call [`Context::dispatch`] to trigger state transition
    + between drains read what the tasks produced via [`EventLoop::state_mut`]
+ [`net`] module with some types to work with TCP/UDP/Unix sockets

### Example

```rust,no_run
use core::{cell::RefCell, time::Duration};

use externally_driven_async_rt::{EventLoop, LoopStorage, Selector};
use rustix::event::{PollFd, PollFlags, poll};

const TASKS: usize = 1;
const TASK_SIZE: usize = 64;
const ROOT_TASK_SIZE: usize = 256;
const TIMERS: usize = 1;
const FDS: usize = 1;

type Storage = LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new()?;
    let mut state = RefCell::new(());
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, &mut state, async |ctx| loop {
        ctx.sleep(Duration::from_secs(1)).await;
        println!("hello");
    })?;
    loop {
        poll(&mut [PollFd::new(&event_loop, PollFlags::IN)], None)?;
        event_loop.drain_events_without_blocking()?;
    }
}
```

As you can see, everything has an upper bound:

1. maximum number of co-existing tasks (spawning a new one either panics or gives your a future depending on whether you use `ctx.spawn()` or `ctx.reserve_task().await` API)
2. maximum size of the future of the task (exceeding it panics at runtime)
3. maximum size of the root task (stored separately, it's usually bigger than sub-tasks, exceeding it panics at runtime)
4. maximum number of co-existing timers (`ctx.sleep(<duration>)` or `ctx.timeout()` API, exceeding it panics at runtime)
5. maximum number of co-existing file descriptors that runtime operates on (exceeding it panics at runtime)
