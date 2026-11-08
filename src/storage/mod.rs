use core::{
    alloc::Layout,
    cell::{Cell, UnsafeCell},
    mem::MaybeUninit,
    ptr::NonNull,
    task::Waker,
};

use crate::{
    capacities::Capacities,
    instant::Instant,
    selectors::{Direction, EVENTS_PER_CALL, Selector, SelectorEvent},
};

pub(crate) use io_waiters::{IoIndex, IoWaiters};
use layout::StorageLayout;
pub(crate) use tasks::{Root, RootStatus, TaskId, Tasks};
pub(crate) use timers::{TimerIndex, Timers};

// `clone_from`, so storing a waker that wakes the same task as the stored one doesn't clone it.
fn store_waker(cell: &Cell<Option<Waker>>, waker: &Waker) {
    let stored = match cell.take() {
        Some(mut stored) => {
            stored.clone_from(waker);
            stored
        }
        None => waker.clone(),
    };
    cell.set(Some(stored));
}

// The runtime: one block of caller-provided memory that never moves, with this struct at its
// start, holding the inline values and a reference to every other part of the allocation (the
// state, each pool, the ready queue, the task slab, the root region). Reached through a
// `&'static Storage` (in the `EventLoop` and every `Context`) and borrows of it. Objects inside
// are addressed by typed indices.
pub(crate) struct Storage<S: 'static, Sel = Selector> {
    pub(crate) selector: Sel,
    // `'static`: they live as long as the storage, which nothing that can reach it outlives.
    // Only shared references: the state is in an `UnsafeCell`.
    pub(crate) state: &'static UnsafeCell<S>,
    pub(crate) timers: Timers,
    pub(crate) io_waiters: IoWaiters,
    pub(crate) tasks: Tasks,
}

impl<S: 'static, Sel: 'static> Storage<S, Sel> {
    /// The size of the memory `new` needs: the storage's layout plus slack for aligning its start,
    /// so memory of this size works wherever it starts.
    pub(crate) fn required_size(capacities: Capacities, root_layout: Layout) -> usize {
        StorageLayout::required_size::<S, Sel>(capacities, root_layout)
    }

    /// Builds the storage inside `memory`, at its first suitably aligned address. Panics if
    /// `memory` is smaller than needed (`required_size` is always enough).
    ///
    /// # Safety
    ///
    /// `memory` must be valid for reads and writes and not be used by anything else until
    /// `destroy` returns.
    pub(crate) unsafe fn new(
        memory: NonNull<[MaybeUninit<u8>]>,
        selector: Sel,
        state: S,
        capacities: Capacities,
        root_layout: Layout,
    ) -> &'static Self {
        let StorageLayout {
            base_ptr,
            state_ptr,
            timers_ptr,
            io_waiters_ptr,
            tasks_ptr,
            ready_ptr,
            task_slab_ptr,
            root_region_ptr,
        } = StorageLayout::new::<S, Sel>(capacities, root_layout, memory);
        // SAFETY: `StorageLayout::new` put every part inside `memory` (it checked the size),
        // aligned for it and not overlapping the others; `memory` is valid and unused (guaranteed
        // by the caller). Each part is written exactly once, the `Storage` struct last. The parts
        // live until `destroy`, which nothing holding the storage outlives, and are only accessed
        // through shared references (and by `destroy`), hence `'static`.
        unsafe {
            let state_ptr = state_ptr.cast::<UnsafeCell<S>>();
            state_ptr.write(UnsafeCell::new(state));
            let storage = Self {
                selector,
                timers: Timers::init(timers_ptr, capacities.max_timers),
                io_waiters: IoWaiters::init(io_waiters_ptr, capacities.max_io),
                tasks: Tasks::init(
                    tasks_ptr,
                    ready_ptr,
                    task_slab_ptr,
                    root_region_ptr,
                    capacities,
                    root_layout,
                ),
                state: state_ptr.as_ref(),
            };
            let ptr = base_ptr.cast::<Self>();
            ptr.write(storage);
            ptr.as_ref()
        }
    }

    /// Drops every task, the state, the pools and the selector. The memory isn't freed: it's the
    /// caller's.
    ///
    /// # Safety
    ///
    /// Called exactly once, while no task is being polled; the storage isn't used afterwards.
    pub(crate) unsafe fn destroy(&self) {
        // Task futures hold `Context`s pointing at the storage and may use them in their `Drop`
        // impls, so they go first, while everything is intact.
        self.tasks.drop_all();
        self.timers.clear();
        self.io_waiters.clear();
        // SAFETY: no task (so no `Context` and no reference into the storage) is left; the state
        // was initialized in `new` and is dropped exactly once here, the other parts hold nothing
        // that needs dropping (the ready queue holds only `Copy` data, the task slab and root
        // region no live future, the pools nothing after `drop_all` and `clear`). The selector is
        // this struct's only field with drop glue; it's moved out (a read) rather than dropped in
        // place, because dropping needs write access to the struct, which callers hold protected
        // shared references to (`&self`, and e.g. an `EventLoop` given to `mem::drop`).
        unsafe {
            core::ptr::drop_in_place(self.state.get());
            drop(core::ptr::read(&self.selector));
        }
    }
}

impl<S: 'static> Storage<S, Selector> {
    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector.poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Readable(token) => {
                        if let Some(waker) = self.io_waiters.take_waker(token, Direction::Read) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Writable(token) => {
                        if let Some(waker) = self.io_waiters.take_waker(token, Direction::Write) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Timer => {
                        let now = Instant::now();
                        while let Some(waker) = self.timers.take_expired(now) {
                            waker.wake();
                        }
                    }
                }
            }
            if !full {
                return Ok(());
            }
        }
    }

    pub(crate) fn flush(&self) -> rustix::io::Result<()> {
        match self.timers.next_deadline() {
            Some(deadline) => self.selector.arm_timer(deadline),
            None => self.selector.disarm_timer(),
        }
        self.selector.flush(self.io_waiters.pending_changes())?;
        self.io_waiters.mark_flushed();
        Ok(())
    }
}

mod io_waiters;
mod layout;
mod tasks;
mod timers;

#[cfg(test)]
mod tests;
