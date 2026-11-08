use core::{alloc::Layout, mem::MaybeUninit, ptr::NonNull};

use crate::{
    capacities::Capacities,
    storage::{IoWaiters, Storage, Tasks, Timers},
};

// Where each part of the runtime's single allocation goes in the memory given to `new`, laid out
// one after another like the fields of a C struct, the `Storage` struct first, each with the
// memory's provenance.
pub(crate) struct StorageLayout {
    // The allocation's start, where the `Storage` struct goes.
    pub(crate) base_ptr: NonNull<u8>,
    pub(crate) state_ptr: NonNull<u8>,
    pub(crate) timers_ptr: NonNull<u8>,
    pub(crate) io_waiters_ptr: NonNull<u8>,
    pub(crate) tasks_ptr: NonNull<u8>,
}

impl StorageLayout {
    /// The size of the memory `new` needs: the layout plus slack for aligning its start, so
    /// memory of this size works wherever it starts.
    pub(crate) fn required_size<S: 'static, Sel>(
        capacities: Capacities,
        root_layout: Layout,
    ) -> usize {
        let (layout, _) = plan::<S, Sel>(capacities, root_layout);
        layout.size() + (layout.align() - 1)
    }

    /// Lays the parts out from the first suitably aligned address of `memory`. Panics if `memory`
    /// is smaller than needed (`required_size` is always enough).
    pub(crate) fn new<S: 'static, Sel>(
        capacities: Capacities,
        root_layout: Layout,
        memory: NonNull<[MaybeUninit<u8>]>,
    ) -> Self {
        let (layout, offsets) = plan::<S, Sel>(capacities, root_layout);
        let start = memory.cast::<u8>().as_ptr().align_offset(layout.align());
        if start.saturating_add(layout.size()) > memory.len() {
            panic!(
                "the EventLoop memory is too small: {} bytes, {} needed",
                memory.len(),
                Self::required_size::<S, Sel>(capacities, root_layout),
            );
        }
        let base_ptr = offset_by(memory.cast(), start);
        let [_, state_ptr, timers_ptr, io_waiters_ptr, tasks_ptr] =
            offsets.map(|offset| offset_by(base_ptr, offset));
        Self {
            base_ptr,
            state_ptr,
            timers_ptr,
            io_waiters_ptr,
            tasks_ptr,
        }
    }
}

// The allocation's layout and the offset of each part, in the order of `StorageLayout`'s fields.
pub(crate) fn plan<S: 'static, Sel>(
    capacities: Capacities,
    root_layout: Layout,
) -> (Layout, [usize; 5]) {
    sequence([
        Layout::new::<Storage<S, Sel>>(),
        Layout::new::<S>(),
        Timers::layout(capacities.max_timers),
        IoWaiters::layout(capacities.max_io),
        Tasks::layout(capacities.max_tasks, capacities.max_task_size, root_layout),
    ])
}

// `parts` laid out one after another like the fields of a C struct: the whole layout and the
// offset of each part.
pub(crate) fn sequence<const N: usize>(parts: [Layout; N]) -> (Layout, [usize; N]) {
    let mut layout = Layout::new::<()>();
    let offsets = parts.map(|part| {
        let (extended, offset) = layout.extend(part).expect("too large");
        layout = extended;
        offset
    });
    (layout.pad_to_align(), offsets)
}

// `ptr + offset` with `ptr`'s provenance. Safe, unlike `byte_add`: the result only has to be in
// bounds once it's dereferenced, which is up to whoever does that.
pub(crate) fn offset_by(ptr: NonNull<u8>, offset: usize) -> NonNull<u8> {
    ptr.map_addr(|addr| addr.checked_add(offset).expect("address overflow"))
}
