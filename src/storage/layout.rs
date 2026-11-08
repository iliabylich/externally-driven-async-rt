use core::{alloc::Layout, mem::MaybeUninit, ptr::NonNull};

use crate::{
    capacities::Capacities,
    pool::ObjectPool,
    ready::ReadyQueue,
    storage::{IoWaiter, Storage, TaskEntry, Timer},
    task_slab::TaskSlab,
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
    pub(crate) ready_ptr: NonNull<u8>,
    pub(crate) task_slab_ptr: NonNull<u8>,
    pub(crate) root_region_ptr: NonNull<u8>,
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
        let [
            _,
            state_ptr,
            timers_ptr,
            io_waiters_ptr,
            tasks_ptr,
            ready_ptr,
            task_slab_ptr,
            root_region_ptr,
        ] = offsets.map(|offset| offset_by(base_ptr, offset));
        Self {
            base_ptr,
            state_ptr,
            timers_ptr,
            io_waiters_ptr,
            tasks_ptr,
            ready_ptr,
            task_slab_ptr,
            root_region_ptr,
        }
    }
}

// The allocation's layout and the offset of each part, in the order of `StorageLayout`'s fields.
pub(crate) fn plan<S: 'static, Sel>(
    capacities: Capacities,
    root_layout: Layout,
) -> (Layout, [usize; 8]) {
    let parts = [
        Layout::new::<Storage<S, Sel>>(),
        Layout::new::<S>(),
        ObjectPool::<Timer>::layout(capacities.max_timers),
        ObjectPool::<IoWaiter>::layout(capacities.max_io),
        ObjectPool::<TaskEntry>::layout(capacities.max_tasks),
        ReadyQueue::layout(capacities.max_tasks),
        TaskSlab::layout(capacities.max_tasks, capacities.max_task_size),
        root_layout,
    ];
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
fn offset_by(ptr: NonNull<u8>, offset: usize) -> NonNull<u8> {
    ptr.map_addr(|addr| addr.checked_add(offset).expect("address overflow"))
}
