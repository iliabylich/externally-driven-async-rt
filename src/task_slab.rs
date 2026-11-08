use core::{alloc::Layout, cell::UnsafeCell, mem::MaybeUninit, ptr::NonNull};

// The largest standard alignment on the supported targets (`malloc`'s guarantee), which every
// future has unless it holds an over-aligned type.
#[repr(C, align(16))]
struct Chunk(UnsafeCell<MaybeUninit<[u8; 16]>>);

pub(crate) const TASK_ALIGN: usize = align_of::<Chunk>();

// One block per task slot, each holding its task's future. `UnsafeCell`s, so the futures can be
// written, polled and dropped in place through pointers from `block` while the slab itself is
// only ever shared. `repr(C)` so `layout` can compute the size and `init` can place it in raw
// memory.
#[repr(C)]
pub(crate) struct TaskSlab {
    chunks_per_block: usize,
    chunks: [Chunk],
}

impl TaskSlab {
    pub(crate) fn layout(blocks: usize, block_size: usize) -> Layout {
        let chunks = Layout::array::<Chunk>(Self::chunks_per_block(block_size) * blocks)
            .expect("task slab is too large");
        let (layout, _) = Layout::new::<usize>()
            .extend(chunks)
            .expect("task slab is too large");
        layout.pad_to_align()
    }

    fn chunks_per_block(block_size: usize) -> usize {
        block_size.div_ceil(size_of::<Chunk>())
    }

    /// Writes a slab of `blocks` blocks of at least `block_size` bytes each (rounded up to a
    /// multiple of `TASK_ALIGN`) at `ptr`.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for writes of `Self::layout(blocks, block_size)` bytes (so `layout`
    /// must have accepted them), aligned to it, and stay valid (and otherwise unused) for as long
    /// as the returned slab is used.
    pub(crate) unsafe fn init(ptr: NonNull<u8>, blocks: usize, block_size: usize) -> NonNull<Self> {
        let chunks_per_block = Self::chunks_per_block(block_size);
        let slab = core::ptr::slice_from_raw_parts_mut(
            ptr.as_ptr().cast::<Chunk>(),
            chunks_per_block * blocks,
        ) as *mut Self;
        // SAFETY: the caller guarantees `ptr` is valid for `Self::layout(blocks, block_size)`,
        // which (`repr(C)`) covers every field; the chunks are `MaybeUninit`, so they need no
        // initialization.
        unsafe {
            (&raw mut (*slab).chunks_per_block).write(chunks_per_block);
            NonNull::new_unchecked(slab)
        }
    }

    pub(crate) fn block_size(&self) -> usize {
        self.chunks_per_block * size_of::<Chunk>()
    }

    /// `block_size` bytes aligned to `TASK_ALIGN`, writable (they're in `UnsafeCell`s).
    pub(crate) fn block(&self, slot: u32) -> NonNull<u8> {
        let start = slot as usize * self.chunks_per_block;
        NonNull::from(&self.chunks[start..start + self.chunks_per_block]).cast()
    }
}

#[cfg(test)]
mod tests {
    use core::{alloc::Layout, mem::MaybeUninit};

    use crate::{
        task_slab::{TASK_ALIGN, TaskSlab},
        test_utils::{aligned_in, stack_memory},
    };

    // The slab needs no drop (it's all `MaybeUninit`).
    fn slab_in(memory: &mut [MaybeUninit<u8>], blocks: usize, block_size: usize) -> &TaskSlab {
        let ptr = aligned_in(memory, TaskSlab::layout(blocks, block_size));
        // SAFETY: `ptr` is aligned and valid for the layout, and `memory` stays borrowed for as
        // long as the returned slab.
        unsafe { TaskSlab::init(ptr, blocks, block_size).as_ref() }
    }

    #[test]
    fn layout_matches_the_layout_of_the_value() {
        let mut memory = stack_memory();
        let slab = slab_in(&mut memory, 3, 100);
        assert_eq!(Layout::for_value(slab), TaskSlab::layout(3, 100));
    }

    #[test]
    fn block_size_is_rounded_up_to_whole_chunks() {
        let mut memory = stack_memory();
        assert_eq!(slab_in(&mut memory, 1, 100).block_size(), 112);
        assert_eq!(slab_in(&mut memory, 1, 112).block_size(), 112);
    }

    #[test]
    fn full_blocks_are_aligned_and_do_not_overlap() {
        let mut memory = stack_memory();
        let slab = slab_in(&mut memory, 3, 100);
        let size = slab.block_size();
        for slot in 0..3 {
            let block = slab.block(slot);
            assert_eq!(block.addr().get() % TASK_ALIGN, 0);
            // SAFETY: a block is valid for writes of `block_size` bytes.
            unsafe { block.write_bytes(slot as u8 + 1, size) };
        }
        for slot in 0..3 {
            // SAFETY: the whole block was written above, and nothing else references it.
            let bytes = unsafe { core::slice::from_raw_parts(slab.block(slot).as_ptr(), size) };
            assert!(bytes.iter().all(|byte| *byte == slot as u8 + 1));
        }
    }
}
