use core::{alloc::Layout, cell::UnsafeCell, mem::MaybeUninit, ptr::NonNull};

// The root future's own memory, laid out exactly for it (any size and alignment), so
// `max_task_size` only has to fit spawned tasks. `UnsafeCell`s, so the root can be written,
// polled and dropped in place through `memory` while the region itself is only ever shared.
#[repr(transparent)]
pub(crate) struct RootRegion([UnsafeCell<MaybeUninit<u8>>]);

impl RootRegion {
    /// # Safety
    ///
    /// `ptr` must be valid for writes of `size` bytes and stay valid (and otherwise unused) for
    /// `'a`.
    pub(crate) unsafe fn init<'a>(ptr: NonNull<u8>, size: usize) -> &'a Self {
        let bytes = core::ptr::slice_from_raw_parts_mut(
            ptr.as_ptr().cast::<UnsafeCell<MaybeUninit<u8>>>(),
            size,
        ) as *mut Self;
        // SAFETY: the caller guarantees `ptr` is valid for `size` bytes for `'a`; the bytes are
        // `MaybeUninit`, so they need no initialization.
        unsafe { &*bytes }
    }

    pub(crate) fn fits(&self, layout: Layout) -> bool {
        layout.size() <= self.0.len() && self.memory().addr().get().is_multiple_of(layout.align())
    }

    /// The region's bytes, writable (they're in `UnsafeCell`s).
    pub(crate) fn memory(&self) -> NonNull<u8> {
        NonNull::from(&self.0).cast()
    }
}

#[cfg(test)]
mod tests {
    use core::{alloc::Layout, mem::MaybeUninit, ptr::NonNull};

    use crate::{
        root_region::RootRegion,
        test_utils::{aligned_in, stack_memory},
    };

    // The region needs no drop (it's all `MaybeUninit`).
    fn region_in(memory: &mut [MaybeUninit<u8>], layout: Layout) -> &RootRegion {
        // SAFETY: `aligned_in` returns memory valid for `layout`, and `memory` stays borrowed for
        // as long as the returned region.
        unsafe { RootRegion::init(aligned_in(memory, layout), layout.size()) }
    }

    #[test]
    fn layout_matches_the_layout_of_the_value() {
        let mut memory = stack_memory();
        let region = region_in(&mut memory, Layout::new::<[u8; 100]>());
        assert_eq!(Layout::for_value(region), Layout::new::<[u8; 100]>());
    }

    #[test]
    fn fits_checks_size_and_alignment() {
        let layout = Layout::from_size_align(100, 64).unwrap();
        let mut memory = stack_memory();
        let region = region_in(&mut memory, layout);
        assert!(region.fits(layout));
        assert!(region.fits(Layout::new::<u64>()));
        assert!(!region.fits(Layout::from_size_align(101, 1).unwrap()));
    }

    #[test]
    fn a_misaligned_region_does_not_fit() {
        let mut memory = stack_memory();
        let start = (0..2)
            .find(|start| memory[*start..].as_ptr().addr() % 2 == 1)
            .unwrap();
        // SAFETY: `memory` is valid for 8 bytes from `start` and stays borrowed for as long as
        // the region.
        let region = unsafe { RootRegion::init(NonNull::from(&mut memory[start..]).cast(), 8) };
        assert!(region.fits(Layout::new::<[u8; 8]>()));
        assert!(!region.fits(Layout::new::<u16>()));
    }

    #[test]
    fn memory_is_fully_writable() {
        let layout = Layout::from_size_align(100, 64).unwrap();
        let mut memory = stack_memory();
        let region = region_in(&mut memory, layout);
        // SAFETY: the region is valid for writes of `layout.size()` bytes.
        unsafe { region.memory().write_bytes(7, layout.size()) };
        // SAFETY: written right above, and nothing else references it.
        let bytes = unsafe { core::slice::from_raw_parts(region.memory().as_ptr(), layout.size()) };
        assert!(bytes.iter().all(|byte| *byte == 7));
    }
}
