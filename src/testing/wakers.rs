use core::{
    cell::Cell,
    task::{RawWaker, RawWakerVTable, Waker},
};

pub(crate) fn counting_waker(drops: &Cell<u32>) -> Waker {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| RawWaker::new(data, &VTABLE),
        |_| {},
        |_| {},
        |data| {
            // SAFETY: the data is the `Cell` from `counting_waker`, which every test keeps
            // alive for as long as its wakers.
            let drops = unsafe { &*data.cast::<Cell<u32>>() };
            drops.set(drops.get() + 1);
        },
    );
    // SAFETY: the vtable only reads the data as a `&Cell<u32>`, and `drops` outlives the
    // waker (see above).
    unsafe { Waker::new(core::ptr::from_ref(drops).cast(), &VTABLE) }
}
