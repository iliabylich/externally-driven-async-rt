#[derive(Clone, Copy)]
pub(crate) enum Slot {
    Vacant {
        version: u16,
        next_free: Option<u32>,
    },
    Occupied {
        version: u16,
        prev: Option<u32>,
        next: Option<u32>,
    },
}
