#[derive(Clone, Copy)]
pub(crate) enum Slot {
    Vacant {
        version: u32,
        next_free: Option<u32>,
    },
    Occupied {
        version: u32,
        prev: Option<u32>,
        next: Option<u32>,
    },
}
