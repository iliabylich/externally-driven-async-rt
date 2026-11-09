#[derive(Clone, Copy)]
pub(super) enum Links {
    Occupied {
        prev: Option<u32>,
        next: Option<u32>,
    },
    Vacant {
        next_free: Option<u32>,
    },
}
