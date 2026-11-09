use core::cell::Cell;

use crate::pool::Links;

pub(super) struct Meta {
    pub(super) generation: Cell<u32>,
    pub(super) links: Cell<Links>,
}
