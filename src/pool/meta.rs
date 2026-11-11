use core::cell::Cell;

use crate::pool::Links;

pub(crate) struct Meta {
    generation: Cell<u32>,
    links: Cell<Links>,
}

impl Meta {
    pub(crate) const fn new(next_free: Option<u32>) -> Self {
        Self {
            generation: Cell::new(0),
            links: Cell::new(Links::Vacant { next_free }),
        }
    }

    pub(crate) const fn generation(&self) -> u32 {
        self.generation.get()
    }

    pub(crate) fn set_generation(&self, generation: u32) {
        self.generation.set(generation);
    }

    pub(crate) const fn links(&self) -> Links {
        self.links.get()
    }

    pub(crate) fn set_links(&self, links: Links) {
        self.links.set(links);
    }
}
