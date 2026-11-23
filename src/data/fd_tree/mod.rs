use core::cell::Cell;

use rustix::fd::RawFd;

use crate::conversions::u32_to_usize;

const MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

#[must_use]
struct Node {
    key: Cell<Option<RawFd>>,
    children: [Cell<Option<u32>>; 4],
}

impl Node {
    fn first_assigned_child(&self) -> Option<(&Cell<Option<u32>>, u32)> {
        for child in &self.children {
            if let Some(idx) = child.get() {
                return Some((child, idx));
            }
        }
        None
    }
}

#[must_use]
pub(super) struct FdTree<const N: usize> {
    root: Cell<Option<u32>>,
    nodes: [Node; N],
}

#[must_use]
pub(super) enum Entry<'t, const N: usize> {
    Occupied(u32),
    Vacant(VacantEntry<'t, N>),
}

#[must_use]
pub(super) struct VacantEntry<'t, const N: usize> {
    tree: &'t FdTree<N>,
    link: &'t Cell<Option<u32>>,
    key: RawFd,
}

impl<const N: usize> FdTree<N> {
    pub(super) const fn new() -> Self {
        Self {
            root: Cell::new(None),
            nodes: [const {
                Node {
                    key: Cell::new(None),
                    children: [const { Cell::new(None) }; 4],
                }
            }; N],
        }
    }

    pub(super) fn entry(&self, key: RawFd) -> Entry<'_, N> {
        let link = self.link(key);
        if let Some(idx) = link.get() {
            return Entry::Occupied(idx);
        }
        Entry::Vacant(VacantEntry {
            tree: self,
            link,
            key,
        })
    }

    pub(super) fn find(&self, key: RawFd) -> Option<u32> {
        self.link(key).get()
    }

    pub(super) fn remove(&self, key: RawFd) -> u32 {
        let link = self.link(key);
        let Some(idx) = link.get() else {
            unreachable!("removing a key that is not in the tree")
        };
        let node = self.node(idx);
        let replacement = self.left_most_set_leaf(idx);
        node.key.set(None);
        let Some((replacement_parent_slot, replacement_idx)) = replacement else {
            link.set(None);
            return idx;
        };
        replacement_parent_slot.set(None);
        let replacement_children = &self.node(replacement_idx).children;
        for (node_child, replacement_child) in node.children.iter().zip(replacement_children) {
            replacement_child.set(node_child.take());
        }
        link.set(Some(replacement_idx));
        idx
    }

    pub(super) fn key(&self, idx: u32) -> Option<RawFd> {
        self.node(idx).key.get()
    }

    fn node(&self, idx: u32) -> &Node {
        let Some(node) = self.nodes.get(u32_to_usize(idx)) else {
            unreachable!("node {idx} is out of bounds")
        };
        node
    }

    fn left_most_set_leaf(&self, idx: u32) -> Option<(&Cell<Option<u32>>, u32)> {
        let mut cursor = self.node(idx).first_assigned_child()?;
        while let Some(next) = self.node(cursor.1).first_assigned_child() {
            cursor = next;
        }
        Some(cursor)
    }

    fn link(&self, key_to_find: RawFd) -> &Cell<Option<u32>> {
        let mut hash = hash(key_to_find);
        let mut link = &self.root;
        while let Some(idx) = link.get() {
            let node = self.node(idx);
            let Some(key) = node.key.get() else {
                unreachable!("a linked node has no key")
            };
            if key == key_to_find {
                break;
            }
            let [c0, c1, c2, c3] = &node.children;
            link = match hash >> 62 {
                0 => c0,
                1 => c1,
                2 => c2,
                _ => c3,
            };
            hash <<= 2;
        }
        link
    }
}

impl<const N: usize> VacantEntry<'_, N> {
    pub(super) fn insert(self, idx: u32) {
        let node = self.tree.node(idx);
        if node.key.get().is_some() {
            unreachable!("{idx} is free but still has a key");
        }
        node.key.set(Some(self.key));
        self.link.set(Some(idx));
    }
}

fn hash(key: RawFd) -> u64 {
    let mut hash: u64 = 0;
    for byte in key.to_ne_bytes() {
        hash = (hash.rotate_left(5) ^ u64::from(byte)).wrapping_mul(MULTIPLIER);
    }
    hash
}

#[cfg(test)]
mod tests;
