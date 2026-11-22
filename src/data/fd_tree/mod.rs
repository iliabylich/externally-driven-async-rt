use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{const_unreachable::const_unreachable, conversions::u32_to_usize, helpers::element};

const MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

#[must_use]
struct Node {
    key: Cell<Option<RawFd>>,
    children: [Cell<Option<u32>>; 4],
}

impl Node {
    const fn first_assigned_child(&self) -> Option<(&Cell<Option<u32>>, u32)> {
        let mut children: &[Cell<Option<u32>>] = &self.children;
        while let [child, rest @ ..] = children {
            if let Some(idx) = child.get() {
                return Some((child, idx));
            }
            children = rest;
        }
        None
    }
}

#[must_use]
pub(crate) struct FdTree<const N: usize> {
    root: Cell<Option<u32>>,
    nodes: [Node; N],
}

#[must_use]
pub(crate) enum Entry<'t, const N: usize> {
    Occupied(u32),
    Vacant(VacantEntry<'t, N>),
}

#[must_use]
pub(crate) struct VacantEntry<'t, const N: usize> {
    tree: &'t FdTree<N>,
    link: &'t Cell<Option<u32>>,
    key: RawFd,
}

impl<const N: usize> FdTree<N> {
    pub(crate) const fn new() -> Self {
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

    pub(crate) const fn entry(&self, key: RawFd) -> Entry<'_, N> {
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

    pub(crate) const fn find(&self, key: RawFd) -> Option<u32> {
        self.link(key).get()
    }

    pub(crate) const fn remove(&self, key: RawFd) -> u32 {
        let link = self.link(key);
        let Some(idx) = link.get() else {
            const_unreachable!("removing a key that is not in the tree")
        };
        let node = self.node(idx);
        let replacement = self.left_most_set_leaf(idx);
        let _ = node.key.replace(None);
        let Some((replacement_parent_slot, replacement_idx)) = replacement else {
            let _ = link.replace(None);
            return idx;
        };
        let _ = replacement_parent_slot.replace(None);
        let mut node_children: &[Cell<Option<u32>>] = &node.children;
        let mut replacement_children: &[Cell<Option<u32>>] = &self.node(replacement_idx).children;
        while let ([node_child, node_rest @ ..], [replacement_child, replacement_rest @ ..]) =
            (node_children, replacement_children)
        {
            let _ = replacement_child.replace(node_child.replace(None));
            node_children = node_rest;
            replacement_children = replacement_rest;
        }
        let _ = link.replace(Some(replacement_idx));
        idx
    }

    pub(crate) const fn key(&self, idx: u32) -> Option<RawFd> {
        self.node(idx).key.get()
    }

    const fn node(&self, idx: u32) -> &Node {
        let Some(node) = element(&self.nodes, u32_to_usize(idx)) else {
            const_unreachable!("node {} is out of bounds", idx)
        };
        node
    }

    const fn left_most_set_leaf(&self, idx: u32) -> Option<(&Cell<Option<u32>>, u32)> {
        let Some(mut cursor) = self.node(idx).first_assigned_child() else {
            return None;
        };
        while let Some(next) = self.node(cursor.1).first_assigned_child() {
            cursor = next;
        }
        Some(cursor)
    }

    const fn link(&self, key_to_find: RawFd) -> &Cell<Option<u32>> {
        let mut hash = hash(key_to_find);
        let mut link = &self.root;
        while let Some(idx) = link.get() {
            let node = self.node(idx);
            let Some(key) = node.key.get() else {
                const_unreachable!("a linked node has no key")
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
    pub(crate) const fn insert(self, idx: u32) {
        let node = self.tree.node(idx);
        if node.key.get().is_some() {
            const_unreachable!("{} is free but still has a key", idx);
        }
        let _ = node.key.replace(Some(self.key));
        let _ = self.link.replace(Some(idx));
    }
}

const fn hash(key: RawFd) -> u64 {
    let bytes = key.to_ne_bytes();
    let mut rest: &[u8] = &bytes;
    let mut hash: u64 = 0;
    while let [byte, tail @ ..] = rest {
        let byte = u64::from_le_bytes([*byte, 0, 0, 0, 0, 0, 0, 0]);
        hash = (hash.rotate_left(5) ^ byte).wrapping_mul(MULTIPLIER);
        rest = tail;
    }
    hash
}

#[cfg(test)]
mod tests;
