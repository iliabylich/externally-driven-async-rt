use core::{
    cell::Cell,
    hash::{Hash, Hasher},
};

use super::fx_hasher::FxHasher;
use crate::conversions::u32_to_usize;

#[must_use]
struct Node<K> {
    key: Cell<Option<K>>,
    children: [Cell<Option<u32>>; 4],
}

impl<K> Node<K> {
    fn first_assigned_child(&self) -> Option<(&Cell<Option<u32>>, u32)> {
        self.children
            .iter()
            .find_map(|child| Some((child, child.get()?)))
    }
}

#[must_use]
pub(crate) struct Tree<K, const N: usize> {
    root: Cell<Option<u32>>,
    nodes: [Node<K>; N],
}

#[must_use]
pub(crate) enum Entry<'t, K, const N: usize> {
    Occupied(u32),
    Vacant(VacantEntry<'t, K, N>),
}

#[must_use]
pub(crate) struct VacantEntry<'t, K, const N: usize> {
    tree: &'t Tree<K, N>,
    link: &'t Cell<Option<u32>>,
    key: K,
}

impl<K: Copy + Eq + Hash, const N: usize> Tree<K, N> {
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

    pub(crate) fn entry(&self, key: K) -> Entry<'_, K, N> {
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

    pub(crate) fn find(&self, key: K) -> Option<u32> {
        self.link(key).get()
    }

    pub(crate) fn remove(&self, key: K) -> u32 {
        let link = self.link(key);
        let idx = link
            .get()
            .unwrap_or_else(|| unreachable!("removing a key that is not in the tree"));
        let node = self.node(idx);
        let replacement = self.left_most_set_leaf(idx);
        if let Some((replacement_parent_slot, replacement_idx)) = replacement {
            replacement_parent_slot.set(None);
            for (node_child, replacement_child) in node
                .children
                .iter()
                .zip(&self.node(replacement_idx).children)
            {
                replacement_child.set(node_child.take());
            }
        }
        node.key.set(None);
        link.set(replacement.map(|(_, replacement_idx)| replacement_idx));
        idx
    }

    pub(crate) fn key(&self, idx: u32) -> Option<K> {
        self.node(idx).key.get()
    }

    fn node(&self, idx: u32) -> &Node<K> {
        self.nodes
            .get(u32_to_usize(idx))
            .unwrap_or_else(|| unreachable!("node {idx} is out of bounds"))
    }

    fn left_most_set_leaf(&self, idx: u32) -> Option<(&Cell<Option<u32>>, u32)> {
        let mut cursor = self.node(idx).first_assigned_child()?;
        while let Some(next) = self.node(cursor.1).first_assigned_child() {
            cursor = next;
        }
        Some(cursor)
    }

    fn link(&self, key_to_find: K) -> &Cell<Option<u32>> {
        let mut hash = hash(key_to_find);
        let mut link = &self.root;
        while let Some(idx) = link.get() {
            let node = self.node(idx);
            let key = node
                .key
                .get()
                .unwrap_or_else(|| unreachable!("a linked node has no key"));
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

impl<K: Copy + Eq + Hash, const N: usize> VacantEntry<'_, K, N> {
    pub(crate) fn insert(self, idx: u32) {
        let node = self.tree.node(idx);
        if node.key.get().is_some() {
            unreachable!("{idx} is free but still has a key");
        }
        node.key.set(Some(self.key));
        self.link.set(Some(idx));
    }
}

fn hash(key: impl Hash) -> u64 {
    let mut hasher = FxHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}
