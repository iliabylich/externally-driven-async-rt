mod tree;

use rustix::fd::RawFd;

use self::tree::{Entry, Tree};
use crate::{
    panic_message::const_unreachable,
    pool::{ObjectPool, VersionedIndex},
};

#[must_use]
pub(crate) struct FdHashMap<V, const N: usize> {
    tree: Tree<N>,
    pool: ObjectPool<V, N>,
}

impl<V, const N: usize> FdHashMap<V, N> {
    pub(crate) const fn new(values: [V; N]) -> Self {
        Self {
            tree: Tree::new(),
            pool: ObjectPool::new(values),
        }
    }

    pub(crate) const fn get(&self, key: RawFd) -> Option<&V> {
        let Some(idx) = self.tree.find(key) else {
            return None;
        };
        let (_, value) = self.pool.get(idx);
        Some(value)
    }

    pub(crate) const fn get_or_insert(&self, key: RawFd) -> Option<&V> {
        match self.tree.entry(key) {
            Entry::Occupied(idx) => {
                let (_, value) = self.pool.get(idx);
                Some(value)
            }
            Entry::Vacant(vacant) => {
                let Some((VersionedIndex { idx, .. }, value)) = self.pool.alloc() else {
                    return None;
                };
                vacant.insert(idx);
                Some(value)
            }
        }
    }

    pub(crate) const fn remove(&self, key: RawFd) {
        let idx = self.tree.remove(key);
        let (index, _) = self.pool.get(idx);
        self.pool.free(index);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (RawFd, &V)> {
        self.pool.iter().map(|(VersionedIndex { idx, .. }, value)| {
            let Some(key) = self.tree.key(idx) else {
                const_unreachable!("{} is allocated but has no key", idx)
            };
            (key, value)
        })
    }
}

#[cfg(test)]
mod tests;
