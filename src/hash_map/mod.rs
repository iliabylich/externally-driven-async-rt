mod fx_hasher;
mod tree;

use core::hash::Hash;

use self::tree::{Entry, Tree};
use crate::pool::{ObjectPool, VersionedIndex};

#[must_use]
pub(crate) struct HashMap<K, V, const N: usize> {
    tree: Tree<K, N>,
    pool: ObjectPool<V, N>,
}

impl<K: Copy + Eq + Hash, V, const N: usize> HashMap<K, V, N> {
    pub(crate) const fn new(values: [V; N]) -> Self {
        Self {
            tree: Tree::new(),
            pool: ObjectPool::new(values),
        }
    }

    pub(crate) fn get(&self, key: K) -> Option<&V> {
        let idx = self.tree.find(key)?;
        let (_, value) = self.pool.get(idx);
        Some(value)
    }

    pub(crate) fn get_or_insert(&self, key: K) -> Option<&V> {
        match self.tree.entry(key) {
            Entry::Occupied(idx) => {
                let (_, value) = self.pool.get(idx);
                Some(value)
            }
            Entry::Vacant(vacant) => {
                let (VersionedIndex { idx, .. }, value) = self.pool.alloc()?;
                vacant.insert(idx);
                Some(value)
            }
        }
    }

    pub(crate) fn remove(&self, key: K) {
        let idx = self.tree.remove(key);
        let (index, _) = self.pool.get(idx);
        self.pool.free(index);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (K, &V)> {
        self.pool.iter().map(|(VersionedIndex { idx, .. }, value)| {
            let key = self
                .tree
                .key(idx)
                .unwrap_or_else(|| unreachable!("{idx} is allocated but has no key"));
            (key, value)
        })
    }
}

#[cfg(test)]
mod tests;
