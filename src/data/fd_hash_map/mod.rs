use rustix::fd::RawFd;

use super::{
    fd_tree::{Entry, FdTree},
    pool::{ObjectPool, VersionedIndex},
};

#[must_use]
pub(crate) struct FdHashMap<V, const N: usize> {
    tree: FdTree<N>,
    pool: ObjectPool<V, N>,
}

impl<V, const N: usize> FdHashMap<V, N> {
    pub(crate) const fn new(values: [V; N]) -> Self {
        Self {
            tree: FdTree::new(),
            pool: ObjectPool::new(values),
        }
    }

    pub(crate) fn get(&self, key: RawFd) -> Option<&V> {
        let idx = self.tree.find(key)?;
        let (_, value) = self.pool.get(idx);
        Some(value)
    }

    pub(crate) fn get_or_insert(&self, key: RawFd) -> Option<&V> {
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

    pub(crate) fn remove(&self, key: RawFd) {
        let idx = self.tree.remove(key);
        let (index, _) = self.pool.get(idx);
        self.pool.free(index);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (RawFd, &V)> {
        self.pool.iter().map(|(VersionedIndex { idx, .. }, value)| {
            let Some(key) = self.tree.key(idx) else {
                unreachable!("{idx} is allocated but has no key")
            };
            (key, value)
        })
    }
}

#[cfg(test)]
mod tests;
