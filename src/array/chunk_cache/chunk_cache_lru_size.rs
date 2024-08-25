use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use lru::LruCache;
use parking_lot::Mutex;

use crate::array::{ArrayBytes, ArrayIndices};

use super::ChunkCache;

type ChunkIndices = ArrayIndices;

/// A chunk cache with a fixed capacity in bytes.
pub struct ChunkCacheLruSize {
    cache: Arc<Mutex<LruCache<ChunkIndices, Arc<ArrayBytes<'static>>>>>,
    size: AtomicUsize,
    size_limit: NonZeroUsize,
}

impl ChunkCacheLruSize {
    /// Create a new [`ChunkCacheLruSize`] with a capacity of `chunk_capacity`.
    #[must_use]
    pub fn new(size_limit_bytes: NonZeroUsize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(LruCache::unbounded())),
            size: AtomicUsize::new(0),
            size_limit: size_limit_bytes,
        }
    }

    /// Return the number of chunks in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    /// Returns true if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the size of the cache in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }
}

impl ChunkCache for ChunkCacheLruSize {
    fn retrieve(&self, chunk_indices: &[u64]) -> Option<Arc<ArrayBytes<'static>>> {
        self.cache.lock().get(chunk_indices).cloned()
    }

    fn insert(&self, chunk_indices: &[u64], chunk: Arc<ArrayBytes<'static>>) {
        let size = self.size.fetch_add(chunk.size(), Ordering::SeqCst);
        if size + chunk.size() > self.size_limit.get() {
            if let Some(old) = self.cache.lock().pop_lru() {
                self.size.fetch_sub(old.1.size(), Ordering::SeqCst);
            } else {
                debug_assert!(false);
            }
        }
        if let Some(old) = self.cache.lock().put(chunk_indices.to_vec(), chunk) {
            self.size.fetch_sub(old.size(), Ordering::SeqCst);
        }
    }
}
