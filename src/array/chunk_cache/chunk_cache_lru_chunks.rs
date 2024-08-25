use std::{num::NonZeroUsize, sync::Arc};

use lru::LruCache;
use parking_lot::Mutex;

use crate::array::{ArrayBytes, ArrayIndices};

use super::ChunkCache;

type ChunkIndices = ArrayIndices;

/// A chunk cache with a fixed chunk capacity.
pub struct ChunkCacheLruChunks {
    cache: Arc<Mutex<LruCache<ChunkIndices, Arc<ArrayBytes<'static>>>>>,
}

impl ChunkCacheLruChunks {
    /// Create a new [`ChunkCacheLruChunks`] with a capacity of `chunk_capacity`.
    #[must_use]
    pub fn new(chunk_capacity: NonZeroUsize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(LruCache::new(chunk_capacity))),
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
}

impl ChunkCache for ChunkCacheLruChunks {
    fn retrieve(&self, chunk_indices: &[u64]) -> Option<Arc<ArrayBytes<'static>>> {
        self.cache.lock().get(chunk_indices).cloned()
    }

    fn insert<'a>(&self, chunk_indices: &[u64], chunk: Arc<ArrayBytes<'static>>) {
        self.cache.lock().put(chunk_indices.to_vec(), chunk);
    }
}
