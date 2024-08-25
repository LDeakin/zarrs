use std::sync::Arc;

use moka::{
    policy::EvictionPolicy,
    sync::{Cache, CacheBuilder},
};

use crate::array::{ArrayBytes, ArrayError, ArrayIndices};

use super::ChunkCache;

type ChunkIndices = ArrayIndices;

/// A chunk cache with a fixed size capacity.
pub struct ChunkCacheLruSizeLimit {
    cache: Cache<ChunkIndices, Arc<ArrayBytes<'static>>>,
}

impl ChunkCacheLruSizeLimit {
    /// Create a new [`ChunkCacheLruSizeLimit`] with a capacity in bytes of `capacity`.
    #[must_use]
    pub fn new(capacity: u64) -> Self {
        let cache = CacheBuilder::new(capacity)
            .eviction_policy(EvictionPolicy::lru())
            .weigher(|_k, v: &Arc<ArrayBytes<'_>>| u32::try_from(v.size()).unwrap_or(u32::MAX))
            .build();
        Self { cache }
    }

    /// Return the size of the cache in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.cache.run_pending_tasks();
        usize::try_from(self.cache.weighted_size()).unwrap_or(usize::MAX)
    }
}

impl ChunkCache for ChunkCacheLruSizeLimit {
    fn get(&self, chunk_indices: &[u64]) -> Option<Arc<ArrayBytes<'static>>> {
        self.cache.get(&chunk_indices.to_vec())
    }

    fn insert(&self, chunk_indices: ChunkIndices, chunk: Arc<ArrayBytes<'static>>) {
        self.cache.insert(chunk_indices, chunk);
    }

    fn try_get_or_insert_with<F, E>(
        &self,
        chunk_indices: Vec<u64>,
        f: F,
    ) -> Result<Arc<ArrayBytes<'static>>, Arc<ArrayError>>
    where
        F: FnOnce() -> Result<Arc<ArrayBytes<'static>>, ArrayError>,
    {
        self.cache.try_get_with(chunk_indices, f)
    }

    fn len(&self) -> usize {
        self.cache.run_pending_tasks();
        usize::try_from(self.cache.entry_count()).unwrap()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
