use std::sync::Arc;

use moka::{
    policy::EvictionPolicy,
    sync::{Cache, CacheBuilder},
};

use crate::array::{ArrayBytes, ArrayError, ArrayIndices};

use super::ChunkCache;

type ChunkIndices = ArrayIndices;

/// A chunk cache with a fixed chunk capacity.
pub struct ChunkCacheLruChunkLimit {
    cache: Cache<ChunkIndices, Arc<ArrayBytes<'static>>>,
}

impl ChunkCacheLruChunkLimit {
    /// Create a new [`ChunkCacheLruChunkLimit`] with a capacity in chunks of `chunk_capacity`.
    #[must_use]
    pub fn new(chunk_capacity: u64) -> Self {
        let cache = CacheBuilder::new(chunk_capacity)
            .eviction_policy(EvictionPolicy::lru())
            .build();
        Self { cache }
    }
}

impl ChunkCache for ChunkCacheLruChunkLimit {
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
