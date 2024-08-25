use std::sync::Arc;

use super::{ArrayBytes, ArrayError};

pub mod array_chunk_cache_sync_readable_ext;
pub mod chunk_cache_lru_chunk_limit;
pub mod chunk_cache_lru_size_limit;

/// Traits for a chunk cache.
pub trait ChunkCache: Send + Sync {
    /// Retrieve a chunk from the cache. Returns [`None`] if the chunk is not present.
    ///
    /// The chunk cache implementation may modify the cache (e.g. update LRU cache) on retrieval.
    fn get(&self, chunk_indices: &[u64]) -> Option<Arc<ArrayBytes<'static>>>;

    /// Insert a chunk into the cache.
    fn insert(&self, chunk_indices: Vec<u64>, chunk: Arc<ArrayBytes<'static>>);

    /// Get or insert a chunk in the cache.
    ///
    /// # Errors
    /// Returns an error if `f` returns an error.
    fn try_get_or_insert_with<F, E>(
        &self,
        key: Vec<u64>,
        f: F,
    ) -> Result<Arc<ArrayBytes<'static>>, Arc<ArrayError>>
    where
        F: FnOnce() -> Result<Arc<ArrayBytes<'static>>, ArrayError>;

    /// Return the number of chunks in the cache.
    #[must_use]
    fn len(&self) -> usize;

    /// Returns true if the cache is empty.
    #[must_use]
    fn is_empty(&self) -> bool;
}

// TODO: AsyncChunkCache
