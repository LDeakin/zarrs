use std::sync::Arc;

use super::ArrayBytes;

pub mod array_chunk_cache_sync_readable_ext;
pub mod chunk_cache_lru_chunks;
pub mod chunk_cache_lru_size;

/// Traits for a chunk cache.
pub trait ChunkCache: Send + Sync {
    /// Retrieve a chunk from the cache. Returns [`None`] if the chunk is not present.
    ///
    /// The chunk cache implementation may modify the cache (e.g. update LRU cache) on retrieval.
    fn retrieve(&self, chunk_indices: &[u64]) -> Option<Arc<ArrayBytes<'static>>>;

    /// Insert a chunk into the cache.
    fn insert(&self, chunk_indices: &[u64], chunk: Arc<ArrayBytes<'static>>);
}

// TODO: AsyncChunkCache
