use std::sync::Arc;

use crate::storage::ReadableStorageTraits;

use super::{codec::CodecOptions, Array, ArrayBytes, ArrayError, RawBytes};

// pub mod array_chunk_cache_ext_decoded_sync;
// pub mod array_chunk_cache_ext_encoded_sync;
pub mod array_chunk_cache_ext_sync;
pub mod chunk_cache_lru;
// pub mod chunk_cache_lru_chunk_limit_thread_local;
// pub mod chunk_cache_lru_size_limit_thread_local;

/// The chunk type of an encoded chunk cache.
pub type ChunkCacheTypeEncoded = Option<RawBytes<'static>>;

/// The chunk type of a decoded chunk cache.
pub type ChunkCacheTypeDecoded = ArrayBytes<'static>;

/// A chunk type ([`ChunkCacheTypeEncoded`] or [`ChunkCacheTypeDecoded`]).
pub trait ChunkCacheType: Send + Sync + 'static {
    /// The size of the chunk in bytes.
    fn size(&self) -> usize;
}

impl ChunkCacheType for ChunkCacheTypeEncoded {
    fn size(&self) -> usize {
        self.as_ref().map_or(0, |v| v.len())
    }
}

impl ChunkCacheType for ChunkCacheTypeDecoded {
    fn size(&self) -> usize {
        ArrayBytes::size(self)
    }
}

/// Traits for a chunk cache.
pub trait ChunkCache<CT: ChunkCacheType>: Send + Sync {
    /// Retrieve and decode a chunk.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if the underlying array retrieval method fails.
    fn retrieve_chunk<TStorage: ?Sized + ReadableStorageTraits + 'static>(
        &self,
        array: &Array<TStorage>,
        chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Arc<ArrayBytes<'static>>, ArrayError>;

    /// Retrieve a chunk from the cache. Returns [`None`] if the chunk is not present.
    ///
    /// The chunk cache implementation may modify the cache (e.g. update LRU cache) on retrieval.
    fn get(&self, chunk_indices: &[u64]) -> Option<Arc<CT>>;

    /// Insert a chunk into the cache.
    fn insert(&self, chunk_indices: Vec<u64>, chunk: Arc<CT>);

    /// Get or insert a chunk in the cache.
    ///
    /// Override the default implementation if a chunk offers a more performant implementation.
    ///
    /// # Errors
    /// Returns an error if `f` returns an error.
    fn try_get_or_insert_with<F, E>(
        &self,
        chunk_indices: Vec<u64>,
        f: F,
    ) -> Result<Arc<CT>, Arc<ArrayError>>
    where
        F: FnOnce() -> Result<Arc<CT>, ArrayError>,
    {
        let chunk_indices = chunk_indices.clone();
        if let Some(chunk) = self.get(&chunk_indices) {
            Ok(chunk)
        } else {
            let chunk = f()?;
            self.insert(chunk_indices, chunk.clone());
            Ok(chunk)
        }
    }

    /// Return the number of chunks in the cache. For a thread-local cache, returns the number of chunks cached on the current thread.
    #[must_use]
    fn len(&self) -> usize;

    /// Returns true if the cache is empty. For a thread-local cache, returns if the cache is empty on the current thread.
    #[must_use]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// TODO: AsyncChunkCache
