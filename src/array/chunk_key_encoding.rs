//! Zarr chunk key encodings. Includes a [default](default::DefaultChunkKeyEncoding) and [v2](v2::V2ChunkKeyEncoding) implementation.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#chunk-key-encoding>.

mod chunk_key_separator;
pub mod default;
pub mod v2;

pub use crate::metadata::v3::chunk_key_encoding::{
    default::DefaultChunkKeyEncodingConfiguration, v2::V2ChunkKeyEncodingConfiguration,
};
pub use chunk_key_separator::ChunkKeySeparator;
pub use default::DefaultChunkKeyEncoding;
pub use v2::V2ChunkKeyEncoding;

use crate::{
    metadata::v3::MetadataV3,
    plugin::{Plugin, PluginCreateError},
    storage::StoreKey,
};

use derive_more::{Deref, From};

/// A chunk key encoding.
#[derive(Debug, Clone, From, Deref)]
pub struct ChunkKeyEncoding(Box<dyn ChunkKeyEncodingTraits>);

/// A chunk key encoding plugin.
pub type ChunkKeyEncodingPlugin = Plugin<ChunkKeyEncoding>;
inventory::collect!(ChunkKeyEncodingPlugin);

impl ChunkKeyEncoding {
    /// Create a chunk key encoding.
    pub fn new<T: ChunkKeyEncodingTraits + 'static>(chunk_key_encoding: T) -> Self {
        let chunk_key_encoding: Box<dyn ChunkKeyEncodingTraits> = Box::new(chunk_key_encoding);
        chunk_key_encoding.into()
    }

    /// Create a chunk key encoding from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered chunk key encoding plugin.
    pub fn from_metadata(metadata: &MetadataV3) -> Result<Self, PluginCreateError> {
        for plugin in inventory::iter::<ChunkKeyEncodingPlugin> {
            if plugin.match_name(metadata.name()) {
                return plugin.create(metadata);
            }
        }
        #[cfg(miri)]
        {
            // Inventory does not work in miri, so manually handle all known chunk key encodings
            match metadata.name() {
                default::IDENTIFIER => {
                    return default::create_chunk_key_encoding_default(metadata);
                }
                v2::IDENTIFIER => {
                    return v2::create_chunk_key_encoding_v2(metadata);
                }
                _ => {}
            }
        }
        Err(PluginCreateError::Unsupported {
            name: metadata.name().to_string(),
            plugin_type: "chunk key encoding".to_string(),
        })
    }
}

impl<T> From<T> for ChunkKeyEncoding
where
    T: ChunkKeyEncodingTraits + 'static,
{
    fn from(chunk_key_encoding: T) -> Self {
        Self::new(chunk_key_encoding)
    }
}

/// Chunk key encoding traits.
pub trait ChunkKeyEncodingTraits: dyn_clone::DynClone + core::fmt::Debug + Send + Sync {
    /// Create the metadata of this chunk key encoding.
    fn create_metadata(&self) -> MetadataV3;

    /// Encode chunk grid indices (grid cell coordinates) into a store key.
    fn encode(&self, chunk_grid_indices: &[u64]) -> StoreKey;
}

dyn_clone::clone_trait_object!(ChunkKeyEncodingTraits);
