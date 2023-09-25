//! Zarr chunk key encodings. Includes a [default](default::DefaultChunkKeyEncoding) and [v2](v2::V2ChunkKeyEncoding) implementation.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#chunk-key-encoding>.

mod default;
mod v2;

pub use default::{DefaultChunkKeyEncoding, DefaultChunkKeyEncodingConfiguration};
pub use v2::{V2ChunkKeyEncoding, V2ChunkKeyEncodingConfiguration};

use crate::{
    metadata::Metadata,
    plugin::{Plugin, PluginCreateError},
    storage::store::StoreKey,
};

use derive_more::Display;

/// A chunk key encoding.
pub type ChunkKeyEncoding = Box<dyn ChunkKeyEncodingTraits>;

/// A chunk key encoding plugin.
pub type ChunkKeyEncodingPlugin = Plugin<ChunkKeyEncoding>;
inventory::collect!(ChunkKeyEncodingPlugin);

/// Create a chunk key encoding from metadata.
///
/// # Errors
///
/// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered chunk key encoding plugin.
pub fn try_create_chunk_key_encoding(
    metadata: &Metadata,
) -> Result<ChunkKeyEncoding, PluginCreateError> {
    for plugin in inventory::iter::<ChunkKeyEncodingPlugin> {
        if plugin.match_name(metadata.name()) {
            return plugin.create(metadata);
        }
    }
    Err(PluginCreateError::Unsupported {
        name: metadata.name().to_string(),
    })
}

/// Chunk key encoding traits.
pub trait ChunkKeyEncodingTraits: dyn_clone::DynClone + core::fmt::Debug + Send + Sync {
    /// Create the metadata of this chunk key encoding.
    fn create_metadata(&self) -> Metadata;

    /// Encode chunk grid indices (grid cell coordinates) into a store key.
    fn encode(&self, chunk_grid_indices: &[usize]) -> StoreKey;
}

dyn_clone::clone_trait_object!(ChunkKeyEncodingTraits);

/// A chunk key separator.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Display)]
pub enum ChunkKeySeparator {
    /// The slash '/' character.
    #[display(fmt = "/")]
    Slash,
    /// The dot '.' character.
    #[display(fmt = ".")]
    Dot,
}

impl serde::Serialize for ChunkKeySeparator {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ChunkKeySeparator::Slash => s.serialize_char('/'),
            ChunkKeySeparator::Dot => s.serialize_char('.'),
        }
    }
}

impl<'de> serde::Deserialize<'de> for ChunkKeySeparator {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(separator) = value {
            if separator == "/" {
                return Ok(ChunkKeySeparator::Slash);
            } else if separator == "." {
                return Ok(ChunkKeySeparator::Dot);
            }
        }
        Err(serde::de::Error::custom(
            "chunk key separator must be a `.` or `/`.",
        ))
    }
}
