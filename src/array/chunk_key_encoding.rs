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
    storage::StoreKey,
};

use derive_more::{Deref, Display, From};

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
    pub fn from_metadata(metadata: &Metadata) -> Result<Self, PluginCreateError> {
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
    fn create_metadata(&self) -> Metadata;

    /// Encode chunk grid indices (grid cell coordinates) into a store key.
    fn encode(&self, chunk_grid_indices: &[u64]) -> StoreKey;
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

impl TryFrom<char> for ChunkKeySeparator {
    type Error = char;

    fn try_from(separator: char) -> Result<Self, Self::Error> {
        if separator == '/' {
            Ok(Self::Slash)
        } else if separator == '.' {
            Ok(Self::Dot)
        } else {
            Err(separator)
        }
    }
}

impl serde::Serialize for ChunkKeySeparator {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Slash => s.serialize_char('/'),
            Self::Dot => s.serialize_char('.'),
        }
    }
}

impl<'de> serde::Deserialize<'de> for ChunkKeySeparator {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(separator) = value {
            if separator == "/" {
                return Ok(Self::Slash);
            } else if separator == "." {
                return Ok(Self::Dot);
            }
        }
        Err(serde::de::Error::custom(
            "chunk key separator must be a `.` or `/`.",
        ))
    }
}
