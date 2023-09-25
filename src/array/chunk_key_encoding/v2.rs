//! The v2 chunk key encoding.

use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::{
    array::chunk_key_encoding::ChunkKeyEncodingPlugin, metadata::Metadata,
    plugin::PluginCreateError, storage::store::StoreKey,
};

use super::{ChunkKeyEncodingTraits, ChunkKeySeparator};

const IDENTIFIER: &str = "v2";

// Register the chunk key encoding.
inventory::submit! {
    ChunkKeyEncodingPlugin::new(IDENTIFIER, is_name_v2, create_chunk_key_encoding_v2)
}

fn is_name_v2(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_chunk_key_encoding_v2(
    metadata: &Metadata,
) -> Result<Box<dyn ChunkKeyEncodingTraits>, PluginCreateError> {
    let configuration: V2ChunkKeyEncodingConfiguration = metadata.to_configuration()?;
    let chunk_key_encoding = V2ChunkKeyEncoding::new(configuration.separator);
    Ok(Box::new(chunk_key_encoding))
}

/// A `v2` chunk key encoding configuration.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct V2ChunkKeyEncodingConfiguration {
    /// The chunk key separator.
    #[serde(default = "v2_separator")]
    pub separator: ChunkKeySeparator,
}

const fn v2_separator() -> ChunkKeySeparator {
    ChunkKeySeparator::Dot
}

/// A `v2` chunk key encoding.
///
/// The identifier for chunk with at least one dimension is formed by concatenating for each dimension:
/// - the ASCII decimal string representation of the chunk index within that dimension, followed by
/// - the separator character, except that it is omitted for the last dimension.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#chunk-key-encoding>.
#[derive(Debug, Clone)]
pub struct V2ChunkKeyEncoding {
    separator: ChunkKeySeparator,
}

impl V2ChunkKeyEncoding {
    /// Create a new v2 chunk key encoding with separator `separator`.
    #[must_use]
    pub fn new(separator: ChunkKeySeparator) -> Self {
        Self { separator }
    }

    /// Create a new v2 chunk key encoding with separator `.`.
    #[must_use]
    pub fn new_dot() -> Self {
        Self {
            separator: ChunkKeySeparator::Dot,
        }
    }

    /// Create a new v2 chunk key encoding with separator `/`.
    #[must_use]
    pub fn new_slash() -> Self {
        Self {
            separator: ChunkKeySeparator::Slash,
        }
    }
}

impl Default for V2ChunkKeyEncoding {
    /// Create a default chunk key encoding with default separator: `.`.
    fn default() -> Self {
        Self {
            separator: v2_separator(),
        }
    }
}

impl ChunkKeyEncodingTraits for V2ChunkKeyEncoding {
    fn create_metadata(&self) -> Metadata {
        let configuration = V2ChunkKeyEncodingConfiguration {
            separator: self.separator,
        };
        Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap()
    }

    fn encode(&self, chunk_grid_indices: &[usize]) -> StoreKey {
        let key = chunk_grid_indices
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<String>>()
            .join(&self.separator.to_string());
        unsafe { StoreKey::new_unchecked(key) }
    }
}
