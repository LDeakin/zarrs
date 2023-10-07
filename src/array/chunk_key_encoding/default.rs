//! The default chunk key encoding.

use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::{
    array::chunk_key_encoding::{ChunkKeyEncodingPlugin, ChunkKeySeparator},
    metadata::Metadata,
    plugin::PluginCreateError,
    storage::store::StoreKey,
};

use super::{ChunkKeyEncoding, ChunkKeyEncodingTraits};

const IDENTIFIER: &str = "default";

// Register the chunk key encoding.
inventory::submit! {
    ChunkKeyEncodingPlugin::new(IDENTIFIER, is_name_default, create_chunk_key_encoding_default)
}

fn is_name_default(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_chunk_key_encoding_default(
    metadata: &Metadata,
) -> Result<ChunkKeyEncoding, PluginCreateError> {
    let configuration: DefaultChunkKeyEncodingConfiguration = metadata.to_configuration()?;
    let default = DefaultChunkKeyEncoding::new(configuration.separator);
    Ok(ChunkKeyEncoding::new(default))
}

/// A `default` chunk key encoding configuration.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct DefaultChunkKeyEncodingConfiguration {
    /// The chunk key separator.
    #[serde(default = "default_separator")]
    pub separator: ChunkKeySeparator,
}

const fn default_separator() -> ChunkKeySeparator {
    ChunkKeySeparator::Slash
}

/// A `default` chunk key encoding.
///
/// The key for a chunk with grid index (k, j, i, …) is formed by taking the initial prefix c, and appending for each dimension:
/// - the separator character, followed by,
/// - the ASCII decimal string representation of the chunk index within that dimension.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#chunk-key-encoding>.
#[derive(Debug, Clone)]
pub struct DefaultChunkKeyEncoding {
    separator: ChunkKeySeparator,
}

impl DefaultChunkKeyEncoding {
    /// Create a new default chunk key encoding with separator `separator`.
    #[must_use]
    pub fn new(separator: ChunkKeySeparator) -> Self {
        Self { separator }
    }

    /// Create a new default chunk key encoding with separator `.`.
    #[must_use]
    pub fn new_dot() -> Self {
        Self {
            separator: ChunkKeySeparator::Dot,
        }
    }

    /// Create a new default chunk key encoding with separator `/`.
    #[must_use]
    pub fn new_slash() -> Self {
        Self {
            separator: ChunkKeySeparator::Slash,
        }
    }
}

impl Default for DefaultChunkKeyEncoding {
    /// Create a default chunk key encoding with default separator: `/`.
    fn default() -> Self {
        Self {
            separator: default_separator(),
        }
    }
}

impl ChunkKeyEncodingTraits for DefaultChunkKeyEncoding {
    fn create_metadata(&self) -> Metadata {
        let configuration = DefaultChunkKeyEncodingConfiguration {
            separator: self.separator,
        };
        Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap()
    }

    fn encode(&self, chunk_grid_indices: &[u64]) -> StoreKey {
        let key = "c".to_string()
            + &self.separator.to_string()
            + &chunk_grid_indices
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<String>>()
                .join(&self.separator.to_string());
        unsafe { StoreKey::new_unchecked(key) }
    }
}
