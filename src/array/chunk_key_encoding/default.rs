//! The default chunk key encoding.

use crate::{
    array::chunk_key_encoding::ChunkKeyEncodingPlugin,
    metadata::v3::{chunk_key_encoding::default, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
    storage::StoreKey,
};

use super::{
    ChunkKeyEncoding, ChunkKeyEncodingTraits, ChunkKeySeparator,
    DefaultChunkKeyEncodingConfiguration,
};

pub use default::IDENTIFIER;

// Register the chunk key encoding.
inventory::submit! {
    ChunkKeyEncodingPlugin::new(IDENTIFIER, is_name_default, create_chunk_key_encoding_default)
}

fn is_name_default(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_chunk_key_encoding_default(
    metadata: &MetadataV3,
) -> Result<ChunkKeyEncoding, PluginCreateError> {
    let configuration: DefaultChunkKeyEncodingConfiguration =
        metadata.to_configuration().map_err(|_| {
            PluginMetadataInvalidError::new(IDENTIFIER, "chunk key encoding", metadata.clone())
        })?;
    let default = DefaultChunkKeyEncoding::new(configuration.separator);
    Ok(ChunkKeyEncoding::new(default))
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
    /// Create a new `default` chunk key encoding with separator `separator`.
    #[must_use]
    pub const fn new(separator: ChunkKeySeparator) -> Self {
        Self { separator }
    }

    /// Create a new `default` chunk key encoding with separator `.`.
    #[must_use]
    pub const fn new_dot() -> Self {
        Self {
            separator: ChunkKeySeparator::Dot,
        }
    }

    /// Create a new `default` chunk key encoding with separator `/`.
    #[must_use]
    pub const fn new_slash() -> Self {
        Self {
            separator: ChunkKeySeparator::Slash,
        }
    }
}

impl Default for DefaultChunkKeyEncoding {
    /// Create a `default` chunk key encoding with default separator: `/`.
    fn default() -> Self {
        Self {
            separator: ChunkKeySeparator::Slash,
        }
    }
}

impl ChunkKeyEncodingTraits for DefaultChunkKeyEncoding {
    fn create_metadata(&self) -> MetadataV3 {
        let configuration = DefaultChunkKeyEncodingConfiguration {
            separator: self.separator,
        };
        MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap()
    }

    fn encode(&self, chunk_grid_indices: &[u64]) -> StoreKey {
        let mut key = "c".to_string();
        if !chunk_grid_indices.is_empty() {
            key = key
                + &self.separator.to_string()
                + &chunk_grid_indices
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(&self.separator.to_string());
        }
        unsafe { StoreKey::new_unchecked(key) }
    }
}

#[cfg(test)]
mod tests {
    use crate::{node::NodePath, storage::data_key};

    use super::*;

    #[test]
    fn slash_nd() {
        let key = data_key(
            &NodePath::root(),
            &[1, 23, 45],
            &DefaultChunkKeyEncoding::new_slash().into(),
        );
        assert_eq!(key, StoreKey::new("c/1/23/45").unwrap());
    }

    #[test]
    fn dot_nd() {
        let key = data_key(
            &NodePath::root(),
            &[1, 23, 45],
            &DefaultChunkKeyEncoding::new_dot().into(),
        );
        assert_eq!(key, StoreKey::new("c.1.23.45").unwrap());
    }

    #[test]
    fn slash_scalar() {
        let key = data_key(
            &NodePath::root(),
            &[],
            &DefaultChunkKeyEncoding::new_slash().into(),
        );
        assert_eq!(key, StoreKey::new("c").unwrap());
    }

    #[test]
    fn dot_scalar() {
        let key = data_key(
            &NodePath::root(),
            &[],
            &DefaultChunkKeyEncoding::new_dot().into(),
        );
        assert_eq!(key, StoreKey::new("c").unwrap());
    }
}
