//! The v2 chunk key encoding.

use crate::{
    array::chunk_key_encoding::ChunkKeyEncodingPlugin,
    metadata::v3::{chunk_key_encoding::v2, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
    storage::StoreKey,
};

use super::{
    ChunkKeyEncoding, ChunkKeyEncodingTraits, ChunkKeySeparator, V2ChunkKeyEncodingConfiguration,
};

pub use v2::IDENTIFIER;

// Register the chunk key encoding.
inventory::submit! {
    ChunkKeyEncodingPlugin::new(IDENTIFIER, is_name_v2, create_chunk_key_encoding_v2)
}

fn is_name_v2(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_chunk_key_encoding_v2(
    metadata: &MetadataV3,
) -> Result<ChunkKeyEncoding, PluginCreateError> {
    let configuration: V2ChunkKeyEncodingConfiguration =
        metadata.to_configuration().map_err(|_| {
            PluginMetadataInvalidError::new(IDENTIFIER, "chunk key encoding", metadata.clone())
        })?;
    let v2 = V2ChunkKeyEncoding::new(configuration.separator);
    Ok(ChunkKeyEncoding::new(v2))
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
    /// Create a new `v2` chunk key encoding with separator `separator`.
    #[must_use]
    pub const fn new(separator: ChunkKeySeparator) -> Self {
        Self { separator }
    }

    /// Create a new `v2` chunk key encoding with separator `.`.
    #[must_use]
    pub const fn new_dot() -> Self {
        Self {
            separator: ChunkKeySeparator::Dot,
        }
    }

    /// Create a new `v2` chunk key encoding with separator `/`.
    #[must_use]
    pub const fn new_slash() -> Self {
        Self {
            separator: ChunkKeySeparator::Slash,
        }
    }
}

impl Default for V2ChunkKeyEncoding {
    /// Create a `v2` chunk key encoding with default separator: `.`.
    fn default() -> Self {
        Self {
            separator: ChunkKeySeparator::Dot,
        }
    }
}

impl ChunkKeyEncodingTraits for V2ChunkKeyEncoding {
    fn create_metadata(&self) -> MetadataV3 {
        let configuration = V2ChunkKeyEncodingConfiguration {
            separator: self.separator,
        };
        MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap()
    }

    fn encode(&self, chunk_grid_indices: &[u64]) -> StoreKey {
        let key = if chunk_grid_indices.is_empty() {
            "0".to_string()
        } else {
            chunk_grid_indices
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<String>>()
                .join(&self.separator.to_string())
        };
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
            &V2ChunkKeyEncoding::new_slash().into(),
        );
        assert_eq!(key, StoreKey::new("1/23/45").unwrap());
    }

    #[test]
    fn dot_nd() {
        let key = data_key(
            &NodePath::root(),
            &[1, 23, 45],
            &V2ChunkKeyEncoding::new_dot().into(),
        );
        assert_eq!(key, StoreKey::new("1.23.45").unwrap());
    }

    #[test]
    fn slash_scalar() {
        let key = data_key(
            &NodePath::root(),
            &[],
            &V2ChunkKeyEncoding::new_slash().into(),
        );
        assert_eq!(key, StoreKey::new("0").unwrap());
    }

    #[test]
    fn dot_scalar() {
        let key = data_key(
            &NodePath::root(),
            &[],
            &V2ChunkKeyEncoding::new_dot().into(),
        );
        assert_eq!(key, StoreKey::new("0").unwrap());
    }
}
