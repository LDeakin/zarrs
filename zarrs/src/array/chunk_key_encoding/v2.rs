//! The v2 chunk key encoding.

use zarrs_metadata::chunk_key_encoding::V2;

use crate::{
    array::chunk_key_encoding::ChunkKeyEncodingPlugin,
    metadata::v3::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
    storage::StoreKey,
};

use super::{
    ChunkKeyEncoding, ChunkKeyEncodingTraits, ChunkKeySeparator, V2ChunkKeyEncodingConfiguration,
};

// Register the chunk key encoding.
inventory::submit! {
    ChunkKeyEncodingPlugin::new(V2, is_name_v2, create_chunk_key_encoding_v2)
}

fn is_name_v2(name: &str) -> bool {
    name.eq(V2)
}

pub(crate) fn create_chunk_key_encoding_v2(
    metadata: &MetadataV3,
) -> Result<ChunkKeyEncoding, PluginCreateError> {
    let configuration: V2ChunkKeyEncodingConfiguration =
        metadata.to_configuration().map_err(|_| {
            PluginMetadataInvalidError::new(V2, "chunk key encoding", metadata.to_string())
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
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/chunk-key-encodings/v2/index.html>.
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
        MetadataV3::new_with_serializable_configuration(V2.to_string(), &configuration).unwrap()
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
    use crate::node::{data_key, NodePath};

    use super::*;

    #[test]
    fn slash_nd() {
        let chunk_key_encoding: ChunkKeyEncoding = V2ChunkKeyEncoding::new_slash().into();
        let key = data_key(&NodePath::root(), &chunk_key_encoding.encode(&[1, 23, 45]));
        assert_eq!(key, StoreKey::new("1/23/45").unwrap());
    }

    #[test]
    fn dot_nd() {
        let chunk_key_encoding: ChunkKeyEncoding = V2ChunkKeyEncoding::new_dot().into();
        let key = data_key(&NodePath::root(), &chunk_key_encoding.encode(&[1, 23, 45]));
        assert_eq!(key, StoreKey::new("1.23.45").unwrap());
    }

    #[test]
    fn slash_scalar() {
        let chunk_key_encoding: ChunkKeyEncoding = V2ChunkKeyEncoding::new_slash().into();
        let key = data_key(&NodePath::root(), &chunk_key_encoding.encode(&[]));
        assert_eq!(key, StoreKey::new("0").unwrap());
    }

    #[test]
    fn dot_scalar() {
        let chunk_key_encoding: ChunkKeyEncoding = V2ChunkKeyEncoding::new_dot().into();
        let key = data_key(&NodePath::root(), &chunk_key_encoding.encode(&[]));
        assert_eq!(key, StoreKey::new("0").unwrap());
    }
}
