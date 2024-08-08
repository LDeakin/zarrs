use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::{array::ChunkShape, metadata::v3::MetadataV3};

/// The identifier for the `sharding_indexed` codec.
pub const IDENTIFIER: &str = "sharding_indexed";

/// A wrapper to handle various versions of Sharding codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ShardingCodecConfiguration {
    /// Version 1.0.
    V1(ShardingCodecConfigurationV1),
}

/// Sharding codec configuration parameters.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/v1.0.html#configuration-parameters>.
///
/// ### Example sharding codec configuration
/// ```rust
/// # let JSON = r#"
/// {
///     "chunk_shape": [32, 32, 32],
///     "codecs": [
///         {
///             "name": "endian",
///             "configuration": {
///                 "endian": "little"
///             }
///         },
///         {
///             "name": "gzip",
///             "configuration": {
///                 "level": 1
///             }
///         }
///     ],
///     "index_codecs": [
///         {
///             "name": "endian",
///             "configuration": {
///                 "endian": "little"
///             }
///         },
///         { "name": "crc32c" }
///     ]
/// }
/// # "#;
/// # use zarrs::metadata::v3::codec::sharding::ShardingCodecConfigurationV1;
/// # let configuration: ShardingCodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
/// ```
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ShardingCodecConfigurationV1 {
    /// An array of integers specifying the shape of the inner chunks in a shard along each dimension of the outer array.
    pub chunk_shape: ChunkShape,
    /// A list of codecs to be used for encoding and decoding inner chunks.
    pub codecs: Vec<MetadataV3>,
    /// A list of codecs to be used for encoding and decoding the shard index.
    pub index_codecs: Vec<MetadataV3>,
    /// Specifies whether the shard index is located at the beginning or end of the file.
    #[serde(default)]
    pub index_location: ShardingIndexLocation,
}

/// The sharding index location.
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug, Display)]
#[serde(rename_all = "lowercase")]
pub enum ShardingIndexLocation {
    /// The index is at the start of the shard, before the chunks.
    Start,
    /// The index is at the end of the shard, after the chunks.
    End,
}

impl Default for ShardingIndexLocation {
    fn default() -> Self {
        Self::End
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_sharding_configuration() {
        const JSON: &str = r#"{
            "chunk_shape": [2, 2],
            "codecs": [
                {
                    "name": "bytes",
                    "configuration": {
                        "endian": "little"
                    }
                }
            ],
            "index_codecs": [
                {
                    "name": "bytes",
                    "configuration": {
                        "endian": "little"
                    }
                }
            ]
        }"#;
        let config = serde_json::from_str::<ShardingCodecConfiguration>(JSON).unwrap();

        // check that index_location is added if missing
        assert_eq!(
            config.to_string(),
            r#"{"chunk_shape":[2,2],"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"index_codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"index_location":"end"}"#
        );

        let ShardingCodecConfiguration::V1(config) = config;
        assert_eq!(config.index_location, ShardingIndexLocation::End);
    }
}
