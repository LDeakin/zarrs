use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::metadata::Metadata;

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
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct ShardingCodecConfigurationV1 {
    /// An array of integers specifying the shape of the inner chunks in a shard along each dimension of the outer array.
    pub chunk_shape: Vec<u64>,
    /// A list of codecs to be used for encoding and decoding inner chunks.
    pub codecs: Vec<Metadata>,
    /// A list of codecs to be used for encoding and decoding shard index.
    pub index_codecs: Vec<Metadata>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_sharding_configuration() {
        const JSON: &'static str = r#"{
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
        serde_json::from_str::<ShardingCodecConfiguration>(JSON).unwrap();
    }
}
