use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::metadata::MetadataV3;

/// The identifier for the `vlen` codec.
pub const IDENTIFIER: &str = "vlen";

/// A wrapper to handle various versions of `vlen` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum VlenCodecConfiguration {
    /// Version 1.0.
    V1(VlenCodecConfigurationV1),
}

/// Configuration parameters for the `vlen` codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct VlenCodecConfigurationV1 {
    /// Encoding for the variable length data indices (offsets).
    pub index_codecs: Vec<MetadataV3>,
    /// Encoding for the variable length data.
    pub data_codecs: Vec<MetadataV3>,
    /// The indices data type.
    pub index_data_type: VlenIndexDataType,
}

/// Data types for variable length chunk data indices.
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug, Display)]
#[serde(rename_all = "lowercase")]
pub enum VlenIndexDataType {
    // /// `uint8` Integer in `[0, 2^8-1]`.
    // UInt8,
    // /// `uint16` Integer in `[0, 2^16-1]`.
    // UInt16,
    /// `uint32` Integer in `[0, 2^32-1]`.
    UInt32,
    /// `uint64` Integer in `[0, 2^64-1]`.
    UInt64,
}

impl VlenCodecConfigurationV1 {
    /// Create a new `vlen` codec configuration.
    #[must_use]
    pub fn new(
        index_codecs: Vec<MetadataV3>,
        data_codecs: Vec<MetadataV3>,
        index_data_type: VlenIndexDataType,
    ) -> Self {
        Self {
            index_codecs,
            data_codecs,
            index_data_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_vlen_simple() {
        serde_json::from_str::<VlenCodecConfiguration>(
            r#"{
            "data_codecs": [{"name": "bytes"}],
            "index_codecs": [{"name": "bytes","configuration": { "endian": "little" }}],
            "index_data_type": "uint32"
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_vlen_compressed() {
        serde_json::from_str::<VlenCodecConfiguration>(r#"{
            "data_codecs": [{"name": "bytes"},{"name": "blosc","configuration": {"cname": "zstd", "clevel":5,"shuffle": "bitshuffle", "typesize":1,"blocksize":0}}],
            "index_codecs": [{"name": "bytes","configuration": { "endian": "little" }},{"name": "blosc","configuration":{"cname": "zstd", "clevel":5,"shuffle": "shuffle", "typesize":4,"blocksize":0}}],
            "index_data_type": "uint32"
        }"#).unwrap();
    }
}
