use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// The identifier for the `crc32c` codec.
pub const IDENTIFIER: &str = "crc32c";

/// A wrapper to handle various versions of `crc32c` (CRC32C checksum) codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum Crc32cCodecConfiguration {
    /// Version 1.0.
    V1(Crc32cCodecConfigurationV1),
}

/// `crc32c` (CRC32C checksum) codec configuration parameters (version 1.0).
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/crc32c/v1.0.html#configuration-parameters>.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct Crc32cCodecConfigurationV1 {}

#[cfg(test)]
mod tests {
    use crate::metadata::v3::MetadataV3;

    use super::*;

    #[test]
    fn codec_crc32c_config1() {
        serde_json::from_str::<Crc32cCodecConfiguration>(r#"{}"#).unwrap();
    }

    #[test]
    fn codec_crc32c_config_outer1() {
        serde_json::from_str::<MetadataV3>(
            r#"{ 
            "name": "crc32c",
            "configuration": {}
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_crc32c_config_outer2() {
        serde_json::from_str::<MetadataV3>(
            r#"{ 
            "name": "crc32c"
        }"#,
        )
        .unwrap();
    }
}
