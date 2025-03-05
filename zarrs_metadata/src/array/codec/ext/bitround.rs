use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::v3::MetadataConfiguration;

/// The identifier for the `bitround` codec.
// TODO: ZEP for bitround
pub const IDENTIFIER: &str = "bitround";

/// A wrapper to handle various versions of `bitround` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum BitroundCodecConfiguration {
    /// Version 1.0 draft.
    V1(BitroundCodecConfigurationV1),
}

impl From<BitroundCodecConfiguration> for MetadataConfiguration {
    fn from(configuration: BitroundCodecConfiguration) -> Self {
        let configuration = serde_json::to_value(configuration).unwrap();
        match configuration {
            serde_json::Value::Object(configuration) => configuration,
            _ => unreachable!(),
        }
    }
}

/// `bitround` codec configuration parameters (version 1.0 draft).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
pub struct BitroundCodecConfigurationV1 {
    /// The number of mantissa bits to keep for a floating point data type.
    pub keepbits: u32,
}

#[cfg(test)]
mod tests {
    use crate::v3::MetadataV3;

    use super::*;

    #[test]
    fn codec_bitround_metadata() {
        serde_json::from_str::<MetadataV3>(
            r#"{ 
            "name": "bitround",
            "configuration": {
                "keepbits": 10
            }
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_bitround_config() {
        serde_json::from_str::<BitroundCodecConfiguration>(
            r#"{
                "keepbits": 10
            }"#,
        )
        .unwrap();
    }
}
