use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::v3::MetadataConfiguration;

/// The identifier for the `vlen-bytes` codec.
pub const IDENTIFIER: &str = "vlen-bytes";

/// A wrapper to handle various versions of `vlen-bytes` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum VlenBytesCodecConfiguration {
    /// Version 1.0 draft.
    V1(VlenBytesCodecConfigurationV1),
}

impl From<VlenBytesCodecConfiguration> for MetadataConfiguration {
    fn from(configuration: VlenBytesCodecConfiguration) -> Self {
        let configuration = serde_json::to_value(configuration).unwrap();
        match configuration {
            serde_json::Value::Object(configuration) => configuration,
            _ => unreachable!(),
        }
    }
}

/// `vlen-bytes` codec configuration parameters (version 1.0 draft).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct VlenBytesCodecConfigurationV1 {}
