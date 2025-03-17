use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::v3::MetadataConfigurationSerialize;

/// A wrapper to handle various versions of `vlen-bytes` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[non_exhaustive]
#[serde(untagged)]
pub enum VlenBytesCodecConfiguration {
    /// Version 1.0 draft.
    V1(VlenBytesCodecConfigurationV1),
}

impl MetadataConfigurationSerialize for VlenBytesCodecConfiguration {}

/// `vlen-bytes` codec configuration parameters (version 1.0 draft).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct VlenBytesCodecConfigurationV1 {}
