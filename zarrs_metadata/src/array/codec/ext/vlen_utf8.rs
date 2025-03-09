use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::v3::MetadataConfigurationSerialize;

/// The identifier for the `vlen-utf8` codec.
pub const IDENTIFIER: &str = "vlen-utf8";

/// A wrapper to handle various versions of `vlen-utf8` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[non_exhaustive]
#[serde(untagged)]
pub enum VlenUtf8CodecConfiguration {
    /// Version 1.0 draft.
    V1(VlenUtf8CodecConfigurationV1),
}

impl MetadataConfigurationSerialize for VlenUtf8CodecConfiguration {}

/// `vlen-utf8` codec configuration parameters (version 1.0 draft).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct VlenUtf8CodecConfigurationV1 {}
