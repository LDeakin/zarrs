use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use zarrs_metadata::ConfigurationSerialize;

/// A wrapper to handle various versions of `squeeze` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[non_exhaustive]
#[serde(untagged)]
pub enum SqueezeCodecConfiguration {
    /// Version 1.0.
    V1(SqueezeCodecConfigurationV1),
}

impl ConfigurationSerialize for SqueezeCodecConfiguration {}

/// `squeeze` codec configuration parameters (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct SqueezeCodecConfigurationV1 {}
