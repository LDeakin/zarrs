use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// A wrapper to handle various versions of `vlen_v2` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[non_exhaustive]
#[serde(untagged)]
pub enum VlenV2CodecConfiguration {
    /// Version 1.0 draft.
    V1(VlenV2CodecConfigurationV1),
}

/// `vlen_v2` codec configuration parameters (version 1.0 draft).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, Default)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct VlenV2CodecConfigurationV1 {}

impl VlenV2CodecConfigurationV1 {
    /// Create a new `vlen_v2` codec configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_vlen_v2() {
        serde_json::from_str::<VlenV2CodecConfiguration>(r#"{}"#).unwrap();
    }
}
