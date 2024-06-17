use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// The identifier for the `vlen_interleaved` codec.
pub const IDENTIFIER: &str = "https://codec.zarrs.dev/array_to_bytes/vlen_interleaved";

/// A wrapper to handle various versions of `vlen_interleaved` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum VlenInterleavedCodecConfiguration {
    /// Version 1.0.
    V1(VlenInterleavedCodecConfigurationV1),
}

/// Configuration parameters for the `vlen_interleaved` codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, Default)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct VlenInterleavedCodecConfigurationV1 {}

impl VlenInterleavedCodecConfigurationV1 {
    /// Create a new `vlen_interleaved` codec configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_vlen_interleaved() {
        serde_json::from_str::<VlenInterleavedCodecConfiguration>(r#"{}"#).unwrap();
    }
}
