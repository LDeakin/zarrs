use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::GzipCompressionLevel;

/// A wrapper to handle various versions of `gzip` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum GzipCodecConfiguration {
    /// Version 1.0.
    V1(GzipCodecConfigurationV1),
}

/// Configuration parameters for the `gzip` codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct GzipCodecConfigurationV1 {
    /// The compression level.
    pub level: GzipCompressionLevel,
}

impl GzipCodecConfigurationV1 {
    /// Create a new `gzip` codec configuration given a [`GzipCompressionLevel`].
    #[must_use]
    pub const fn new(level: GzipCompressionLevel) -> Self {
        Self { level }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_gzip_configuration_valid() {
        const JSON_VALID: &str = r#"{
            "level": 1
        }"#;
        serde_json::from_str::<GzipCodecConfiguration>(JSON_VALID).unwrap();
    }

    #[test]
    fn codec_gzip_configuration_invalid1() {
        const JSON_INVALID1: &str = r#"{
            "level": -1
        }"#;
        assert!(serde_json::from_str::<GzipCodecConfiguration>(JSON_INVALID1).is_err());
    }

    #[test]
    fn codec_gzip_configuration_invalid2() {
        const JSON_INVALID2: &str = r#"{
            "level": 10
        }"#;
        assert!(serde_json::from_str::<GzipCodecConfiguration>(JSON_INVALID2).is_err());
    }
}
