use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::LzmaCompressionLevel;

/// A wrapper to handle various versions of `lzma` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum LzmaCodecConfiguration {
    /// Version 1.0 draft.
    V1(LzmaCodecConfigurationV1),
}

/// Configuration parameters for the `lzma` codec (version 1.0 draft).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct LzmaCodecConfigurationV1 {
    /// The compression level.
    pub level: LzmaCompressionLevel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_lzma_valid1() {
        let json = r#"{"level":5}"#;
        assert!(serde_json::from_str::<LzmaCodecConfiguration>(json).is_ok());
    }

    #[test]
    fn codec_lzma_invalid_level() {
        let json = r#"{"level": 10}"#;
        let codec_configuration = serde_json::from_str::<LzmaCodecConfiguration>(json);
        assert!(codec_configuration.is_err());
    }
}
