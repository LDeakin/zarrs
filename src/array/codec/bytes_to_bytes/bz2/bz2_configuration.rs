use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::Bz2CompressionLevel;

/// A wrapper to handle various versions of `bz2` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum Bz2CodecConfiguration {
    /// Version 1.0 draft.
    V1(Bz2CodecConfigurationV1),
}

/// Configuration parameters for the `bz2` codec (version 1.0 draft).
///
/// ### Example: encode with a compression level of 9
/// ```rust
/// # let JSON = r#"
/// {
///     "level": 9
/// }
/// # "#;
/// # let configuration: zarrs::array::codec::Bz2CodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct Bz2CodecConfigurationV1 {
    /// The compression level.
    pub level: Bz2CompressionLevel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_bz2_valid1() {
        let json = r#"
        {
            "level": 5
        }"#;
        assert!(serde_json::from_str::<Bz2CodecConfiguration>(json).is_ok());
    }

    #[test]
    fn codec_bz2_invalid_level() {
        let json = r#"
        {
            "level": 10
        }"#;
        let codec_configuration = serde_json::from_str::<Bz2CodecConfiguration>(json);
        assert!(codec_configuration.is_err());
    }
}
