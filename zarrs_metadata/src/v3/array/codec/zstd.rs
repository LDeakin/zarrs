use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// The identifier for the `zstd` codec.
pub const IDENTIFIER: &str = "zstd";

/// A wrapper to handle various versions of `zstd` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ZstdCodecConfiguration {
    /// Version 1.0.
    V1(ZstdCodecConfigurationV1),
}

/// Configuration parameters for the `zstd` codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ZstdCodecConfigurationV1 {
    /// The compression level.
    pub level: ZstdCompressionLevel,
    /// A boolean that indicates whether to store a checksum when writing that will be verified when reading.
    pub checksum: bool,
}

impl ZstdCodecConfigurationV1 {
    /// Create a new `zstd` codec configuration given a [`ZstdCompressionLevel`].
    #[must_use]
    pub const fn new(level: ZstdCompressionLevel, checksum: bool) -> Self {
        Self { level, checksum }
    }
}

/// A `Zstd` compression level. An integer from -131072 to 22 which controls the speed and level of compression (has no impact on decoding).
///
/// A value of 0 indicates to use the default compression level.
/// Otherwise, a higher level is expected to achieve a higher compression ratio at the cost of lower speed.
#[derive(Serialize, Clone, Eq, PartialEq, Debug)]
pub struct ZstdCompressionLevel(i32);

impl<'de> serde::Deserialize<'de> for ZstdCompressionLevel {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let number = serde_json::Number::deserialize(d)?;
        if let Some(number) = number.as_i64() {
            if (-131_072..=22).contains(&number) {
                #[allow(clippy::cast_possible_truncation)]
                return Ok(Self(number as i32));
            }
        }
        Err(serde::de::Error::custom(
            "Zstd compression level must be an integer between -131072 and 22",
        ))
    }
}

impl ZstdCompressionLevel {
    /// Create a new `Zstd` compression level.
    #[must_use]
    pub const fn new(level: i32) -> Self {
        Self(level)
    }
}

impl From<i32> for ZstdCompressionLevel {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<ZstdCompressionLevel> for i32 {
    fn from(value: ZstdCompressionLevel) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_zstd_configuration_valid() {
        const JSON_VALID: &str = r#"{
        "level": 22,
        "checksum": false
    }"#;
        serde_json::from_str::<ZstdCodecConfiguration>(JSON_VALID).unwrap();
    }

    #[test]
    fn codec_zstd_configuration_invalid1() {
        const JSON_INVALID1: &str = r#"{
        "level": 5
    }"#;
        assert!(serde_json::from_str::<ZstdCodecConfiguration>(JSON_INVALID1).is_err());
    }

    #[test]
    fn codec_zstd_configuration_invalid2() {
        const JSON_INVALID2: &str = r#"{
        "level": 23,
        "checksum": true
    }"#;
        assert!(serde_json::from_str::<ZstdCodecConfiguration>(JSON_INVALID2).is_err());
    }
}
