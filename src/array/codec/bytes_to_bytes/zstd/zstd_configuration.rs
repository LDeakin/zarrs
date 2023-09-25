use derive_more::{Display, From};
use serde::{Deserialize, Serialize};
use zstd::zstd_safe;

/// A wrapper to handle various versions of Zstd codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ZstdCodecConfiguration {
    /// Version 1.0.
    V1(ZstdCodecConfigurationV1),
}

/// Configuration parameters for the Zstd codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct ZstdCodecConfigurationV1 {
    /// The compression level.
    pub level: ZstdCompressionLevel,
    /// A boolean that indicates whether to store a checksum when writing that will be verified when reading.
    pub checksum: bool,
}

impl ZstdCodecConfigurationV1 {
    /// Create a new Zstd codec configuration given a [`ZstdCompressionLevel`].
    #[must_use]
    pub fn new(level: ZstdCompressionLevel, checksum: bool) -> ZstdCodecConfigurationV1 {
        ZstdCodecConfigurationV1 { level, checksum }
    }
}

/// A Zstd compression level. An integer from -131072 to 22 which controls the speed and level of compression (has no impact on decoding).
///
/// A value of 0 indicates to use the default compression level.
/// Otherwise, a higher level is expected to achieve a higher compression ratio at the cost of lower speed.
#[derive(Serialize, Clone, Eq, PartialEq, Debug)]
pub struct ZstdCompressionLevel(zstd_safe::CompressionLevel);

impl<'de> serde::Deserialize<'de> for ZstdCompressionLevel {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let number = serde_json::Number::deserialize(d)?;
        if let Some(number) = number.as_i64() {
            if (-131_072..=22).contains(&number) {
                #[allow(clippy::cast_possible_truncation)]
                return Ok(ZstdCompressionLevel(number as i32));
            }
        }
        Err(serde::de::Error::custom(
            "Zstd compression level must be an integer between -131072 and 22",
        ))
    }
}

impl ZstdCompressionLevel {
    /// Create a new zstd compression level.
    #[must_use]
    pub fn new(level: zstd_safe::CompressionLevel) -> Self {
        Self(level)
    }
}

impl From<zstd_safe::CompressionLevel> for ZstdCompressionLevel {
    fn from(value: zstd_safe::CompressionLevel) -> Self {
        Self(value)
    }
}

impl From<ZstdCompressionLevel> for zstd_safe::CompressionLevel {
    fn from(value: ZstdCompressionLevel) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_zstd_configuration_valid() {
        const JSON_VALID: &'static str = r#"{
        "level": 22,
        "checksum": false
    }"#;
        serde_json::from_str::<ZstdCodecConfiguration>(JSON_VALID).unwrap();
    }

    #[test]
    fn codec_zstd_configuration_invalid1() {
        const JSON_INVALID1: &'static str = r#"{
        "level": 5
    }"#;
        assert!(serde_json::from_str::<ZstdCodecConfiguration>(JSON_INVALID1).is_err());
    }

    #[test]
    fn codec_zstd_configuration_invalid2() {
        const JSON_INVALID2: &'static str = r#"{
        "level": 23,
        "checksum": true
    }"#;
        assert!(serde_json::from_str::<ZstdCodecConfiguration>(JSON_INVALID2).is_err());
    }
}
