use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::{PcodecCompressionLevel, PcodecDeltaEncodingOrder};

/// A wrapper to handle various versions of `pcodec` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum PcodecCodecConfiguration {
    /// Version 1.0 draft.
    V1(PcodecCodecConfigurationV1),
}

impl Default for PcodecCodecConfiguration {
    fn default() -> Self {
        Self::V1(PcodecCodecConfigurationV1::default())
    }
}

/// Configuration parameters for the `pcodec` codec (version 1.0 draft).
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct PcodecCodecConfigurationV1 {
    /// A compression level from 0-12, where 12 takes the longest and compresses the most.
    ///
    /// The default is 8.
    #[serde(default = "default_compression")]
    pub level: PcodecCompressionLevel,
    /// Either a delta encoding level from 0-7 or None.
    ///
    /// If set to None, pcodec will try to infer the optimal delta encoding order.
    /// The default is None.
    #[serde(default)]
    pub delta_encoding_order: Option<PcodecDeltaEncodingOrder>,
    /// If enabled, pcodec will consider using int mult mode, which can substantially improve compression ratio but decrease speed in some cases for integer types.
    ///
    /// The default is true.
    #[serde(default = "default_mult_spec")]
    pub int_mult_spec: bool,
    /// If enabled, pcodec will consider using float mult mode, which can substantially improve compression ratio but decrease speed in some cases for float types.
    ///
    /// The default is true.
    #[serde(default = "default_mult_spec")]
    pub float_mult_spec: bool,
    /// The maximum number of values to encode per pcodec page.
    ///
    /// If set too high or too low, pcodec's compression ratio may drop.
    /// See <https://docs.rs/pco/latest/pco/enum.PagingSpec.html#variant.EqualPagesUpTo>.
    ///
    /// The default is `1 << 18`.
    #[serde(default = "default_max_page_n")]
    pub max_page_n: usize,
}

impl Default for PcodecCodecConfigurationV1 {
    fn default() -> Self {
        PcodecCodecConfigurationV1 {
            level: default_compression(),
            delta_encoding_order: None,
            int_mult_spec: default_mult_spec(),
            float_mult_spec: default_mult_spec(),
            max_page_n: default_max_page_n(),
        }
    }
}

fn default_compression() -> PcodecCompressionLevel {
    PcodecCompressionLevel::default()
}

fn default_mult_spec() -> bool {
    true
}

fn default_max_page_n() -> usize {
    // pco::constants::DEFAULT_MAX_PAGE_N
    1 << 18
}

impl PcodecCodecConfigurationV1 {
    // /// Create a new `pcodec` codec configuration.
    // #[must_use]
    // pub const fn new(endian: Option<Endianness>) -> Self {
    //     Self { endian }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_pcodec_valid_empty() {
        serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_valid() {
        serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 8,
            "delta_encoding_order": 2,
            "int_mult_spec": true,
            "float_mult_spec": true,
            "max_page_n": 262144
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_invalid_level() {
        assert!(serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 13,
            "delta_encoding_order": 2,
            "int_mult_spec": true,
            "float_mult_spec": true,
            "max_page_n": 262144
        }"#,
        )
        .is_err());
    }

    #[test]
    fn codec_pcodec_invalid_delta_encoding_order() {
        assert!(serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 8,
            "delta_encoding_order": 8,
            "int_mult_spec": true,
            "float_mult_spec": true,
            "max_page_n": 262144
        }"#,
        )
        .is_err());
    }
}
