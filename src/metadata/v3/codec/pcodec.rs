use derive_more::{Display, From};
use serde::{Deserialize, Deserializer, Serialize};

/// The identifier for the `pcodec` codec.
// TODO: ZEP for pcodec when stabilised
pub const IDENTIFIER: &str = "pcodec";

/// A wrapper to handle various versions of `pcodec` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display, From)]
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
///
/// Based upon [`pco::ChunkConfig`].
///
/// ### Example: encode with a compression level of 12 and otherwise default parameters
/// ```rust
/// # let JSON = r#"
/// {
///     "level": 12
/// }
/// # "#;
/// # use zarrs::metadata::v3::codec::pcodec::PcodecCodecConfigurationV1;
/// # let configuration: PcodecCodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
// TODO: Examples for more advanced configurations
// TODO: Docs about valid usage with base/k
#[derive(Clone, PartialEq, Debug, Display)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct PcodecCodecConfigurationV1 {
    /// A compression level from 0-12, where 12 takes the longest and compresses the most.
    ///
    /// The default is 8.
    pub level: PcodecCompressionLevel,
    /// Either a delta encoding level from 0-7 or None.
    ///
    /// If set to None, pcodec will try to infer the optimal delta encoding order.
    /// The default is None.
    pub delta_encoding_order: Option<PcodecDeltaEncodingOrder>,
    /// The pcodec mode spec.
    pub mode_spec: PcodecModeSpecConfiguration,
    /// The maximum number of values to encode per pcodec page.
    ///
    /// If set too high or too low, pcodec's compression ratio may drop.
    /// See <https://docs.rs/pco/latest/pco/enum.PagingSpec.html#variant.EqualPagesUpTo>.
    ///
    /// The default is `1 << 18`.
    pub equal_pages_up_to: usize,
}

/// Specifies how Pco should choose a [`mode`][pco::Mode] to compress this
/// chunk of data.
///
/// see [`pco::ModeSpec`].
#[derive(Deserialize, Clone, Copy, PartialEq, Debug, Display)]
pub enum PcodecModeSpecConfiguration {
    /// Automatically detect a good mode.
    ///
    /// This works well most of the time, but costs some compression time and can
    /// select a bad mode in adversarial cases.
    Auto,
    /// Only use `Classic` mode.
    Classic,
    /// Try using `FloatMult` mode with a given `base`.
    ///
    /// Only applies to floating-point types.
    TryFloatMult(f64),
    /// Try using `FloatQuant` mode with `k` bits of quantization.
    ///
    /// Only applies to floating-point types.
    TryFloatQuant(u32),
    /// Try using `IntMult` mode with a given `base`.
    ///
    /// Only applies to integer types.
    TryIntMult(u64),
}

#[derive(Serialize, Deserialize, Debug)]
// #[serde(untagged)]
#[serde(rename_all = "snake_case")]
enum PcodecModeSpecConfigurationIntermediate {
    Auto,
    Classic,
    TryFloatMult,
    TryFloatQuant,
    TryIntMult,
}

impl Default for PcodecModeSpecConfigurationIntermediate {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum UIntOrFloat {
    UInt(u64),
    Float(f64),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct PcodecCodecConfigurationIntermediate {
    #[serde(default)]
    level: PcodecCompressionLevel,
    #[serde(default)]
    delta_encoding_order: Option<PcodecDeltaEncodingOrder>,
    #[serde(default)]
    mode_spec: PcodecModeSpecConfigurationIntermediate,
    #[serde(skip_serializing_if = "Option::is_none")]
    base: Option<UIntOrFloat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    k: Option<u32>,
    #[serde(default = "default_equal_pages_up_to")]
    equal_pages_up_to: usize,
}

impl serde::Serialize for PcodecCodecConfigurationV1 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let (mode_spec, base, k) = match self.mode_spec {
            PcodecModeSpecConfiguration::Auto => {
                (PcodecModeSpecConfigurationIntermediate::Auto, None, None)
            }
            PcodecModeSpecConfiguration::Classic => {
                (PcodecModeSpecConfigurationIntermediate::Classic, None, None)
            }
            PcodecModeSpecConfiguration::TryFloatMult(base) => (
                PcodecModeSpecConfigurationIntermediate::TryFloatMult,
                Some(UIntOrFloat::Float(base)),
                None,
            ),
            PcodecModeSpecConfiguration::TryFloatQuant(k) => (
                PcodecModeSpecConfigurationIntermediate::TryFloatQuant,
                None,
                Some(k),
            ),
            PcodecModeSpecConfiguration::TryIntMult(base) => (
                PcodecModeSpecConfigurationIntermediate::TryIntMult,
                Some(UIntOrFloat::UInt(base)),
                None,
            ),
        };

        let config = PcodecCodecConfigurationIntermediate {
            level: self.level,
            delta_encoding_order: self.delta_encoding_order,
            mode_spec,
            base,
            k,
            equal_pages_up_to: self.equal_pages_up_to,
        };
        config.serialize(s)
    }
}

impl<'de> serde::Deserialize<'de> for PcodecCodecConfigurationV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let config = PcodecCodecConfigurationIntermediate::deserialize(d)?;
        let mode_spec = match (config.mode_spec, config.base, config.k) {
            (PcodecModeSpecConfigurationIntermediate::Auto, None, None) => {
                Ok(PcodecModeSpecConfiguration::Auto)
            }
            (PcodecModeSpecConfigurationIntermediate::Classic, None, None) => {
                Ok(PcodecModeSpecConfiguration::Classic)
            }
            (
                PcodecModeSpecConfigurationIntermediate::TryFloatMult,
                Some(UIntOrFloat::Float(base)),
                None,
            ) => Ok(PcodecModeSpecConfiguration::TryFloatMult(base)),
            (PcodecModeSpecConfigurationIntermediate::TryFloatQuant, None, Some(k)) => {
                Ok(PcodecModeSpecConfiguration::TryFloatQuant(k))
            }
            (
                PcodecModeSpecConfigurationIntermediate::TryIntMult,
                Some(UIntOrFloat::UInt(base)),
                None,
            ) => Ok(PcodecModeSpecConfiguration::TryIntMult(base)),
            _ => Err(serde::de::Error::custom(
                "For requested mode_spec, base or k incompatible/missing",
            )),
        }?;
        let config = Self {
            level: config.level,
            delta_encoding_order: config.delta_encoding_order,
            mode_spec,
            equal_pages_up_to: config.equal_pages_up_to,
        };
        Ok(config)
    }
}

impl Default for PcodecCodecConfigurationV1 {
    fn default() -> Self {
        Self {
            level: PcodecCompressionLevel::default(),
            delta_encoding_order: None,
            mode_spec: PcodecModeSpecConfiguration::Auto,
            equal_pages_up_to: default_equal_pages_up_to(),
        }
    }
}

/// An integer from 0 to 12 controlling the compression level.
///
/// See <https://docs.rs/pco/latest/pco/struct.ChunkConfig.html#structfield.compression_level>.
///
/// - Level 0 achieves only a small amount of compression.
/// - Level 8 achieves very good compression and runs only slightly slower.
/// - Level 12 achieves marginally better compression than 8 and may run several times slower.
#[derive(Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub struct PcodecCompressionLevel(u8);

impl Default for PcodecCompressionLevel {
    fn default() -> Self {
        Self(8)
    }
}

macro_rules! pcodec_compression_level_try_from {
    ( $t:ty ) => {
        impl TryFrom<$t> for PcodecCompressionLevel {
            type Error = $t;
            fn try_from(level: $t) -> Result<Self, Self::Error> {
                if level <= 12 {
                    Ok(Self(unsafe { u8::try_from(level).unwrap_unchecked() }))
                } else {
                    Err(level)
                }
            }
        }
    };
}

pcodec_compression_level_try_from!(u8);
pcodec_compression_level_try_from!(u16);
pcodec_compression_level_try_from!(u32);
pcodec_compression_level_try_from!(u64);
pcodec_compression_level_try_from!(usize);

impl<'de> Deserialize<'de> for PcodecCompressionLevel {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let level = u8::deserialize(d)?;
        if level <= 12 {
            Ok(Self(level))
        } else {
            Err(serde::de::Error::custom(
                "pcodec compression level must be between 0 and 12",
            ))
        }
    }
}

impl PcodecCompressionLevel {
    /// Create a new compression level.
    ///
    /// # Errors
    /// Errors if `compression_level` is not between 0-12.
    pub fn new<N: num::Unsigned + std::cmp::PartialOrd<u8>>(compression_level: N) -> Result<Self, N>
    where
        u8: TryFrom<N>,
    {
        if compression_level <= 12 {
            Ok(Self(unsafe {
                u8::try_from(compression_level).unwrap_unchecked()
            }))
        } else {
            Err(compression_level)
        }
    }

    /// The underlying integer compression level.
    #[must_use]
    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

/// An integer from 0 to 7 controlling the delta encoding order.
///
/// It is the number of times to apply delta encoding before compressing.
/// See <https://docs.rs/pco/latest/pco/struct.ChunkConfig.html#structfield.delta_encoding_order>.
///
/// - 0th order takes numbers as-is. This is perfect for columnar data were the order is essentially random.
/// - 1st order takes consecutive differences, leaving `[0, 2, 0, 2, 0, 2, 0]`. This is best for continuous but noisy time series data, like stock prices or most time series data.
/// - 2nd order takes consecutive differences again, leaving `[2, -2, 2, -2, 2, -2]`. This is best for piecewise-linear or somewhat quadratic data.
/// - Even higher-order is best for time series that are very smooth, like temperature or light sensor readings.
#[derive(Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub struct PcodecDeltaEncodingOrder(u8);

macro_rules! pcodec_delta_encoding_order_level_try_from {
    ( $t:ty ) => {
        impl TryFrom<$t> for PcodecDeltaEncodingOrder {
            type Error = $t;
            fn try_from(level: $t) -> Result<Self, Self::Error> {
                if level <= 7 {
                    Ok(Self(unsafe { u8::try_from(level).unwrap_unchecked() }))
                } else {
                    Err(level)
                }
            }
        }
    };
}

pcodec_delta_encoding_order_level_try_from!(u8);
pcodec_delta_encoding_order_level_try_from!(u16);
pcodec_delta_encoding_order_level_try_from!(u32);
pcodec_delta_encoding_order_level_try_from!(u64);
pcodec_delta_encoding_order_level_try_from!(usize);

impl<'de> Deserialize<'de> for PcodecDeltaEncodingOrder {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let level = u8::deserialize(d)?;
        if level <= 7 {
            Ok(Self(level))
        } else {
            Err(serde::de::Error::custom(
                "pcodec compression level must be between 0 and 7",
            ))
        }
    }
}

impl PcodecDeltaEncodingOrder {
    /// Create a new delta encoding order.
    ///
    /// # Errors
    /// Errors if `delta_encoding_order` is not between 0-7.
    pub fn new<N: num::Unsigned + std::cmp::PartialOrd<u8>>(
        delta_encoding_order: N,
    ) -> Result<Self, N>
    where
        u8: TryFrom<N>,
    {
        if delta_encoding_order <= 7 {
            Ok(Self(unsafe {
                u8::try_from(delta_encoding_order).unwrap_unchecked()
            }))
        } else {
            Err(delta_encoding_order)
        }
    }

    /// The underlying delta encoding order.
    #[must_use]
    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

const fn default_equal_pages_up_to() -> usize {
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
    fn codec_pcodec_valid_auto() {
        serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 8,
            "delta_encoding_order": 2,
            "mode_spec": "auto",
            "equal_pages_up_to": 262144
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_valid_classic() {
        serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 8,
            "delta_encoding_order": 2,
            "mode_spec": "classic",
            "equal_pages_up_to": 262144
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_valid_try_float_mult() {
        serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 8,
            "delta_encoding_order": 2,
            "mode_spec": "try_float_mult",
            "base": 0.1,
            "equal_pages_up_to": 262144
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_valid_try_float_quant() {
        serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 8,
            "delta_encoding_order": 2,
            "mode_spec": "try_float_quant",
            "k": 1,
            "equal_pages_up_to": 262144
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_valid_try_int_mult() {
        serde_json::from_str::<PcodecCodecConfiguration>(
            r#"{
            "level": 8,
            "delta_encoding_order": 2,
            "mode_spec": "try_int_mult",
            "base": 1,
            "equal_pages_up_to": 262144
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
            "mode_spec": "auto",
            "equal_pages_up_to": 262144
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
            "mode_spec": "auto",
            "equal_pages_up_to": 262144
        }"#,
        )
        .is_err());
    }
}
