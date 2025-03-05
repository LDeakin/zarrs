use derive_more::{Display, From};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    array::codec::zfp::{ZfpCodecConfigurationV1, ZfpMode},
    v3::MetadataConfiguration,
};

/// The identifier for the `zfpy` codec.
pub const IDENTIFIER: &str = "zfpy";

/// A wrapper to handle various versions of `zfpy` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display, From)]
#[non_exhaustive]
#[serde(untagged)]
pub enum ZfpyCodecConfiguration {
    /// `numcodecs` version 0.8.
    Numcodecs(ZfpyCodecConfigurationNumcodecs),
}

impl From<ZfpyCodecConfiguration> for MetadataConfiguration {
    fn from(configuration: ZfpyCodecConfiguration) -> Self {
        let configuration = serde_json::to_value(configuration).unwrap();
        match configuration {
            serde_json::Value::Object(configuration) => configuration,
            _ => unreachable!(),
        }
    }
}

/// `zfpy` codec configuration parameters (numcodecs).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display)]
// #[serde(deny_unknown_fields)] // FIXME: zarr-python includes redundant compression_kwargs. Report upstream
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ZfpyCodecConfigurationNumcodecs {
    /// The zfp codec configuration mode.
    #[serde(flatten)]
    pub mode: ZfpyCodecConfigurationMode,
}

/// The `zfpy` codec configuration mode.
#[derive(Clone, PartialEq, Debug, Display)]
// #[serde(tag = "mode")]
pub enum ZfpyCodecConfigurationMode {
    /// Fixed rate mode.
    // #[serde(rename = 2)]
    FixedRate {
        /// The rate is the number of compressed bits per value.
        rate: f64,
    },
    /// Fixed precision mode.
    // #[serde(rename = 3)]
    FixedPrecision {
        /// The precision specifies how many uncompressed bits per value to store, and indirectly governs the relative error.
        precision: u32,
    },
    /// Fixed accuracy mode.
    // #[serde(rename = 4)]
    FixedAccuracy {
        /// The tolerance ensures that values in the decompressed array differ from the input array by no more than this tolerance.
        tolerance: f64,
    },
}

// Custom deserialize because serde does not support integer tags https://github.com/serde-rs/serde/issues/745
impl<'de> Deserialize<'de> for ZfpyCodecConfigurationMode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Tagged {
            mode: u64,
            rate: Option<f64>,
            precision: Option<i32>, // zarr-python/numcodecs defaults to -1
            tolerance: Option<f64>,
        }

        let value = Tagged::deserialize(d)?;
        match value {
            Tagged{mode: 2, rate: Some(rate), precision: _, tolerance: _} => {
                Ok(ZfpyCodecConfigurationMode::FixedRate { rate })
            }
            Tagged{mode: 3, rate: _, precision: Some(precision), tolerance: _} => {
                Ok(ZfpyCodecConfigurationMode::FixedPrecision { precision:
                    u32::try_from(precision).map_err(|_| serde::de::Error::custom("`precision` must be a positive integer"))?
                })
            }
            Tagged{mode: 4, rate: _, precision: _, tolerance: Some(tolerance)} => {
                Ok(ZfpyCodecConfigurationMode::FixedAccuracy { tolerance })
            }
            _ => Err(serde::de::Error::custom("expected `mode` to be 2, 3, or 4 with `rate`/`precision`/`tolerance` set appropriately")),
        }
    }
}

impl Serialize for ZfpyCodecConfigurationMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize, Default)]
        struct Tagged {
            mode: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            rate: Option<f64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            precision: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            tolerance: Option<f64>,
        }
        match self {
            ZfpyCodecConfigurationMode::FixedRate { rate } => Tagged {
                mode: 2,
                rate: Some(*rate),
                ..Default::default()
            },
            ZfpyCodecConfigurationMode::FixedPrecision { precision } => Tagged {
                mode: 3,
                precision: Some(*precision),
                ..Default::default()
            },
            ZfpyCodecConfigurationMode::FixedAccuracy { tolerance } => Tagged {
                mode: 4,
                tolerance: Some(*tolerance),
                ..Default::default()
            },
        }
        .serialize(serializer)
    }
}

/// Convert [`ZfpyCodecConfigurationNumcodecs`] to [`ZfpCodecConfigurationV1`].
#[must_use]
pub fn codec_zfpy_v2_numcodecs_to_v3(
    zfpy: &ZfpyCodecConfigurationNumcodecs,
) -> ZfpCodecConfigurationV1 {
    let mode = match zfpy.mode {
        ZfpyCodecConfigurationMode::FixedRate { rate } => ZfpMode::FixedRate { rate },
        ZfpyCodecConfigurationMode::FixedPrecision { precision } => {
            ZfpMode::FixedPrecision { precision }
        }
        ZfpyCodecConfigurationMode::FixedAccuracy { tolerance } => {
            ZfpMode::FixedAccuracy { tolerance }
        }
    };
    ZfpCodecConfigurationV1 { mode }
}

#[cfg(test)]
mod tests {
    use crate::array::codec::zfp::ZfpCodecConfigurationV1;

    use super::*;

    #[test]
    fn codec_zfpy_fixed_rate() {
        let v2 = serde_json::from_str::<ZfpyCodecConfigurationNumcodecs>(
            r#"
        {
            "mode": 2,
            "rate": 0.123
        }
        "#,
        )
        .unwrap();
        assert_eq!(
            v2.mode,
            ZfpyCodecConfigurationMode::FixedRate { rate: 0.123 }
        );
        let ZfpCodecConfigurationV1 { mode } = codec_zfpy_v2_numcodecs_to_v3(&v2);
        let ZfpMode::FixedRate { rate } = mode else {
            panic!()
        };
        assert_eq!(rate, 0.123);
    }

    #[test]
    fn codec_zfpy_fixed_precision() {
        let v2 = serde_json::from_str::<ZfpyCodecConfigurationNumcodecs>(
            r#"
        {
            "mode": 3,
            "precision": 10
        }
        "#,
        )
        .unwrap();
        assert_eq!(
            v2.mode,
            ZfpyCodecConfigurationMode::FixedPrecision { precision: 10 }
        );
        let ZfpCodecConfigurationV1 { mode } = codec_zfpy_v2_numcodecs_to_v3(&v2);
        let ZfpMode::FixedPrecision { precision } = mode else {
            panic!()
        };
        assert_eq!(precision, 10);
    }

    #[test]
    fn codec_zfpy_fixed_accuracy() {
        let v2 = serde_json::from_str::<ZfpyCodecConfigurationNumcodecs>(
            r#"
        {
            "mode": 4,
            "tolerance": 0.123
        }
        "#,
        )
        .unwrap();
        assert_eq!(
            v2.mode,
            ZfpyCodecConfigurationMode::FixedAccuracy { tolerance: 0.123 }
        );
        let ZfpCodecConfigurationV1 { mode } = codec_zfpy_v2_numcodecs_to_v3(&v2);
        let ZfpMode::FixedAccuracy { tolerance } = mode else {
            panic!()
        };
        assert_eq!(tolerance, 0.123);
    }

    // #[test]
    // fn codec_zfpy_reversible() {
    //     let v2 = serde_json::from_str::<ZfpyCodecConfigurationNumcodecs>(
    //         r#"
    //     {
    //         "mode": 5
    //     }
    //     "#,
    //     )
    //     .unwrap();
    //     assert_eq!(v2.mode, ZfpyCodecConfigurationMode::Reversible);
    //     let ZfpCodecConfigurationV1 { write_header, mode } = codec_zfpy_v2_numcodecs_to_v3(&v2);
    //     assert_eq!(write_header, Some(true));
    //     let ZfpMode::Reversible = mode else { panic!() };
    // }
}
