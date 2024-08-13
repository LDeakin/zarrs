use derive_more::Display;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::metadata::{
    v3::codec::zfp::{ZfpCodecConfiguration, ZfpCodecConfigurationV1, ZfpMode},
    ArrayMetadataV2ToV3ConversionError,
};

/// The identifier for the `zfpy` codec.
pub const IDENTIFIER: &str = "zfpy";

/// Configuration parameters for the `zfpy` codec (numcodecs).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display)]
// #[serde(deny_unknown_fields)] TODO: zarr-python includes redundant compression_kwargs. Report upstream
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ZfpyCodecConfigurationNumcodecs {
    /// The zfp
    pub mode: ZfpyCodecConfigurationMode,
    /// The tolerance ensures that values in the decompressed array differ from the input array by no more than this tolerance.
    pub tolerance: Option<f64>,
    /// The rate is the number of compressed bits per value.
    pub rate: Option<f64>,
    /// The precision specifies how many uncompressed bits per value to store, and indirectly governs the relative error.
    pub precision: Option<i32>, // TODO: -1 is the default for zarr-python
}

/// The `zfpy` codec configuration mode.
#[derive(Serialize_repr, Deserialize_repr, Clone, PartialEq, Debug, Display)]
#[repr(u8)]
pub enum ZfpyCodecConfigurationMode {
    /// Fixed rate.
    FixedRate = 2,
    /// Fixed precision.
    FixedPrecision = 3,
    /// Fixed accuracy.
    FixedAccuracy = 4,
    /// Reversible.
    Reversible = 5,
}

/// Convert [`ZfpyCodecConfigurationNumcodecs`] to [`ZfpCodecConfiguration`].
///
/// # Errors
/// Returns an error if
///  - `rate` is missing for fixed rate mode
///  - `precision` is missing for fixed precision mode or is negative
///  - `tolerance` is missing for fixed accuracy mode
pub fn codec_zfpy_v2_numcodecs_to_v3(
    zfpy: &ZfpyCodecConfigurationNumcodecs,
) -> Result<ZfpCodecConfiguration, ArrayMetadataV2ToV3ConversionError> {
    let mode = match zfpy.mode {
        ZfpyCodecConfigurationMode::FixedRate => ZfpMode::FixedRate {
            rate: zfpy.rate.ok_or_else(|| {
                ArrayMetadataV2ToV3ConversionError::Other(
                    "missing rate in zfpy metadata".to_string(),
                )
            })?,
        },
        ZfpyCodecConfigurationMode::FixedPrecision => ZfpMode::FixedPrecision {
            precision: u32::try_from(zfpy.precision.ok_or_else(|| {
                ArrayMetadataV2ToV3ConversionError::Other(
                    "missing precision in zfpy metadata".to_string(),
                )
            })?)
            .map_err(|_| {
                ArrayMetadataV2ToV3ConversionError::Other(
                    "zfpy precision metadata does not convert to u32".to_string(),
                )
            })?,
        },
        ZfpyCodecConfigurationMode::FixedAccuracy => ZfpMode::FixedAccuracy {
            tolerance: zfpy.tolerance.ok_or_else(|| {
                ArrayMetadataV2ToV3ConversionError::Other(
                    "missing tolerance in zfpy metadata".to_string(),
                )
            })?,
        },
        ZfpyCodecConfigurationMode::Reversible => ZfpMode::Reversible,
    };
    Ok(ZfpCodecConfiguration::V1(ZfpCodecConfigurationV1 {
        write_header: Some(true),
        mode,
    }))
}

#[cfg(test)]
mod tests {
    use crate::metadata::v3::codec::zfp::ZfpCodecConfigurationV1;

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
        assert_eq!(v2.mode, ZfpyCodecConfigurationMode::FixedRate);
        assert_eq!(v2.rate, Some(0.123));
        let ZfpCodecConfiguration::V1(ZfpCodecConfigurationV1 { write_header, mode }) =
            codec_zfpy_v2_numcodecs_to_v3(&v2).unwrap();
        let ZfpMode::FixedRate { rate } = mode else {
            panic!()
        };
        assert_eq!(write_header, Some(true));
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
        assert_eq!(v2.mode, ZfpyCodecConfigurationMode::FixedPrecision);
        assert_eq!(v2.precision, Some(10));
        let ZfpCodecConfiguration::V1(ZfpCodecConfigurationV1 { write_header, mode }) =
            codec_zfpy_v2_numcodecs_to_v3(&v2).unwrap();
        let ZfpMode::FixedPrecision { precision } = mode else {
            panic!()
        };
        assert_eq!(write_header, Some(true));
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
        assert_eq!(v2.mode, ZfpyCodecConfigurationMode::FixedAccuracy);
        assert_eq!(v2.tolerance, Some(0.123));
        let ZfpCodecConfiguration::V1(ZfpCodecConfigurationV1 { write_header, mode }) =
            codec_zfpy_v2_numcodecs_to_v3(&v2).unwrap();
        let ZfpMode::FixedAccuracy { tolerance } = mode else {
            panic!()
        };
        assert_eq!(write_header, Some(true));
        assert_eq!(tolerance, 0.123);
    }

    #[test]
    fn codec_zfpy_reversible() {
        let v2 = serde_json::from_str::<ZfpyCodecConfigurationNumcodecs>(
            r#"
        {
            "mode": 5
        }
        "#,
        )
        .unwrap();
        assert_eq!(v2.mode, ZfpyCodecConfigurationMode::Reversible);
        let ZfpCodecConfiguration::V1(ZfpCodecConfigurationV1 { write_header, mode }) =
            codec_zfpy_v2_numcodecs_to_v3(&v2).unwrap();
        assert_eq!(write_header, Some(true));
        let ZfpMode::Reversible = mode else { panic!() };
    }
}
