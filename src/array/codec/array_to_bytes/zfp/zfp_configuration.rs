use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::ZfpExpertParams;

/// A wrapper to handle various versions of Zfp codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, From)]
#[serde(untagged)]
pub enum ZfpCodecConfiguration {
    /// Version 1.0.
    V1(ZfpCodecConfigurationV1),
}

/// Configuration parameters for the Zfp codec (version 1.0).
///
/// Further information on the meaning of these parameters can be found in the [zfp documentation](https://zfp.readthedocs.io/en/latest/).
///
/// Valid examples:
///
/// ### Encode in fixed rate mode with 10.5 compressed bits per value
/// ```rust
/// # let JSON = r#"
/// {
///     "mode": "fixedrate",
///     "rate": 10.5
/// }
/// # "#;
/// # let configuration: zarrs::array::codec::ZfpCodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
/// ```
///
/// ### Encode in fixed precision mode with 19 uncompressed bits per value
/// ```rust
/// # let JSON = r#"
/// {
///     "mode": "fixedprecision",
///     "precision": 19
/// }
/// # "#;
/// # let configuration: zarrs::array::codec::ZfpCodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
/// ```
///
/// ### Encode in fixed accuracy mode with a tolerance of 0.05
/// ```rust
/// # let JSON = r#"
/// {
///     "mode": "fixedaccuracy",
///     "tolerance": 0.05
/// }
/// # "#;
/// # let configuration: zarrs::array::codec::ZfpCodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
/// ```
///
/// ### Encode in reversible mode
/// ```rust
/// # let JSON = r#"
/// {
///     "mode": "reversible"
/// }
/// # "#;
/// # let configuration: zarrs::array::codec::ZfpCodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
/// ```
///
/// ### Encode in expert mode
/// ```rust
/// # let JSON = r#"
/// {
///     "mode": "expert",
///     "minbits": 1,
///     "maxbits": 13,
///     "maxprec": 19,
///     "minexp": -2
/// }
/// # "#;
/// # let configuration: zarrs::array::codec::ZfpCodecConfigurationV1 = serde_json::from_str(JSON).unwrap();
/// ```
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum ZfpCodecConfigurationV1 {
    /// Expert mode.
    Expert(ZfpExpertParams),
    /// Fixed rate mode.
    FixedRate(ZfpFixedRateConfiguration),
    /// Fixed precision mode.
    FixedPrecision(ZfpFixedPrecisionConfiguration),
    /// Fixed accuracy mode.
    FixedAccuracy(ZfpFixedAccuracyConfiguration),
    /// Reversible mode.
    Reversible,
}

/// The zfp configuration for expert mode.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Display)]
pub struct ZfpExpertConfiguration {
    /// The rate is the number of compressed bits per value.
    pub rate: f64,
}

/// The zfp configuration for fixed rate mode.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Display)]
pub struct ZfpFixedRateConfiguration {
    /// The rate is the number of compressed bits per value.
    pub rate: f64,
}

/// The zfp configuration for fixed precision mode.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Display)]
pub struct ZfpFixedPrecisionConfiguration {
    /// The precision specifies how many uncompressed bits per value to store, and indirectly governs the relative error.
    pub precision: u32,
}

/// The zfp configuration for fixed accuracy mode.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Display)]
pub struct ZfpFixedAccuracyConfiguration {
    /// The tolerance ensures that values in the decompressed array differ from the input array by no more than this tolerance.
    pub tolerance: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_zfp_configuration_expert() {
        const JSON: &'static str = r#"{
        "mode": "expert",
        "minbits": 1,
        "maxbits": 12,
        "maxprec": 10,
        "minexp": -2
    }"#;
        serde_json::from_str::<ZfpCodecConfiguration>(JSON).unwrap();
    }

    #[test]
    fn codec_zfp_configuration_fixed_rate() {
        const JSON: &'static str = r#"{
        "mode": "fixedrate",
        "rate": 12
    }"#;
        serde_json::from_str::<ZfpCodecConfiguration>(JSON).unwrap();
    }

    #[test]
    fn codec_zfp_configuration_fixed_precision() {
        const JSON: &'static str = r#"{
        "mode": "fixedprecision",
        "precision": 12
    }"#;
        serde_json::from_str::<ZfpCodecConfiguration>(JSON).unwrap();
    }

    #[test]
    fn codec_zfp_configuration_fixed_accuracy() {
        const JSON: &'static str = r#"{
        "mode": "fixedaccuracy",
        "tolerance": 0.001
    }"#;
        serde_json::from_str::<ZfpCodecConfiguration>(JSON).unwrap();
    }

    #[test]
    fn codec_zfp_configuration_reversible() {
        const JSON: &'static str = r#"{
        "mode": "reversible"
    }"#;
        serde_json::from_str::<ZfpCodecConfiguration>(JSON).unwrap();
    }

    #[test]
    fn codec_zfp_configuration_invalid2() {
        const JSON_INVALID2: &'static str = r#"{
        "mode": "unknown"
    }"#;
        assert!(serde_json::from_str::<ZfpCodecConfiguration>(JSON_INVALID2).is_err());
    }
}
