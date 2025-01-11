use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// The identifier for the `fletcher32` codec.
pub const IDENTIFIER: &str = "fletcher32";

/// A wrapper to handle various versions of `fletcher32` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum Fletcher32CodecConfiguration {
    /// Version 1.0 draft.
    V1(Fletcher32CodecConfigurationV1),
}

/// `fletcher32` (checksum) codec configuration parameters (version 1.0 draft).
///
/// ### Example (Zarr V3)
/// ```json
/// {
///     "name": "fletcher32",
///     "configuration": {}
/// }
/// ```
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
// TODO: deny_unknown_fields could be disabled to support numcodecs, which adds "id": "fletcher32"
//       However, I would rather be intentionally incompatible and push forward with standardisation instead
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct Fletcher32CodecConfigurationV1 {}

#[cfg(test)]
mod tests {
    use crate::v3::MetadataV3;

    use super::*;

    #[test]
    fn codec_fletcher32_config1() {
        serde_json::from_str::<Fletcher32CodecConfiguration>(r#"{}"#).unwrap();
    }

    #[test]
    fn codec_fletcher32_config_outer1() {
        serde_json::from_str::<MetadataV3>(
            r#"{
            "name": "fletcher32",
            "configuration": {}
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_fletcher32_config_outer2() {
        serde_json::from_str::<MetadataV3>(
            r#"{
            "name": "fletcher32"
        }"#,
        )
        .unwrap();
    }
}
