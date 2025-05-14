use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use zarrs_metadata::v3::MetadataConfigurationSerialize;

/// A wrapper to handle various versions of `packbits` codec configuration parameters.
///
/// ### Specification
/// - <https://github.com/zarr-developers/zarr-extensions/blob/main/codecs/packbits/README.md>
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[non_exhaustive]
#[serde(untagged)]
pub enum PackBitsCodecConfiguration {
    /// Version 1.0 draft.
    V1(PackBitsCodecConfigurationV1),
}

impl MetadataConfigurationSerialize for PackBitsCodecConfiguration {}

/// `packbits` codec configuration parameters (version 1.0 draft).
///
/// ### Example (Zarr V3)
/// ```json
/// {
///     "name": "packbits",
///     "configuration": {
///        "padding_encoding": "start_byte"
///     }
/// }
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct PackBitsCodecConfigurationV1 {
    /// Specifies how the number of padding bits is encoded, such that the number of decoded elements may be determined from the encoded representation alone.
    pub padding_encoding: Option<PackBitsPaddingEncoding>,
    /// Specifies the index (starting from the least-significant bit) of the first bit to be encoded.
    ///
    /// If omitted, or specified as `null`, defaults to `0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_bit: Option<u64>,
    /// Specifies the index (starting from the least-significant bit) of the (inclusive) last bit to be encoded.
    ///
    /// If omitted, or specified as `null`, defaults to `N - 1`, where `N` is the total number of bits per component of the data type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_bit: Option<u64>,
}

/// `padding_encoding` for the `packbits` codec.
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug, Display, Default)]
#[serde(rename_all = "snake_case")]
pub enum PackBitsPaddingEncoding {
    /// The number of padding bits is not encoded.
    #[default]
    None,
    /// The first byte specifies the number of padding bits that were added.
    FirstByte,
    /// The final byte specifies the number of padding bits that were added.
    LastByte,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packbits_default() {
        let configuration = serde_json::from_str::<PackBitsCodecConfigurationV1>(
            r#"
        {
        }
        "#,
        )
        .unwrap();
        assert_eq!(configuration.padding_encoding, None);
    }

    #[test]
    fn packbits_none() {
        let configuration = serde_json::from_str::<PackBitsCodecConfigurationV1>(
            r#"
        {
            "padding_encoding": "none"
        }
        "#,
        )
        .unwrap();
        assert_eq!(
            configuration.padding_encoding,
            Some(PackBitsPaddingEncoding::None)
        );
    }

    #[test]
    fn packbits_start_byte() {
        let configuration = serde_json::from_str::<PackBitsCodecConfigurationV1>(
            r#"
        {
            "padding_encoding": "first_byte"
        }
        "#,
        )
        .unwrap();
        assert_eq!(
            configuration.padding_encoding,
            Some(PackBitsPaddingEncoding::FirstByte)
        );
    }

    #[test]
    fn packbits_end_byte() {
        let configuration = serde_json::from_str::<PackBitsCodecConfigurationV1>(
            r#"
        {
            "padding_encoding": "last_byte"
        }
        "#,
        )
        .unwrap();
        assert_eq!(
            configuration.padding_encoding,
            Some(PackBitsPaddingEncoding::LastByte)
        );
    }
}
