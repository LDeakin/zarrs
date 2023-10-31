use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::{validate_permutation, InvalidPermutationError};

/// A wrapper to handle various versions of Transpose codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum TransposeCodecConfiguration {
    /// Version 1.0.
    V1(TransposeCodecConfigurationV1),
}

/// Configuration parameters for the Transpose codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct TransposeCodecConfigurationV1 {
    /// The transpose order defining how to permute the array.
    pub order: TransposeOrder,
}

impl TransposeCodecConfigurationV1 {
    /// Create a new Transpose codec configuration given a [`TransposeOrder`].
    #[must_use]
    pub const fn new(order: TransposeOrder) -> Self {
        Self { order }
    }
}

/// The transpose order defining how to permute the array.
///
/// An array of integers specifying a permutation of 0, 1, …, n-1, where n is the number of dimensions in the decoded chunk representation provided as input to this codec.
#[derive(Serialize, Clone, Eq, PartialEq, Debug)]
pub struct TransposeOrder(pub Vec<usize>);

impl TransposeOrder {
    /// Create a new [`TransposeOrder`].
    ///
    /// # Errors
    /// Returns [`InvalidPermutationError`] if the permutation order is invalid.
    pub fn new(order: &[usize]) -> Result<Self, InvalidPermutationError> {
        if validate_permutation(order) {
            Ok(Self(order.to_vec()))
        } else {
            Err(InvalidPermutationError::from(order.to_vec()))
        }
    }
}

impl<'de> serde::Deserialize<'de> for TransposeOrder {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let permutation = Vec::<usize>::deserialize(d)?;
        if validate_permutation(&permutation) {
            Ok(Self(permutation))
        } else {
            Err(serde::de::Error::custom(
                "transpose order must be an array of integers specifying a permutation of 0, 1, …, n-1, where n is the number of dimensions",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_transpose_array() {
        let json = r#"{
            "order": [0, 2, 1]
        }"#;
        serde_json::from_str::<TransposeCodecConfiguration>(json).unwrap();
    }

    #[test]
    fn codec_transpose_invalid1() {
        let json = r#"{
            "order": ""
        }"#;
        assert!(serde_json::from_str::<TransposeCodecConfiguration>(json).is_err());
    }

    #[test]
    fn codec_transpose_invalid2() {
        let json = r#"{
            "order": [0, 2]
        }"#;
        assert!(serde_json::from_str::<TransposeCodecConfiguration>(json).is_err());
    }
}
