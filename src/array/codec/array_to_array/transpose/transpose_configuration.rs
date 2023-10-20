use derive_more::{Display, From};
use serde::{ser::SerializeSeq, Deserialize, Serialize};

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
    /// An array of integers specifying the permutation, "C", or "F".
    pub order: TransposeOrder,
}

impl TransposeCodecConfigurationV1 {
    /// Create a new Transpose codec configuration given a [`TransposeOrder`].
    #[must_use]
    pub fn new(order: TransposeOrder) -> Self {
        Self { order }
    }
}

/// The transpose order defining how to permute the array.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum TransposeOrder {
    /// The string `"C"`, equivalent to specifying the identity permutation `0`, `1`, …, `n-1`. This makes the codec a no-op.
    C,

    /// The string `"F"`, equivalent to specifying the permutation `n-1`, …, `1`, `0`.
    F,

    /// An array of integers specifying a permutation of 0, 1, …, n-1, where n is the number of dimensions in the decoded chunk representation provided as input to this codec.
    Permutation(Vec<usize>),
}

impl serde::Serialize for TransposeOrder {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match &self {
            Self::C => s.serialize_str("C"),
            Self::F => s.serialize_str("F"),
            Self::Permutation(permutation) => {
                let mut seq = s.serialize_seq(Some(permutation.len()))?;
                for v in permutation {
                    seq.serialize_element(&v)?;
                }
                seq.end()
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for TransposeOrder {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(d)?;
        match value {
            serde_json::Value::String(string) => {
                if string == "C" {
                    return Ok(Self::C);
                } else if string == "F" {
                    return Ok(Self::F);
                }
            }
            serde_json::Value::Array(array) => {
                if array.iter().all(serde_json::Value::is_u64) {
                    let permutation: Vec<usize> = array
                        .iter()
                        .map(|v| v.as_u64().unwrap().try_into().unwrap())
                        .collect();
                    return Ok(Self::Permutation(permutation));
                }
            }
            _ => {}
        }
        Err(serde::de::Error::custom(
            "transpose order must be C, F, or an array of integers specifying a permutation",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const JSON_C: &str = r#"{
        "order": "C"
    }"#;

    const JSON_F: &str = r#"{
        "order": "F"
    }"#;

    const JSON_ARRAY: &str = r#"{
        "order": [0, 2, 1]
    }"#;

    #[test]
    fn codec_transpose_c() {
        serde_json::from_str::<TransposeCodecConfiguration>(JSON_C).unwrap();
    }

    #[test]
    fn codec_transpose_f() {
        serde_json::from_str::<TransposeCodecConfiguration>(JSON_F).unwrap();
    }

    #[test]
    fn codec_transpose_array() {
        serde_json::from_str::<TransposeCodecConfiguration>(JSON_ARRAY).unwrap();
    }

    #[test]
    fn codec_transpose_invalid() {
        let json = r#"{
            "order": ""
        }"#;
        assert!(serde_json::from_str::<TransposeCodecConfiguration>(json).is_err());
    }
}
