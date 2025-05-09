//! [Zarr](https://zarr-specs.readthedocs.io/) metadata support for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! This crate supports serialisation and deserialisation of Zarr V2 and V3 metadata.
//!
//! [`ArrayMetadata`] and [`GroupMetadata`] can hold any conformant array/group metadata.
//!
//! This crate includes known metadata for Zarr V3 extension points (chunk grids, chunk key encodings, codecs, and data types), including:
//! - _Core_ extensions defined in the [Zarr V3 specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html),
//! - _Registered_ extensions defined at [zarr-developers/zarr-extensions](https://github.com/zarr-developers/zarr-extensions/), and
//! - `numcodecs` codecs and _experimental_ extensions in `zarrs` that have yet to be registered.
//!
//! Functions for converting Zarr V2 to equivalent Zarr V3 metadata are included.

use derive_more::derive::{Display, From};
use serde::{Deserialize, Serialize};

mod array;

/// Zarr V3 metadata.
pub mod v3;

/// Zarr V2 metadata.
pub mod v2;

/// Zarr V2 to V3 conversion.
pub mod v2_to_v3;

pub use crate::v3::array::{chunk_grid, chunk_key_encoding, codec, data_type};

pub use array::{
    ArrayShape, ChunkKeySeparator, ChunkShape, DimensionName, Endianness, IntoDimensionName,
};

/// A wrapper to handle various versions of Zarr array metadata.
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug, Display, From)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum ArrayMetadata {
    /// Zarr Version 3.
    V3(v3::ArrayMetadataV3),
    /// Zarr Version 2.
    V2(v2::ArrayMetadataV2),
}

impl ArrayMetadata {
    /// Serialize the metadata as a pretty-printed String of JSON.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn to_string_pretty(&self) -> String {
        serde_json::to_string_pretty(self).expect("array metadata is valid JSON")
    }
}

impl TryFrom<&str> for ArrayMetadata {
    type Error = serde_json::Error;
    fn try_from(metadata_json: &str) -> Result<Self, Self::Error> {
        serde_json::from_str::<Self>(metadata_json)
    }
}

/// A wrapper to handle various versions of Zarr group metadata.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum GroupMetadata {
    /// Zarr Version 3.
    V3(v3::GroupMetadataV3),
    /// Zarr Version 2.
    V2(v2::GroupMetadataV2),
}

impl GroupMetadata {
    /// Serialize the metadata as a pretty-printed String of JSON.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn to_string_pretty(&self) -> String {
        serde_json::to_string_pretty(self).expect("group metadata is valid JSON")
    }
}

impl TryFrom<&str> for GroupMetadata {
    type Error = serde_json::Error;
    fn try_from(metadata_json: &str) -> Result<Self, Self::Error> {
        serde_json::from_str::<Self>(metadata_json)
    }
}

/// Node metadata ([`ArrayMetadata`] or [`GroupMetadata`]).
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum NodeMetadata {
    /// Array metadata.
    Array(ArrayMetadata),

    /// Group metadata.
    Group(GroupMetadata),
}

impl NodeMetadata {
    /// Serialize the metadata as a pretty-printed String of JSON.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn to_string_pretty(&self) -> String {
        serde_json::to_string_pretty(self).expect("node metadata is valid JSON")
    }
}

/// A data type size. Fixed or variable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DataTypeSize {
    /// Fixed size (in bytes).
    Fixed(usize),
    /// Variable sized.
    ///
    /// <https://github.com/zarr-developers/zeps/pull/47>
    Variable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use v3::{AdditionalField, AdditionalFields, MetadataV3};

    #[test]
    fn metadata() {
        let metadata = MetadataV3::try_from(r#""bytes""#);
        assert!(metadata.is_ok());
        assert_eq!(metadata.unwrap().to_string(), r#"bytes"#);
        assert!(MetadataV3::try_from(r#"{ "name": "bytes" }"#).is_ok());
        let metadata =
            MetadataV3::try_from(r#"{ "name": "bytes", "configuration": { "endian": "little" } }"#);
        assert!(metadata.is_ok());
        let metadata = metadata.unwrap();
        assert_eq!(metadata.to_string(), r#"bytes {"endian":"little"}"#);
        assert_eq!(metadata.name(), "bytes");
        assert!(metadata.configuration().is_some());
        let configuration = metadata.configuration().unwrap();
        assert!(configuration.contains_key("endian"));
        assert_eq!(
            configuration.get("endian").unwrap().as_str().unwrap(),
            "little"
        );
        assert_eq!(
            MetadataV3::try_from(r#"{ "name": "bytes", "invalid": { "endian": "little" } }"#)
                .unwrap_err()
                .to_string(),
            r#"Expected metadata "<name>" or {"name":"<name>"} or {"name":"<name>","configuration":{}}"#
        );
        let metadata =
            MetadataV3::try_from(r#"{ "name": "bytes", "configuration": { "endian": "little" } }"#)
                .unwrap();
        let mut configuration = serde_json::Map::new();
        configuration.insert("endian".to_string(), "little".into());
        assert_eq!(metadata.configuration(), Some(&configuration.into()));
    }

    #[test]
    fn additional_fields_constructors() {
        let additional_field = serde_json::Map::new();
        let additional_field: AdditionalField = additional_field.into();
        assert!(additional_field.must_understand());
        assert!(
            additional_field.as_value() == &serde_json::Value::Object(serde_json::Map::default())
        );
        assert!(serde_json::to_string(&additional_field).unwrap() == r#"{"must_understand":true}"#);

        let additional_field: AdditionalField = AdditionalField::new("test", true);
        assert!(additional_field.must_understand());
        assert!(additional_field.as_value() == &serde_json::Value::String("test".to_string()));
        assert!(serde_json::to_string(&additional_field).unwrap() == r#""test""#);

        let additional_field: AdditionalField = AdditionalField::new(123, false);
        assert!(!additional_field.must_understand());
        assert!(
            additional_field.as_value()
                == &serde_json::Value::Number(serde_json::Number::from(123))
        );
        assert!(serde_json::to_string(&additional_field).unwrap() == "123");
    }

    #[test]
    fn additional_fields_valid() {
        let json = r#"{
            "unknown_field": {
                "key": "value",
                "must_understand": false
            },
            "unsupported_field_1": {
                "key": "value",
                "must_understand": true
            },
            "unsupported_field_2": {
                "key": "value"
            },
            "unsupported_field_3": [],
            "unsupported_field_4": "test"
        }"#;
        let additional_fields = serde_json::from_str::<AdditionalFields>(json).unwrap();
        assert!(additional_fields.len() == 5);
        assert!(!additional_fields["unknown_field"].must_understand());
        assert!(additional_fields["unsupported_field_1"].must_understand());
        assert!(additional_fields["unsupported_field_2"].must_understand());
        assert!(additional_fields["unsupported_field_3"].must_understand());
        assert!(additional_fields["unsupported_field_4"].must_understand());
    }
}
