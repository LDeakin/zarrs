//! [Zarr](https://zarr-specs.readthedocs.io/) metadata support for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! This crate supports serialisation and deserialisation of Zarr V2 and V3 metadata.
//!
//! [`ArrayMetadata`] and [`GroupMetadata`] can hold any conformant array/group metadata.
//!
//! All known array metadata is defined in this module.
//! This includes experimental data types, codecs, etc. supported by the `zarrs` crate.

use derive_more::derive::{Display, From};
use serde::{Deserialize, Serialize};

mod array;

/// Zarr V3 metadata.
pub mod v3;

/// Zarr V2 metadata.
pub mod v2;

/// Zarr V2 to V3 conversion.
pub mod v2_to_v3;

/// An alias for [`v3::MetadataV3`].
#[deprecated = "use v3::MetadataV3 explicitly"]
pub type Metadata = v3::MetadataV3;

pub use array::{ArrayShape, ChunkKeySeparator, ChunkShape, DimensionName, Endianness};

/// A wrapper to handle various versions of Zarr array metadata.
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ArrayMetadata {
    /// Zarr Version 3.0.
    V3(v3::ArrayMetadataV3),
    /// Zarr Version 2.0.
    V2(v2::ArrayMetadataV2),
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
    /// Zarr Version 3.0.
    V3(v3::GroupMetadataV3),
    /// Zarr Version 2.0.
    V2(v2::GroupMetadataV2),
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
pub enum NodeMetadata {
    /// Array metadata.
    Array(ArrayMetadata),

    /// Group metadata.
    Group(GroupMetadata),
}

#[cfg(test)]
mod tests {
    use super::*;
    use v3::{AdditionalFields, MetadataV3};

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
        assert_eq!(metadata.configuration(), Some(&configuration));
    }

    #[test]
    fn additional_fields_auto() {
        let mut additional_fields = AdditionalFields::new();
        let additional_field = serde_json::Map::new();
        additional_fields.insert("key".to_string(), additional_field.into());
        assert!(!additional_fields.contains_key("must_understand"));
        assert!(serde_json::to_string(&additional_fields)
            .unwrap()
            .contains(r#""must_understand":false"#));
    }

    #[test]
    fn additional_fields_valid() {
        let json = r#"{
            "unknown_field": {
                "key": "value",
                "must_understand": false
            }
        }"#;
        let additional_fields = serde_json::from_str::<AdditionalFields>(json);
        assert!(additional_fields.is_ok());
    }

    #[test]
    fn additional_fields_invalid() {
        let json = r#"{
            "unknown_field": {
                "key": "value"
            }
        }"#;
        let additional_fields = serde_json::from_str::<AdditionalFields>(json);
        assert!(additional_fields.is_err());
    }
}
