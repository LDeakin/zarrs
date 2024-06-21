//! Zarr metadata.
//!
//! [`ArrayMetadata`] and [`GroupMetadata`] can hold any conformant array/group metadata.
//!
//! [`Array`](crate::array::Array) creation will error if [`ArrayMetadata`] contains:
//!  - unsupported extension points, including extensions which are supported by `zarrs` but have not been enabled with the appropriate features gates, or
//!  - incompatible codecs (e.g. codecs in wrong order, codecs incompatible with data type, etc.).
//!
//! All known array metadata is defined in this module, even if `zarrs` has not been compiled with the appropriate flags to use it.
//! An exception is the configuration of experimental codecs, which are feature gated.

mod array;
mod group;

/// Zarr V3 metadata.
pub mod v3;

/// Zarr V2 metadata.
pub mod v2;

pub use array::{array_metadata_v2_to_v3, ArrayMetadata, ArrayMetadataV2ToV3ConversionError};
pub use group::{group_metadata_v2_to_v3, GroupMetadata};
pub use v3::{
    AdditionalFields, ArrayMetadataV3, ConfigurationInvalidError, GroupMetadataV3, MetadataV3,
    UnsupportedAdditionalFieldError,
};

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
    fn additional_fields_valid() {
        let mut additional_fields_map = serde_json::Map::new();
        let mut additional_field = serde_json::Map::new();
        additional_field.insert("must_understand".to_string(), false.into());
        additional_fields_map.insert("key".to_string(), additional_field.into());
        let additional_fields: AdditionalFields = additional_fields_map.clone().into();
        assert!(additional_fields.validate().is_ok());
        assert_eq!(additional_fields.as_map(), &additional_fields_map);
    }

    #[test]
    fn additional_fields_invalid1() {
        let mut additional_fields = serde_json::Map::new();
        let mut additional_field = serde_json::Map::new();
        additional_field.insert("must_understand".to_string(), true.into());
        additional_fields.insert("key".to_string(), additional_field.clone().into());
        let additional_fields: AdditionalFields = additional_fields.into();
        let validate = additional_fields.validate();
        assert!(validate.is_err());
        let err = validate.unwrap_err();
        assert_eq!(err.name(), "key");
        assert_eq!(err.value(), &serde_json::Value::Object(additional_field));
    }

    #[test]
    fn additional_fields_invalid2() {
        let mut additional_fields = serde_json::Map::new();
        let additional_field = serde_json::Map::new();
        additional_fields.insert("key".to_string(), additional_field.into());
        let additional_fields: AdditionalFields = additional_fields.into();
        assert!(additional_fields.validate().is_err());
    }

    #[test]
    fn additional_fields_invalid3() {
        let mut additional_fields = serde_json::Map::new();
        let mut additional_field = serde_json::Map::new();
        additional_field.insert("must_understand".to_string(), 0.into());
        additional_fields.insert("key".to_string(), additional_field.into());
        let additional_fields: AdditionalFields = additional_fields.into();
        assert!(additional_fields.validate().is_err());
    }
}
