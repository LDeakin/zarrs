//! Zarr metadata utilities.
//!
//! The [`Metadata`] structure represents most fields in array metadata (see [`ArrayMetadata`](crate::array::ArrayMetadata)), which is structured as JSON with a name and optional configuration, or just a string representing the name.
//! It provides convenience functions for converting metadata to and from a configuration specific to each:
//!  - [data type](`crate::array::data_type`),
//!  - [chunk grid][`crate::array::chunk_grid`]
//!  - [chunk key encoding](`crate::array::chunk_key_encoding`)
//!  - [codec](`crate::array::codec`), and
//!  - [storage transformer](`crate::storage::storage_transformer`).
//!
//! Additionally, this module provides [`AdditionalFields`] for additional fields in array or group metadata, which can be validated.

use derive_more::From;
use serde::{de::DeserializeOwned, ser::SerializeMap, Deserialize, Serialize};
use thiserror::Error;

/// Metadata with a name and optional configuration.
///
/// Can be deserialised from a JSON string or name/configuration map.
/// For example:
/// ```json
/// "bytes"
/// ```
/// or
/// ```json
/// {
///     "name": "bytes",
/// }
/// ```
/// or
/// ```json
/// {
///     "name": "bytes",
///     "configuration": {
///       "endian": "little"
///     }
/// }
///
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Metadata {
    name: String,
    configuration: Option<MetadataConfiguration>,
}

impl TryFrom<&str> for Metadata {
    type Error = serde_json::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(s)
    }
}

impl core::fmt::Display for Metadata {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if let Some(configuration) = &self.configuration {
            write!(
                f,
                "{} {}",
                self.name,
                serde_json::to_string(configuration).unwrap_or_default()
            )
        } else {
            write!(f, "{}", self.name)
        }
    }
}

impl serde::Serialize for Metadata {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if let Some(configuration) = &self.configuration {
            if configuration.is_empty() {
                let mut s = s.serialize_map(Some(1))?;
                s.serialize_entry("name", &self.name)?;
                s.end()
            } else {
                let mut s = s.serialize_map(Some(2))?;
                s.serialize_entry("name", &self.name)?;
                s.serialize_entry("configuration", configuration)?;
                s.end()
            }
        } else {
            s.serialize_str(self.name.as_str())
        }
    }
}

impl<'de> serde::Deserialize<'de> for Metadata {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct MetadataNameConfiguration {
            name: String,
            #[serde(default)]
            configuration: Option<MetadataConfiguration>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum MetadataIntermediate {
            Name(String),
            NameConfiguration(MetadataNameConfiguration),
        }

        let metadata = MetadataIntermediate::deserialize(d).map_err(|_| {
            serde::de::Error::custom(r#"Expected metadata "<name>" or {"name":"<name>"} or {"name":"<name>","configuration":{}}"#)
        })?;
        match metadata {
            MetadataIntermediate::Name(name) => Ok(Self {
                name,
                configuration: None,
            }),
            MetadataIntermediate::NameConfiguration(metadata) => Ok(Self {
                name: metadata.name,
                configuration: metadata.configuration,
            }),
        }
    }
}

impl Metadata {
    /// Create metadata from `name`.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            configuration: None,
        }
    }

    /// Create metadata from `name` and `configuration`.
    #[must_use]
    pub fn new_with_configuration(name: &str, configuration: MetadataConfiguration) -> Self {
        Self {
            name: name.into(),
            configuration: Some(configuration),
        }
    }

    /// Convert a serializable configuration to [`Metadata`].
    ///
    /// # Errors
    /// Returns [`serde_json::Error`] if `configuration` cannot be converted to [`Metadata`].
    pub fn new_with_serializable_configuration<TConfiguration: serde::Serialize>(
        name: &str,
        configuration: &TConfiguration,
    ) -> Result<Self, serde_json::Error> {
        let configuration = serde_json::to_value(configuration)?;
        if let serde_json::Value::Object(configuration) = configuration {
            Ok(Self::new_with_configuration(name, configuration))
        } else {
            Err(serde::ser::Error::custom(
                "the configuration cannot be serialized to a JSON struct",
            ))
        }
    }

    /// Try and convert [`Metadata`] to a serializable configuration.
    ///
    /// # Errors
    /// Returns a [`ConfigurationInvalidError`] if the metadata cannot be converted.
    pub fn to_configuration<TConfiguration: DeserializeOwned>(
        &self,
    ) -> Result<TConfiguration, ConfigurationInvalidError> {
        self.configuration.as_ref().map_or_else(
            || {
                Err(ConfigurationInvalidError::new(
                    self.name.clone(),
                    self.configuration.clone(),
                ))
            },
            |configuration| {
                let value = serde_json::to_value(configuration);
                value.map_or_else(
                    |_| {
                        Err(ConfigurationInvalidError::new(
                            self.name.clone(),
                            self.configuration.clone(),
                        ))
                    },
                    |value| {
                        serde_json::from_value(value).map_or_else(
                            |_| {
                                Err(ConfigurationInvalidError::new(
                                    self.name.clone(),
                                    self.configuration.clone(),
                                ))
                            },
                            |configuration| Ok(configuration),
                        )
                    },
                )
            },
        )
    }

    /// Returns the metadata name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the metadata configuration.
    #[must_use]
    pub const fn configuration(&self) -> Option<&MetadataConfiguration> {
        self.configuration.as_ref()
    }

    /// Returns true if the configuration is none or an empty map.
    #[must_use]
    pub fn configuration_is_none_or_empty(&self) -> bool {
        self.configuration
            .as_ref()
            .map_or(true, serde_json::Map::is_empty)
    }
}

/// An invalid configuration error.
#[derive(Debug, Error, From)]
#[error("{name} is unsupported, configuration: {configuration:?}")]
pub struct ConfigurationInvalidError {
    name: String,
    configuration: Option<MetadataConfiguration>,
}

impl ConfigurationInvalidError {
    /// Create a new invalid configuration error.
    #[must_use]
    pub fn new(name: String, configuration: Option<MetadataConfiguration>) -> Self {
        Self {
            name,
            configuration,
        }
    }

    /// Return the name of the invalid configuration.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the underlying configuration metadata of the invalid configuration.
    #[must_use]
    pub const fn configuration(&self) -> Option<&MetadataConfiguration> {
        self.configuration.as_ref()
    }
}

/// Configuration metadata.
pub type MetadataConfiguration = serde_json::Map<String, serde_json::Value>;

/// An unsupported additional field error.
///
/// An unsupported field in array or group metadata is an unrecognised field without `"must_understand": false`.
#[derive(Debug, Error)]
#[error("unsupported additional field {name} with value {value}")]
pub struct UnsupportedAdditionalFieldError {
    name: String,
    value: serde_json::Value,
}

impl UnsupportedAdditionalFieldError {
    /// Return the name of the unsupported additional field.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the value of the unsupported additional field.
    #[must_use]
    pub const fn value(&self) -> &serde_json::Value {
        &self.value
    }
}

/// Additional fields in array or group metadata.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Default, From)]
pub struct AdditionalFields(serde_json::Map<String, serde_json::Value>);

impl AdditionalFields {
    /// Checks if additional fields are valid.
    ///
    /// # Errors
    /// Returns an [`UnsupportedAdditionalFieldError`] if an unsupported additional field is identified.
    pub fn validate(&self) -> Result<(), UnsupportedAdditionalFieldError> {
        fn is_unknown_field_allowed(field: &serde_json::Value) -> bool {
            field.as_object().map_or(false, |value| {
                if value.contains_key("must_understand") {
                    let must_understand = &value["must_understand"];
                    match must_understand {
                        serde_json::Value::Bool(must_understand) => !must_understand,
                        _ => false,
                    }
                } else {
                    false
                }
            })
        }

        for (key, value) in &self.0 {
            if !is_unknown_field_allowed(value) {
                return Err(UnsupportedAdditionalFieldError {
                    name: key.to_string(),
                    value: value.clone(),
                });
            }
        }
        Ok(())
    }

    /// Return the underlying map.
    #[must_use]
    pub const fn as_map(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata() {
        let metadata = Metadata::try_from(r#""bytes""#);
        assert!(metadata.is_ok());
        assert_eq!(metadata.unwrap().to_string(), r#"bytes"#);
        assert!(Metadata::try_from(r#"{ "name": "bytes" }"#).is_ok());
        let metadata =
            Metadata::try_from(r#"{ "name": "bytes", "configuration": { "endian": "little" } }"#);
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
            Metadata::try_from(r#"{ "name": "bytes", "invalid": { "endian": "little" } }"#)
                .unwrap_err()
                .to_string(),
            r#"Expected metadata "<name>" or {"name":"<name>"} or {"name":"<name>","configuration":{}}"#
        );
        let metadata =
            Metadata::try_from(r#"{ "name": "bytes", "configuration": { "endian": "little" } }"#)
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
