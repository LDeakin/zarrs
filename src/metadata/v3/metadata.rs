use derive_more::From;
use serde::{de::DeserializeOwned, ser::SerializeMap, Deserialize, Serialize};
use thiserror::Error;

/// Metadata with a name and optional configuration.
///
/// Represents most fields in Zarr V3 array metadata (see [`ArrayMetadataV3`](crate::metadata::v3::ArrayMetadataV3)), which is structured as JSON with a name and optional configuration, or just a string representing the name.
/// It provides convenience functions for converting metadata to and from a configuration specific to each:
///  - [data type](`crate::array::data_type`),
///  - [chunk grid][`crate::array::chunk_grid`]
///  - [chunk key encoding](`crate::array::chunk_key_encoding`)
///  - [codec](`crate::array::codec`), and
///  - [storage transformer](`crate::storage::storage_transformer`).
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
pub struct MetadataV3 {
    name: String,
    configuration: Option<MetadataConfiguration>,
}

/// Configuration metadata.
pub type MetadataConfiguration = serde_json::Map<String, serde_json::Value>;

impl TryFrom<&str> for MetadataV3 {
    type Error = serde_json::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(s)
    }
}

impl core::fmt::Display for MetadataV3 {
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

impl serde::Serialize for MetadataV3 {
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

impl<'de> serde::Deserialize<'de> for MetadataV3 {
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

impl MetadataV3 {
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

    /// Convert a serializable configuration to [`MetadataV3`].
    ///
    /// # Errors
    /// Returns [`serde_json::Error`] if `configuration` cannot be converted to [`MetadataV3`].
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

    /// Try and convert [`MetadataV3`] to a serializable configuration.
    ///
    /// # Errors
    /// Returns a [`ConfigurationInvalidError`] if the metadata cannot be converted.
    pub fn to_configuration<TConfiguration: DeserializeOwned>(
        &self,
    ) -> Result<TConfiguration, ConfigurationInvalidError> {
        let err = |_| ConfigurationInvalidError::new(self.name.clone(), self.configuration.clone());
        let configuration = self.configuration.clone().unwrap_or_default();
        let value = serde_json::to_value(configuration).map_err(err)?;
        serde_json::from_value(value).map_err(err)
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

/// An additional field in array or group metadata.
///
/// Must be an object with a `"must_understand": false` field.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Default, From)]
pub struct AdditionalField {
    must_understand: monostate::MustBe!(false),
    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
}

impl AdditionalField {
    /// Return the underlying map.
    #[must_use]
    pub const fn as_map(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.fields
    }
}

impl From<AdditionalField> for serde_json::Map<String, serde_json::Value> {
    fn from(value: AdditionalField) -> Self {
        value.fields
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for AdditionalField {
    fn from(value: serde_json::Map<String, serde_json::Value>) -> Self {
        Self {
            must_understand: monostate::MustBe!(false),
            fields: value,
        }
    }
}

/// Additional fields in array or group metadata.
///
/// Additional fields are a JSON object with a `"must_understand": false` key-value pair.
///
/// ### Example additional field JSON
/// ```json
/// "unknown_field": {
///   "key": "value",
///   "must_understand": false
/// }
/// ```
// NOTE: It would be nice if this was just a serde_json::Map, but it only has implementations for `<String, serde_json::Value>`.
pub type AdditionalFields = std::collections::BTreeMap<String, AdditionalField>;
