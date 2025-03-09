use std::fmt::Debug;

use derive_more::{Deref, From, Into};
use serde::{de::DeserializeOwned, ser::SerializeMap, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Metadata with a name and optional configuration.
///
/// Represents most fields in Zarr V3 array metadata (see [`ArrayMetadataV3`](crate::v3::ArrayMetadataV3)) which is either:
/// - a string name / identifier, or
/// - a JSON object with a required `name` field and optional `configuration` and `must_understand` fields.
///
/// `must_understand` is implicitly set to [`true`] if omitted.
/// See [ZEP0009](https://zarr.dev/zeps/draft/ZEP0009.html) for more information on this field and Zarr V3 extensions.
///
/// ### Example Metadata
/// ```json
/// "bytes"
/// ```
///
/// ```json
/// {
///     "name": "bytes",
/// }
/// ```
///
/// ```json
/// {
///     "name": "bytes",
///     "configuration": {
///       "endian": "little"
///     }
/// }
/// ```
///
/// ```json
/// {
///     "name": "bytes",
///     "configuration": {
///       "endian": "little"
///     },
///     "must_understand": False
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MetadataV3 {
    name: String,
    configuration: Option<MetadataConfiguration>,
    must_understand: bool,
}

/// Configuration metadata.
#[derive(Default, Serialize, Deserialize, Debug, Clone, Deref, From, Into, Eq, PartialEq)]
pub struct MetadataConfiguration(serde_json::Map<String, Value>);

impl<T: MetadataConfigurationSerialize> From<T> for MetadataConfiguration {
    fn from(value: T) -> Self {
        match serde_json::to_value(value) {
            Ok(serde_json::Value::Object(configuration)) => configuration.into(),
            _ => {
                panic!("the configuration could not be converted to a JSON object")
            }
        }
    }
}

/// A trait for metadata configurations.
///
/// Implementors of this trait guarantee that the configuration is always serialisable to a JSON object.
pub trait MetadataConfigurationSerialize: Serialize + DeserializeOwned {}

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
                let mut s = s.serialize_map(Some(if self.must_understand { 2 } else { 3 }))?;
                s.serialize_entry("name", &self.name)?;
                s.serialize_entry("configuration", configuration)?;
                if !self.must_understand {
                    s.serialize_entry("must_understand", &false)?;
                }
                s.end()
            }
        } else {
            s.serialize_str(self.name.as_str())
        }
    }
}

fn default_must_understand() -> bool {
    true
}

impl<'de> serde::Deserialize<'de> for MetadataV3 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct MetadataNameConfiguration {
            name: String,
            #[serde(default)]
            configuration: Option<MetadataConfiguration>,
            #[serde(default = "default_must_understand")]
            must_understand: bool,
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
                must_understand: true,
            }),
            MetadataIntermediate::NameConfiguration(metadata) => Ok(Self {
                name: metadata.name,
                configuration: metadata.configuration,
                must_understand: metadata.must_understand,
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
            must_understand: true,
        }
    }

    /// Create metadata from `name` and `configuration`.
    #[must_use]
    pub fn new_with_configuration(
        name: &str,
        configuration: impl Into<MetadataConfiguration>,
    ) -> Self {
        Self {
            name: name.into(),
            configuration: Some(configuration.into()),
            must_understand: true,
        }
    }

    /// Set the value of the `must_understand` field.
    #[must_use]
    pub fn with_must_understand(mut self, must_understand: bool) -> Self {
        self.must_understand = must_understand;
        self
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
        if let Value::Object(configuration) = configuration {
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

    /// Return whether the metadata must be understood as indicated by the `must_understand` field.
    ///
    /// The `must_understand` field is implicitly `true` if omitted.
    #[must_use]
    pub fn must_understand(&self) -> bool {
        self.must_understand
    }

    /// Returns true if the configuration is none or an empty map.
    #[must_use]
    pub fn configuration_is_none_or_empty(&self) -> bool {
        self.configuration
            .as_ref()
            .map_or(true, |configuration| configuration.is_empty())
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

// FIXME: Move to `zarrs` itself in 0.4.0
/// An unsupported additional field error.
///
/// An unsupported field in array or group metadata is an unrecognised field without `"must_understand": false`.
#[derive(Debug, Error)]
#[error("unsupported additional field {name} with value {value}")]
pub struct UnsupportedAdditionalFieldError {
    name: String,
    value: Value,
}

impl UnsupportedAdditionalFieldError {
    /// Create a new [`UnsupportedAdditionalFieldError`].
    #[must_use]
    pub fn new(name: String, value: Value) -> UnsupportedAdditionalFieldError {
        Self { name, value }
    }

    /// Return the name of the unsupported additional field.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the value of the unsupported additional field.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }
}

/// An additional field in array or group metadata.
///
/// A field that is not recognised / supported by `zarrs` will be considered an additional field.
/// Additional fields can be any JSON type.
/// An array / group cannot be created with an additional field, unless the additional field is an object with a `"must_understand": false` field.
///
/// ### Example additional field JSON
/// ```json
//  "unknown_field": {
//      "key": "value",
//      "must_understand": false
//  },
//  "unsupported_field_1": {
//      "key": "value",
//      "must_understand": true
//  },
//  "unsupported_field_2": {
//      "key": "value"
//  },
//  "unsupported_field_3": [],
//  "unsupported_field_4": "test"
/// ```
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct AdditionalField {
    field: Value,
    must_understand: bool,
}

impl AdditionalField {
    /// Create a new additional field.
    #[must_use]
    pub fn new(field: impl Into<Value>, must_understand: bool) -> AdditionalField {
        Self {
            field: field.into(),
            must_understand,
        }
    }

    /// Return the underlying value.
    #[must_use]
    pub const fn as_value(&self) -> &Value {
        &self.field
    }

    /// Return the `must_understand` component of the additional field.
    #[must_use]
    pub const fn must_understand(&self) -> bool {
        self.must_understand
    }
}

impl Serialize for AdditionalField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.field {
            Value::Object(object) => {
                let mut map = serializer.serialize_map(Some(object.len() + 1))?;
                map.serialize_entry("must_understand", &Value::Bool(self.must_understand))?;
                for (k, v) in object {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
            _ => self.field.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for AdditionalField {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        Ok(value.into())
    }
}

impl<T> From<T> for AdditionalField
where
    T: Into<Value>,
{
    fn from(field: T) -> Self {
        let mut value: Value = field.into();
        let must_understand = if let Some(object) = value.as_object_mut() {
            if let Some(Value::Bool(must_understand)) = object.remove("must_understand") {
                must_understand
            } else {
                true
            }
        } else {
            true
        };
        Self {
            must_understand,
            field: value,
        }
    }
}

/// Additional fields in array or group metadata.
// NOTE: It would be nice if this was just a serde_json::Map, but it only has implementations for `<String, Value>`.
pub type AdditionalFields = std::collections::BTreeMap<String, AdditionalField>;

#[cfg(test)]
mod tests {
    use super::MetadataV3;

    #[test]
    fn metadata_must_understand_implicit_string() {
        let metadata = r#""test""#;
        let metadata: MetadataV3 = serde_json::from_str(&metadata).unwrap();
        assert!(metadata.name() == "test");
        assert!(metadata.must_understand());
    }

    #[test]
    fn metadata_must_understand_implicit() {
        let metadata = r#"{
    "name": "test"
}"#;
        let metadata: MetadataV3 = serde_json::from_str(&metadata).unwrap();
        assert!(metadata.name() == "test");
        assert!(metadata.must_understand());
    }

    #[test]
    fn metadata_must_understand_true() {
        let metadata = r#"{
    "name": "test",
    "must_understand": true
}"#;
        let metadata: MetadataV3 = serde_json::from_str(&metadata).unwrap();
        assert!(metadata.name() == "test");
        assert!(metadata.must_understand());
    }

    #[test]
    fn metadata_must_understand_false() {
        let metadata = r#"{
    "name": "test",
    "must_understand": false
}"#;
        let metadata: MetadataV3 = serde_json::from_str(&metadata).unwrap();
        assert!(metadata.name() == "test");
        assert!(!metadata.must_understand());
        assert_ne!(metadata, MetadataV3::new("test"));
        assert_eq!(
            metadata,
            MetadataV3::new("test").with_must_understand(false)
        );
    }
}
