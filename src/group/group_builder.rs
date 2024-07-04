use std::sync::Arc;

use crate::metadata::{v3::GroupMetadataV3, AdditionalFields, GroupMetadata};

use super::{Group, GroupCreateError};

/// A [`Group`] builder.
pub struct GroupBuilder {
    metadata: GroupMetadata,
}

impl Default for GroupBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBuilder {
    /// Create a new group builder for a group at `path`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            metadata: GroupMetadataV3::default().into(),
        }
    }

    /// Set the attributes.
    pub fn attributes(
        &mut self,
        attributes: serde_json::Map<String, serde_json::Value>,
    ) -> &mut Self {
        match &mut self.metadata {
            GroupMetadata::V3(metadata) => metadata.attributes = attributes,
            GroupMetadata::V2(metadata) => metadata.attributes = attributes,
        }
        self
    }

    /// Set the additional fields.
    ///
    /// Set additional fields not defined in the Zarr specification.
    /// Use this cautiously. In general, store user defined attributes using [`GroupBuilder::attributes`].
    ///
    /// Note that array metadata must not contain any additional fields, unless they are annotated with `"must_understand": false`.
    /// `zarrs` will error when opening an array with additional fields without this annotation.
    pub fn additional_fields(&mut self, additional_fields: AdditionalFields) -> &mut Self {
        match &mut self.metadata {
            GroupMetadata::V3(metadata) => metadata.additional_fields = additional_fields,
            GroupMetadata::V2(metadata) => metadata.additional_fields = additional_fields,
        };
        self
    }

    /// Build into a [`Group`].
    ///
    /// # Errors
    ///
    /// Returns [`GroupCreateError`] if the group could not be created.
    pub fn build<TStorage: ?Sized>(
        &self,
        storage: Arc<TStorage>,
        path: &str,
    ) -> Result<Group<TStorage>, GroupCreateError> {
        Group::new_with_metadata(storage, path, self.metadata.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::store::MemoryStore;

    use super::*;

    #[test]
    fn group_builder() {
        let mut builder = GroupBuilder::default();

        let mut attributes = serde_json::Map::new();
        attributes.insert("key".to_string(), "value".into());
        builder.attributes(attributes.clone());

        let mut additional_fields = AdditionalFields::new();
        let additional_field = serde_json::Map::new();
        additional_fields.insert("key".to_string(), additional_field.into());
        builder.additional_fields(additional_fields.clone());

        let storage = Arc::new(MemoryStore::new());
        println!("{:?}", builder.build(storage.clone(), "/"));
        let mut group = builder.build(storage, "/").unwrap();

        assert_eq!(group.attributes(), &attributes);
        assert_eq!(group.additional_fields(), &additional_fields);
        assert_eq!(group.attributes_mut(), &attributes);
        assert_eq!(group.additional_fields_mut(), &additional_fields);
    }
}
