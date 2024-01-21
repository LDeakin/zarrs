use std::sync::Arc;

use crate::metadata::AdditionalFields;

use super::{Group, GroupCreateError, GroupMetadata, GroupMetadataV3};

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
        let GroupMetadata::V3(metadata) = &mut self.metadata;
        metadata.attributes = attributes;
        self
    }

    /// Set the additional fields.
    pub fn additional_fields(&mut self, additional_fields: AdditionalFields) -> &mut Self {
        let GroupMetadata::V3(metadata) = &mut self.metadata;
        metadata.additional_fields = additional_fields;
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

        let mut additional_fields = serde_json::Map::new();
        additional_fields.insert("key".to_string(), "value".into());
        let additional_fields: AdditionalFields = additional_fields.into();
        builder.additional_fields(additional_fields.clone());

        let storage = Arc::new(MemoryStore::new());
        println!("{:?}", builder.build(storage.clone(), "/"));
        let _group = builder.build(storage, "/");
    }
}
