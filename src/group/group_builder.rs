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
    #[must_use]
    pub fn attributes(mut self, attributes: serde_json::Map<String, serde_json::Value>) -> Self {
        let GroupMetadata::V3(metadata) = &mut self.metadata;
        metadata.attributes = attributes;
        self
    }

    /// Set the additional fields.
    #[must_use]
    pub fn additional_fields(mut self, additional_fields: AdditionalFields) -> Self {
        let GroupMetadata::V3(metadata) = &mut self.metadata;
        metadata.additional_fields = additional_fields;
        self
    }

    /// Build into a [`Group`].
    ///
    /// # Errors
    ///
    /// Returns [`GroupCreateError`] if the group could not be created.
    pub fn build<TStorage>(
        self,
        storage: TStorage,
        path: &str,
    ) -> Result<Group<TStorage>, GroupCreateError> {
        Group::new_with_metadata(storage, path, self.metadata)
    }
}
