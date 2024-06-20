use crate::config::{MetadataOptionsEraseVersion, MetadataOptionsStoreVersion};

/// Options for writing group metadata.
#[derive(Debug, Clone, Default)]
pub struct GroupMetadataOptions {
    store_version: MetadataOptionsStoreVersion,
    erase_version: MetadataOptionsEraseVersion,
}

impl GroupMetadataOptions {
    /// Get the [metadata store version behaviour](crate::config::Config#metadata-store-version-behaviour) configuration.
    #[must_use]
    pub fn metadata_store_version(&self) -> &MetadataOptionsStoreVersion {
        &self.store_version
    }

    /// Set the [metadata store version behaviour](crate::config::Config#metadata-store-version-behaviour) configuration.
    pub fn set_metadata_store_version(
        &mut self,
        store_version: MetadataOptionsStoreVersion,
    ) -> &mut Self {
        self.store_version = store_version;
        self
    }

    /// Get the [metadata erase version behaviour](crate::config::Config#metadata-erase-version-behaviour) configuration.
    #[must_use]
    pub fn metadata_erase_version(&self) -> &MetadataOptionsEraseVersion {
        &self.erase_version
    }

    /// Set the [metadata erase version behaviour](crate::config::Config#metadata-erase-version-behaviour) configuration.
    pub fn set_metadata_erase_version(
        &mut self,
        erase_version: MetadataOptionsEraseVersion,
    ) -> &mut Self {
        self.erase_version = erase_version;
        self
    }
}
