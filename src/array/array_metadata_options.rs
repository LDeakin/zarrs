use crate::config::{global_config, MetadataOptionsEraseVersion, MetadataOptionsStoreVersion};

/// Options for writing array metadata.
#[derive(Debug, Clone)]
pub struct ArrayMetadataOptions {
    experimental_codec_store_metadata_if_encode_only: bool,
    store_version: MetadataOptionsStoreVersion,
    erase_version: MetadataOptionsEraseVersion,
    include_zarrs_metadata: bool,
}

impl Default for ArrayMetadataOptions {
    fn default() -> Self {
        Self {
            experimental_codec_store_metadata_if_encode_only: false,
            store_version: MetadataOptionsStoreVersion::default(),
            erase_version: MetadataOptionsEraseVersion::default(),
            include_zarrs_metadata: global_config().include_zarrs_metadata(),
        }
    }
}

impl ArrayMetadataOptions {
    /// Return the [experimental codec store metadata if encode only](crate::config::Config#experimental-codec-store-metadata-if-encode-only) setting.
    #[must_use]
    pub fn experimental_codec_store_metadata_if_encode_only(&self) -> bool {
        self.experimental_codec_store_metadata_if_encode_only
    }

    /// Set the [experimental codec store metadata if encode only](crate::config::Config#experimental-codec-store-metadata-if-encode-only) setting.
    pub fn set_experimental_codec_store_metadata_if_encode_only(
        &mut self,
        enabled: bool,
    ) -> &mut Self {
        self.experimental_codec_store_metadata_if_encode_only = enabled;
        self
    }

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

    /// Get the [include zarrs metadata](crate::config::Config#include-zarrs-metadata) configuration.
    #[must_use]
    pub fn include_zarrs_metadata(&self) -> bool {
        self.include_zarrs_metadata
    }

    /// Set the [include zarrs metadata](crate::config::Config#include-zarrs-metadata) configuration.
    pub fn set_include_zarrs_metadata(&mut self, include_zarrs_metadata: bool) -> &mut Self {
        self.include_zarrs_metadata = include_zarrs_metadata;
        self
    }
}
