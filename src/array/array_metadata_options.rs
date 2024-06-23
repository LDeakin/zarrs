use crate::{config::global_config, metadata::MetadataConvertVersion};

/// Options for writing array metadata.
#[derive(Debug, Clone)]
pub struct ArrayMetadataOptions {
    experimental_codec_store_metadata_if_encode_only: bool,
    convert_version: MetadataConvertVersion,
    include_zarrs_metadata: bool,
}

impl Default for ArrayMetadataOptions {
    fn default() -> Self {
        Self {
            experimental_codec_store_metadata_if_encode_only: false,
            convert_version: MetadataConvertVersion::default(),
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

    /// Get the [metadata convert version](crate::config::Config#metadata-convert-version) configuration.
    #[must_use]
    pub fn metadata_convert_version(&self) -> &MetadataConvertVersion {
        &self.convert_version
    }

    /// Set the [metadata convert version](crate::config::Config#metadata-convert-version) configuration.
    pub fn set_metadata_convert_version(
        &mut self,
        convert_version: MetadataConvertVersion,
    ) -> &mut Self {
        self.convert_version = convert_version;
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
