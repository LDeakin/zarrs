use crate::config::{global_config, MetadataConvertVersion};

use super::codec::CodecMetadataOptions;

/// Options for writing array metadata.
#[derive(Debug, Clone)]
pub struct ArrayMetadataOptions {
    codec_options: CodecMetadataOptions,
    convert_version: MetadataConvertVersion,
    include_zarrs_metadata: bool,
}

impl Default for ArrayMetadataOptions {
    fn default() -> Self {
        Self {
            codec_options: CodecMetadataOptions::default(),
            convert_version: global_config().metadata_convert_version(),
            include_zarrs_metadata: global_config().include_zarrs_metadata(),
        }
    }
}

impl ArrayMetadataOptions {
    /// Return the codec options.
    #[must_use]
    pub fn codec_options(&self) -> &CodecMetadataOptions {
        &self.codec_options
    }

    /// Return a mutable reference to the codec options.
    #[must_use]
    pub fn codec_options_mut(&mut self) -> &mut CodecMetadataOptions {
        &mut self.codec_options
    }

    /// Get the [metadata convert version](crate::config::Config#metadata-convert-version) configuration.
    #[must_use]
    pub fn metadata_convert_version(&self) -> MetadataConvertVersion {
        self.convert_version
    }

    /// Set the [metadata convert version](crate::config::Config#metadata-convert-version) configuration.
    #[must_use]
    pub fn with_metadata_convert_version(
        mut self,
        convert_version: MetadataConvertVersion,
    ) -> Self {
        self.convert_version = convert_version;
        self
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
    #[must_use]
    pub fn with_include_zarrs_metadata(mut self, include_zarrs_metadata: bool) -> Self {
        self.include_zarrs_metadata = include_zarrs_metadata;
        self
    }

    /// Set the [include zarrs metadata](crate::config::Config#include-zarrs-metadata) configuration.
    pub fn set_include_zarrs_metadata(&mut self, include_zarrs_metadata: bool) -> &mut Self {
        self.include_zarrs_metadata = include_zarrs_metadata;
        self
    }
}
