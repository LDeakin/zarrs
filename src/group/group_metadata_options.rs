use crate::metadata::MetadataConvertVersion;

/// Options for writing group metadata.
#[derive(Debug, Clone, Default)]
pub struct GroupMetadataOptions {
    convert_version: MetadataConvertVersion,
}

impl GroupMetadataOptions {
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
}
