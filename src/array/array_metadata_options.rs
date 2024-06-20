use crate::config::global_config;

/// Options for writing array metadata.
#[derive(Debug, Clone)]
pub struct ArrayMetadataOptions {
    experimental_codec_store_metadata_if_encode_only: bool,
    version: ArrayMetadataOptionsVersion,
}

/// Array [`Array::store_metadata`](crate::array::Array::store_metadata) version options.
#[derive(Debug, Clone, Copy)]
pub enum ArrayMetadataOptionsVersion {
    /// Write the same version as the input metadata.
    Unchanged,
    /// Write Zarr V3 metadata. Zarr V2 will not be automatically removed if it exists.
    V3,
}

impl Default for ArrayMetadataOptions {
    fn default() -> Self {
        Self {
            experimental_codec_store_metadata_if_encode_only: global_config()
                .experimental_codec_store_metadata_if_encode_only(),
            version: *global_config().array_metadata_version(),
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

    /// Get the [array metadata version behaviour](crate::config::Config#array-metadata-version-behaviour) configuration.
    #[must_use]
    pub fn array_metadata_version(&self) -> &ArrayMetadataOptionsVersion {
        &self.version
    }

    /// Set the [array metadata version behaviour](crate::config::Config#array-metadata-version-behaviour) configuration.
    pub fn set_array_metadata_version(
        &mut self,
        version: ArrayMetadataOptionsVersion,
    ) -> &mut Self {
        self.version = version;
        self
    }
}
