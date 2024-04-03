use crate::config::global_config;

/// Options for writing array metadata.
#[derive(Debug, Clone)]
pub struct ArrayMetadataOptions {
    experimental_codec_store_metadata_if_encode_only: bool,
}

impl Default for ArrayMetadataOptions {
    fn default() -> Self {
        Self {
            experimental_codec_store_metadata_if_encode_only: global_config()
                .experimental_codec_store_metadata_if_encode_only(),
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
    pub fn set_experimental_codec_store_metadata_if_encode_only(&mut self, enabled: bool) {
        self.experimental_codec_store_metadata_if_encode_only = enabled;
    }
}
