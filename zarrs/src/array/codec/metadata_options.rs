//! Codec metadata options.

/// Options for codec metadata.
#[derive(Debug, Clone, Default)]
pub struct CodecMetadataOptions {
    experimental_codec_store_metadata_if_encode_only: bool,
}

// impl Default for CodecMetadataOptions {
//     fn default() -> Self {
//         Self {
//             experimental_codec_store_metadata_if_encode_only: false,
//         }
//     }
// }

impl CodecMetadataOptions {
    /// Return the [experimental codec store metadata if encode only](crate::config::Config#experimental-codec-store-metadata-if-encode-only) setting.
    #[must_use]
    pub fn experimental_codec_store_metadata_if_encode_only(&self) -> bool {
        self.experimental_codec_store_metadata_if_encode_only
    }

    /// Set the [experimental codec store metadata if encode only](crate::config::Config#experimental-codec-store-metadata-if-encode-only) setting.
    #[must_use]
    pub fn with_experimental_codec_store_metadata_if_encode_only(mut self, enabled: bool) -> Self {
        self.experimental_codec_store_metadata_if_encode_only = enabled;
        self
    }

    /// Set the [experimental codec store metadata if encode only](crate::config::Config#experimental-codec-store-metadata-if-encode-only) setting.
    pub fn set_experimental_codec_store_metadata_if_encode_only(
        &mut self,
        enabled: bool,
    ) -> &mut Self {
        self.experimental_codec_store_metadata_if_encode_only = enabled;
        self
    }
}
