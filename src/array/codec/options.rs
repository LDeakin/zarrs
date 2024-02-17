//! Options for codec encoding and decoding.

use crate::config::global_config;

/// Encode options.
pub struct EncodeOptions {
    concurrent_limit: usize,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            concurrent_limit: global_config().codec_concurrent_limit(),
        }
    }
}

impl EncodeOptions {
    /// Return the concurrent limit.
    #[must_use]
    pub fn concurrent_limit(&self) -> usize {
        self.concurrent_limit
    }

    /// Set the concurrent limit.
    pub fn set_concurrent_limit(&mut self, concurrent_limit: usize) -> &mut Self {
        self.concurrent_limit = concurrent_limit;
        self
    }
}

/// Decode options.
pub type DecodeOptions = EncodeOptions;

/// Partial decoder options.
pub type PartialDecoderOptions = DecodeOptions;

/// Partial decode options.
pub type PartialDecodeOptions = DecodeOptions;
