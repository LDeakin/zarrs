//! Options for codec encoding and decoding.

use crate::config::global_config;

/// Encode options.
#[derive(Debug, Clone)]
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
    /// Create a new encode options builder.
    #[must_use]
    pub fn builder() -> EncodeOptionsBuilder {
        EncodeOptionsBuilder::new()
    }

    /// Return the concurrent limit.
    #[must_use]
    pub fn concurrent_limit(&self) -> usize {
        self.concurrent_limit
    }

    /// Set the concurrent limit.
    pub fn set_concurrent_limit(&mut self, concurrent_limit: usize) {
        self.concurrent_limit = concurrent_limit;
    }
}

/// Builder for [`EncodeOptions`].
#[derive(Debug, Clone)]
pub struct EncodeOptionsBuilder {
    concurrent_limit: usize,
}

impl Default for EncodeOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EncodeOptionsBuilder {
    /// Create a new encode options builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            concurrent_limit: global_config().codec_concurrent_limit(),
        }
    }

    /// Build into encode options.
    #[must_use]
    pub fn build(&self) -> EncodeOptions {
        EncodeOptions {
            concurrent_limit: self.concurrent_limit,
        }
    }

    /// Set the concurrent limit for parallel operations.
    #[must_use]
    pub fn concurrent_limit(mut self, concurrent_limit: usize) -> Self {
        self.concurrent_limit = concurrent_limit;
        self
    }
}

/// Decode options.
pub type DecodeOptions = EncodeOptions;

/// Decode options builder.
pub type DecodeOptionsBuilder = EncodeOptionsBuilder;

/// Partial decoder options.
pub type PartialDecoderOptions = DecodeOptions;

/// Partial decoder options builder.
pub type PartialDecoderOptionsBuilder = EncodeOptionsBuilder;

/// Partial decode options.
pub type PartialDecodeOptions = DecodeOptions;

/// Partial decode options builder.
pub type PartialDecodeOptionsBuilder = EncodeOptionsBuilder;
