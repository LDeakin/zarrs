//! Options for codec encoding and decoding.

use crate::config::global_config;

/// Encode options.
#[derive(Debug, Clone)]
pub struct EncodeOptions {
    concurrent_target: usize,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            concurrent_target: global_config().codec_concurrent_target(),
        }
    }
}

impl EncodeOptions {
    /// Create a new encode options builder.
    #[must_use]
    pub fn builder() -> EncodeOptionsBuilder {
        EncodeOptionsBuilder::new()
    }

    /// Return the concurrent target.
    #[must_use]
    pub fn concurrent_target(&self) -> usize {
        self.concurrent_target
    }

    /// Set the concurrent target.
    pub fn set_concurrent_target(&mut self, concurrent_target: usize) {
        self.concurrent_target = concurrent_target;
    }
}

/// Builder for [`EncodeOptions`].
#[derive(Debug, Clone)]
pub struct EncodeOptionsBuilder {
    concurrent_target: usize,
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
            concurrent_target: global_config().codec_concurrent_target(),
        }
    }

    /// Build into encode options.
    #[must_use]
    pub fn build(&self) -> EncodeOptions {
        EncodeOptions {
            concurrent_target: self.concurrent_target,
        }
    }

    /// Set the concurrent target for parallel operations.
    #[must_use]
    pub fn concurrent_target(mut self, concurrent_target: usize) -> Self {
        self.concurrent_target = concurrent_target;
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
