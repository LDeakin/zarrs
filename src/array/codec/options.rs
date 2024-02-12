//! Options for codec encoding and decoding.

use std::num::NonZeroUsize;

use crate::config::global_config;

/// Encode options.
pub struct EncodeOptions {
    concurrent_limit: NonZeroUsize,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            concurrent_limit: global_config().concurrent_limit(),
        }
    }
}

impl EncodeOptions {
    /// Return the concurrent limit.
    #[must_use]
    pub fn concurrent_limit(&self) -> NonZeroUsize {
        self.concurrent_limit
    }

    /// Set the concurrent limit.
    pub fn set_concurrent_limit(&mut self, concurrent_limit: NonZeroUsize) {
        self.concurrent_limit = concurrent_limit;
    }
}

/// Decode options.
pub struct DecodeOptions {
    concurrent_limit: NonZeroUsize,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            concurrent_limit: global_config().concurrent_limit(),
        }
    }
}

impl DecodeOptions {
    /// Return the concurrent limit.
    #[must_use]
    pub fn concurrent_limit(&self) -> NonZeroUsize {
        self.concurrent_limit
    }

    /// Set the concurrent limit.
    pub fn set_concurrent_limit(&mut self, concurrent_limit: NonZeroUsize) {
        self.concurrent_limit = concurrent_limit;
    }
}

/// Partial decoder options.
pub type PartialDecoderOptions = DecodeOptions;

/// Partial decode options.
pub type PartialDecodeOptions = DecodeOptions;
