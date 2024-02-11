//! Options for codec encoding and decoding.

use std::num::NonZeroUsize;

/// Encode options.
pub struct EncodeOptions {
    concurrent_limit: NonZeroUsize,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            concurrent_limit: std::thread::available_parallelism().unwrap(),
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

    /// FIXME: Temporary, remove
    #[must_use]
    pub fn is_parallel(&self) -> bool {
        self.concurrent_limit.get() > 1
    }
}

/// Decode options.
pub struct DecodeOptions {
    concurrent_limit: NonZeroUsize,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            concurrent_limit: std::thread::available_parallelism().unwrap(),
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

    /// FIXME: Temporary, remove
    #[must_use]
    pub fn is_parallel(&self) -> bool {
        self.concurrent_limit.get() > 1
    }
}

/// Partial decoder options.
pub type PartialDecoderOptions = DecodeOptions;

/// Partial decode options.
pub type PartialDecodeOptions = DecodeOptions;
