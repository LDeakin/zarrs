//! Codec options for encoding and decoding.

use crate::config::global_config;

/// Codec options for encoding/decoding.
#[derive(Debug, Clone)]
pub struct CodecOptions {
    concurrent_target: usize,
}

impl Default for CodecOptions {
    fn default() -> Self {
        Self {
            concurrent_target: global_config().codec_concurrent_target(),
        }
    }
}

impl CodecOptions {
    /// Create a new encode options builder.
    #[must_use]
    pub fn builder() -> CodecOptionsBuilder {
        CodecOptionsBuilder::new()
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

/// Builder for [`CodecOptions`].
#[derive(Debug, Clone)]
pub struct CodecOptionsBuilder {
    concurrent_target: usize,
}

impl Default for CodecOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecOptionsBuilder {
    /// Create a new encode options builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            concurrent_target: global_config().codec_concurrent_target(),
        }
    }

    /// Build into encode options.
    #[must_use]
    pub fn build(&self) -> CodecOptions {
        CodecOptions {
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
