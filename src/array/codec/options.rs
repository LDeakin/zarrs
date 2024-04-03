//! Codec options for encoding and decoding.

use crate::config::global_config;

/// Codec options for encoding/decoding.
#[derive(Debug, Clone)]
pub struct CodecOptions {
    validate_checksums: bool,
    concurrent_target: usize,
}

impl Default for CodecOptions {
    fn default() -> Self {
        Self {
            validate_checksums: global_config().validate_checksums(),
            concurrent_target: global_config().codec_concurrent_target(),
        }
    }
}

impl CodecOptions {
    /// Create a new default codec options builder.
    #[must_use]
    pub fn builder() -> CodecOptionsBuilder {
        CodecOptionsBuilder::new()
    }

    /// Copy codec options into a new [`CodecOptionsBuilder`].
    #[must_use]
    pub fn into_builder(&self) -> CodecOptionsBuilder {
        CodecOptionsBuilder {
            validate_checksums: self.validate_checksums,
            concurrent_target: self.concurrent_target,
        }
    }

    /// Return the validate checksums setting.
    #[must_use]
    pub fn validate_checksums(&self) -> bool {
        self.validate_checksums
    }

    /// Set whether or not to validate checksums.
    pub fn set_validate_checksums(&mut self, validate_checksums: bool) {
        self.validate_checksums = validate_checksums;
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
    validate_checksums: bool,
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
            validate_checksums: global_config().validate_checksums(),
            concurrent_target: global_config().codec_concurrent_target(),
        }
    }

    /// Build into encode options.
    #[must_use]
    pub fn build(&self) -> CodecOptions {
        CodecOptions {
            validate_checksums: self.validate_checksums,
            concurrent_target: self.concurrent_target,
        }
    }

    /// Set whether or not to validate checksums.
    #[must_use]
    pub fn validate_checksums(mut self, validate_checksums: bool) -> Self {
        self.validate_checksums = validate_checksums;
        self
    }

    /// Set the concurrent target for parallel operations.
    #[must_use]
    pub fn concurrent_target(mut self, concurrent_target: usize) -> Self {
        self.concurrent_target = concurrent_target;
        self
    }
}
