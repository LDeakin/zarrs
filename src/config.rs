//! Zarrs global configuration options.

use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(doc)]
use crate::array::codec::CodecOptions;

/// Global configuration options for the zarrs crate.
///
/// Retrieve the global [`Config`] with [`global_config`] and modify it with [`global_config_mut`].
///
/// ## Validate Checksums
///  > default: [`true`]
///
/// [`CodecOptions::validate_checksums()`] defaults to [`Config::validate_checksums()`].
///
/// If validate checksums is enabled, checksum codecs (e.g. `crc32c`) will validate that encoded data matches stored checksums, otherwise validation is skipped.
/// Note that regardless of this configuration option, checksum codecs may skip validation when partial decoding.
///
/// ## Default Codec Concurrent Target
/// > default: [`std::thread::available_parallelism`]`()`
///
/// [`CodecOptions::concurrent_target()`] defaults to [`Config::codec_concurrent_target()`].
///
/// The default number of concurrent operations to target for codec encoding and decoding.
/// Limiting concurrent operations is needed to reduce memory usage and improve performance.
/// Concurrency is unconstrained if the concurrent target if set to zero.
///
/// Note that the default codec concurrent target can be overridden for any encode/decode operation.
/// This is performed automatically for many array operations (see the [chunk concurrent minimum](#chunk-concurrent-minimum) option).
///
/// ## Chunk Concurrent Minimum
/// > default: `4`
///
/// For array operations involving multiple chunks, this is the preferred minimum chunk concurrency.
/// For example, `array_store_chunks` will concurrently encode and store up to four chunks at a time by default.
/// The concurrency of internal codecs is adjusted to accomodate for the chunk concurrency in accordance with the concurrent target set in the [`CodecOptions`] parameter of an encode or decode method.
#[derive(Debug)]
pub struct Config {
    validate_checksums: bool,
    codec_concurrent_target: usize,
    chunk_concurrent_minimum: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        let concurrency_multiply = 1;
        let concurrency_add = 0;
        Self {
            validate_checksums: true,
            codec_concurrent_target: std::thread::available_parallelism().unwrap().get()
                * concurrency_multiply
                + concurrency_add,
            chunk_concurrent_minimum: 4,
        }
    }
}

impl Config {
    /// Get the [validate checksums](#validate-checksums) configuration.
    #[must_use]
    pub fn validate_checksums(&self) -> bool {
        self.validate_checksums
    }

    /// Set the [validate checksums](#validate-checksums) configuration.
    pub fn set_validate_checksums(&mut self, validate_checksums: bool) {
        self.validate_checksums = validate_checksums;
    }

    /// Get the [codec concurrent target](#codec-concurrent-target) configuration.
    #[must_use]
    pub fn codec_concurrent_target(&self) -> usize {
        self.codec_concurrent_target
    }

    /// Set the [codec concurrent target](#codec-concurrent-target) configuration.
    pub fn set_codec_concurrent_target(&mut self, concurrent_target: usize) {
        self.codec_concurrent_target = concurrent_target;
    }

    /// Get the [chunk concurrent minimum](#chunk-concurrent-minimum) configuration.
    #[must_use]
    pub fn chunk_concurrent_minimum(&self) -> usize {
        self.chunk_concurrent_minimum
    }

    /// Set the [chunk concurrent minimum](#chunk-concurrent-minimum) configuration.
    pub fn set_chunk_concurrent_minimum(&mut self, concurrent_minimum: usize) {
        self.chunk_concurrent_minimum = concurrent_minimum;
    }
}

static CONFIG: OnceLock<RwLock<Config>> = OnceLock::new();

/// Returns a reference to the global zarrs configuration.
///
/// # Panics
/// This function panics if the underlying lock has been poisoned and might panic if the global config is already held by the current thread.
pub fn global_config() -> RwLockReadGuard<'static, Config> {
    CONFIG
        .get_or_init(|| RwLock::new(Config::default()))
        .read()
        .unwrap()
}

/// Returns a mutable reference to the global zarrs configuration.
///
/// # Panics
/// This function panics if the underlying lock has been poisoned and might panic if the global config is already held by the current thread.
pub fn global_config_mut() -> RwLockWriteGuard<'static, Config> {
    CONFIG
        .get_or_init(|| RwLock::new(Config::default()))
        .write()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validate_checksums() {
        assert!(global_config().validate_checksums());
        global_config_mut().set_validate_checksums(false);
        assert!(!global_config().validate_checksums());
        global_config_mut().set_validate_checksums(true);
    }
}
