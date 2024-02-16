//! Zarrs global configuration options.

use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Global configuration options for the zarrs crate.
///
/// Retrieve the global [`Config`] with [`global_config`] and modify it with [`global_config_mut`].
///
/// # Miscellaneous Configuration Options
///
/// ## Validate Checksums
///  > default: [`true`]
///
/// If enabled, checksum codecs (e.g. `crc32c`) will validate that encoded data matches stored checksums, otherwise validation is skipped.
/// Note that regardless of this configuration option, checksum codecs may skip validation when partial decoding.
///
/// ## Concurrency Configuration Options
/// ## Default Codec Concurrent Limit
/// > default: [`std::thread::available_parallelism`]`()`
///
/// The default concurrent limit for codec encoding and decoding.
/// Limiting concurrency can reduce memory usage and improve performance.
/// The concurrent limit is disabled if set to zero.
///
/// Note that the default codec concurrent limit can be overridden for any encode/decode operation.
///
/// ## Default Chunk Concurrency Minimum
/// > default: `4`
///
/// For array operations involving multiple chunks, this is the preferred minimum chunk concurrency.
/// For example, `array_store_chunks` will concurrently encode and store four chunks at a time by default.
/// The concurrency of internal codecs is adjusted to accomodate for the chunk concurrency in accordance with the concurrent limit set in the [`EncodeOptions`](crate::array::codec::EncodeOptions) or [`DecodeOptions`](crate::array::codec::DecodeOptions) parameter of an encode or decode method.

#[derive(Debug)]
pub struct Config {
    validate_checksums: bool,
    codec_concurrent_limit: usize,
    chunk_concurrent_minimum: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        let concurrency_multiply = 1;
        let concurrency_add = 0;
        Config {
            validate_checksums: true,
            codec_concurrent_limit: std::thread::available_parallelism().unwrap().get()
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

    /// Get the [default codec concurrent limit](#default-codec-concurrent-limit) configuration.
    #[must_use]
    pub fn codec_concurrent_limit(&self) -> usize {
        self.codec_concurrent_limit
    }

    /// Set the [default codec concurrent limit](#default-codec-concurrent-limit) configuration.
    pub fn set_codec_concurrent_limit(&mut self, concurrent_limit: usize) {
        self.codec_concurrent_limit = concurrent_limit;
    }

    /// Get the [default chunk concurrent limit](#default-chunk-concurrent-minimum) configuration.
    #[must_use]
    pub fn chunk_concurrent_minimum(&self) -> usize {
        self.chunk_concurrent_minimum
    }

    /// Set the [default chunk concurrent limit](#default-chunk-concurrent-minimum) configuration.
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
