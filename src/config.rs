//! Zarrs global configuration options.

use std::{
    num::NonZeroUsize,
    sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// Global configuration options for the zarrs crate.
///
/// Retrieve the global [`Config`] with [`global_config`] and modify it with [`global_config_mut`].
///
/// ## Configuration Options
///
/// ### Validate Checksums
///  > default: [`true`]
///
/// If enabled, checksum codecs (e.g. `crc32c`) will validate that encoded data matches stored checksums, otherwise validation is skipped.
/// Note that regardless of this configuration option, checksum codecs may skip validation when partial decoding.
///
/// ### Default Concurrent Limit
/// > default: [`std::thread::available_parallelism`]`()`
///
/// The default concurrent limit for parallel operations, such as encoding and decoding.
#[derive(Debug)]
pub struct Config {
    validate_checksums: bool,
    concurrent_limit: NonZeroUsize,
}

#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        let concurrency_multiply = unsafe { NonZeroUsize::new_unchecked(1) };
        let concurrency_add = 0;
        Config {
            validate_checksums: true,
            concurrent_limit: std::thread::available_parallelism()
                .unwrap()
                .saturating_mul(concurrency_multiply)
                .saturating_add(concurrency_add),
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

    /// Get the [default concurrent limit](#default-concurrent-limit) configuration.
    #[must_use]
    pub fn concurrent_limit(&self) -> NonZeroUsize {
        self.concurrent_limit
    }

    /// Set the [validate checksums](#validate-checksums) configuration.
    pub fn set_concurrent_limit(&mut self, concurrent_limit: NonZeroUsize) {
        self.concurrent_limit = concurrent_limit;
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
