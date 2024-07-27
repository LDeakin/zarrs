//! `zarrs` global configuration options.
//!
//! See [`Config`] for the list of options.

use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

#[cfg(doc)]
use crate::array::{codec::CodecOptions, ArrayMetadataOptions};
use crate::metadata::{v3::codec, MetadataConvertVersion, MetadataEraseVersion};

/// Global configuration options for the `zarrs` crate.
///
/// Retrieve the global [`Config`] with [`global_config`] and modify it with [`global_config_mut`].
///
/// ## Codec / Chunk Options
///
/// ### Validate Checksums
///  > default: [`true`]
///
/// [`CodecOptions::validate_checksums()`] defaults to [`Config::validate_checksums()`].
///
/// If validate checksums is enabled, checksum codecs (e.g. `crc32c`) will validate that encoded data matches stored checksums, otherwise validation is skipped.
/// Note that regardless of this configuration option, checksum codecs may skip validation when partial decoding.
///
/// ### Store Empty Chunks
///  > default: [`false`]
///
/// [`CodecOptions::store_empty_chunks()`] defaults to [`Config::store_empty_chunks()`].
///
/// If `false`, empty chunks (where all elements match the fill value) will not be stored.
/// This incurs a computational overhead as each element must be tested for equality to the fill value before a chunk is encoded.
/// If `true`, the aforementioned test is skipped and all chunks are stored.
///
/// ### Codec Concurrent Target
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
/// ### Chunk Concurrent Minimum
/// > default: `4`
///
/// For array operations involving multiple chunks, this is the preferred minimum chunk concurrency.
/// For example, `array_store_chunks` will concurrently encode and store up to four chunks at a time by default.
/// The concurrency of internal codecs is adjusted to accomodate for the chunk concurrency in accordance with the concurrent target set in the [`CodecOptions`] parameter of an encode or decode method.
///
/// ## Metadata Options
///
/// ### Experimental Codec Store Metadata If Encode Only
/// > default: [`false`]
///
/// Some codecs perform potentially irreversible transformations during encoding that decoders do not need to be aware of.
/// If this option is `false`, experimental codecs with this behaviour will not write their metadata.
/// This enables arrays to be consumed by other zarr3 implementations that do not support the experimental codec.
/// Currently, this options only affects the `bitround` codec.
///
/// ### Metadata Convert Version
/// > default: [`MetadataConvertVersion::Default`] (keep existing version)
///
/// Determines the Zarr version of metadata created with [`Array::metadata_opt`](crate::array::Array::metadata_opt) and [`Group::metadata_opt`](crate::group::Group::metadata_opt).
/// These methods are used internally by the `store_metadata` and `store_metadata_opt` methods of [`crate::array::Array`] and [`crate::group::Group`].
///
/// ### Metadata Erase Version
/// > default: [`MetadataEraseVersion::Default`] (erase existing version)
///
/// The default behaviour for the `erase_metadata` methods of [`crate::array::Array`] and [`crate::group::Group`].
/// Determines whether to erase metadata of a specific Zarr version, the same version as the array/group was created with, or all known versions.
///
/// ### Include `zarrs` Metadata
/// > default: [`true`]
///
/// [`ArrayMetadataOptions::include_zarrs_metadata`](crate::array::ArrayMetadataOptions::include_zarrs_metadata) defaults to [`Config::include_zarrs_metadata`].
///
/// If true, array metadata generated with [`Array::metadata_opt`](crate::array::Array::metadata_opt) (used internally by [`Array::store_metadata`](crate::array::Array::store_metadata)) includes the `zarrs` version and a link to its source code.
/// For example:
/// ```json
/// "_zarrs": {
///    "description": "This array was created with zarrs",
///    "repository": "https://github.com/LDeakin/zarrs",
///    "version": "0.15.0"
///  }
/// ```
///
/// ### Experimental Codec Names
/// > default: See the [crate root documentation](crate#array-support).
///
/// Sets the names used when serialising and deserialising the names of experimental codecs.
/// Deserialisation also accepts the standard `IDENTIFIER` of the codec.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct Config {
    validate_checksums: bool,
    store_empty_chunks: bool,
    codec_concurrent_target: usize,
    chunk_concurrent_minimum: usize,
    experimental_codec_store_metadata_if_encode_only: bool,
    metadata_convert_version: MetadataConvertVersion,
    metadata_erase_version: MetadataEraseVersion,
    include_zarrs_metadata: bool,
    experimental_codec_names: HashMap<&'static str, String>,
}

#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        #[rustfmt::skip]
        let experimental_codec_names = HashMap::from([
            // Array to array
            #[cfg(feature = "bitround")]
            (codec::bitround::IDENTIFIER, "https://codec.zarrs.dev/array_to_array/bitround".to_string()),
            // Array to bytes
            #[cfg(feature = "zfp")]
            (codec::zfp::IDENTIFIER, "https://codec.zarrs.dev/array_to_bytes/zfp".to_string()),
            #[cfg(feature = "pcodec")]
            (codec::pcodec::IDENTIFIER, "https://codec.zarrs.dev/array_to_bytes/pcodec".to_string()),
            (codec::vlen::IDENTIFIER, "https://codec.zarrs.dev/array_to_bytes/vlen".to_string()),
            (codec::vlen_v2::IDENTIFIER, "https://codec.zarrs.dev/array_to_bytes/vlen_v2".to_string()),
            // Bytes to bytes
            #[cfg(feature = "bz2")]
            (codec::bz2::IDENTIFIER, "https://codec.zarrs.dev/bytes_to_bytes/bz2".to_string()),
        ]);

        let concurrency_multiply = 1;
        let concurrency_add = 0;
        Self {
            validate_checksums: true,
            store_empty_chunks: false,
            codec_concurrent_target: std::thread::available_parallelism().unwrap().get()
                * concurrency_multiply
                + concurrency_add,
            chunk_concurrent_minimum: 4,
            experimental_codec_store_metadata_if_encode_only: false,
            metadata_convert_version: MetadataConvertVersion::Default,
            metadata_erase_version: MetadataEraseVersion::Default,
            include_zarrs_metadata: true,
            experimental_codec_names,
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
    pub fn set_validate_checksums(&mut self, validate_checksums: bool) -> &mut Self {
        self.validate_checksums = validate_checksums;
        self
    }

    /// Get the [store empty chunks](#store-empty-chunks) configuration.
    #[must_use]
    pub fn store_empty_chunks(&self) -> bool {
        self.store_empty_chunks
    }

    /// Set the [store empty chunks](#store-empty-chunks) configuration.
    pub fn set_store_empty_chunks(&mut self, store_empty_chunks: bool) -> &mut Self {
        self.store_empty_chunks = store_empty_chunks;
        self
    }

    /// Get the [codec concurrent target](#codec-concurrent-target) configuration.
    #[must_use]
    pub fn codec_concurrent_target(&self) -> usize {
        self.codec_concurrent_target
    }

    /// Set the [codec concurrent target](#codec-concurrent-target) configuration.
    pub fn set_codec_concurrent_target(&mut self, concurrent_target: usize) -> &mut Self {
        self.codec_concurrent_target = concurrent_target;
        self
    }

    /// Get the [chunk concurrent minimum](#chunk-concurrent-minimum) configuration.
    #[must_use]
    pub fn chunk_concurrent_minimum(&self) -> usize {
        self.chunk_concurrent_minimum
    }

    /// Set the [chunk concurrent minimum](#chunk-concurrent-minimum) configuration.
    pub fn set_chunk_concurrent_minimum(&mut self, concurrent_minimum: usize) -> &mut Self {
        self.chunk_concurrent_minimum = concurrent_minimum;
        self
    }

    /// Get the [experimental codec store metadata if encode only](#experimental-codec-store-metadata-if-encode-only) configuration.
    #[must_use]
    pub fn experimental_codec_store_metadata_if_encode_only(&self) -> bool {
        self.experimental_codec_store_metadata_if_encode_only
    }

    /// Set the [experimental codec store metadata if encode only](#experimental-codec-store-metadata-if-encode-only) configuration.
    pub fn set_experimental_codec_store_metadata_if_encode_only(
        &mut self,
        enabled: bool,
    ) -> &mut Self {
        self.experimental_codec_store_metadata_if_encode_only = enabled;
        self
    }

    /// Get the [metadata convert version](#metadata-convert-version) configuration.
    #[must_use]
    pub fn metadata_convert_version(&self) -> &MetadataConvertVersion {
        &self.metadata_convert_version
    }

    /// Set the [metadata convert version](#metadata-convert-version) configuration.
    pub fn set_metadata_convert_version(&mut self, version: MetadataConvertVersion) -> &mut Self {
        self.metadata_convert_version = version;
        self
    }

    /// Get the [metadata erase version behaviour](#metadata-erase-version-behaviour) configuration.
    #[must_use]
    pub fn metadata_erase_version(&self) -> &MetadataEraseVersion {
        &self.metadata_erase_version
    }

    /// Set the [metadata erase version behaviour](#metadata-erase-version-behaviour) configuration.
    pub fn set_metadata_erase_version(&mut self, version: MetadataEraseVersion) -> &mut Self {
        self.metadata_erase_version = version;
        self
    }

    /// Get the [include zarrs metadata](#include-zarrs-metadata) configuration.
    #[must_use]
    pub fn include_zarrs_metadata(&self) -> bool {
        self.include_zarrs_metadata
    }

    /// Set the [include zarrs metadata](#include-zarrs-metadata) configuration.
    pub fn set_include_zarrs_metadata(&mut self, include_zarrs_metadata: bool) -> &mut Self {
        self.include_zarrs_metadata = include_zarrs_metadata;
        self
    }

    /// Get the [experimental codec names](#experimental-codec-names) configuration.
    #[must_use]
    pub fn experimental_codec_names(&self) -> &HashMap<&'static str, String> {
        &self.experimental_codec_names
    }

    /// Get a mutable reference to the [experimental codec names](#experimental-codec-names) configuration.
    pub fn experimental_codec_names_mut(&mut self) -> &mut HashMap<&'static str, String> {
        &mut self.experimental_codec_names
    }
}

static CONFIG: OnceLock<RwLock<Config>> = OnceLock::new();

/// Returns a reference to the global `zarrs` configuration.
///
/// # Panics
/// This function panics if the underlying lock has been poisoned and might panic if the global config is already held by the current thread.
pub fn global_config() -> RwLockReadGuard<'static, Config> {
    CONFIG
        .get_or_init(|| RwLock::new(Config::default()))
        .read()
        .unwrap()
}

/// Returns a mutable reference to the global `zarrs` configuration.
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
