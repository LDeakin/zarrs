# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
 - **Breaking**: Added `UnsupportedDataTypeError`
 - **Breaking**: Added `CodecError::UnsupportedDataType`
 - Added `array_subset::IncompatibleArrayShapeError`
 - Added `array_subset::iter_linearised_indices_unchecked`
 - Added parallel encoding/decoding to tests
 - Added `array_subset::ArrayStoreBytesError`, `store_bytes`, and `store_bytes_unchecked`
 - Added `parallel_chunks` option to `Array`, enabled by default. Lets `store_array_subset` and `retrieve_array_subset` (and their variants) encode/decode chunks in parallel
 - Added experimental `zfp` codec implementation behind `zfp` feature flag (disabled by default)
 - Added experimental `bitround` codec implementation behind `bitround` feature flag (disabled by default)
 - Added `ShardingCodecBuilder`
 - Added `ReadableListableStorage`, `ReadableListableStorageTraits`, `StorageTransformerExtension::create_readable_listable_transformer`
 - Added `ByteRange::to_range()` and `to_range_usize()`

### Changed
 - **Breaking**: `array::data_type::DataType` is now marked `#[non_exhaustive]`
 - **Breaking**: Promote the `r*` (raw bits), `float16` and `bfloat16` data types to standard data types in `array::data_type::DataType`, rather than extension data types
   - **Breaking**: Remove the crate features: `raw_bits`, `float16`, `bfloat16`
   - **Breaking**: Removes `array::data_type::RawBitsDataType/Bfloat16DataType/Float16DataType`
   - **Breaking**: `half` is now a required dependency
 - **Breaking**: Rename `TransposeCodec::new` to `new_with_configuration`
 - **Breaking**: Array subset methods dependent on an `array_shape` now use the `IncompatibleArrayShapeError` error type instead of `IncompatibleDimensionalityError`
 - **Breaking**: Various array subset iterators now have validated `new` and unvalidated `new_unchecked` constructors
 - Blosc codec: disable parallel encoding/decoding for small inputs/outputs
 - Bytes codec: optimise implementation
 - **Breaking**: `ArrayToArrayCodecTraits::compute_encoded_size` and `ArrayToBytesCodecTraits::compute_encoded_size` can now return a `CodecError`
 - `ArrayBuilder::build()` and `GroupBuilder::build` now accept unsized storage
 - **Breaking**: `StoragePartialDecoder` now takes a `ReadableStorage` input
 - `sharded_array_write_read` example now prints storage operations and demonstrates retrieving inner chunks directly from a partial decoder
 - the zarrs version and a link to the source code is now written to the `_zarrs` attribute in array metadata, this can be disabled with `set_include_zarrs_metadata(false)`

### Fixed
 - Bytes codec handling of complex and raw bits data types
 - Additional debug assertions to validate input in array subset `_unchecked` functions
 - **Breaking**: `array_subset::iter_linearised_indices` now returns a `Result<_, IncompatibleArrayShapeError>`, previously it could not fail even if the `array_shape` did not enclose the array subset
 - The `array_subset_iter_contiguous_indices3` test was incorrect as the array shape was invalid for the array subset
 - `ArraySubset::extract_bytes` now reserves the appropriate amount of memory
 - Sharding codec performance optimisations
 - `FilesystemStore::erase_prefix` now correctly removes non-empty directories

### Removed
 - **Breaking**: Disabled data type extensions `array::data_type::DataType::Extension`.

## [0.5.1] - 2023-10-10

### Added
 - Tests for `DataType::fill_value_from_metadata`
 - A paragraph on concurrency in the `Array` docs

### Changed
 - Fix some docs typos

### Fixed
 - `FillValueMetadata::try_as_uint/int` can both now handle `FillValueMetadata::Uint/Int`
 - `Array::store_chunk` now erases empty chunks
 - Fixed a race in `Array::store_chunk_subset` and add a fast path if the subset spans the whole chunk
 - Fix complex number handling in `DataType::fill_value_from_metadata`
 - Fix byte arrays being interpreted as complex number fill value metadata

## [0.5.0] - 2023-10-08

### Added
 - `MaybeBytes` an alias for `Option<Vec<u8>>`
   - When a value is read from the store but the key is not found, the returned value is now `None` instead of an error indicating a missing key
 - Add `storage::erase_chunk` and `Array::erase_chunk`
   - The `array_write_read` example is updated to demonstrate `erase_chunk`
 - Add `TryFrom::<&str>` for `Metadata` and `FillValueMetadata`
 - Add `chunk_key_encoding` and `chunk_key_encoding_default_separator` to `ArrayBuilder`
 - Add `TryFrom<char>` for `ChunkKeySeparator`
 - Add `ArraySubset::new_with_start_end_inc/new_with_start_end_exc`
 - Add `codec::extract_byte_ranges_read` utility function to read byte ranges from a `Read` source which does not support `Seek`

### Changed

 - **Breaking**: Remove `StorageError::KeyNotFound` in favour of returning `MaybeBytes`
 - **Breaking**: Add `StorageError::UnknownKeySize` if a method requires a known key size, but the key size cannot be determined
 - **Breaking**: Add `ArrayCreateError::MissingMetadata` if attempting to create an array in a store without specifying metadata, and the metadata does not exist
 - **Breaking**: `UsageLogStorageTransformer` now takes a prefix function rather than a string
   - The `http_array_read` example now logs store requests to stdout using `UsageLogStorageTransformer`
 - **Breaking**: `WritableStorageTraits::erase/erase_values/erase_prefix` return a boolean indicating if they actually deleted something
 - **Breaking**: Add `ReadableStorageTraits::get_partial_values_key` which reads byte ranges for a store key
 - **Breaking**: Changed `data_type::try_create_data_type` to `DataType::from_metadata`
 - **Breaking**: Changed `try_create_codec` to `Codec::from_metadata`
 - Make `ArrayBuilder` and `GroupBuilder` non-consuming
 - Add a fast-path to `Array::store_array_subset` if the array subset matches a chunk subset
 - **Breaking**: Make `ChunkKeyEncoding` a newtype.
   - **Breaking**: Changed `try_create_chunk_key_encoding` to `ChunkKeyEncoding::from_metadata`.
 - **Breaking**: Make `ChunkGrid` a newtype.
   - **Breaking**: Changed `try_create_chunk_grid` to `ChunkGrid::from_metadata`.
 - **Breaking**: Rename `StorageTransformerChain::new_with_metadatas` to `from_metadata`
 - **Breaking**: Rename `CodecChain::new_with_metadatas` to `from_metadata`
 - **Breaking**: Rename `DataTypeExtension::try_create_fill_value` to `fill_value_from_metadata`
 - **Breaking**: Rename `codec::extract_byte_ranges_rs` to `extract_byte_ranges_read_seek`

### Fixed

 - `BytesCodec` now defaults to native endian encoding as opposed to no encoding (only valid for 8 bit data types).
 - `storage::get_child_nodes` and `Node::new_with_store` now correctly propagate storage errors instead of treating all errors as missing metadata
 - `Group::new` now handles an implicit group (with a missing `zarr.json`)
 - `ZipStore` handle missing files
 - `ZipStore` no longer reads an internal file multiple times when extracting multiple byte ranges
 - `HTTPStore` improve error handling, check status codes

## [0.4.2] - 2023-10-06

### Added

 - Add `From<&str>` and `From<String>` for `Other` variants of `CodecError`, `StorageError`, `ChunkGridShapeError`, `StorePluginCreateError`

### Changed

 - Support parallel encoding, parallel decoding, and partial decoding with the `blosc` codec
   - Previously the blosc codec would read the whole chunk when partial decoding
 - `HTTPStore` range requests are now batched by default

### Fixed

 - Fixed `retrieve_chunk_subset` returning fill values on any `StorageError` instead of just `StorageError::KeyNotFound`

## [0.4.1] - 2023-10-04

### Added
 - Add `StorageHandle`, a clonable handle to borrowed unsized storage

### Changed
 - Support unsized storage (`TStorage: ?Sized` everywhere)

## [0.4.0] - 2023-10-04

### Added
 - Readable and listable `ZipStorageAdapter` behind `zip` feature
 - Readable `HTTPStore` behind `http` feature
 - Add `StorageValueIO`, a `std::io::Read` interface to a storage value.
 - Add `zip_array_write_read` and `http_array_read` examples

### Changed
 - Relax some dependency minimum versions
 - Add `size_hint()` to some array subset iterators
 - **Breaking**: Fix `LinearisedIndicesIterator` to use array shape instead of subset shape
   - `LinearisedIndicesIterator::new` and `ArraySubset::iter_linearised_indices` now require `array_shape` parameter
 - **Breaking**: Array subset shape no longer needs to be evenly divisible by chunk shape when creating a chunk iterator
   - Removes `ChunksIteratorError`
 - Add array subset iterator tests
 - **Breaking**: Remove unvalidated `from(String)` for store key/prefix
 - Validate that store prefixes and keys do not start with `/`
 - **Breaking**: Add `StorageError::Other` and `StorageError::Unsupported` variants
 - **Breaking** `FilesystemStore` now accepts a file and does not create directory on creation
   - adds `FilesystemStoreCreateError:InvalidBasePath` and removes `FilesystemStoreCreateError:InvalidBaseDirectory/ExistingFile`
 - **Breaking**: `ReadableStorageTraits::size()` to `u64` from `usize`
 - **Breaking**: Add `ReadableStorageTraits::size_key()`
 - Storage and store traits no longer require `Debug`
 - Add a default implementation for `WritableStorageTraits::erase_values`
 - Make array/group explicitly store `Arc<TStorage>`
 - `StorageTransformerChain` now only accepts `Arc` storage.
 - **Breaking**: Various group methods are now `#[must_use]`
 - Change `NodePath` internal representation to `PathBuf`
 - Remove unneeded '/' prefix strip in `StorePrefix`
 - **Breaking**: Remove storage trait implementations for `Arc<TStorage>`, now must explicitly deref
 - Implement `Eq` and `PartialEq` for `DataType`
 - **Breaking**: Use `u64` instead of `usize` for byte ranges/array index/shape etc. This makes it possible to index into larger arrays on 32-bit systems.

### Fixed
 - Fix store prefix to node path conversion and vice versa
 - Fix `StorePrefix::parent()` so it outputs a valid `StorePrefix`
 - Fix `StoreKey::parent()` for top level keys, e.g `"a"` now has parent prefix `"/"` instead of `None`
 - Print root node with `Node::hierarchy_tree`

## [0.3.0] - 2023-09-27

### Added
 - Add `CHANGELOG.md`

### Changed
 - Require the `ndarray` *feature* for examples
 - Remove the `ndarray` dev dependency
 - Remove the `ndarray` dependency for the `sharding` feature
 - Replace deprecated `tempdir` with `tempfile` for tests
 - **Breaking**: Change `ByteRange` enum to have `FromStart` and `FromEnd` variants
 - Substitute `blosc` dependency for `blosc-src` which builds blosc from source
 - **Breaking**: Rename `compression` field to `cname` in `BloscCodecConfigurationV1` for consistency with the zarr spec

## [0.2.0] - 2023-09-25

### Added
 - Initial public release

[unreleased]: https://github.com/LDeakin/zarrs/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/LDeakin/zarrs/releases/tag/v0.5.1
[0.5.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.5.0
[0.4.2]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.2
[0.4.1]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.1
[0.4.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.0
[0.3.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.3.0
[0.2.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.2.0
