# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
 - Added `ReadableWritableStorage` and `ReadableWritableStore` and async variants
 - Added `{async_}store_set_partial_values`
 - **Breaking** Added `create_readable_writable_transformer` to `StorageTransformerExtension` trait
 - Added `storage::store_lock` module
   - Adds generic `StoreLocks`, `StoreKeyMutex`, and `StoreKeyMutexGuard` with associated traits and async variants
   - Includes `DefaultStoreLocks` and `DisabledStoreLocks` implementations
 - Readable and writable stores include a `new_with_locks` method to choose the store lock implementation
 - Added `ArraySubset::new_with_ranges`
 - Added `ByteRange::offset`
 - Added `object_store` feature with `AsyncObjectStore` store (wraps `object_store::ObjectStore`)
   - **Breaking** Removes the explicit `object_store`-based stores (e.g. `AsyncAmazonS3Store`, `AsyncHTTPStore`)
   - **Breaking** Removes the `object_store_impl` macro
   - **Breaking** Removes the `s3`, `gcp`, and `azure` crate features
 - Added `opendal` feature
   - `OpendalStore` store (wraps `opendal::BlockingOperator`)
   - `AsyncOpendalStore` store (wraps `opendal::Operator`)

### Changed
 - **Breaking**  `ReadableStorageTraits` is no longer a supertrait of `WritableStorageTraits`
   - `WritableStorage` is no longer implicitly readable. Use `ReadableWritableStorage`
 - **Breaking**: `{Async}WritableStorageTraits::set_partial_values()` no longer include default implementations
   - Use new `{async_}store_set_partial_values` utility functions instead
 - Add `#[must_use]` to `Array::builder`, `Array::chunk_grid_shape`, and `ArrayBuilder::from_array`
 - **Breaking** Remove `http` and `zip` from default features
 - Locking functionality for arrays is moved into stores
 - Improved `Array` documentation
 - Add store testing utility functions for unified store testing

### Fixed
 - Fixed `MemoryStore::get_partial_values_key` if given an invalid byte range, now returns `InvalidByteRangeError` instead of panicking

## [0.7.3] - 2023-12-22

### Added
 - Add `From<ChunkKeyEncodingTraits>` for `ChunkKeyEncoding`
 - Add chunk key encoding tests

### Changed
 - Revise code coverage section in `BUILD.md` to use `cargo-llvm-cov`
 - Increased code coverage in some modules
 - Add `--all-features` to clippy usage in `BUILD.md` and `ci.yml`

### Fixed
 - Fixed chunk key encoding for 0 dimensional arrays with `default` and `v2` encoding
 - Fixed various clippy warnings

## [0.7.2] - 2023-12-17

### Added
 - `ArraySubset::{extract_elements/extract_elements_unchecked}` and `ArrayExtractElementsError`

### Changed
 - Add `ArraySubset::{overlap,overlap_unchecked}` and `ArraySubset::{relative_to,relative_to_unchecked}`
   - These replace `ArraySubset::{in_subset,in_subset_unchecked}`, which are now deprecated
 - Add `From<String>` for `StorePrefixError` and deprecate `StorePrefixError::new`

### Fixed
 - Fix `cargo test` with `async` crate feature disabled

## [0.7.1] - 2023-12-11

### Fixed
 - Fix use of `impl_trait_projections` in `{Array/Bytes}PartialDecoderCache`, which was only stabilised in Rust 1.74

## [0.7.0] - 2023-12-05

### Highlights
 - Experimental `async` feature: Support async stores and array/group operations
   - See `async_array_write_read` and `async_http_array_read` examples 
 - Experimental `s3`/`gcp`/`azure` features for experimental async Amazon S3/Google Cloud Storage/Microsoft Azure Blob Storage stores

### Added
 - Add `storage_async` module with asynchronous storage traits: `Async{Readable,Writable,Listable,ReadableListable}StorageTraits`
  - Implemented for `StorageHandle`
 - Add async filesystem/http/memory stores in `storage::store::async` module and a wrapper for any store provided by the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate
 - Add async support to zarr `Group` and `Array`
 - Add `Async{Readable,Writable,Listable,ReadableListable}Storage`
 - Add `StorageTransformerExtension::create_async_{readable,writable,listable,readable_listable}_transformer`
 - Add `Node::async_new_with_store` and implement `storage::async_get_child_nodes`
 - Add `async_array_write_read` and `async_http_array_read` examples
 - Add experimental `async` feature (disabled by default)
 - Add experimental async `Amazon S3`, `Google Cloud`, `Microsoft Azure` stores (untested)

### Changed
 - Bump itertools to `0.12` and zstd to `0.13`
 - Set minimum supported rust version (MSRV) to `1.70` (1 June, 2023)
   - Required by `half` since `2.3.1` (26 June, 2023) 
 - Make `StoreKeyRange` and `StoreKeyStartValue` clonable
 - **Breaking**: Remove `ReadableWritableStorageTraits`, `ReadableWritableStorage`, `ReadableWritableStore`, and `StorageTransformerExtension::create_readable_writable_transformer`
   - These were redundant because `WritableStorageTraits` requires `ReadableStorageTraits` since 6e69a4d
 - Move sync stores to `storage::store::sync`
 - Move sync storage traits to `storage_sync.rs`
 - Move array sync storage trait impls into `array_sync.rs`
 - Use `required-features` for examples

## [0.6.0] - 2023-11-16

### Highlights
 - Revisions for recent updates to the Zarr V3 specification (e.g. sharding `index_location` and removal of `"order": "C"/"F"` from transpose codec)
 - API changes to improve usability
 - Performance improvements and a few bug fixes
 - Experimental `zfp` and `bitround` codecs

### Added
 - **Breaking**: Added `UnsupportedDataTypeError`
 - **Breaking**: Added `CodecError::UnsupportedDataType`
 - Added `array_subset::IncompatibleArrayShapeError`
 - Added `array_subset::iter_linearised_indices_unchecked`
 - Added parallel encoding/decoding to tests
 - Added `array_subset::ArrayStoreBytesError`, `store_bytes`, and `store_bytes_unchecked`
 - Added experimental `zfp` codec implementation behind `zfp` feature flag (disabled by default)
 - Added experimental `bitround` codec implementation behind `bitround` feature flag (disabled by default)
   - This is similar to [numcodecs BitRound](https://numcodecs.readthedocs.io/en/stable/bitround.html#numcodecs.bitround.BitRound), but it supports rounding integers from the most significant set bit
 - Added `ShardingCodecBuilder`
 - Added `ReadableListableStorage`, `ReadableListableStorageTraits`, `StorageTransformerExtension::create_readable_listable_transformer`
 - Added `ByteRange::to_range()` and `to_range_usize()`
 - Added `StoreKeyStartValue::end()`
 - Added default implementation for `WritableStorageTraits::set_partial_values`
    - `WritableStorageTraits` now requires `ReadableStorageTraits`
 - Added `From<&[u8]>` for `FillValue`
 - Added `FillValue::all_equal` and fill value benchmark
    - Implements a much faster fill value test
 - Added `Array::chunk_grid_shape`/`chunk_subset`
 - Added `par_` variants for the `store_array_subset`/`retrieve_array_subset` variants in `Array`, which can encode/decode multiple chunks in parallel
 - Added `ArraySubset::bound` and `Array::chunk_subset_bounded`
 - Added methods to `CodecChain` to retrieve underlying codecs
 - Added `Array::builder()` and `ArrayBuilder::from_array()`
 - Added more `ArrayBuilder` modifiers and made internals public
 - Added a fast path to `Array::retrieve_array_subset` methods if the array subset matches a chunk
 - Added `array::{ZARR_NAN_F64,ZARR_NAN_F32,ZARR_NAN_F16,ZARR_NAN_BF16}` which match the zarr nan bit representation on all implementations
 - Added `size_usize()` to `ArrayRepresentation`
 - Add generic implementations of storage supertraits (`ReadableWritableStorageTraits` and `ReadableListableStorageTraits`)
 - Add `size_prefix()` to stores

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
 - **Breaking**: Rename `FillValueMetadata::Uint` to `UInt` for consistency with the `DataType::UInt` variants.
 - `StorePrefix`, `StoreKey`, `NodeName` `new` methods are now `: impl Into<String>`
 - **Breaking**: `ArrayBuilder::dimension_names` now takes an `Option` input, matching the output of `Array::dimension_names`
 - **Breaking**: `ArrayError::InvalidChunkGridIndicesError` now holds array indices directly, not a `chunk_grid::InvalidArrayIndicesError`
 - **Breaking**: `Array::chunk_array_representation` now returns `ArrayError` instead of `InvalidChunkGridIndicesError` and the `array_shape` parameter is removed
 - **Breaking**: `Array::chunks_in_array_subset` now returns `IncompatibleDimensionalityError` instead of `ArrayError`
 - **Breaking**: `ArraySubset::new_with_start_end_inc`/`new_with_start_end_exc` now take `end: ArrayIndices` instead of `end: &[u64]
 - **Breaking**: Move `array_subset::validate_array_subset` to `ArraySubset::inbounds`
 - Allow out-of-bounds `Array::store_array_subset` and `Array::retrieve_array_subset`
    - retrieved out-of-bounds elements are populated with the fill value
 - Derive `Clone` for `StorageTransformerChain`
 - **Breaking**: `ArrayBuilder::storage_transformers` use `StorageTransformerChain`
 - **Breaking**: change `ArrayError::InvalidFillValue` to `InvalidFillValueMetadata` and add a new `InvalidFillValue`
 - **Breaking**: The transpose codec order configuration parameter no longer supports the constants "C" or "F" and must instead always be specified as an explicit permutation [zarr-developers/zarr-specs #264](https://github.com/zarr-developers/zarr-specs/pull/264)
   - Removes `TransposeOrderImpl`
   - `TransposeOrder` is now validated on creation/deserialisation and `TransposeCodec::new` no longer returns a `Result`
 - **Breaking**: Change `HexString::as_bytes()` to `as_be_bytes()`
 - Support `index_location` for sharding codec
 - Optimise `unravel_index`
 - **Breaking**: Array subset iterator changes
   - Simplify the implementations
   - Remove `next` inputs
   - Make constructors consistent, remove `inner` in constructors
   - Add `size_hint` to all iterators, implement `ExactSizeIterator`/`FusedIterator`
 - **Major breaking**: Output boxed slices `Box<[..]>` from array retrieve methods instead of `Vec<..>`
 - **Major breaking**: Input `Vec<..>` instead of `&[..]` to array store methods
   - Supports some perf improvements
 - **Breaking**: Change `BytesRepresentation` enum from `KnownSize(u64)`/`VariableSize` to `FixedSize(u64)`/`BoundedSize(u64)`/`UnboundedSize`
 - Preallocate sharding codec encoded output when the encoded representation has a fixed or bounded size
 - Add `par_encode` for `zstd` codec
 - **Breaking**: Codecs now must implement `encode_opt`, `decode_opt`, and `partial_decoder_opt`
 - **Breaking**: Partial decoders now must implement `partial_decode_opt` and the `decoded_representation` is now supplied on creation, rather than when calling `partial_decode`/`partial_decode_par`/`partial_decode_opt`
 - Sharding partial decoder now decodes inner chunks in full rather than partially decoding them, this is much faster with some codecs (e.g. blosc)
    - In future, this will probably become configurable
 - Moved `storage/store/{key.rs,prefix.rs}` to `storage/{store_key.rs,store_prefix.rs}`

### Fixed
 - Bytes codec handling of complex and raw bits data types
 - Additional debug assertions to validate input in array subset `_unchecked` functions
 - **Breaking**: `array_subset::iter_linearised_indices` now returns a `Result<_, IncompatibleArrayShapeError>`, previously it could not fail even if the `array_shape` did not enclose the array subset
 - The `array_subset_iter_contiguous_indices3` test was incorrect as the array shape was invalid for the array subset
 - `ArraySubset::extract_bytes` now reserves the appropriate amount of memory
 - Sharding codec performance optimisations
 - `FilesystemStore::erase_prefix` now correctly removes non-empty directories
 - **Breaking**: `ArrayBuilder::storage_transformers` remove `#[must_use]`
 - Validate data type and fill value compatibility in `ArrayBuilder`
 - Handling of `"NaN"` fill values, they are now guaranteed to match the byte representation specified in the zarr v3 spec
 - Add a fast path to array retrieve methods which avoids a copy
 - Optimise sharding codec decode by removing initial population by fill value
 - Include checksum with `zstd` codec if enabled, previously this did nothing

### Removed
 - **Breaking**: Disabled data type extensions `array::data_type::DataType::Extension`.
 - **Breaking**: Remove `StoreExtension` traits
 - **Breaking**: Remove `chunk_grid::InvalidArrayIndicesError`/`ChunkGridShapeError`
 - **Breaking**: Remove `ArrayError::InvalidArrayIndicesError`

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

[unreleased]: https://github.com/LDeakin/zarrs/compare/v0.7.3...HEAD
[0.7.3]: https://github.com/LDeakin/zarrs/releases/tag/v0.7.3
[0.7.2]: https://github.com/LDeakin/zarrs/releases/tag/v0.7.2
[0.7.1]: https://github.com/LDeakin/zarrs/releases/tag/v0.7.1
[0.7.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.7.0
[0.6.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.6.0
[0.5.1]: https://github.com/LDeakin/zarrs/releases/tag/v0.5.1
[0.5.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.5.0
[0.4.2]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.2
[0.4.1]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.1
[0.4.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.0
[0.3.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.3.0
[0.2.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.2.0
