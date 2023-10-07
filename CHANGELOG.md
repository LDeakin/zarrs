# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
 - `MaybeBytes` an alias for `Option<Vec<u8>>`
   - When a value is read from the store but the key is not found, the returned value is now `None` instead of an error indicating a missing key
 - Add `storage::erase_chunk` and `Array::erase_chunk`
   - The `array_write_read` example is updated to demonstrate `erase_chunk`
 - Add `TryFrom::<&str>` for `Metadata` and `FillValueMetadata`
 - Add `chunk_key_encoding` and `chunk_key_encoding_default_separator` to `ArrayBuilder`
 - Add `TryFrom<char>` for `ChunkKeySeparator`

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

### Fixed

 - `BytesCodec` now defaults to native endian encoding as opposed to no encoding (only valid for 8 bit data types).
 - `storage::get_child_nodes` and `Node::new_with_store` now correctly propagate storage errors instead of treating all errors as missing metadata
 - `Group::new` now handles an implicit group (with a missing `zarr.json`)
 - `ZipStore` handle missing files
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

[unreleased]: https://github.com/LDeakin/zarrs/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.2
[0.4.1]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.1
[0.4.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.4.0
[0.3.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.3.0
[0.2.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.2.0
