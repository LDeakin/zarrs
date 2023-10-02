# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
 - Read only `ZipStorageAdapter` behind `zip` feature
 - Add `StorageValueIO`, a [`std::io::Read`] interface to a storage value.

### Changed
 - Relax some dependency minimum versions
 - Add `size_hint()` to some array subset iterators
 - **Breaking**: Fix `LinearisedIndicesIterator` to use array shape instead of subset shape
 - **Breaking**: Array subset shape no longer needs to be evenly divisible by chunk shape when creating a chunk iterator, removes `ChunksIteratorError`
 - Add array subset iterator tests
 - **Breaking**: Remove unvalidated `from(String)` for store key/prefix
 - Validate that store prefixes and keys do not start with `/`
 - **Breaking**: Add `StorageError::Other` variant
 - `FilesystemStore` now accepts a file and does not create directory on creation
 - **Breaking**: `ReadableStorageTraits::size()` to `u64` from `usize`
 - **Breaking**: Add `ReadableStorageTraits::size_key()`
 - Storage and store traits no longer require `Debug`
 - Add a default implementation for `WritableStorageTraits::erase_values`
 - Make array/group explicitly store `Arc<TStorage>`
 - `StorageTransformerChain` now only accepts `Arc` storage.
 - Various group methods are now `#[must_use]`
 - Change `NodePath` internal representation to `PathBuf`
 - Remove unneeded '/' prefix strip in `StorePrefix`

### Fixed
 - Fix store prefix to node path conversion and vice versa
 - Fix `StorePrefix::parent()` so it outputs a valid `StorePrefix`

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

[unreleased]: https://github.com/LDeakin/zarrs/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.3.0
[0.2.0]: https://github.com/LDeakin/zarrs/releases/tag/v0.2.0
