# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2025-01-10

### Changed
- Bump `itertools` to 0.14

### Fixed
- Fix `unsafe_op_in_unsafe_fn` in lint

## [0.3.0] - 2024-11-15

### Added
 - Add `ByteRange::new` and `From` for `RangeBounds<u64>`
 - Add `PerformanceMetricsStorageAdapter::{keys_erased,reset}()`
 - Implement `Ord` and `PartialOrd` for `ByteRange`

### Changed
 - Bump `unsafe_cell_slice` to 0.2.0
 - **Breaking**: Change `ByteRange::FromEnd` to `ByteRange::Suffix`
 - **Breaking**: implement `AsyncReadableWritableStorageTraits` for `T: AsyncReadableStorageTraits + AsyncWritableStorageTraits`
 - **Breaking**: Bump MSRV to 1.77 (21 March, 2024)
 - **Breaking**: Rename `StoreKeyStartValue` to `StoreKeyOffsetValue`
   - Adds `offset` method and removes `start` and `end`
 - Count missing values as reads in `PerformanceMetricsStorageAdapter`
 - Print value lengths rather than values in `UsageLogStorageAdapter::set_partial_values()`

### Removed
 - **Breaking**: Remove `ByteRange::offset()`

## [0.2.2] - 2024-10-17

### Changed
 - Validate that chunk keys do not contain '//'

### Fixed
 - Fix new clippy warnings

## [0.2.1] - 2024-09-22

### Added
 - Add `storage_adapter::usage_log::UsageLogStorageAdapter`
 - Add `storage_adapter::performance_metrics::PerformanceMetricsStorageAdapter`

## [0.2.0] - 2024-09-15

### Changed 
 - Remove unused code related to store plugins
 - **Breaking**: Move filesystem/http/zip store implementations into separate crates:
   - `zarrs_filesystem`
   - `zarrs_http`
   - `zarrs_zip`

### Removed
 - **Breaking**: remove `http` and `zip` features

## [0.1.2] - 2024-09-03

### Changed
 - Use `doc_auto_cfg` on [docs.rs](https://docs.rs/)

## [0.1.1] - 2024-09-03

### Changed
 - Build with all features on [docs.rs](https://docs.rs/)

## [0.1.0] - 2024-09-02

### Added
 - Initial release
 - Split from the `storage` module of `zarrs` 0.17.0-dev

[unreleased]: https://github.com/LDeakin/zarrs/compare/zarrs_storage-v0.3.1...HEAD
[0.3.1]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.3.1
[0.3.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.3.0
[0.2.2]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.2.2
[0.2.1]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.2.1
[0.2.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.2.0
[0.1.2]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.1.2
[0.1.1]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.1.1
[0.1.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.1.0
