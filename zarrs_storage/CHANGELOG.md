# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2024-10-17

### Fixed
 - Fix new clippy warnings
 - Fix `data_key` encoding on windows (it contained '//')

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

[unreleased]: https://github.com/LDeakin/zarrs/compare/zarrs_storage-v0.2.2...HEAD
[0.2.2]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.2.2
[0.2.1]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.2.1
[0.2.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.2.0
[0.1.2]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.1.2
[0.1.1]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.1.1
[0.1.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_storage-v0.1.0
