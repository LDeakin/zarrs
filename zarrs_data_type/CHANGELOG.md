# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Add support for data type extensions
  - Adds `DataTypeExtension[BytesCodec]`, `DataTypeExtension[BytesCodec]Error`, and `DataTypePlugin`
  - Add `Extension` variant to `DataType`

### Changed
- Bump `derive_more` to 0.2.0
- **Breaking**: `DataType::metadata_fill_value` is now fallible
- **Breaking**: `DataType::{identifier,size,fixed_size}()` are no longer `const`
- **Breaking**: `DataType::from_metadata()` now returns a `PluginCreateError`
- **Breaking**: `DataType::metadata_fill_value()` is now fallible

### Removed
- **Breaking**: Remove `UnsupportedDataTypeError`
- **Breaking**: Remove `DataType.identifier()`

## [0.1.0] - 2025-01-24

### Added
- Initial release
- Split from the `zarrs::array::{data_type,fill_value}` modules of `zarrs` 0.20.0-dev

[unreleased]: https://github.com/LDeakin/zarrs/compare/zarrs_data_type-v0.1.0...HEAD
[0.1.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_data_type-v0.1.0
