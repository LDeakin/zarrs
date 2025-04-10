# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Add support for data type extensions
  - Add `DataTypePlugin` and `DataTypeExtension`
  - Add `Extension` variant to `DataType`
  - Add `DataTypeExtensionBytesCodec`, `DataTypeExtensionBytesCodecError`
  - Add `DataTypeExtensionPackBitsCodec`

### Changed
- **Breaking**: `DataType::metadata_fill_value()` is now fallible
- **Breaking**: `DataType::{size,fixed_size}()` are no longer `const`
- **Breaking**: `DataType::from_metadata()` now returns a `PluginCreateError` on error instead of `UnsupportedDataTypeError`
- **Breaking**: `DataType::from_metadata()` has an additional `ExtensionAliasesDataTypeV3` parameter
- **Breaking**: `DataType::[fixed_]size()` are no longer `const`
- Bump `derive_more` to 0.2.0
- Bump `half` to 2.3.1
- Bump `thiserror` to 2.0.12

### Removed
- **Breaking**: Remove `UnsupportedDataTypeError`
- **Breaking**: Remove `DataType::identifier()`
- **Breaking**: Remove `TryFrom<DataTypeMetadataV3>` for `DataType`

## [0.1.0] - 2025-01-24

### Added
- Initial release
- Split from the `zarrs::array::{data_type,fill_value}` modules of `zarrs` 0.20.0-dev

[unreleased]: https://github.com/LDeakin/zarrs/compare/zarrs_data_type-v0.1.0...HEAD
[0.1.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_data_type-v0.1.0
