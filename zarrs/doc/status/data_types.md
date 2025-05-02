| Data Type<sup>†</sup> | Specification | V3 | V2 | Feature Flag |
| --------- | --- | ----- | -- | ------------ |
| [bool]<br>[int8] [int16] [int32] [int64] [uint8] [uint16] [uint32] [uint64]<br>[float16] [float32] [float64]<br>[complex64] [complex128] | [ZEP0001] | &check; | &check; | |
[r* (raw bits)] | [ZEP0001] | &check; | | |
| [bfloat16] | [zarr-specs #130] | &check; | | |
| [string] | [zarr-extensions/data-types/string] | &check; | | |
| [bytes](crate::array::DataType::Bytes) | [zarr-extensions/data-types/bytes] | &check; | | |

<sup>† Experimental data types are recommended for evaluation only.</sup>

[bool]: crate::array::DataType::Bool
[int8]: crate::array::DataType::Int8
[int16]: crate::array::DataType::Int16
[int32]: crate::array::DataType::Int32
[int64]: crate::array::DataType::Int64
[uint8]: crate::array::DataType::UInt8
[uint16]: crate::array::DataType::UInt16
[uint32]: crate::array::DataType::UInt32
[uint64]: crate::array::DataType::UInt64
[float16]: crate::array::DataType::Float16
[float32]: crate::array::DataType::Float32
[float64]: crate::array::DataType::Float64
[complex64]: crate::array::DataType::Complex64
[complex128]: crate::array::DataType::Complex128
[bfloat16]: crate::array::DataType::BFloat16
[r* (raw bits)]: crate::array::DataType::RawBits
[string]: crate::array::DataType::String

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[zarr-specs #130]: https://github.com/zarr-developers/zarr-specs/issues/130
[ZEP0007 (draft)]: https://github.com/zarr-developers/zeps/pull/47
[zarr-extensions/data-types/string]: https://github.com/zarr-developers/zarr-extensions/tree/main/data-types/string
[zarr-extensions/data-types/bytes]: https://github.com/zarr-developers/zarr-extensions/tree/main/data-types/bytes
