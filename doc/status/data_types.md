| Data Type<sup>†</sup> | ZEP | V3 | V2 | Feature Flag |
| --------- | --- | ----- | -- | ------------ |
| [bool]<br>[int8] [int16] [int32] [int64] [uint8] [uint16] [uint32] [uint64]<br>[float16] [float32] [float64]<br>[complex64] [complex128] | [ZEP0001] | &check; | &check; | |
[r* (raw bits)] | [ZEP0001] | &check; | | |
| [bfloat16] | [zarr-specs #130] | &check; | | |
| [string] (experimental) | [ZEP0007 (draft)] | &check; | | |
| [binary] (experimental) | [ZEP0007 (draft)] | &check; | | |

<sup>† Experimental data types are recommended for evaluation only.</sup>

[bool]: crate::array::data_type::DataType::Bool
[int8]: crate::array::data_type::DataType::Int8
[int16]: crate::array::data_type::DataType::Int16
[int32]: crate::array::data_type::DataType::Int32
[int64]: crate::array::data_type::DataType::Int64
[uint8]: crate::array::data_type::DataType::UInt8
[uint16]: crate::array::data_type::DataType::UInt16
[uint32]: crate::array::data_type::DataType::UInt32
[uint64]: crate::array::data_type::DataType::UInt64
[float16]: crate::array::data_type::DataType::Float16
[float32]: crate::array::data_type::DataType::Float32
[float64]: crate::array::data_type::DataType::Float64
[complex64]: crate::array::data_type::DataType::Complex64
[complex128]: crate::array::data_type::DataType::Complex128
[bfloat16]: crate::array::data_type::DataType::BFloat16
[r* (raw bits)]: crate::array::data_type::DataType::RawBits
[string]: crate::array::data_type::DataType::String
[binary]: crate::array::data_type::DataType::Binary

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[zarr-specs #130]: https://github.com/zarr-developers/zarr-specs/issues/130
[ZEP0007 (draft)]: https://github.com/zarr-developers/zeps/pull/47
