| V3 `name`<sup>†</sup>           | V2 `dtype`  | [`ElementOwned`] / [`Element`]  | Specification                       | Feature Flag |
| ------------------------------- | ----------- | ------------------------------- | ----------------------------------- | ------------ |
| [bool]                          | \|b1        | [`bool`]                        | [ZEP0001]                           |              |
| [int8]                          | \|i1        | [`i8`]                          | [ZEP0001]                           |              |
| [int16]                         | >i2 <i2     | [`i16`]                         | [ZEP0001]                           |              |
| [int32]                         | >i4 <i4     | [`i32`]                         | [ZEP0001]                           |              |
| [int64]                         | >i8 <i8     | [`i64`]                         | [ZEP0001]                           |              |
| [uint8]                         | \|u1        | [`u8`]                          | [ZEP0001]                           |              |
| [uint16]                        | >u2 <u2     | [`u16`]                         | [ZEP0001]                           |              |
| [uint32]                        | >u4 <u4     | [`u32`]                         | [ZEP0001]                           |              |
| [uint64]                        | >u8 <u8     | [`u64`]                         | [ZEP0001]                           |              |
| [float16]                       | >f2 <f2     | [`half::f16`]                   | [ZEP0001]                           |              |
| [bfloat16]                      |             | [`half::bf16`]                  | [zarr-specs #130]                   |              |
| [float32]                       | >f4 <f4     | [`f32`]                         | [ZEP0001]                           |              |
| [float64]                       | >f8 <f8     | [`f64`]                         | [ZEP0001]                           |              |
| [complex64]<br>complex_float32  | >c8 <c8     | [`num::complex::Complex32`]     | [ZEP0001]                           |              |
| [complex128]<br>complex_float64 | >c16 <c16   | [`num::complex::Complex64`]     | [ZEP0001]                           |              |
| [r (raw bits)]                  |             | `[u8; N]` / `&[u8; N]`          | [ZEP0001]                           |              |
| [string]                        | \|O         | [`String`] / [`&str`]           | [data-types/string]                 |              |
| [bytes]<br>binary               | \|VX        | [`Vec<u8>`] / `&[u8]`           | [data-types/bytes]                  |              |

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
[r (raw bits)]: crate::array::DataType::RawBits
[string]: crate::array::DataType::String
[bytes]: crate::array::DataType::Bytes
[`Element`]: crate::array::Element
[`ElementOwned`]: crate::array::ElementOwned

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[zarr-specs #130]: https://github.com/zarr-developers/zarr-specs/issues/130
[ZEP0007 (draft)]: https://github.com/zarr-developers/zeps/pull/47
[data-types/string]: https://github.com/zarr-developers/zarr-extensions/tree/main/data-types/string
[data-types/bytes]: https://github.com/zarr-developers/zarr-extensions/tree/main/data-types/bytes
