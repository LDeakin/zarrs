//! Zarr V3 data type metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#data-types>.

/// Unique identifier for the `bool` data type (core).
pub const BOOL: &str = "bool";

/// Unique identifier for the `int8` data type (core).
pub const INT8: &str = "int8";

/// Unique identifier for the `int16` data type (core).
pub const INT16: &str = "int16";

/// Unique identifier for the `int32` data type (core).
pub const INT32: &str = "int32";

/// Unique identifier for the `int64` data type (core).
pub const INT64: &str = "int64";

/// Unique identifier for the `uint8` data type (core).
pub const UINT8: &str = "uint8";

/// Unique identifier for the `uint16` data type (core).
pub const UINT16: &str = "uint16";

/// Unique identifier for the `uint32` data type (core).
pub const UINT32: &str = "uint32";

/// Unique identifier for the `uint64` data type (core).
pub const UINT64: &str = "uint64";

/// Unique identifier for the `float16` data type (core).
pub const FLOAT16: &str = "float16";

/// Unique identifier for the `float32` data type (core).
pub const FLOAT32: &str = "float32";

/// Unique identifier for the `float64` data type (core).
pub const FLOAT64: &str = "float64";

/// Unique identifier for the `complex64` data type (core).
pub const COMPLEX64: &str = "complex64";

/// Unique identifier for the `complex128` data type (core).
pub const COMPLEX128: &str = "complex128";

/// Unique identifier for the `r*` data type (core).
pub const RAWBITS: &str = "r*";

/// Unique identifier for the `bfloat16` data type (registered).
pub const BFLOAT16: &str = "bfloat16";

/// Unique identifier for the `string` data type (registered).
pub const STRING: &str = "string";

/// Unique identifier for the `bytes` data type (registered).
pub const BYTES: &str = "bytes";
