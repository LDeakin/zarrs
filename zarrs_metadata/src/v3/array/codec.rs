//! Zarr V3 codec metadata.

/// `core` codec metadata.
mod core {
    /// `blosc` codec metadata.
    pub mod blosc;
    /// `bytes` codec metadata.
    pub mod bytes;
    /// `crc32c` codec metadata.
    pub mod crc32c;
    /// `gzip` codec metadata.
    pub mod gzip;
    /// `sharding` codec metadata.
    pub mod sharding;
    /// `transpose` codec metadata.
    pub mod transpose;
}
pub use core::*;

/// `ext` codec metadata.
mod ext {
    /// `bz2` codec metadata.
    pub mod bz2;
    /// `fletcher32` codec metadata.
    pub mod fletcher32;
    /// `pcodec` codec metadata.
    pub mod pcodec;
    /// `shuffle` codec metadata.
    pub mod shuffle;
    /// `vlen-array` codec metadata.
    pub mod vlen_array;
    /// `vlen-bytes` codec metadata.
    pub mod vlen_bytes;
    /// `vlen-utf8` codec metadata.
    pub mod vlen_utf8;
    /// `zlib` codec metadata.
    pub mod zlib;
    /// `zstd` codec metadata.
    pub mod zstd;
}
pub use ext::*;

/// `zarrs` codec metadata.
mod zarrs {
    /// `bitround` codec metadata.
    pub mod bitround;
    /// `gdeflate` codec metadata.
    pub mod gdeflate;
    /// `vlen` codec metadata.
    pub mod vlen;
    /// `vlen_v2` codec metadata.
    pub mod vlen_v2;
    /// `zfp` codec metadata.
    pub mod zfp;
}
pub use zarrs::*;

/// `numcodecs` codec metadata.
mod numcodecs {
    /// `fixedscaleoffset` codec metadata.
    pub mod fixedscaleoffset;
    /// `zfpy` codec metadata.
    pub mod zfpy;
}
pub use numcodecs::*;

/// Unique identifier for the `blosc` codec (core).
pub const BLOSC: &str = "blosc";

/// Unique identifier for the `bytes` codec (core).
pub const BYTES: &str = "bytes";

/// Unique identifier for the `crc32c` codec (core).
pub const CRC32C: &str = "crc32c";

/// Unique identifier for the `gzip` codec (core).
pub const GZIP: &str = "gzip";

/// Unique identifier for the `sharding_indexed` codec (core).
pub const SHARDING: &str = "sharding_indexed";

/// Unique identifier for the `transpose` codec (core).
pub const TRANSPOSE: &str = "transpose";

/// Unique identifier for the `bz2` codec (extension).
pub const BZ2: &str = "bz2";

/// Unique identifier for the `fletcher32` codec (extension).
pub const FLETCHER32: &str = "fletcher32";

/// Unique identifier for the `pcodec` codec (extension).
pub const PCODEC: &str = "pcodec";

/// Unique identifier for the `shuffle` codec (extension).
pub const SHUFFLE: &str = "shuffle";

/// Unique identifier for the `vlen-array` codec (extension).
pub const VLEN_ARRAY: &str = "vlen-array";

/// Unique identifier for the `vlen-bytes` codec (extension).
pub const VLEN_BYTES: &str = "vlen-bytes";

/// Unique identifier for the `vlen-utf8` codec (extension).
pub const VLEN_UTF8: &str = "vlen-utf8";

/// Unique identifier for the `zlib` codec (extension).
pub const ZLIB: &str = "zlib";

/// Unique identifier for the `zstd` codec (extension).
pub const ZSTD: &str = "zstd";

/// Unique identifier for the `fixedscaleoffset` codec (extension).
pub const FIXEDSCALEOFFSET: &str = "fixedscaleoffset";

/// Unique identifier for the `zfpy` codec (extension).
pub const ZFPY: &str = "zfpy";

/// Unique identifier for the `bitround` codec (extension).
pub const BITROUND: &str = "bitround";

/// Unique identifier for the `gdeflate` codec (extension).
pub const GDEFLATE: &str = "gdeflate";

/// Unique identifier for the `vlen_v2` codec (extension).
pub const VLEN_V2: &str = "vlen_v2";

/// Unique identifier for the `vlen` codec (extension).
pub const VLEN: &str = "vlen";

/// Unique identifier for the `zfp` codec (extension).
pub const ZFP: &str = "zfp";
