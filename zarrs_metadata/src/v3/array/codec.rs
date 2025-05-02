//! Zarr V3 codec metadata.

/// Registered codec metadata.
/// See <https://github.com/zarr-developers/zarr-extensions>.
mod registered {
    /// `blosc` codec metadata (registered, core).
    pub mod blosc;
    /// `bytes` codec metadata (registered, core).
    pub mod bytes;
    /// `crc32c` codec metadata (registered, ZEP0002).
    pub mod crc32c;
    /// `gzip` codec metadata (registered, core).
    pub mod gzip;
    /// `packbits` codec metadata (registered).
    pub mod packbits;
    /// `sharding` codec metadata (registered, ZEP0002).
    pub mod sharding;
    /// `transpose` codec metadata (registered, core).
    pub mod transpose;
    /// `vlen-bytes` codec metadata (registered).
    pub mod vlen_bytes;
    /// `vlen-utf8` codec metadata (registered).
    pub mod vlen_utf8;
    /// `zfp` codec metadata (registered).
    pub mod zfp;
    /// `zstd` codec metadata (registered).
    pub mod zstd;
}
pub use registered::*;

/// Unique identifier for the `blosc` codec (registered, core).
pub const BLOSC: &str = "blosc";

/// Unique identifier for the `bytes` codec (registered, core).
pub const BYTES: &str = "bytes";

/// Unique identifier for the `crc32c` codec (registered, ZEP0002).
pub const CRC32C: &str = "crc32c";

/// Unique identifier for the `gzip` codec (registered, core).
pub const GZIP: &str = "gzip";

/// Unique identifier for the `packbits` codec (registered).
pub const PACKBITS: &str = "packbits";

/// Unique identifier for the `sharding_indexed` codec (registered, ZEP0002).
pub const SHARDING: &str = "sharding_indexed";

/// Unique identifier for the `transpose` codec (registered, core).
pub const TRANSPOSE: &str = "transpose";

/// Unique identifier for the `vlen-bytes` codec (registered).
pub const VLEN_BYTES: &str = "vlen-bytes";

/// Unique identifier for the `vlen-utf8` codec (registered).
pub const VLEN_UTF8: &str = "vlen-utf8";

/// Unique identifier for the `zfp` codec (registered).
pub const ZFP: &str = "zfp";

/// Unique identifier for the `zstd` codec (registered).
pub const ZSTD: &str = "zstd";

/// `zarrs` codec metadata.
mod zarrs {
    /// `gdeflate` codec metadata (`zarrs` experimental).
    pub mod gdeflate;
    /// `squeeze` codec metadata (`zarrs` experimental).
    pub mod squeeze;
    /// `vlen` codec metadata (`zarrs` experimental).
    pub mod vlen;
    /// `vlen_v2` codec metadata (`zarrs` experimental).
    pub mod vlen_v2;
}
pub use zarrs::*;

/// Unique identifier for the `gdeflate` codec (`zarrs` experimental).
pub const GDEFLATE: &str = "gdeflate";

/// Unique identifier for the `squeeze` codec (`zarrs` experimental).
pub const SQUEEZE: &str = "squeeze";

/// Unique identifier for the `vlen_v2` codec (`zarrs` experimental).
pub const VLEN_V2: &str = "vlen_v2";

/// Unique identifier for the `vlen` codec (`zarrs` experimental).
pub const VLEN: &str = "vlen";

/// `numcodecs` codec metadata.
mod numcodecs {
    /// `bitround` codec metadata (`numcodecs`).
    pub mod bitround;
    /// `bz2` codec metadata (`numcodecs`).
    pub mod bz2;
    /// `fixedscaleoffset` codec metadata (`numcodecs`).
    pub mod fixedscaleoffset;
    /// `fletcher32` codec metadata (`numcodecs`).
    pub mod fletcher32;
    /// `pcodec` codec metadata (`numcodecs`).
    pub mod pcodec;
    /// `shuffle` codec metadata (`numcodecs`).
    pub mod shuffle;
    /// `vlen-array` codec metadata (`numcodecs`).
    pub mod vlen_array;
    /// `zfpy` codec metadata (`numcodecs`).
    pub mod zfpy;
    /// `zlib` codec metadata (`numcodecs`).
    pub mod zlib;
}
pub use numcodecs::*;

/// Unique identifier for the `bitround` codec (`numcodecs`).
pub const BITROUND: &str = "bitround";

/// Unique identifier for the `bz2` codec (`numcodecs`).
pub const BZ2: &str = "bz2";

/// Unique identifier for the `fixedscaleoffset` codec (`numcodecs`).
pub const FIXEDSCALEOFFSET: &str = "fixedscaleoffset";

/// Unique identifier for the `fletcher32` codec (`numcodecs`).
pub const FLETCHER32: &str = "fletcher32";

/// Unique identifier for the `pcodec` codec (`numcodecs`).
pub const PCODEC: &str = "pcodec";

/// Unique identifier for the `shuffle` codec (`numcodecs`).
pub const SHUFFLE: &str = "shuffle";

/// Unique identifier for the `vlen-array` codec (`numcodecs`).
pub const VLEN_ARRAY: &str = "vlen-array";

/// Unique identifier for the `zfpy` codec (`numcodecs`).
pub const ZFPY: &str = "zfpy";

/// Unique identifier for the `zlib` codec (`numcodecs`).
pub const ZLIB: &str = "zlib";
