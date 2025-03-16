//! Codec metadata.

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
