/// Zarr V3 group metadata.
pub mod group;

/// Zarr V3 array metadata.
pub mod array;

/// Zarr V3 codec metadata.
pub mod codec {
    /// `bitround` codec metadata.
    pub mod bitround;
    /// `blosc` codec metadata.
    pub mod blosc;
    /// `bytes` codec metadata.
    pub mod bytes;

    /// `bz2` codec metadata.
    pub mod bz2;
    /// `crc32c` codec metadata.
    pub mod crc32c;
    /// `gdeflate` codec metadata.
    pub mod gdeflate;
    /// `gzip` codec metadata.
    pub mod gzip;
    /// `pcodec` codec metadata.
    pub mod pcodec;
    /// `sharding` codec metadata.
    pub mod sharding;
    /// `transpose` codec metadata.
    pub mod transpose;
    /// `vlen` codec metadata.
    pub mod vlen;
    /// `vlen_v2` codec metadata.
    pub mod vlen_v2;
    /// `zfp` codec metadata.
    pub mod zfp;
    /// `zstd` codec metadata.
    pub mod zstd;
}

/// Zarr V3 chunk grid metadata.
pub mod chunk_grid {
    /// `rectangular` chunk grid metadata.
    pub mod rectangular;
    /// `regular` chunk grid metadata.
    pub mod regular;
}

/// Zarr V3 chunk key encoding metadata.
pub mod chunk_key_encoding {
    /// `default` chunk key encoding metadata.
    pub mod default;
    /// `v2` chunk key encoding metadata.
    pub mod v2;
}

pub mod fill_value;

pub use array::ArrayMetadataV3;
pub use group::GroupMetadataV3;

pub use crate::array::ChunkKeySeparator;

mod metadata;
pub use metadata::{
    AdditionalFields, ConfigurationInvalidError, MetadataV3, UnsupportedAdditionalFieldError,
};
