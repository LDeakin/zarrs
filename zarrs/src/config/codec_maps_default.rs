use std::collections::HashMap;

use zarrs_metadata::{codec, ExtensionMapsCodec};

/// The default extension codec mapping.
#[rustfmt::skip]
pub(crate) fn codec_maps_default() -> ExtensionMapsCodec {
    ExtensionMapsCodec::new(
        // Default codec names
        HashMap::from([
            // array to array
            (codec::transpose::IDENTIFIER, "transpose".into()),
            (codec::bitround::IDENTIFIER, "zarrs.bitround".into()),
            // array to bytes
            (codec::bytes::IDENTIFIER, "bytes".into()),
            (codec::sharding::IDENTIFIER, "sharding_indexed".into()),
            (codec::vlen_array::IDENTIFIER, "vlen-array".into()),
            (codec::vlen_bytes::IDENTIFIER, "vlen-bytes".into()),
            (codec::vlen_utf8::IDENTIFIER, "vlen-utf8".into()),
            (codec::pcodec::IDENTIFIER, "numcodecs.pcodec".into()),
            (codec::zfpy::IDENTIFIER, "numcodecs.zfpy".into()),
            (codec::vlen::IDENTIFIER, "zarrs.vlen".into()),
            (codec::vlen_v2::IDENTIFIER, "zarrs.vlen_v2".into()),
            (codec::zfp::IDENTIFIER, "zarrs.zfp".into()),
            // bytes to bytes
            (codec::blosc::IDENTIFIER, "blosc".into()),
            (codec::crc32c::IDENTIFIER, "crc32c".into()),
            (codec::gzip::IDENTIFIER, "gzip".into()),
            (codec::zstd::IDENTIFIER, "zstd".into()),
            (codec::bz2::IDENTIFIER, "numcodecs.bz2".into()),
            (codec::fletcher32::IDENTIFIER, "numcodecs.fletcher32".into()),
            (codec::gdeflate::IDENTIFIER, "zarrs.gdeflate".into()),
        ]),
        // Zarr v3 aliases
        HashMap::from([
            // core
            ("transpose".into(), codec::transpose::IDENTIFIER),
            ("bytes".into(), codec::bytes::IDENTIFIER),
            ("endian".into(), codec::bytes::IDENTIFIER), // changed after provisional acceptance
            ("sharding_indexed".into(), codec::sharding::IDENTIFIER),
            ("blosc".into(), codec::blosc::IDENTIFIER),
            ("crc32c".into(), codec::crc32c::IDENTIFIER),
            ("gzip".into(), codec::gzip::IDENTIFIER),
            // zarrs 0.20
            ("zarrs.bitround".into(), codec::bitround::IDENTIFIER),
            ("zarrs.pcodec".into(), codec::pcodec::IDENTIFIER),
            ("zarrs.vlen".into(), codec::vlen::IDENTIFIER),
            ("zarrs.vlen_v2".into(), codec::vlen_v2::IDENTIFIER),
            ("zarrs.zfp".into(), codec::zfp::IDENTIFIER),
            ("zarrs.zfpy".into(), codec::zfpy::IDENTIFIER),
            ("zarrs.bz2".into(), codec::bz2::IDENTIFIER),
            ("zarrs.fletcher32".into(), codec::fletcher32::IDENTIFIER),
            ("zarrs.gdeflate".into(), codec::gdeflate::IDENTIFIER),
            // zarrs 0.20 / zarr-python 3.0
            ("numcodecs.bitround".into(), codec::bitround::IDENTIFIER),
            ("numcodecs.pcodec".into(), codec::pcodec::IDENTIFIER),
            ("numcodecs.zfpy".into(), codec::zfpy::IDENTIFIER),
            ("numcodecs.bz2".into(), codec::bz2::IDENTIFIER),
            ("numcodecs.fletcher32".into(), codec::fletcher32::IDENTIFIER),
            // zarrs 0.18 / zarr-python 3.0
            ("zstd".into(), codec::zstd::IDENTIFIER),
            ("vlen-array".into(), codec::vlen_array::IDENTIFIER),
            ("vlen-bytes".into(), codec::vlen_bytes::IDENTIFIER),
            ("vlen-utf8".into(), codec::vlen_utf8::IDENTIFIER),
            // zarrs < 0.20
            ("https://codec.zarrs.dev/array_to_bytes/bitround".into(), codec::bitround::IDENTIFIER),
            ("https://codec.zarrs.dev/array_to_bytes/pcodec".into(), codec::pcodec::IDENTIFIER),
            ("https://codec.zarrs.dev/array_to_bytes/vlen".into(), codec::vlen::IDENTIFIER),
            ("https://codec.zarrs.dev/array_to_bytes/vlen_v2".into(), codec::vlen_v2::IDENTIFIER),
            ("https://codec.zarrs.dev/array_to_bytes/zfp".into(), codec::zfp::IDENTIFIER),
            ("https://codec.zarrs.dev/bytes_to_bytes/bz2".into(), codec::bz2::IDENTIFIER),
            ("https://codec.zarrs.dev/bytes_to_bytes/fletcher32".into(), codec::fletcher32::IDENTIFIER),
            ("https://codec.zarrs.dev/bytes_to_bytes/gdeflate".into(), codec::gdeflate::IDENTIFIER),
        ]),
        // Zarr v2 aliases
        HashMap::from([
            // array to array
            ("bitround".into(), codec::bitround::IDENTIFIER),
            // array to bytes
            ("vlen-array".into(), codec::vlen_array::IDENTIFIER),
            ("vlen-bytes".into(), codec::vlen_bytes::IDENTIFIER),
            ("vlen-utf8".into(), codec::vlen_utf8::IDENTIFIER),
            ("pcodec".into(), codec::pcodec::IDENTIFIER),
            ("zfpy".into(), codec::zfpy::IDENTIFIER),
            // bytes to bytes
            ("blosc".into(), codec::blosc::IDENTIFIER),
            ("bz2".into(), codec::bz2::IDENTIFIER),
            ("crc32c".into(), codec::crc32c::IDENTIFIER),
            ("gzip".into(), codec::gzip::IDENTIFIER),
            ("zstd".into(), codec::zstd::IDENTIFIER),
            ("fletcher32".into(), codec::fletcher32::IDENTIFIER),
        ]),
    )
}
