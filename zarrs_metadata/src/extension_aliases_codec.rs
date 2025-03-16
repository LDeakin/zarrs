use std::collections::HashMap;

use crate::{codec, ExtensionAliases, ExtensionTypeCodec, ZarrVersion2, ZarrVersion3};

/// Zarr V3 codec extension aliases.
pub type ExtensionAliasesCodecV3 = ExtensionAliases<ZarrVersion3, ExtensionTypeCodec>;

/// Zarr V2 codec extension aliases.
pub type ExtensionAliasesCodecV2 = ExtensionAliases<ZarrVersion2, ExtensionTypeCodec>;

impl Default for ExtensionAliasesCodecV3 {
    #[rustfmt::skip]
    fn default() -> Self {
        Self::new(
            // The default serialised `name`s
            HashMap::from([
                // The default serialised `name`s
                (codec::bitround::IDENTIFIER, "numcodecs.bitround".into()),
                (codec::fixedscaleoffset::IDENTIFIER, "numcodecs.fixedscaleoffset".into()),
                // array to bytes
                (codec::pcodec::IDENTIFIER, "numcodecs.pcodec".into()),
                (codec::zfpy::IDENTIFIER, "numcodecs.zfpy".into()),
                (codec::vlen::IDENTIFIER, "zarrs.vlen".into()),
                (codec::vlen_v2::IDENTIFIER, "zarrs.vlen_v2".into()),
                (codec::zfp::IDENTIFIER, "zarrs.zfp".into()),
                // bytes to bytes
                (codec::bz2::IDENTIFIER, "numcodecs.bz2".into()),
                (codec::gdeflate::IDENTIFIER, "zarrs.gdeflate".into()),
                (codec::fletcher32::IDENTIFIER, "numcodecs.fletcher32".into()),
                (codec::shuffle::IDENTIFIER, "numcodecs.shuffle".into()),
                (codec::zlib::IDENTIFIER, "numcodecs.zlib".into()),
            ]),
            // `name` aliases (string match)
            HashMap::from([
                // core
                ("endian".into(), codec::bytes::IDENTIFIER), // changed to bytes after provisional acceptance
                // zarrs 0.20
                ("zarrs.vlen".into(), codec::vlen::IDENTIFIER),
                ("zarrs.vlen_v2".into(), codec::vlen_v2::IDENTIFIER),
                ("zarrs.zfp".into(), codec::zfp::IDENTIFIER),
                ("zarrs.gdeflate".into(), codec::gdeflate::IDENTIFIER),
                // zarrs 0.20 / zarr-python 3.0
                ("numcodecs.bitround".into(), codec::bitround::IDENTIFIER),
                ("numcodecs.fixedscaleoffset".into(), codec::fixedscaleoffset::IDENTIFIER),
                ("numcodecs.pcodec".into(), codec::pcodec::IDENTIFIER),
                ("numcodecs.zfpy".into(), codec::zfpy::IDENTIFIER),
                ("numcodecs.bz2".into(), codec::bz2::IDENTIFIER),
                ("numcodecs.fletcher32".into(), codec::fletcher32::IDENTIFIER),
                ("numcodecs.shuffle".into(), codec::shuffle::IDENTIFIER),
                ("numcodecs.zlib".into(), codec::zlib::IDENTIFIER),
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
            // `name` aliases (regex match)
            Vec::from([]),
        )
    }
}

impl Default for ExtensionAliasesCodecV2 {
    #[rustfmt::skip]
    fn default() -> Self {
        Self::new(
            // The default serialised `name`s
            HashMap::from([
                // array to array
                // array to bytes
                (codec::vlen::IDENTIFIER, "zarrs.vlen".into()),
                (codec::vlen_v2::IDENTIFIER, "zarrs.vlen_v2".into()),
                (codec::zfp::IDENTIFIER, "zarrs.zfp".into()),
                // bytes to bytes
                (codec::gdeflate::IDENTIFIER, "zarrs.gdeflate".into()),
            ]),
            // `name` aliases (string match)
            HashMap::from([
                // zarrs 0.20
                ("zarrs.vlen".into(), codec::vlen::IDENTIFIER),
                ("zarrs.vlen_v2".into(), codec::vlen_v2::IDENTIFIER),
                ("zarrs.zfp".into(), codec::zfp::IDENTIFIER),
                ("zarrs.gdeflate".into(), codec::gdeflate::IDENTIFIER),
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
            // `name` aliases (regex match)
            Vec::from([]),
        )
    }
}
