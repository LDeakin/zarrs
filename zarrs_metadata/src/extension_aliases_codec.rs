use std::collections::HashMap;

use crate::{ExtensionAliases, ExtensionTypeCodec, ZarrVersion2, ZarrVersion3};

use crate::v3::array::codec;

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
                (codec::BITROUND, "numcodecs.bitround".into()),
                (codec::FIXEDSCALEOFFSET, "numcodecs.fixedscaleoffset".into()),
                // array to bytes
                (codec::PCODEC, "numcodecs.pcodec".into()),
                (codec::ZFPY, "numcodecs.zfpy".into()),
                (codec::VLEN, "zarrs.vlen".into()),
                (codec::VLEN_V2, "zarrs.vlen_v2".into()),
                (codec::ZFP, "zarrs.zfp".into()),
                // bytes to bytes
                (codec::BZ2, "numcodecs.bz2".into()),
                (codec::GDEFLATE, "zarrs.gdeflate".into()),
                (codec::FLETCHER32, "numcodecs.fletcher32".into()),
                (codec::SHUFFLE, "numcodecs.shuffle".into()),
                (codec::ZLIB, "numcodecs.zlib".into()),
            ]),
            // `name` aliases (string match)
            HashMap::from([
                // core
                ("endian".into(), codec::BYTES), // changed to bytes after provisional acceptance
                // zarrs 0.20
                ("zarrs.vlen".into(), codec::VLEN),
                ("zarrs.vlen_v2".into(), codec::VLEN_V2),
                ("zarrs.zfp".into(), codec::ZFP),
                ("zarrs.gdeflate".into(), codec::GDEFLATE),
                // zarrs 0.20 / zarr-python 3.0
                ("numcodecs.bitround".into(), codec::BITROUND),
                ("numcodecs.fixedscaleoffset".into(), codec::FIXEDSCALEOFFSET),
                ("numcodecs.pcodec".into(), codec::PCODEC),
                ("numcodecs.zfpy".into(), codec::ZFPY),
                ("numcodecs.bz2".into(), codec::BZ2),
                ("numcodecs.fletcher32".into(), codec::FLETCHER32),
                ("numcodecs.shuffle".into(), codec::SHUFFLE),
                ("numcodecs.zlib".into(), codec::ZLIB),
                // zarrs < 0.20
                ("https://codec.zarrs.dev/array_to_bytes/bitround".into(), codec::BITROUND),
                ("https://codec.zarrs.dev/array_to_bytes/pcodec".into(), codec::PCODEC),
                ("https://codec.zarrs.dev/array_to_bytes/vlen".into(), codec::VLEN),
                ("https://codec.zarrs.dev/array_to_bytes/vlen_v2".into(), codec::VLEN_V2),
                ("https://codec.zarrs.dev/array_to_bytes/zfp".into(), codec::ZFP),
                ("https://codec.zarrs.dev/bytes_to_bytes/bz2".into(), codec::BZ2),
                ("https://codec.zarrs.dev/bytes_to_bytes/fletcher32".into(), codec::FLETCHER32),
                ("https://codec.zarrs.dev/bytes_to_bytes/gdeflate".into(), codec::GDEFLATE),
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
                (codec::VLEN, "zarrs.vlen".into()),
                (codec::VLEN_V2, "zarrs.vlen_v2".into()),
                (codec::ZFP, "zarrs.zfp".into()),
                // bytes to bytes
                (codec::GDEFLATE, "zarrs.gdeflate".into()),
            ]),
            // `name` aliases (string match)
            HashMap::from([
                // zarrs 0.20
                ("zarrs.vlen".into(), codec::VLEN),
                ("zarrs.vlen_v2".into(), codec::VLEN_V2),
                ("zarrs.zfp".into(), codec::ZFP),
                ("zarrs.gdeflate".into(), codec::GDEFLATE),
                // zarrs < 0.20
                ("https://codec.zarrs.dev/array_to_bytes/bitround".into(), codec::BITROUND),
                ("https://codec.zarrs.dev/array_to_bytes/pcodec".into(), codec::PCODEC),
                ("https://codec.zarrs.dev/array_to_bytes/vlen".into(), codec::VLEN),
                ("https://codec.zarrs.dev/array_to_bytes/vlen_v2".into(), codec::VLEN_V2),
                ("https://codec.zarrs.dev/array_to_bytes/zfp".into(), codec::ZFP),
                ("https://codec.zarrs.dev/bytes_to_bytes/bz2".into(), codec::BZ2),
                ("https://codec.zarrs.dev/bytes_to_bytes/fletcher32".into(), codec::FLETCHER32),
                ("https://codec.zarrs.dev/bytes_to_bytes/gdeflate".into(), codec::GDEFLATE),
            ]),
            // `name` aliases (regex match)
            Vec::from([]),
        )
    }
}
