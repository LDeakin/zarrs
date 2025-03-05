use zarrs_metadata::{codec, CodecMap, CodecName};

/// The default extension codec mapping.
pub(crate) fn codec_map_default() -> CodecMap {
    [
        // Array to array
        (
            codec::bitround::IDENTIFIER.into(),
            CodecName {
                name: "zarrs.bitround".into(),
                aliases: [
                    "https://codec.zarrs.dev/array_to_array/bitround".into(),
                    // NOTE: `numcodecs.bitround` does not support all of the data types supported by the zarrs implementation
                    "numcodecs.bitround".into(),
                ]
                .into(),
                aliases_v2: ["bitround".into()].into(),
            },
        ),
        // Array to bytes
        (
            codec::vlen::IDENTIFIER.into(),
            CodecName {
                name: "zarrs.vlen".into(),
                aliases: ["https://codec.zarrs.dev/array_to_bytes/vlen".into()].into(),
                aliases_v2: [].into(),
            },
        ),
        (
            codec::vlen_v2::IDENTIFIER.into(),
            CodecName {
                name: "zarrs.vlen_v3".into(),
                aliases: ["https://codec.zarrs.dev/array_to_bytes/vlen_v2".into()].into(),
                aliases_v2: [].into(),
            },
        ),
        (
            codec::zfp::IDENTIFIER.into(),
            CodecName {
                name: "zarrs.zfp".into(),
                aliases: ["https://codec.zarrs.dev/array_to_bytes/zfp".into()].into(),
                aliases_v2: [].into(),
            },
        ),
        (
            codec::zfpy::IDENTIFIER.into(),
            CodecName {
                name: "numcodecs.zfpy".into(),
                aliases: [].into(),
                aliases_v2: ["zfpy".into()].into(),
            },
        ),
        (
            codec::pcodec::IDENTIFIER.into(),
            CodecName {
                name: "numcodecs.pcodec".into(),
                aliases: ["https://codec.zarrs.dev/array_to_bytes/pcodec".into()].into(),
                aliases_v2: ["pcodec".into()].into(),
            },
        ),
        // Bytes to bytes
        (
            codec::bz2::IDENTIFIER.into(),
            CodecName {
                name: "numcodecs.bz2".into(),
                aliases: ["https://codec.zarrs.dev/bytes_to_bytes/bz2".into()].into(),
                aliases_v2: ["bz2".into()].into(),
            },
        ),
        (
            codec::fletcher32::IDENTIFIER.into(),
            CodecName {
                name: "numcodecs.fletcher32".into(),
                aliases: ["https://codec.zarrs.dev/bytes_to_bytes/fletcher32".into()].into(),
                aliases_v2: ["fletcher32".into()].into(),
            },
        ),
        (
            codec::gdeflate::IDENTIFIER.into(),
            CodecName {
                name: "zarrs.gdeflate".into(),
                aliases: ["https://codec.zarrs.dev/bytes_to_bytes/gdeflate".into()].into(),
                aliases_v2: [].into(),
            },
        ),
    ]
    .into()
}
