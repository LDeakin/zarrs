//! The `blosc` bytes to bytes codec.
//!
//! It uses the [blosc](https://www.blosc.org/) container format.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/blosc/v1.0.html>.

// NOTE: Zarr implementations MAY provide users an option to choose a shuffle mode automatically based on the typesize or other information, but MUST record in the metadata the mode that is chosen.
// TODO: Need to validate blosc typesize matches element size and also that endianness is specified if typesize > 1

mod blosc_codec;
mod blosc_partial_decoder;

/// The input length needed to to run `blosc_compress_bytes` in parallel,
/// and the output length needed to run `blosc_decompress_bytes` in parallel.
/// Otherwise, these functions will use one thread regardless of the `numinternalthreads` parameter.
const MIN_PARALLEL_LENGTH: usize = 4_000_000;

use std::{
    ffi::{c_char, c_int, c_void},
    sync::Arc,
};

pub use crate::metadata::v3::array::codec::blosc::{
    BloscCodecConfiguration, BloscCodecConfigurationV1, BloscCompressionLevel, BloscCompressor,
    BloscShuffleMode,
};
pub use blosc_codec::BloscCodec;
use blosc_sys::{
    blosc_cbuffer_metainfo, blosc_cbuffer_sizes, blosc_cbuffer_validate, blosc_compress_ctx,
    blosc_decompress_ctx, blosc_getitem, BLOSC_MAX_OVERHEAD, BLOSC_MAX_THREADS,
};
use derive_more::From;
use thiserror::Error;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v3::{array::codec::blosc, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use blosc::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_blosc, create_codec_blosc)
}

fn is_name_blosc(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_blosc(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: BloscCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Arc::new(BloscCodec::new_with_configuration(&configuration)?);
    Ok(Codec::BytesToBytes(codec))
}

#[derive(Debug, Error, From)]
#[error("{0}")]
struct BloscError(String);

impl From<&str> for BloscError {
    fn from(err: &str) -> Self {
        Self(err.to_string())
    }
}

const fn compressor_as_cstr(compressor: BloscCompressor) -> *const u8 {
    match compressor {
        BloscCompressor::BloscLZ => blosc_sys::BLOSC_BLOSCLZ_COMPNAME.as_ptr(),
        BloscCompressor::LZ4 => blosc_sys::BLOSC_LZ4_COMPNAME.as_ptr(),
        BloscCompressor::LZ4HC => blosc_sys::BLOSC_LZ4HC_COMPNAME.as_ptr(),
        BloscCompressor::Snappy => blosc_sys::BLOSC_SNAPPY_COMPNAME.as_ptr(),
        BloscCompressor::Zlib => blosc_sys::BLOSC_ZLIB_COMPNAME.as_ptr(),
        BloscCompressor::Zstd => blosc_sys::BLOSC_ZSTD_COMPNAME.as_ptr(),
    }
}

fn blosc_compress_bytes(
    src: &[u8],
    clevel: BloscCompressionLevel,
    shuffle_mode: BloscShuffleMode,
    typesize: usize,
    compressor: BloscCompressor,
    blocksize: usize,
    numinternalthreads: usize,
) -> Result<Vec<u8>, BloscError> {
    let numinternalthreads = if src.len() >= MIN_PARALLEL_LENGTH {
        std::cmp::min(numinternalthreads, BLOSC_MAX_THREADS as usize)
    } else {
        1
    };

    // let mut dest = vec![0; src.len() + BLOSC_MAX_OVERHEAD as usize];
    let destsize = src.len() + BLOSC_MAX_OVERHEAD as usize;
    let mut dest: Vec<u8> = Vec::with_capacity(destsize);
    let destsize = unsafe {
        let clevel: u8 = clevel.into();
        blosc_compress_ctx(
            c_int::from(clevel),
            shuffle_mode as c_int,
            std::cmp::max(1, typesize), // 0 is an error, even with noshuffle?
            src.len(),
            src.as_ptr().cast::<c_void>(),
            dest.as_mut_ptr().cast::<c_void>(),
            destsize,
            compressor_as_cstr(compressor).cast::<c_char>(),
            blocksize,
            i32::try_from(numinternalthreads).unwrap(),
        )
    };
    if destsize > 0 {
        unsafe {
            #[allow(clippy::cast_sign_loss)]
            dest.set_len(destsize as usize);
        }
        dest.shrink_to_fit();
        Ok(dest)
    } else {
        let clevel: u8 = clevel.into();
        Err(BloscError::from(format!("blosc_compress_ctx(clevel: {}, doshuffle: {shuffle_mode:?}, typesize: {typesize}, nbytes: {}, destsize {destsize}, compressor {compressor:?}, bloscksize: {blocksize}) -> {destsize} (failure)", clevel, src.len())))
    }
}

fn blosc_validate(src: &[u8]) -> Option<usize> {
    let mut destsize: usize = 0;
    let valid = unsafe {
        blosc_cbuffer_validate(
            src.as_ptr().cast::<c_void>(),
            src.len(),
            std::ptr::addr_of_mut!(destsize),
        )
    } == 0;
    valid.then_some(destsize)
}

/// # Safety
///
/// Validate first
fn blosc_typesize(src: &[u8]) -> Option<usize> {
    let mut typesize: usize = 0;
    let mut flags: i32 = 0;
    unsafe {
        blosc_cbuffer_metainfo(
            src.as_ptr().cast::<c_void>(),
            std::ptr::addr_of_mut!(typesize),
            std::ptr::addr_of_mut!(flags),
        );
    };
    (typesize != 0).then_some(typesize)
}

/// Returns the length of the uncompress bytes of a `blosc` buffer.
///
/// # Safety
///
/// Validate first
fn blosc_nbytes(src: &[u8]) -> Option<usize> {
    let mut uncompressed_bytes: usize = 0;
    let mut cbytes: usize = 0;
    let mut blocksize: usize = 0;
    unsafe {
        blosc_cbuffer_sizes(
            src.as_ptr().cast::<c_void>(),
            std::ptr::addr_of_mut!(uncompressed_bytes),
            std::ptr::addr_of_mut!(cbytes),
            std::ptr::addr_of_mut!(blocksize),
        );
    };
    (uncompressed_bytes > 0 && cbytes > 0 && blocksize > 0).then_some(uncompressed_bytes)
}

fn blosc_decompress_bytes(
    src: &[u8],
    destsize: usize,
    numinternalthreads: usize,
) -> Result<Vec<u8>, BloscError> {
    let numinternalthreads = if destsize >= MIN_PARALLEL_LENGTH {
        std::cmp::min(numinternalthreads, BLOSC_MAX_THREADS as usize)
    } else {
        1
    };

    let mut dest: Vec<u8> = Vec::with_capacity(destsize);
    let destsize = unsafe {
        blosc_decompress_ctx(
            src.as_ptr().cast::<c_void>(),
            dest.as_mut_ptr().cast::<c_void>(),
            destsize,
            i32::try_from(numinternalthreads).unwrap(),
        )
    };
    if destsize > 0 {
        unsafe {
            #[allow(clippy::cast_sign_loss)]
            dest.set_len(destsize as usize);
        }
        dest.shrink_to_fit();
        Ok(dest)
    } else {
        Err(BloscError::from("blosc_decompress_ctx failed"))
    }
}

fn blosc_decompress_bytes_partial(
    src: &[u8],
    offset: usize,
    length: usize,
    typesize: usize,
) -> Result<Vec<u8>, BloscError> {
    let start = i32::try_from(offset / typesize).unwrap();
    let nitems = i32::try_from(length / typesize).unwrap();
    let mut dest: Vec<u8> = Vec::with_capacity(length);
    let destsize = unsafe {
        blosc_getitem(
            src.as_ptr().cast::<c_void>(),
            start,
            nitems,
            dest.as_mut_ptr().cast::<c_void>(),
        )
    };
    if destsize <= 0 {
        Err(BloscError::from(format!(
            "blosc_getitem(src: len {}, start: {start}, nitems: {nitems}) -> {destsize} (failure)",
            src.len()
        )))
    } else {
        unsafe {
            #[allow(clippy::cast_sign_loss)]
            dest.set_len(destsize as usize);
        }
        dest.shrink_to_fit();
        Ok(dest)
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use crate::{
        array::{
            codec::{BytesToBytesCodecTraits, CodecOptions},
            ArrayRepresentation, BytesRepresentation, DataType, FillValue,
        },
        array_subset::ArraySubset,
        byte_range::ByteRange,
    };

    use super::*;

    const JSON_VALID1: &str = r#"
{
    "cname": "lz4",
    "clevel": 5,
    "shuffle": "shuffle",
    "typesize": 2,
    "blocksize": 0
}"#;

    const JSON_VALID2: &str = r#"
{
    "cname": "lz4",
    "clevel": 4,
    "shuffle": "bitshuffle",
    "typesize": 2,
    "blocksize": 0
}"#;

    const JSON_VALID3: &str = r#"
{
    "cname": "lz4",
    "clevel": 4,
    "shuffle": "noshuffle",
    "blocksize": 0
}"#;

    const JSON_INVALID1: &str = r#"
{
    "cname": "lz4",
    "clevel": 4,
    "shuffle": "bitshuffle",
    "typesize": 0,
    "blocksize": 0
}"#;

    fn codec_blosc_round_trip(json: &str) {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: BloscCodecConfiguration = serde_json::from_str(json).unwrap();
        let codec = BloscCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded = codec
            .decode(encoded, &bytes_representation, &CodecOptions::default())
            .unwrap();
        assert_eq!(bytes, decoded.to_vec());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_blosc_round_trip1() {
        codec_blosc_round_trip(JSON_VALID1);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_blosc_round_trip2() {
        codec_blosc_round_trip(JSON_VALID2);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_blosc_round_trip3() {
        codec_blosc_round_trip(JSON_VALID3);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_blosc_round_trip_snappy() {
        let json = r#"
{
    "cname": "snappy",
    "clevel": 4,
    "shuffle": "noshuffle",
    "blocksize": 0
}"#;
        codec_blosc_round_trip(json);
    }

    #[test]
    #[should_panic]
    #[cfg_attr(miri, ignore)]
    fn codec_blosc_invalid_typesize_with_shuffling() {
        codec_blosc_round_trip(JSON_INVALID1);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_blosc_partial_decode() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let data_type_size = array_representation.data_type().fixed_size().unwrap();
        let array_size = array_representation.num_elements_usize() * data_type_size;
        let bytes_representation = BytesRepresentation::FixedSize(array_size as u64);

        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = Arc::new(BloscCodec::new_with_configuration(&codec_configuration).unwrap());

        let encoded = codec
            .encode(Cow::Owned(bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions: Vec<ByteRange> = ArraySubset::new_with_ranges(&[0..2, 1..2, 0..1])
            .byte_ranges(array_representation.shape(), data_type_size)
            .unwrap();
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(
                input_handle,
                &bytes_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded = partial_decoder
            .partial_decode_concat(&decoded_regions, &CodecOptions::default())
            .unwrap()
            .unwrap();

        let decoded: Vec<u16> = decoded
            .to_vec()
            .chunks_exact(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn codec_blosc_async_partial_decode() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let data_type_size = array_representation.data_type().fixed_size().unwrap();
        let array_size = array_representation.num_elements_usize() * data_type_size;
        let bytes_representation = BytesRepresentation::FixedSize(array_size as u64);

        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = Arc::new(BloscCodec::new_with_configuration(&codec_configuration).unwrap());

        let encoded = codec
            .encode(Cow::Owned(bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions: Vec<ByteRange> = ArraySubset::new_with_ranges(&[0..2, 1..2, 0..1])
            .byte_ranges(array_representation.shape(), data_type_size)
            .unwrap();
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .async_partial_decoder(
                input_handle,
                &bytes_representation,
                &CodecOptions::default(),
            )
            .await
            .unwrap();
        let decoded = partial_decoder
            .partial_decode_concat(&decoded_regions, &CodecOptions::default())
            .await
            .unwrap()
            .unwrap();

        let decoded: Vec<u16> = decoded
            .to_vec()
            .chunks_exact(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded);
    }
}
