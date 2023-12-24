//! The `blosc` bytes to bytes codec.
//!
//! It uses the [blosc](https://www.blosc.org/) container format.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/blosc/v1.0.html>.

// NOTE: Zarr implementations MAY provide users an option to choose a shuffle mode automatically based on the typesize or other information, but MUST record in the metadata the mode that is chosen.
// TODO: Need to validate blosc typesize matches element size and also that endianness is specified if typesize > 1

mod blosc_codec;
mod blosc_configuration;
mod blosc_partial_decoder;

/// The input length needed to to run `blosc_compress_bytes` in parallel,
/// and the output length needed to run `blosc_decompress_bytes` in parallel.
/// Otherwise, these functions will use one thread regardless of the `numinternalthreads` parameter.
const MIN_PARALLEL_LENGTH: usize = 4_000_000;

use std::{
    ffi::c_int,
    ffi::{c_char, c_void},
};

pub use blosc_codec::BloscCodec;
pub use blosc_configuration::{BloscCodecConfiguration, BloscCodecConfigurationV1};
use blosc_sys::{
    blosc_cbuffer_metainfo, blosc_cbuffer_sizes, blosc_cbuffer_validate, blosc_compress_ctx,
    blosc_decompress_ctx, blosc_getitem, BLOSC_BITSHUFFLE, BLOSC_BLOSCLZ_COMPNAME,
    BLOSC_LZ4HC_COMPNAME, BLOSC_LZ4_COMPNAME, BLOSC_MAX_OVERHEAD, BLOSC_MAX_THREADS,
    BLOSC_NOSHUFFLE, BLOSC_SHUFFLE, BLOSC_SNAPPY_COMPNAME, BLOSC_ZLIB_COMPNAME,
    BLOSC_ZSTD_COMPNAME,
};
use derive_more::From;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, From)]
#[error("{0}")]
struct BloscError(String);

impl From<&str> for BloscError {
    fn from(err: &str) -> Self {
        Self(err.to_string())
    }
}

/// An integer from 0 to 9 controlling the compression level
///
/// A level of 1 is the fastest compression method and produces the least compressions, while 9 is slowest and produces the most compression.
/// Compression is turned off when the compression level is 0.
#[derive(Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub struct BloscCompressionLevel(u8);

impl TryFrom<u8> for BloscCompressionLevel {
    type Error = u8;
    fn try_from(level: u8) -> Result<Self, Self::Error> {
        if level <= 9 {
            Ok(Self(level))
        } else {
            Err(level)
        }
    }
}

impl<'de> serde::Deserialize<'de> for BloscCompressionLevel {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let level = u8::deserialize(d)?;
        if level <= 9 {
            Ok(Self(level))
        } else {
            Err(serde::de::Error::custom("clevel must be between 0 and 9"))
        }
    }
}

/// The `blosc` shuffle mode.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
#[repr(u32)]
pub enum BloscShuffleMode {
    /// No shuffling.
    NoShuffle = BLOSC_NOSHUFFLE,
    /// Byte-wise shuffling.
    Shuffle = BLOSC_SHUFFLE,
    /// Bit-wise shuffling.
    BitShuffle = BLOSC_BITSHUFFLE,
}

/// The `blosc` compressor.
///
/// See <https://www.blosc.org/pages/>.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BloscCompressor {
    /// [BloscLZ](https://github.com/Blosc/c-blosc/blob/master/blosc/blosclz.h): blosc default compressor, heavily based on [FastLZ](http://fastlz.org/).
    BloscLZ,
    /// [LZ4](http://fastcompression.blogspot.com/p/lz4.html): a compact, very popular and fast compressor.
    LZ4,
    /// [LZ4HC](http://fastcompression.blogspot.com/p/lz4.html): a tweaked version of LZ4, produces better compression ratios at the expense of speed.
    LZ4HC,
    /// [Snappy](https://code.google.com/p/snappy): a popular compressor used in many places.
    Snappy,
    /// [Zlib](http://www.zlib.net/): a classic; somewhat slower than the previous ones, but achieving better compression ratios.
    Zlib,
    /// [Zstd](http://www.zstd.net/): an extremely well balanced codec; it provides the best compression ratios among the others above, and at reasonably fast speed.
    Zstd,
}

impl BloscCompressor {
    const fn as_cstr(&self) -> *const u8 {
        match self {
            Self::BloscLZ => BLOSC_BLOSCLZ_COMPNAME.as_ptr(),
            Self::LZ4 => BLOSC_LZ4_COMPNAME.as_ptr(),
            Self::LZ4HC => BLOSC_LZ4HC_COMPNAME.as_ptr(),
            Self::Snappy => BLOSC_SNAPPY_COMPNAME.as_ptr(),
            Self::Zlib => BLOSC_ZLIB_COMPNAME.as_ptr(),
            Self::Zstd => BLOSC_ZSTD_COMPNAME.as_ptr(),
        }
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
        blosc_compress_ctx(
            c_int::from(clevel.0),
            shuffle_mode as c_int,
            typesize,
            src.len(),
            src.as_ptr().cast::<c_void>(),
            dest.as_mut_ptr().cast::<c_void>(),
            destsize,
            compressor.as_cstr().cast::<c_char>(),
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
        Err(BloscError::from(format!("blosc_compress_ctx(clevel: {}, doshuffle: {shuffle_mode:?}, typesize: {typesize}, nbytes: {}, destsize {destsize}, compressor {compressor:?}, bloscksize: {blocksize}) -> {destsize} (failure)", clevel.0, src.len())))
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
    (typesize != 0 && flags != 0).then_some(typesize)
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
    use crate::{
        array::{
            codec::BytesToBytesCodecTraits, ArrayRepresentation, BytesRepresentation, DataType,
            FillValue,
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

    #[test]
    fn codec_blosc_round_trip1() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID1).unwrap();
        let codec = BloscCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec.decode(encoded, &bytes_representation).unwrap();
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_blosc_round_trip2() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = BloscCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec.decode(encoded, &bytes_representation).unwrap();
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_blosc_partial_decode() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let bytes_representation = BytesRepresentation::FixedSize(array_representation.size());

        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = BloscCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes).unwrap();
        let decoded_regions: Vec<ByteRange> = ArraySubset::new_with_ranges(&[0..2, 1..2, 0..1])
            .byte_ranges(
                array_representation.shape(),
                array_representation.element_size(),
            )
            .unwrap();
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(input_handle, &bytes_representation)
            .unwrap();
        let decoded = partial_decoder
            .partial_decode(&decoded_regions)
            .unwrap()
            .unwrap();

        let decoded: Vec<u16> = decoded
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded);
    }
}
