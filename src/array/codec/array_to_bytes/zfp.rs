//! The `zfp` array to bytes codec.
//!
//! [zfp](https://zfp.io/) is a compressed number format for 1D to 4D arrays of 32/64-bit floating point or integer data.
//!
//! This codec requires the `zfp` feature, which is disabled by default.
//!
//! See [`ZfpCodecConfigurationV1`] for example `JSON` metadata.
//!

mod zfp_bitstream;
mod zfp_codec;
mod zfp_configuration;
mod zfp_field;
mod zfp_partial_decoder;
mod zfp_stream;

use serde::{Deserialize, Serialize};

pub use zfp_codec::ZfpCodec;
pub use zfp_configuration::{
    ZfpCodecConfiguration, ZfpCodecConfigurationV1, ZfpExpertConfiguration,
    ZfpFixedAccuracyConfiguration, ZfpFixedPrecisionConfiguration, ZfpFixedRateConfiguration,
};

use zfp_sys::{
    zfp_decompress, zfp_exec_policy_zfp_exec_omp, zfp_stream_rewind, zfp_stream_set_bit_stream,
    zfp_stream_set_execution, zfp_type, zfp_type_zfp_type_double, zfp_type_zfp_type_float,
    zfp_type_zfp_type_int32, zfp_type_zfp_type_int64,
};

use crate::array::{codec::CodecError, ChunkRepresentation, DataType};

use self::{zfp_bitstream::ZfpBitstream, zfp_field::ZfpField, zfp_stream::ZfpStream};

/// The `zfp` mode.
#[derive(Clone, Copy, Debug)]
pub enum ZfpMode {
    /// Expert mode.
    Expert(ZfpExpertParams),
    /// Fixed rate mode.
    FixedRate(f64),
    /// Fixed precision mode.
    FixedPrecision(u32),
    /// Fixed accuracy mode.
    FixedAccuracy(f64),
    /// Reversible mode.
    Reversible,
}

/// `zfp` expert parameters.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZfpExpertParams {
    /// The minimum number of compressed bits used to represent a block.
    ///
    /// Usually this parameter equals one bit, unless each and every block is to be stored using a fixed number of bits to facilitate random access, in which case it should be set to the same value as `maxbits`.
    pub minbits: u32,
    /// The maximum number of bits used to represent a block.
    ///
    /// This parameter sets a hard upper bound on compressed block size and governs the rate in fixed-rate mode. It may also be used as an upper storage limit to guard against buffer overruns in combination with the accuracy constraints given by `zfp_stream.maxprec` and `zfp_stream.minexp`.
    /// `maxbits` must be large enough to allow the common block exponent and any control bits to be encoded. This implies `maxbits` ≥ 9 for single-precision data and `maxbits` ≥ 12 for double-precision data.
    pub maxbits: u32,
    /// The maximum number of bit planes encoded.
    ///
    /// This parameter governs the number of most significant uncompressed bits encoded per transform coefficient.
    /// It does not directly correspond to the number of uncompressed mantissa bits for the floating-point or integer values being compressed, but is closely related.
    /// This is the parameter that specifies the precision in fixed-precision mode, and it provides a mechanism for controlling the relative error.
    /// Note that this parameter selects how many bits planes to encode regardless of the magnitude of the common floating-point exponent within the block.
    pub maxprec: u32,
    /// The smallest absolute bit plane number encoded (applies to floating-point data only; this parameter is ignored for integer data).
    ///
    /// The place value of each transform coefficient bit depends on the common floating-point exponent, $e$, that scales the integer coefficients. If the most significant coefficient bit has place value $2^e$, then the number of bit planes encoded is (one plus) the difference between e and `zfp_stream.minexp`.
    /// This parameter governs the absolute error in fixed-accuracy mode.
    pub minexp: i32,
}

const fn zarr_data_type_to_zfp_data_type(data_type: &DataType) -> Option<zfp_type> {
    match data_type {
        DataType::Int32 | DataType::UInt32 => Some(zfp_type_zfp_type_int32),
        DataType::Int64 | DataType::UInt64 => Some(zfp_type_zfp_type_int64),
        DataType::Float32 => Some(zfp_type_zfp_type_float),
        DataType::Float64 => Some(zfp_type_zfp_type_double),
        _ => None,
    }
}

fn zfp_decode(
    zfp_mode: &ZfpMode,
    zfp_type: zfp_type,
    mut encoded_value: Vec<u8>,
    decoded_representation: &ChunkRepresentation,
    parallel: bool,
) -> Result<Vec<u8>, CodecError> {
    let mut decoded_value = vec![0u8; usize::try_from(decoded_representation.size()).unwrap()];
    let Some(field) = ZfpField::new(
        &mut decoded_value,
        zfp_type,
        &decoded_representation
            .shape()
            .iter()
            .map(|u| usize::try_from(u.get()).unwrap())
            .collect::<Vec<usize>>(),
    ) else {
        return Err(CodecError::from("failed to create zfp field"));
    };
    let Some(zfp) = ZfpStream::new(zfp_mode, zfp_type) else {
        return Err(CodecError::from("failed to create zfp stream"));
    };

    let Some(stream) = ZfpBitstream::new(&mut encoded_value) else {
        return Err(CodecError::from("failed to create zfp field"));
    };
    unsafe {
        zfp_stream_set_bit_stream(zfp.as_zfp_stream(), stream.as_bitstream());
        zfp_stream_rewind(zfp.as_zfp_stream());
    }

    if parallel {
        // Number of threads is set automatically
        unsafe {
            zfp_stream_set_execution(zfp.as_zfp_stream(), zfp_exec_policy_zfp_exec_omp);
        }
    }

    let ret = unsafe { zfp_decompress(zfp.as_zfp_stream(), field.as_zfp_field()) };
    if ret == 0 {
        Err(CodecError::from("zfp decompression failed"))
    } else {
        Ok(decoded_value)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::{
        array::{
            codec::{ArrayCodecTraits, ArrayToBytesCodecTraits},
            DataType,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    const JSON_VALID: &'static str = r#"{
        "mode": "fixedprecision",
        "precision": 12
    }"#;

    #[test]
    fn codec_zfp_round_trip1() {
        let chunk_shape = vec![
            NonZeroU64::new(3).unwrap(),
            NonZeroU64::new(3).unwrap(),
            NonZeroU64::new(3).unwrap(),
        ];
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape, DataType::Float32, 0.0f32.into()).unwrap();
        let elements: Vec<f32> = (0..27).map(|i| i as f32).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements.clone());

        let configuration: ZfpCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = ZfpCodec::new_with_configuration(&configuration);

        let encoded = codec.encode(bytes.clone(), &chunk_representation).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &chunk_representation)
            .unwrap();

        let decoded_elements = crate::array::transmute_from_bytes_vec::<f32>(decoded);
        assert_eq!(elements, decoded_elements);
    }

    #[test]
    fn codec_zfp_partial_decode() {
        let chunk_shape = vec![
            NonZeroU64::new(3).unwrap(),
            NonZeroU64::new(3).unwrap(),
            NonZeroU64::new(3).unwrap(),
        ];
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape, DataType::Float32, 0.0f32.into()).unwrap();
        let elements: Vec<f32> = (0..27).map(|i| i as f32).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let configuration: ZfpCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = ZfpCodec::new_with_configuration(&configuration);

        let encoded = codec.encode(bytes.clone(), &chunk_representation).unwrap();
        let decoded_regions = [
            ArraySubset::new_with_shape(vec![1, 2, 3]),
            ArraySubset::new_with_ranges(&[0..3, 1..3, 2..3]),
        ];

        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(input_handle, &chunk_representation)
            .unwrap();
        let decoded_partial_chunk = partial_decoder.partial_decode(&decoded_regions).unwrap();

        let decoded_partial_chunk: Vec<f32> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<f32>())
            .map(|b| f32::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<f32> = vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 8.0, 14.0, 17.0, 23.0, 26.0,
        ];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
