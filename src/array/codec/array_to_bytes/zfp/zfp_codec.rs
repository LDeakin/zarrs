use std::borrow::Cow;

use zfp_sys::{
    zfp_compress,
    zfp_stream_maximum_size,
    zfp_stream_rewind,
    zfp_stream_set_bit_stream,
    // zfp_exec_policy_zfp_exec_omp, zfp_stream_set_execution
};

use crate::{
    array::{
        codec::{
            ArrayBytes, ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, CodecError, CodecOptions, CodecTraits, RawBytes,
            RecommendedConcurrency,
        },
        ArrayMetadataOptions, BytesRepresentation, ChunkRepresentation, DataType,
    },
    metadata::v3::MetadataV3,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{
    promote_before_zfp_encoding, zarr_to_zfp_data_type, zfp_bitstream::ZfpBitstream, zfp_decode,
    zfp_field::ZfpField, zfp_partial_decoder, zfp_stream::ZfpStream, ZfpCodecConfiguration,
    ZfpCodecConfigurationV1, ZfpExpertConfiguration, ZfpFixedAccuracyConfiguration,
    ZfpFixedPrecisionConfiguration, ZfpFixedRateConfiguration, ZfpMode, IDENTIFIER,
};

/// A `zfp` codec implementation.
#[derive(Clone, Copy, Debug)]
pub struct ZfpCodec {
    mode: ZfpMode,
}

impl ZfpCodec {
    /// Create a new `Zfp` codec in expert mode.
    #[must_use]
    pub const fn new_expert(expert_params: ZfpExpertConfiguration) -> Self {
        Self {
            mode: ZfpMode::Expert(expert_params),
        }
    }

    /// Create a new `Zfp` codec in fixed rate mode.
    #[must_use]
    pub const fn new_fixed_rate(rate: f64) -> Self {
        Self {
            mode: ZfpMode::FixedRate(rate),
        }
    }

    /// Create a new `Zfp` codec in fixed precision mode.
    #[must_use]
    pub const fn new_fixed_precision(precision: u32) -> Self {
        Self {
            mode: ZfpMode::FixedPrecision(precision),
        }
    }

    /// Create a new `Zfp` codec in fixed accuracy mode.
    #[must_use]
    pub const fn new_fixed_accuracy(tolerance: f64) -> Self {
        Self {
            mode: ZfpMode::FixedAccuracy(tolerance),
        }
    }

    /// Create a new `Zfp` codec in reversible mode.
    #[must_use]
    pub const fn new_reversible() -> Self {
        Self {
            mode: ZfpMode::Reversible,
        }
    }

    /// Create a new `Zfp` codec from configuration.
    #[must_use]
    pub const fn new_with_configuration(configuration: &ZfpCodecConfiguration) -> Self {
        type V1 = ZfpCodecConfigurationV1;
        let ZfpCodecConfiguration::V1(configuration) = configuration;
        match configuration {
            V1::Expert(cfg) => Self::new_expert(*cfg),
            V1::FixedRate(cfg) => Self::new_fixed_rate(cfg.rate),
            V1::FixedPrecision(cfg) => Self::new_fixed_precision(cfg.precision),
            V1::FixedAccuracy(cfg) => Self::new_fixed_accuracy(cfg.tolerance),
            V1::Reversible => Self::new_reversible(),
        }
    }
}

impl CodecTraits for ZfpCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = match self.mode {
            ZfpMode::Expert(expert) => ZfpCodecConfigurationV1::Expert(expert),
            ZfpMode::FixedRate(rate) => {
                ZfpCodecConfigurationV1::FixedRate(ZfpFixedRateConfiguration { rate })
            }
            ZfpMode::FixedPrecision(precision) => {
                ZfpCodecConfigurationV1::FixedPrecision(ZfpFixedPrecisionConfiguration {
                    precision,
                })
            }
            ZfpMode::FixedAccuracy(tolerance) => {
                ZfpCodecConfigurationV1::FixedAccuracy(ZfpFixedAccuracyConfiguration { tolerance })
            }
            ZfpMode::Reversible => ZfpCodecConfigurationV1::Reversible,
        };
        Some(MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

impl ArrayCodecTraits for ZfpCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // TODO: zfp supports multi thread, when is it optimal to kick in?
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for ZfpCodec {
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        let bytes = bytes.into_fixed()?;
        let mut bytes_promoted = promote_before_zfp_encoding(&bytes, decoded_representation)?;
        let zfp_type = bytes_promoted.zfp_type();
        let Some(field) = ZfpField::new(
            &mut bytes_promoted,
            &decoded_representation
                .shape()
                .iter()
                .map(|u| usize::try_from(u.get()).unwrap())
                .collect::<Vec<usize>>(),
        ) else {
            return Err(CodecError::from("failed to create zfp field"));
        };
        let Some(zfp) = ZfpStream::new(&self.mode, zfp_type) else {
            return Err(CodecError::from("failed to create zfp stream"));
        };

        let bufsize = unsafe { zfp_stream_maximum_size(zfp.as_zfp_stream(), field.as_zfp_field()) };
        let mut encoded_value: Vec<u8> = vec![0; bufsize];

        let Some(stream) = ZfpBitstream::new(&mut encoded_value) else {
            return Err(CodecError::from("failed to create zfp field"));
        };
        unsafe {
            zfp_stream_set_bit_stream(zfp.as_zfp_stream(), stream.as_bitstream());
            zfp_stream_rewind(zfp.as_zfp_stream()); // needed?
        }

        // FIXME
        // if parallel {
        //     // Number of threads is set automatically
        //     unsafe {
        //         zfp_stream_set_execution(zfp.as_zfp_stream(), zfp_exec_policy_zfp_exec_omp);
        //     }
        // }

        // Compress array
        let size = unsafe { zfp_compress(zfp.as_zfp_stream(), field.as_zfp_field()) };

        if size == 0 {
            Err(CodecError::from("zfp compression failed"))
        } else {
            encoded_value.truncate(size);
            Ok(Cow::Owned(encoded_value))
        }
    }

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        zfp_decode(
            &self.mode,
            &mut bytes.to_vec(), // FIXME: Does zfp **really** need the encoded value as mutable?
            decoded_representation,
            false, // FIXME
        )
        .map(ArrayBytes::from)
    }

    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(zfp_partial_decoder::ZfpPartialDecoder::new(
            input_handle,
            decoded_representation,
            self.mode,
        )?))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(zfp_partial_decoder::AsyncZfpPartialDecoder::new(
            input_handle,
            decoded_representation,
            self.mode,
        )?))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        let data_type = decoded_representation.data_type();
        let Some(zfp_type) = zarr_to_zfp_data_type(decoded_representation.data_type()) else {
            return Err(CodecError::from(
                "data type {} is unsupported for zfp codec",
            ));
        };

        let bufsize = unsafe {
            // safety: zfp_stream_maximum_size does not use the data in the field, so it can be empty
            let field = ZfpField::new_empty(
                zfp_type,
                &decoded_representation
                    .shape()
                    .iter()
                    .map(|u| usize::try_from(u.get()).unwrap())
                    .collect::<Vec<usize>>(),
            );
            let Some(field) = field else {
                return Err(CodecError::from("failed to create zfp field"));
            };

            let Some(zfp) = ZfpStream::new(&self.mode, zfp_type) else {
                return Err(CodecError::from("failed to create zfp stream"));
            };

            zfp_stream_maximum_size(zfp.as_zfp_stream(), field.as_zfp_field())
        };

        match data_type {
            DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => Ok(BytesRepresentation::BoundedSize(bufsize as u64)),
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }
    }
}
