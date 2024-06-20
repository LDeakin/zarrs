use pco::{standalone::guarantee::file_size, ChunkConfig, FloatMultSpec, IntMultSpec, PagingSpec};

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, CodecError, CodecOptions, CodecTraits,
            RecommendedConcurrency,
        },
        transmute_from_bytes_vec, transmute_to_bytes_vec, ArrayMetadataOptions,
        BytesRepresentation, ChunkRepresentation, DataType,
    },
    metadata::v3::MetadataV3,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{
    pcodec_partial_decoder, PcodecCodecConfiguration, PcodecCodecConfigurationV1,
    PcodecCompressionLevel, PcodecDeltaEncodingOrder, IDENTIFIER,
};

/// A `pcodec` codec implementation.
#[derive(Debug, Clone)]
pub struct PcodecCodec {
    chunk_config: ChunkConfig,
}

fn configuration_to_chunk_config(configuration: &PcodecCodecConfigurationV1) -> ChunkConfig {
    ChunkConfig::default()
        .with_compression_level(configuration.level.as_usize())
        .with_delta_encoding_order(
            configuration
                .delta_encoding_order
                .map(|order| order.as_usize()),
        )
        .with_int_mult_spec(if configuration.int_mult_spec {
            IntMultSpec::Enabled
        } else {
            IntMultSpec::Disabled
        })
        .with_float_mult_spec(if configuration.float_mult_spec {
            FloatMultSpec::Enabled
        } else {
            FloatMultSpec::Disabled
        })
        .with_paging_spec(PagingSpec::EqualPagesUpTo(configuration.max_page_n))
}

impl PcodecCodec {
    /// Create a new `pcodec` codec from configuration.
    #[must_use]
    pub fn new_with_configuration(configuration: &PcodecCodecConfiguration) -> Self {
        let PcodecCodecConfiguration::V1(configuration) = configuration;
        let chunk_config = configuration_to_chunk_config(configuration);
        Self { chunk_config }
    }
}

impl CodecTraits for PcodecCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let PagingSpec::EqualPagesUpTo(max_page_n) = self.chunk_config.paging_spec else {
            unreachable!()
        };
        let configuration = PcodecCodecConfiguration::V1(PcodecCodecConfigurationV1 {
            level: PcodecCompressionLevel::try_from(self.chunk_config.compression_level).unwrap(),
            delta_encoding_order: self
                .chunk_config
                .delta_encoding_order
                .map(|order| PcodecDeltaEncodingOrder::try_from(order).unwrap()),
            int_mult_spec: self.chunk_config.int_mult_spec == IntMultSpec::Enabled,
            float_mult_spec: self.chunk_config.float_mult_spec == FloatMultSpec::Enabled,
            max_page_n,
        });

        Some(MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

impl ArrayCodecTraits for PcodecCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // pcodec does not support parallel decode
        Ok(RecommendedConcurrency::new_maximum(1))
    }

    fn encode(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Vec<u8>, CodecError> {
        let data_type = decoded_representation.data_type();
        macro_rules! pcodec_encode {
            ( $t:ty ) => {
                pco::standalone::simple_compress(
                    transmute_from_bytes_vec::<$t>(decoded_value).as_slice(),
                    &self.chunk_config,
                )
                .map_err(|err| CodecError::Other(err.to_string()))
            };
        }

        match data_type {
            DataType::UInt32 => {
                pcodec_encode!(u32)
            }
            DataType::UInt64 => {
                pcodec_encode!(u64)
            }
            DataType::Int32 => {
                pcodec_encode!(i32)
            }
            DataType::Int64 => {
                pcodec_encode!(i64)
            }
            DataType::Float32 | DataType::Complex64 => {
                pcodec_encode!(f32)
            }
            DataType::Float64 | DataType::Complex128 => {
                pcodec_encode!(f64)
            }
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }
    }

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Vec<u8>, CodecError> {
        let data_type = decoded_representation.data_type();
        macro_rules! pcodec_decode {
            ( $t:ty ) => {
                pco::standalone::simple_decompress(encoded_value.as_slice())
                    .map(|bytes| transmute_to_bytes_vec::<$t>(bytes))
                    .map_err(|err| CodecError::Other(err.to_string()))
            };
        }

        match data_type {
            DataType::UInt32 => {
                pcodec_decode!(u32)
            }
            DataType::UInt64 => {
                pcodec_decode!(u64)
            }
            DataType::Int32 => {
                pcodec_decode!(i32)
            }
            DataType::Int64 => {
                pcodec_decode!(i64)
            }
            DataType::Float32 | DataType::Complex64 => {
                pcodec_decode!(f32)
            }
            DataType::Float64 | DataType::Complex128 => {
                pcodec_decode!(f64)
            }
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for PcodecCodec {
    fn partial_decoder<'a>(
        &self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(pcodec_partial_decoder::PcodecPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            pcodec_partial_decoder::AsyncPCodecPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
            ),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        let data_type = decoded_representation.data_type();
        let mut num_elements = decoded_representation.num_elements_usize();
        if data_type == &DataType::Complex64 || data_type == &DataType::Complex128 {
            num_elements *= 2;
        }

        let size = match data_type {
            DataType::UInt32 | DataType::Int32 | DataType::Float32 | DataType::Complex64 => Ok(
                file_size::<u32>(num_elements, &self.chunk_config.paging_spec)
                    .map_err(|err| CodecError::from(err.to_string()))?,
            ),
            DataType::UInt64 | DataType::Int64 | DataType::Float64 | DataType::Complex128 => Ok(
                file_size::<u64>(num_elements, &self.chunk_config.paging_spec)
                    .map_err(|err| CodecError::from(err.to_string()))?,
            ),
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }?;
        Ok(BytesRepresentation::BoundedSize(size.try_into().unwrap()))
    }
}
