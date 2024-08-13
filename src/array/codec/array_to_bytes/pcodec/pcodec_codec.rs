use std::{borrow::Cow, sync::Arc};

use pco::{standalone::guarantee::file_size, ChunkConfig, ModeSpec, PagingSpec};

use crate::{
    array::{
        codec::{
            ArrayBytes, ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, CodecError, CodecOptions, CodecTraits, RawBytes,
            RecommendedConcurrency,
        },
        convert_from_bytes_slice, transmute_to_bytes_vec, ArrayMetadataOptions,
        BytesRepresentation, ChunkRepresentation, DataType,
    },
    config::global_config,
    metadata::v3::{codec::pcodec::PcodecModeSpecConfiguration, MetadataV3},
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

fn mode_spec_config_to_pco(mode_spec: &PcodecModeSpecConfiguration) -> ModeSpec {
    match mode_spec {
        PcodecModeSpecConfiguration::Auto => ModeSpec::Auto,
        PcodecModeSpecConfiguration::Classic => ModeSpec::Classic,
        PcodecModeSpecConfiguration::TryFloatMult(base) => ModeSpec::TryFloatMult(*base),
        PcodecModeSpecConfiguration::TryFloatQuant(k) => ModeSpec::TryFloatQuant(*k),
        PcodecModeSpecConfiguration::TryIntMult(base) => ModeSpec::TryIntMult(*base),
    }
}

fn mode_spec_pco_to_config(mode_spec: &ModeSpec) -> PcodecModeSpecConfiguration {
    match mode_spec {
        ModeSpec::Auto => PcodecModeSpecConfiguration::Auto,
        ModeSpec::Classic => PcodecModeSpecConfiguration::Classic,
        ModeSpec::TryFloatMult(base) => PcodecModeSpecConfiguration::TryFloatMult(*base),
        ModeSpec::TryFloatQuant(k) => PcodecModeSpecConfiguration::TryFloatQuant(*k),
        ModeSpec::TryIntMult(base) => PcodecModeSpecConfiguration::TryIntMult(*base),
        _ => unreachable!("Mode spec is not supported"),
    }
}

fn configuration_to_chunk_config(configuration: &PcodecCodecConfigurationV1) -> ChunkConfig {
    let mode_spec = mode_spec_config_to_pco(&configuration.mode_spec);
    ChunkConfig::default()
        .with_compression_level(configuration.level.as_usize())
        .with_delta_encoding_order(
            configuration
                .delta_encoding_order
                .map(|order| order.as_usize()),
        )
        .with_mode_spec(mode_spec)
        .with_paging_spec(PagingSpec::EqualPagesUpTo(configuration.equal_pages_up_to))
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
        let PagingSpec::EqualPagesUpTo(equal_pages_up_to) = self.chunk_config.paging_spec else {
            unreachable!()
        };
        let configuration = PcodecCodecConfiguration::V1(PcodecCodecConfigurationV1 {
            level: PcodecCompressionLevel::try_from(self.chunk_config.compression_level).unwrap(),
            delta_encoding_order: self
                .chunk_config
                .delta_encoding_order
                .map(|order| PcodecDeltaEncodingOrder::try_from(order).unwrap()),
            mode_spec: mode_spec_pco_to_config(&self.chunk_config.mode_spec),
            equal_pages_up_to,
        });

        Some(
            MetadataV3::new_with_serializable_configuration(
                global_config()
                    .experimental_codec_names()
                    .get(super::IDENTIFIER)
                    .expect("experimental codec identifier in global map"),
                &configuration,
            )
            .unwrap(),
        )
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
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for PcodecCodec {
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        let data_type = decoded_representation.data_type();
        let bytes = bytes.into_fixed()?;
        macro_rules! pcodec_encode {
            ( $t:ty ) => {
                pco::standalone::simple_compress(
                    &convert_from_bytes_slice::<$t>(&bytes),
                    &self.chunk_config,
                )
                .map(Cow::Owned)
                .map_err(|err| CodecError::Other(err.to_string()))
            };
        }

        match data_type {
            DataType::UInt16 => {
                pcodec_encode!(u16)
            }
            DataType::UInt32 => {
                pcodec_encode!(u32)
            }
            DataType::UInt64 => {
                pcodec_encode!(u64)
            }
            DataType::Int16 => {
                pcodec_encode!(i16)
            }
            DataType::Int32 => {
                pcodec_encode!(i32)
            }
            DataType::Int64 => {
                pcodec_encode!(i64)
            }
            DataType::Float16 => {
                pcodec_encode!(half::f16)
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

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        let data_type = decoded_representation.data_type();
        macro_rules! pcodec_decode {
            ( $t:ty ) => {
                pco::standalone::simple_decompress(&bytes)
                    .map(|bytes| Cow::Owned(transmute_to_bytes_vec::<$t>(bytes)))
                    .map_err(|err| CodecError::Other(err.to_string()))
            };
        }

        let bytes = match data_type {
            DataType::UInt16 => {
                pcodec_decode!(u16)
            }
            DataType::UInt32 => {
                pcodec_decode!(u32)
            }
            DataType::UInt64 => {
                pcodec_decode!(u64)
            }
            DataType::Int16 => {
                pcodec_decode!(i16)
            }
            DataType::Int32 => {
                pcodec_decode!(i32)
            }
            DataType::Int64 => {
                pcodec_decode!(i64)
            }
            DataType::Float16 => {
                pcodec_decode!(half::f16)
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
        }?;
        Ok(ArrayBytes::from(bytes))
    }

    fn partial_decoder<'a>(
        &self,
        input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(pcodec_partial_decoder::PcodecPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
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
            DataType::UInt16 | DataType::Int16 | DataType::Float16 => Ok(file_size::<u16>(
                num_elements,
                &self.chunk_config.paging_spec,
            )
            .map_err(|err| CodecError::from(err.to_string()))?),
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
