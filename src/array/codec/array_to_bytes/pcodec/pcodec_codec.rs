// Note: No validation that this codec is created *without* a specified endianness for multi-byte data types.

use pco::{ChunkConfig, FloatMultSpec, IntMultSpec, PagingSpec};

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, Codec, CodecError, CodecPlugin, CodecTraits,
        },
        transmute_from_bytes_vec, transmute_to_bytes_vec, BytesRepresentation, ChunkRepresentation,
        DataType,
    },
    metadata::Metadata,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{
    pcodec_partial_decoder, PcodecCodecConfiguration, PcodecCodecConfigurationV1,
    PcodecCompressionLevel, PcodecDeltaEncodingOrder, IDENTIFIER,
};

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_pcodec, create_codec_pcodec)
}

fn is_name_pcodec(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_pcodec(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration = if metadata.configuration_is_none_or_empty() {
        PcodecCodecConfiguration::default()
    } else {
        metadata
            .to_configuration()
            .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?
    };
    let codec = Box::new(PcodecCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}

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
    fn create_metadata(&self) -> Option<Metadata> {
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

        Some(Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayCodecTraits for PcodecCodec {
    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        _parallel: bool,
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

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        let data_type = decoded_representation.data_type();
        macro_rules! pcodec_decode {
            ( $t:ty ) => {
                pco::standalone::auto_decompress(encoded_value.as_slice())
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
    fn partial_decoder_opt<'a>(
        &self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(pcodec_partial_decoder::PcodecPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _parallel: bool,
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
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        // FIXME: pcodec is likely bounded, but it doesn't have a nice API to figure out what the bounded size is
        Ok(BytesRepresentation::UnboundedSize)
    }
}
