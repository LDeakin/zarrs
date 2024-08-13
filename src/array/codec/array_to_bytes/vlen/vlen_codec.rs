use std::{mem::size_of, num::NonZeroU64, sync::Arc};

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits, BytesCodec,
            BytesPartialDecoderTraits, CodecError, CodecOptions, CodecTraits,
            RecommendedConcurrency,
        },
        transmute_to_bytes_vec, ArrayBytes, ArrayMetadataOptions, BytesRepresentation,
        ChunkRepresentation, CodecChain, DataType, DataTypeSize, Endianness, FillValue, RawBytes,
    },
    config::global_config,
    metadata::v3::{codec::vlen::VlenIndexDataType, MetadataV3},
    plugin::PluginCreateError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{vlen_partial_decoder, VlenCodecConfiguration, VlenCodecConfigurationV1};

/// A `bytes` codec implementation.
#[derive(Debug, Clone)]
pub struct VlenCodec {
    index_codecs: CodecChain,
    data_codecs: CodecChain,
    index_data_type: VlenIndexDataType,
}

impl Default for VlenCodec {
    fn default() -> Self {
        let index_codecs = CodecChain::new(
            vec![],
            Box::new(BytesCodec::new(Some(Endianness::Little))),
            vec![],
        );
        let data_codecs = CodecChain::new(vec![], Box::new(BytesCodec::new(None)), vec![]);
        Self {
            index_codecs,
            data_codecs,
            index_data_type: VlenIndexDataType::UInt64,
        }
    }
}

impl VlenCodec {
    /// Create a new `vlen` codec.
    #[must_use]
    pub fn new(
        index_codecs: CodecChain,
        data_codecs: CodecChain,
        index_data_type: VlenIndexDataType,
    ) -> Self {
        Self {
            index_codecs,
            data_codecs,
            index_data_type,
        }
    }

    /// Create a new `vlen` codec from configuration.
    ///
    /// # Errors
    /// Returns a [`PluginCreateError`] if the codecs cannot be constructed from the codec metadata.
    pub fn new_with_configuration(
        configuration: &VlenCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let VlenCodecConfiguration::V1(configuration) = configuration;
        let index_codecs = CodecChain::from_metadata(&configuration.index_codecs)?;
        let data_codecs = CodecChain::from_metadata(&configuration.data_codecs)?;
        Ok(Self::new(
            index_codecs,
            data_codecs,
            configuration.index_data_type,
        ))
    }
}

impl CodecTraits for VlenCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = VlenCodecConfigurationV1 {
            index_codecs: self.index_codecs.create_metadatas(),
            data_codecs: self.data_codecs.create_metadatas(),
            index_data_type: self.index_data_type,
        };
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
        true // TODO: Vlen could do partial decoding, but needs coalescing etc
    }
}

impl ArrayCodecTraits for VlenCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for VlenCodec {
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        bytes.validate(
            decoded_representation.num_elements(),
            decoded_representation.data_type().size(),
        )?;
        let (data, offsets) = bytes.into_variable()?;
        assert_eq!(
            offsets.len(),
            decoded_representation.num_elements_usize() + 1
        );

        // Encode offsets
        let num_offsets =
            NonZeroU64::try_from(decoded_representation.num_elements_usize() as u64 + 1).unwrap();
        let offsets = match self.index_data_type {
            // VlenIndexDataType::UInt8 => {
            //     let offsets = offsets
            //         .iter()
            //         .map(|offset| u8::try_from(*offset))
            //         .collect::<Result<Vec<_>, _>>()
            //         .map_err(|_| {
            //             CodecError::Other(
            //                 "index offsets are too large for a uint8 index_data_type".to_string(),
            //             )
            //         })?;
            //     let offsets = transmute_to_bytes_vec(offsets);
            //     let index_chunk_rep = ChunkRepresentation::new(
            //         vec![num_offsets],
            //         DataType::UInt8,
            //         FillValue::from(0u8),
            //     )
            //     .unwrap();
            //     self.index_codecs
            //         .encode(offsets.into(), &index_chunk_rep, options)?
            // }
            // VlenIndexDataType::UInt16 => {
            //     let offsets = offsets
            //         .iter()
            //         .map(|offset| u16::try_from(*offset))
            //         .collect::<Result<Vec<_>, _>>()
            //         .map_err(|_| {
            //             CodecError::Other(
            //                 "index offsets are too large for a uint16 index_data_type".to_string(),
            //             )
            //         })?;
            //     let offsets = transmute_to_bytes_vec(offsets);
            //     let index_chunk_rep = ChunkRepresentation::new(
            //         vec![num_offsets],
            //         DataType::UInt16,
            //         FillValue::from(0u16),
            //     )
            //     .unwrap();
            //     self.index_codecs
            //         .encode(offsets.into(), &index_chunk_rep, options)?
            // }
            VlenIndexDataType::UInt32 => {
                let offsets = offsets
                    .iter()
                    .map(|offset| u32::try_from(*offset))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| {
                        CodecError::Other(
                            "index offsets are too large for a uint32 index_data_type".to_string(),
                        )
                    })?;
                let offsets = transmute_to_bytes_vec(offsets);
                let index_chunk_rep = ChunkRepresentation::new(
                    vec![num_offsets],
                    DataType::UInt32,
                    FillValue::from(0u32),
                )
                .unwrap();
                self.index_codecs
                    .encode(offsets.into(), &index_chunk_rep, options)?
            }
            VlenIndexDataType::UInt64 => {
                let offsets = offsets
                    .iter()
                    .map(|offset| u64::try_from(*offset).unwrap())
                    .collect::<Vec<u64>>();
                let offsets = transmute_to_bytes_vec(offsets);
                let index_chunk_rep = ChunkRepresentation::new(
                    vec![num_offsets],
                    DataType::UInt64,
                    FillValue::from(0u64),
                )
                .unwrap();
                self.index_codecs
                    .encode(offsets.into(), &index_chunk_rep, options)?
            }
        };

        // Encode data
        let data = if let Ok(data_len) = NonZeroU64::try_from(data.len() as u64) {
            self.data_codecs.encode(
                data.into(),
                &ChunkRepresentation::new(vec![data_len], DataType::UInt8, FillValue::from(0u8))
                    .unwrap(),
                options,
            )?
        } else {
            vec![].into()
        };

        // Pack encoded offsets length, encoded offsets, and encoded data
        let mut bytes = Vec::with_capacity(size_of::<u64>() + offsets.len() + data.len());
        bytes.extend_from_slice(&u64::try_from(offsets.len()).unwrap().to_le_bytes()); // offsets length as u64 little endian
        bytes.extend_from_slice(&offsets);
        bytes.extend_from_slice(&data);
        Ok(bytes.into())
    }

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        let num_elements = decoded_representation.num_elements_usize();
        let index_shape = vec![NonZeroU64::try_from(num_elements as u64 + 1).unwrap()];
        let index_chunk_rep = match self.index_data_type {
            // VlenIndexDataType::UInt8 => {
            //     ChunkRepresentation::new(index_shape, DataType::UInt8, FillValue::from(0u8))
            // }
            // VlenIndexDataType::UInt16 => {
            //     ChunkRepresentation::new(index_shape, DataType::UInt16, FillValue::from(0u16))
            // }
            VlenIndexDataType::UInt32 => {
                ChunkRepresentation::new(index_shape, DataType::UInt32, FillValue::from(0u32))
            }
            VlenIndexDataType::UInt64 => {
                ChunkRepresentation::new(index_shape, DataType::UInt64, FillValue::from(0u64))
            }
        }
        .unwrap();
        let (data, index) = super::get_vlen_bytes_and_offsets(
            &index_chunk_rep,
            &bytes,
            &self.index_codecs,
            &self.data_codecs,
            options,
        )?;
        Ok(ArrayBytes::new_vlen(data, index))
    }

    fn partial_decoder<'a>(
        &'a self,
        input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(vlen_partial_decoder::VlenPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
            &self.index_codecs,
            &self.data_codecs,
            self.index_data_type,
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
            vlen_partial_decoder::AsyncVlenPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                &self.index_codecs,
                &self.data_codecs,
                self.index_data_type,
            ),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        match decoded_representation.data_type().size() {
            DataTypeSize::Variable => Ok(BytesRepresentation::UnboundedSize),
            DataTypeSize::Fixed(_) => {
                return Err(CodecError::UnsupportedDataType(
                    decoded_representation.data_type().clone(),
                    super::IDENTIFIER.to_string(),
                ))
            }
        }
    }
}
