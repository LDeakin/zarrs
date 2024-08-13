use std::sync::Arc;

use crate::{
    array::{
        codec::{
            options::CodecOptions, ArrayBytes, ArrayCodecTraits, ArrayPartialDecoderTraits,
            ArrayToArrayCodecTraits, CodecError, CodecTraits, RecommendedConcurrency,
        },
        ArrayMetadataOptions, ChunkRepresentation,
    },
    metadata::v3::{codec::transpose::TransposeCodecConfigurationV1, MetadataV3},
    plugin::PluginCreateError,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncArrayPartialDecoderTraits;

use super::{
    calculate_order_decode, calculate_order_encode, permute, transpose_array,
    TransposeCodecConfiguration, TransposeOrder, IDENTIFIER,
};

/// A Transpose codec implementation.
#[derive(Clone, Debug)]
pub struct TransposeCodec {
    order: TransposeOrder,
}

impl TransposeCodec {
    /// Create a new transpose codec from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if there is a configuration issue.
    pub fn new_with_configuration(
        configuration: &TransposeCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let TransposeCodecConfiguration::V1(configuration) = configuration;
        Ok(Self::new(configuration.order.clone()))
    }

    /// Create a new transpose codec.
    #[must_use]
    pub const fn new(order: TransposeOrder) -> Self {
        Self { order }
    }
}

impl CodecTraits for TransposeCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = TransposeCodecConfigurationV1 {
            order: self.order.clone(),
        };
        Some(MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToArrayCodecTraits for TransposeCodec {
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        bytes.validate(
            decoded_representation.num_elements(),
            decoded_representation.data_type().size(),
        )?;

        match bytes {
            ArrayBytes::Variable(bytes, offsets) => {
                let order_encode = self.order.0.clone();
                let shape = decoded_representation
                    .shape()
                    .iter()
                    .map(|s| usize::try_from(s.get()).unwrap())
                    .collect::<Vec<_>>();
                Ok(super::transpose_vlen(
                    &bytes,
                    &offsets,
                    &shape,
                    order_encode,
                ))
            }
            ArrayBytes::Fixed(bytes) => {
                let order_encode =
                    calculate_order_encode(&self.order, decoded_representation.shape().len());
                let data_type_size = decoded_representation.data_type().fixed_size().unwrap();
                let bytes = transpose_array(
                    &order_encode,
                    &decoded_representation.shape_u64(),
                    data_type_size,
                    &bytes,
                )
                .map_err(|_| CodecError::Other("transpose_array invalid arguments?".to_string()))?;
                Ok(ArrayBytes::from(bytes))
            }
        }
    }

    fn decode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        bytes.validate(
            decoded_representation.num_elements(),
            decoded_representation.data_type().size(),
        )?;

        match bytes {
            ArrayBytes::Variable(bytes, offsets) => {
                let mut order_decode = vec![0; decoded_representation.shape().len()];
                for (i, val) in self.order.0.iter().enumerate() {
                    order_decode[*val] = i;
                }
                let shape = decoded_representation
                    .shape()
                    .iter()
                    .map(|s| usize::try_from(s.get()).unwrap())
                    .collect::<Vec<_>>();
                Ok(super::transpose_vlen(
                    &bytes,
                    &offsets,
                    &shape,
                    order_decode,
                ))
            }
            ArrayBytes::Fixed(bytes) => {
                let order_decode =
                    calculate_order_decode(&self.order, decoded_representation.shape().len());
                let transposed_shape = permute(&decoded_representation.shape_u64(), &self.order);
                let data_type_size = decoded_representation.data_type().fixed_size().unwrap();
                let bytes =
                    transpose_array(&order_decode, &transposed_shape, data_type_size, &bytes)
                        .map_err(|_| CodecError::Other("transpose_array error".to_string()))?;
                Ok(ArrayBytes::from(bytes))
            }
        }
    }

    fn partial_decoder<'a>(
        &'a self,
        input_handle: Arc<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
            super::transpose_partial_decoder::TransposePartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.order.clone(),
            ),
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Arc<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
            super::transpose_partial_decoder::AsyncTransposePartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.order.clone(),
            ),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<ChunkRepresentation, CodecError> {
        let transposed_shape = permute(decoded_representation.shape(), &self.order);
        Ok(unsafe {
            ChunkRepresentation::new_unchecked(
                transposed_shape,
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        })
    }
}

impl ArrayCodecTraits for TransposeCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // TODO: This could be increased, need to implement `transpose_array` without ndarray
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}
