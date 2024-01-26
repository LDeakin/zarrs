use derive_more::From;
use thiserror::Error;

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToArrayCodecTraits, Codec,
            CodecError, CodecPlugin, CodecTraits,
        },
        ArrayRepresentation,
    },
    metadata::Metadata,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncArrayPartialDecoderTraits;

use super::{
    calculate_order_decode, calculate_order_encode, permute, transpose_array,
    transpose_configuration::TransposeCodecConfigurationV1, TransposeCodecConfiguration,
    TransposeOrder,
};

const IDENTIFIER: &str = "transpose";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_transpose, create_codec_transpose)
}

fn is_name_transpose(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_transpose(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: TransposeCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(TransposeCodec::new_with_configuration(&configuration)?);
    Ok(Codec::ArrayToArray(codec))
}

/// A Transpose codec implementation.
#[derive(Clone, Debug)]
pub struct TransposeCodec {
    order: TransposeOrder,
}

/// An invalid permutation order error.
#[derive(Clone, Debug, Error, From)]
#[error("permutation order {0:?} is invalid. It must be an array of integers specifying a permutation of 0, 1, …, n-1, where n is the number of dimensions")]
pub struct InvalidPermutationError(Vec<usize>);

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
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = TransposeCodecConfigurationV1 {
            order: self.order.clone(),
        };
        Some(Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
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
    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            super::transpose_partial_decoder::TransposePartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.order.clone(),
            ),
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            super::transpose_partial_decoder::AsyncTransposePartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.order.clone(),
            ),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<ArrayRepresentation, CodecError> {
        let transposed_shape = permute(decoded_representation.shape(), &self.order);
        Ok(unsafe {
            ArrayRepresentation::new_unchecked(
                transposed_shape,
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        })
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayCodecTraits for TransposeCodec {
    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != decoded_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                decoded_representation.size(),
            ));
        }

        let order_encode =
            calculate_order_encode(&self.order, decoded_representation.shape().len());
        transpose_array(
            &order_encode,
            decoded_representation.shape(),
            decoded_representation.element_size(),
            &decoded_value,
        )
        .map_err(|_| {
            CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                decoded_representation.size(),
            )
        })
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        let order_decode =
            calculate_order_decode(&self.order, decoded_representation.shape().len());
        let transposed_shape = permute(decoded_representation.shape(), &self.order);
        transpose_array(
            &order_decode,
            &transposed_shape,
            decoded_representation.element_size(),
            &encoded_value,
        )
        .map_err(|_| {
            CodecError::UnexpectedChunkDecodedSize(
                encoded_value.len(),
                decoded_representation.size(),
            )
        })
    }
}
