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
    plugin::PluginCreateError,
};

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
    let configuration: TransposeCodecConfiguration = metadata.to_configuration()?;
    let codec = Box::new(TransposeCodec::new_with_configuration(&configuration)?);
    Ok(Codec::ArrayToArray(codec))
}

/// A Transpose codec implementation.
#[derive(Clone, Debug)]
pub struct TransposeCodec {
    order: TransposeOrder,
}

/// An invalid permutation order error.
#[derive(Clone, Debug, Error)]
#[error("permutation order {0:?} is invalid")]
pub struct InvalidPermutationError(Vec<usize>);

impl TransposeCodec {
    /// Create a new transpose codec from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if there is a configuration issue.
    pub fn new_with_configuration(
        configuration: &TransposeCodecConfiguration,
    ) -> Result<TransposeCodec, PluginCreateError> {
        let TransposeCodecConfiguration::V1(configuration) = configuration;
        Self::new_with_order(configuration.order.clone()).map_err(|e| PluginCreateError::Other {
            error_str: e.to_string(),
        })
    }

    /// Create a new transpose codec.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPermutationError`] if the permutation order is invalid.
    pub fn new_with_order(
        order: TransposeOrder,
    ) -> Result<TransposeCodec, InvalidPermutationError> {
        if let TransposeOrder::Permutation(permutation) = &order {
            if !validate_permutation(permutation) {
                return Err(InvalidPermutationError(permutation.clone()));
            }
        }
        Ok(TransposeCodec { order })
    }
}

fn validate_permutation(permutation: &[usize]) -> bool {
    let permutation_unique = to_vec_unique(permutation);
    !permutation.is_empty()
        && permutation_unique.len() == permutation.len()
        && *permutation_unique.iter().max().unwrap() == permutation.len() - 1
}

fn to_vec_unique(v: &[usize]) -> Vec<usize> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v.dedup();
    v
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

impl ArrayToArrayCodecTraits for TransposeCodec {
    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
    ) -> Box<dyn ArrayPartialDecoderTraits + 'a> {
        Box::new(
            super::transpose_partial_decoder::TransposePartialDecoder::new(
                input_handle,
                self.order.clone(),
            ),
        )
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ArrayRepresentation,
    ) -> ArrayRepresentation {
        let transposed_shape = permute(decoded_representation.shape(), &self.order);
        unsafe {
            ArrayRepresentation::new_unchecked(
                transposed_shape,
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        }
    }
}

impl ArrayCodecTraits for TransposeCodec {
    fn encode(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
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

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
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
