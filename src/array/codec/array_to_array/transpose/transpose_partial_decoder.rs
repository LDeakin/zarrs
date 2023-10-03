use super::{calculate_order_decode, permute, transpose_array, TransposeOrder};
use crate::array::{
    codec::{ArrayPartialDecoderTraits, ArraySubset, CodecError},
    ArrayRepresentation,
};

/// The partial decoder for the Transpose codec.
pub struct TransposePartialDecoder<'a> {
    input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
    order: TransposeOrder,
}

impl<'a> TransposePartialDecoder<'a> {
    /// Create a new partial decoder for the Transpose codec.
    pub fn new(
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        order: TransposeOrder,
    ) -> Self {
        Self {
            input_handle,
            order,
        }
    }
}

impl ArrayPartialDecoderTraits for TransposePartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &ArrayRepresentation,
        decoded_regions: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        // Get transposed array subsets
        let mut decoded_regions_transposed = Vec::with_capacity(decoded_regions.len());
        for decoded_region in decoded_regions {
            let start = permute(decoded_region.start(), &self.order);
            let size = permute(decoded_region.shape(), &self.order);
            let decoded_region_transpose =
                unsafe { ArraySubset::new_with_start_shape_unchecked(start, size) };
            decoded_regions_transposed.push(decoded_region_transpose);
        }
        let mut encoded_value = self
            .input_handle
            .partial_decode(decoded_representation, &decoded_regions_transposed)?;

        // Reverse the transpose on each subset
        let order_decode =
            calculate_order_decode(&self.order, decoded_representation.shape().len());
        for (subset, bytes) in std::iter::zip(decoded_regions, &mut encoded_value) {
            transpose_array(
                &order_decode,
                subset.shape(),
                decoded_representation.element_size(),
                bytes.as_mut_slice(),
            )
            .map_err(|_| {
                CodecError::UnexpectedChunkDecodedSize(
                    bytes.len(),
                    subset.num_elements() * decoded_representation.element_size() as u64,
                )
            })?;
        }
        Ok(encoded_value)
    }
}
