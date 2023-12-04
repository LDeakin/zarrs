use super::{calculate_order_decode, permute, transpose_array, TransposeOrder};
use crate::array::{
    codec::{ArrayPartialDecoderTraits, ArraySubset, CodecError},
    ArrayRepresentation,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncArrayPartialDecoderTraits;

/// Partial decoder for the Transpose codec.
pub struct TransposePartialDecoder<'a> {
    input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
    decoded_representation: ArrayRepresentation,
    order: TransposeOrder,
}

impl<'a> TransposePartialDecoder<'a> {
    /// Create a new partial decoder for the Transpose codec.
    pub fn new(
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: ArrayRepresentation,
        order: TransposeOrder,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            order,
        }
    }
}

impl ArrayPartialDecoderTraits for TransposePartialDecoder<'_> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
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
            .partial_decode_opt(&decoded_regions_transposed, parallel)?;

        // Reverse the transpose on each subset
        let order_decode =
            calculate_order_decode(&self.order, self.decoded_representation.shape().len());
        for (subset, bytes) in std::iter::zip(decoded_regions, &mut encoded_value) {
            transpose_array(
                &order_decode,
                subset.shape(),
                self.decoded_representation.element_size(),
                bytes.as_mut_slice(),
            )
            .map_err(|_| {
                CodecError::UnexpectedChunkDecodedSize(
                    bytes.len(),
                    subset.num_elements() * self.decoded_representation.element_size() as u64,
                )
            })?;
        }
        Ok(encoded_value)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the Transpose codec.
pub struct AsyncTransposePartialDecoder<'a> {
    input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
    decoded_representation: ArrayRepresentation,
    order: TransposeOrder,
}

#[cfg(feature = "async")]
impl<'a> AsyncTransposePartialDecoder<'a> {
    /// Create a new partial decoder for the Transpose codec.
    pub fn new(
        input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: ArrayRepresentation,
        order: TransposeOrder,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            order,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncTransposePartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
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
            .partial_decode_opt(&decoded_regions_transposed, parallel)
            .await?;

        // Reverse the transpose on each subset
        let order_decode =
            calculate_order_decode(&self.order, self.decoded_representation.shape().len());
        for (subset, bytes) in std::iter::zip(decoded_regions, &mut encoded_value) {
            transpose_array(
                &order_decode,
                subset.shape(),
                self.decoded_representation.element_size(),
                bytes.as_mut_slice(),
            )
            .map_err(|_| {
                CodecError::UnexpectedChunkDecodedSize(
                    bytes.len(),
                    subset.num_elements() * self.decoded_representation.element_size() as u64,
                )
            })?;
        }
        Ok(encoded_value)
    }
}
