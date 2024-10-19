use std::sync::Arc;

use crate::{
    array::{array_bytes::update_array_bytes, ChunkRepresentation},
    array_subset::ArraySubset,
};

use super::{
    ArrayToArrayCodecTraits, AsyncArrayPartialDecoderTraits, AsyncArrayPartialEncoderTraits,
};

/// The default asynchronous array (chunk) partial encoder. Decodes the entire chunk, updates it, and writes the entire chunk.
pub struct AsyncArrayToArrayPartialEncoderDefault {
    input_handle: Arc<dyn AsyncArrayPartialDecoderTraits>,
    output_handle: Arc<dyn AsyncArrayPartialEncoderTraits>,
    decoded_representation: ChunkRepresentation,
    codec: Arc<dyn ArrayToArrayCodecTraits>,
}

impl AsyncArrayToArrayPartialEncoderDefault {
    /// Create a new [`AsyncArrayToArrayPartialEncoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn AsyncArrayPartialDecoderTraits>,
        output_handle: Arc<dyn AsyncArrayPartialEncoderTraits>,
        decoded_representation: ChunkRepresentation,
        codec: Arc<dyn ArrayToArrayCodecTraits>,
    ) -> Self {
        Self {
            input_handle,
            output_handle,
            decoded_representation,
            codec,
        }
    }
}

#[async_trait::async_trait]
impl AsyncArrayPartialEncoderTraits for AsyncArrayToArrayPartialEncoderDefault {
    async fn erase(&self) -> Result<(), super::CodecError> {
        self.output_handle.erase().await
    }

    async fn partial_encode(
        &self,
        chunk_subsets: &[&ArraySubset],
        chunk_subsets_bytes: Vec<crate::array::ArrayBytes<'_>>,
        options: &super::CodecOptions,
    ) -> Result<(), super::CodecError> {
        // Read the entire chunk
        let chunk_shape = self.decoded_representation.shape_u64();
        let array_subset_all = ArraySubset::new_with_shape(chunk_shape.clone());
        let encoded_value = self
            .input_handle
            .partial_decode(&[array_subset_all.clone()], options)
            .await?
            .pop()
            .unwrap();
        let mut decoded_value =
            self.codec
                .decode(encoded_value, &self.decoded_representation, options)?;

        // Validate the bytes
        decoded_value.validate(
            self.decoded_representation.num_elements(),
            self.decoded_representation.data_type().size(),
        )?;

        // Update the chunk
        // FIXME: More efficient update for multiple chunk subsets?
        for (chunk_subset, chunk_subset_bytes) in std::iter::zip(chunk_subsets, chunk_subsets_bytes)
        {
            decoded_value = update_array_bytes(
                decoded_value,
                chunk_shape.clone(), // FIXME
                chunk_subset_bytes,
                chunk_subset,
                self.decoded_representation.data_type().size(),
            );
        }

        let is_fill_value = !options.store_empty_chunks()
            && decoded_value.is_fill_value(self.decoded_representation.fill_value());
        if is_fill_value {
            self.output_handle.erase().await
        } else {
            // Store the updated chunk
            let encoded_value =
                self.codec
                    .encode(decoded_value, &self.decoded_representation, options)?;
            self.output_handle.erase().await?; // this is necessary in the absence of a truncation API
            self.output_handle
                .partial_encode(&[&array_subset_all], vec![encoded_value], options)
                .await
        }
    }
}
