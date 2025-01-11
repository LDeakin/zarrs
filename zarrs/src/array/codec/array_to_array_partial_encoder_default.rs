use std::sync::Arc;

use crate::{
    array::{array_bytes::update_array_bytes, ArrayBytes, ChunkRepresentation},
    array_subset::ArraySubset,
    indexer::IncompatibleIndexerAndShapeError,
};

use super::{
    ArrayPartialDecoderTraits, ArrayPartialEncoderTraits, ArrayToArrayCodecTraits, CodecError,
};

/// The default array (chunk) partial encoder. Decodes the entire chunk, updates it, and writes the entire chunk.
pub struct ArrayToArrayPartialEncoderDefault {
    input_handle: Arc<dyn ArrayPartialDecoderTraits>,
    output_handle: Arc<dyn ArrayPartialEncoderTraits>,
    decoded_representation: ChunkRepresentation,
    codec: Arc<dyn ArrayToArrayCodecTraits>,
}

impl ArrayToArrayPartialEncoderDefault {
    /// Create a new [`ArrayToArrayPartialEncoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn ArrayPartialDecoderTraits>,
        output_handle: Arc<dyn ArrayPartialEncoderTraits>,
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

impl ArrayPartialEncoderTraits for ArrayToArrayPartialEncoderDefault {
    fn erase(&self) -> Result<(), super::CodecError> {
        self.output_handle.erase()
    }

    fn partial_encode(
        &self,
        subsets_and_bytes: &[(&ArraySubset, ArrayBytes<'_>)],
        options: &super::CodecOptions,
    ) -> Result<(), super::CodecError> {
        // Read the entire chunk
        let chunk_shape = self.decoded_representation.shape_u64();
        let array_subset_all = ArraySubset::new_with_shape(chunk_shape.clone());
        let encoded_value = self
            .input_handle
            .partial_decode(&[array_subset_all.clone()], options)?
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
        // TODO: More efficient update for multiple chunk subsets?
        for (chunk_subset, chunk_subset_bytes) in subsets_and_bytes {
            // Check the subset is within the chunk shape
            if chunk_subset
                .end_exc()
                .iter()
                .zip(self.decoded_representation.shape())
                .any(|(a, b)| *a > b.get())
            {
                return Err(CodecError::InvalidIndexerError(
                    IncompatibleIndexerAndShapeError::new(
                        (*chunk_subset).clone(),
                        self.decoded_representation.shape_u64(),
                    ),
                ));
            }

            chunk_subset_bytes.validate(
                chunk_subset.num_elements(),
                self.decoded_representation.data_type().size(),
            )?;

            decoded_value = unsafe {
                update_array_bytes(
                    decoded_value,
                    &chunk_shape,
                    chunk_subset,
                    chunk_subset_bytes,
                    self.decoded_representation.data_type().size(),
                )
            };
        }

        let is_fill_value = !options.store_empty_chunks()
            && decoded_value.is_fill_value(self.decoded_representation.fill_value());
        if is_fill_value {
            self.output_handle.erase()
        } else {
            // Store the updated chunk
            let encoded_value =
                self.codec
                    .encode(decoded_value, &self.decoded_representation, options)?;
            self.output_handle
                .partial_encode(&[(&array_subset_all, encoded_value)], options)
        }
    }
}
