use std::sync::Arc;

use crate::{
    array::{array_bytes::update_array_bytes, ArrayBytes, ArraySize, ChunkRepresentation},
    array_subset::ArraySubset,
};

use super::{
    ArrayPartialEncoderTraits, ArrayToBytesCodecTraits, BytesPartialDecoderTraits,
    BytesPartialEncoderTraits,
};

/// The default array (chunk) partial encoder. Decodes the entire chunk, updates it, and writes the entire chunk.
pub struct ArrayPartialEncoderDefault {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
    output_handle: Arc<dyn BytesPartialEncoderTraits>,
    decoded_representation: ChunkRepresentation,
    codec: Arc<dyn ArrayToBytesCodecTraits>,
}

impl ArrayPartialEncoderDefault {
    /// Create a new [`ArrayPartialEncoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: ChunkRepresentation,
        codec: Arc<dyn ArrayToBytesCodecTraits>,
    ) -> Self {
        Self {
            input_handle,
            output_handle,
            decoded_representation,
            codec,
        }
    }
}

impl ArrayPartialEncoderTraits for ArrayPartialEncoderDefault {
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
        let chunk_bytes = self.input_handle.decode(options)?;

        // Handle a missing chunk
        let mut chunk_bytes = if let Some(chunk_bytes) = chunk_bytes {
            self.codec
                .decode(chunk_bytes, &self.decoded_representation, options)?
        } else {
            let array_size = ArraySize::new(
                self.decoded_representation.data_type().size(),
                self.decoded_representation.num_elements(),
            );
            ArrayBytes::new_fill_value(array_size, self.decoded_representation.fill_value())
        };

        // Validate the bytes
        chunk_bytes.validate(
            self.decoded_representation.num_elements(),
            self.decoded_representation.data_type().size(),
        )?;

        // Update the chunk
        // TODO: More efficient update for multiple chunk subsets?
        for (chunk_subset, chunk_subset_bytes) in subsets_and_bytes {
            chunk_subset_bytes.validate(
                chunk_subset.num_elements(),
                self.decoded_representation.data_type().size(),
            )?;

            chunk_bytes = unsafe {
                update_array_bytes(
                    chunk_bytes,
                    &chunk_shape,
                    chunk_subset,
                    chunk_subset_bytes,
                    self.decoded_representation.data_type().size(),
                )
            };
        }

        let is_fill_value = !options.store_empty_chunks()
            && chunk_bytes.is_fill_value(self.decoded_representation.fill_value());
        if is_fill_value {
            self.output_handle.erase()
        } else {
            // Store the updated chunk
            let chunk_bytes =
                self.codec
                    .encode(chunk_bytes, &self.decoded_representation, options)?;
            self.output_handle
                .partial_encode(&[(0, chunk_bytes)], options)
        }
    }
}
