use crate::{
    array::{array_bytes::update_array_bytes, ChunkRepresentation},
    array_subset::ArraySubset,
    byte_range::ByteRange,
};

use super::{
    ArrayPartialDecoderTraits, ArrayPartialEncoderTraits, ArrayToBytesCodecTraits,
    BytesPartialEncoderTraits,
};

/// The default array (chunk) partial encoder. Decodes the entire chunk, updates it, and writes the entire chunk.
pub struct ArrayPartialEncoderDefault<'a> {
    input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
    output_handle: Box<dyn BytesPartialEncoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    codecs: &'a dyn ArrayToBytesCodecTraits,
}

impl<'a> ArrayPartialEncoderDefault<'a> {
    /// Create a new [`ArrayPartialEncoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        output_handle: Box<dyn BytesPartialEncoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        codecs: &'a dyn ArrayToBytesCodecTraits,
    ) -> Self {
        Self {
            input_handle,
            output_handle,
            decoded_representation,
            codecs,
        }
    }
}

impl ArrayPartialEncoderTraits for ArrayPartialEncoderDefault<'_> {
    fn partial_encode_opt(
        &self,
        chunk_subsets: &[crate::array_subset::ArraySubset],
        chunk_subsets_bytes: Vec<crate::array::ArrayBytes<'_>>,
        options: &super::CodecOptions,
    ) -> Result<(), super::CodecError> {
        // Read the entire chunk
        let chunk_shape = self.decoded_representation.shape_u64();
        let subset_all = ArraySubset::new_with_shape(chunk_shape.clone());
        let mut chunk_bytes = self
            .input_handle
            .partial_decode_opt(&[subset_all.clone()], options)?
            .pop()
            .unwrap();
        chunk_bytes.validate(
            self.decoded_representation.num_elements(),
            self.decoded_representation.data_type().size(),
        )?;

        // Update the chunk
        // FIXME: More efficient update for multiple chunk subsets?
        for (chunk_subset, chunk_subset_bytes) in std::iter::zip(chunk_subsets, chunk_subsets_bytes)
        {
            chunk_bytes = update_array_bytes(
                chunk_bytes,
                chunk_shape.clone(), // FIXME
                chunk_subset_bytes,
                chunk_subset,
                self.decoded_representation.data_type().size(),
            );
        }

        let is_fill_value = !options.store_empty_chunks()
            && chunk_bytes.is_fill_value(self.decoded_representation.fill_value());
        if is_fill_value {
            self.output_handle.erase()
        } else {
            // Store the updated chunk
            let chunk_bytes =
                self.codecs
                    .encode(chunk_bytes, &self.decoded_representation, options)?;
            self.output_handle.partial_encode_opt(
                &[ByteRange::FromStart(0, Some(chunk_bytes.len() as u64))],
                vec![chunk_bytes],
                options,
            )
        }
    }
}
