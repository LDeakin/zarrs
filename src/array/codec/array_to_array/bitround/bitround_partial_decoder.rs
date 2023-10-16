use crate::{
    array::{
        codec::{ArrayPartialDecoderTraits, CodecError},
        ArrayRepresentation,
    },
    array_subset::ArraySubset,
};

use super::round_bytes;

/// The partial decoder for the Bitround codec.
pub struct BitroundPartialDecoder<'a> {
    input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
    keepbits: u32,
}

impl<'a> BitroundPartialDecoder<'a> {
    /// Create a new partial decoder for the Bitround codec.
    pub fn new(input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>, keepbits: u32) -> Self {
        Self {
            input_handle,
            keepbits,
        }
    }
}

impl ArrayPartialDecoderTraits for BitroundPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &ArrayRepresentation,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut bytes = self
            .input_handle
            .partial_decode(decoded_representation, array_subsets)?;

        for bytes in &mut bytes {
            round_bytes(bytes, decoded_representation.data_type(), self.keepbits)?;
        }

        Ok(bytes)
    }
}
