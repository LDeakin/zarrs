use crate::{
    array::{
        codec::{ArrayPartialDecoderTraits, BytesPartialDecoderTraits, CodecError},
        ArrayRepresentation, BytesRepresentation,
    },
    array_subset::ArraySubset,
    byte_range::extract_byte_ranges,
};

use super::{zfp_decode, ZfpMode};

/// The partial decoder for the Zfp codec.
pub struct ZfpPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    mode: &'a ZfpMode,
}

impl<'a> ZfpPartialDecoder<'a> {
    /// Create a new partial decoder for the Zfp codec.
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>, mode: &'a ZfpMode) -> Self {
        Self { input_handle, mode }
    }
}

impl ArrayPartialDecoderTraits for ZfpPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &ArrayRepresentation,
        decoded_regions: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let encoded_value = self
            .input_handle
            .decode(&BytesRepresentation::UnboundedSize)?; // FIXME: Fixed/bounded?
        let mut out = Vec::with_capacity(decoded_regions.len());
        match encoded_value {
            Some(encoded_value) => {
                let decoded_value = zfp_decode(self.mode, encoded_value, decoded_representation)?;
                for array_subset in decoded_regions {
                    let byte_ranges = unsafe {
                        array_subset.byte_ranges_unchecked(
                            decoded_representation.shape(),
                            decoded_representation.element_size(),
                        )
                    };
                    let bytes = extract_byte_ranges(&decoded_value, &byte_ranges)?;
                    out.push(bytes.concat());
                }
            }
            None => {
                for decoded_region in decoded_regions {
                    out.push(
                        decoded_representation
                            .fill_value()
                            .as_ne_bytes()
                            .repeat(decoded_region.num_elements_usize()),
                    );
                }
            }
        }
        Ok(out)
    }
}
