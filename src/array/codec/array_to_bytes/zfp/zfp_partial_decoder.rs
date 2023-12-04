use zfp_sys::zfp_type;

use crate::{
    array::{
        codec::{
            ArrayPartialDecoderTraits, AsyncArrayPartialDecoderTraits,
            AsyncBytesPartialDecoderTraits, BytesPartialDecoderTraits, CodecError,
        },
        ArrayRepresentation,
    },
    array_subset::ArraySubset,
    byte_range::extract_byte_ranges,
};

use super::{zarr_data_type_to_zfp_data_type, zfp_decode, ZfpMode};

/// Partial decoder for the `zfp` codec.
pub struct ZfpPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ArrayRepresentation,
    mode: ZfpMode,
    zfp_type: zfp_type,
}

impl<'a> ZfpPartialDecoder<'a> {
    /// Create a new partial decoder for the `zfp` codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        mode: ZfpMode,
    ) -> Result<Self, CodecError> {
        match zarr_data_type_to_zfp_data_type(decoded_representation.data_type()) {
            Some(zfp_type) => Ok(Self {
                input_handle,
                decoded_representation: decoded_representation.clone(),
                mode,
                zfp_type,
            }),
            None => Err(CodecError::from(
                "data type {} is unsupported for zfp codec",
            )),
        }
    }
}

impl ArrayPartialDecoderTraits for ZfpPartialDecoder<'_> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let encoded_value = self.input_handle.decode_opt(parallel)?;
        let mut out = Vec::with_capacity(decoded_regions.len());
        match encoded_value {
            Some(encoded_value) => {
                let decoded_value = zfp_decode(
                    &self.mode,
                    self.zfp_type,
                    encoded_value,
                    &self.decoded_representation,
                    parallel,
                )?;
                for array_subset in decoded_regions {
                    let byte_ranges = unsafe {
                        array_subset.byte_ranges_unchecked(
                            self.decoded_representation.shape(),
                            self.decoded_representation.element_size(),
                        )
                    };
                    let bytes = extract_byte_ranges(&decoded_value, &byte_ranges)?;
                    out.push(bytes.concat());
                }
            }
            None => {
                for decoded_region in decoded_regions {
                    out.push(
                        self.decoded_representation
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

/// Asynchronous partial decoder for the `zfp` codec.
pub struct AsyncZfpPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ArrayRepresentation,
    mode: ZfpMode,
    zfp_type: zfp_type,
}

impl<'a> AsyncZfpPartialDecoder<'a> {
    /// Create a new partial decoder for the `zfp` codec.
    pub fn new(
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        mode: ZfpMode,
    ) -> Result<Self, CodecError> {
        match zarr_data_type_to_zfp_data_type(decoded_representation.data_type()) {
            Some(zfp_type) => Ok(Self {
                input_handle,
                decoded_representation: decoded_representation.clone(),
                mode,
                zfp_type,
            }),
            None => Err(CodecError::from(
                "data type {} is unsupported for zfp codec",
            )),
        }
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl AsyncArrayPartialDecoderTraits for AsyncZfpPartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let encoded_value = self.input_handle.decode_opt(parallel).await?;
        let mut out = Vec::with_capacity(decoded_regions.len());
        match encoded_value {
            Some(encoded_value) => {
                let decoded_value = zfp_decode(
                    &self.mode,
                    self.zfp_type,
                    encoded_value,
                    &self.decoded_representation,
                    parallel,
                )?;
                for array_subset in decoded_regions {
                    let byte_ranges = unsafe {
                        array_subset.byte_ranges_unchecked(
                            self.decoded_representation.shape(),
                            self.decoded_representation.element_size(),
                        )
                    };
                    let bytes = extract_byte_ranges(&decoded_value, &byte_ranges)?;
                    out.push(bytes.concat());
                }
            }
            None => {
                for decoded_region in decoded_regions {
                    out.push(
                        self.decoded_representation
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
