// TODO: Support actual partial decoding, coalescing required

use std::{num::NonZeroU64, sync::Arc};

use crate::{
    array::{
        array_bytes::extract_decoded_regions_vlen,
        codec::{
            ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError,
            CodecOptions,
        },
        ArrayBytes, ArraySize, ChunkRepresentation, CodecChain, DataType, FillValue, RawBytes,
    },
    metadata::v3::codec::vlen::VlenIndexDataType,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

/// Partial decoder for the `bytes` codec.
pub struct VlenPartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    index_codecs: &'a CodecChain,
    data_codecs: &'a CodecChain,
    index_data_type: VlenIndexDataType,
}

impl<'a> VlenPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        index_codecs: &'a CodecChain,
        data_codecs: &'a CodecChain,
        index_data_type: VlenIndexDataType,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            index_codecs,
            data_codecs,
            index_data_type,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_vlen_bytes<'a>(
    index_codecs: &CodecChain,
    data_codecs: &CodecChain,
    index_data_type: VlenIndexDataType,
    bytes: Option<RawBytes>,
    decoded_regions: &[ArraySubset],
    fill_value: &FillValue,
    shape: &[u64],
    options: &CodecOptions,
) -> Result<Vec<ArrayBytes<'a>>, CodecError> {
    if let Some(bytes) = bytes {
        let num_elements = usize::try_from(shape.iter().product::<u64>()).unwrap();
        let index_shape = vec![unsafe { NonZeroU64::new_unchecked(1 + num_elements as u64) }];
        let index_chunk_representation = match index_data_type {
            // VlenIndexDataType::UInt8 => {
            //     ChunkRepresentation::new(index_shape, DataType::UInt8, FillValue::from(0u8))
            // }
            // VlenIndexDataType::UInt16 => {
            //     ChunkRepresentation::new(index_shape, DataType::UInt16, FillValue::from(0u16))
            // }
            VlenIndexDataType::UInt32 => {
                ChunkRepresentation::new(index_shape, DataType::UInt32, FillValue::from(0u32))
            }
            VlenIndexDataType::UInt64 => {
                ChunkRepresentation::new(index_shape, DataType::UInt64, FillValue::from(0u64))
            }
        }
        .expect("all data types/fill values are compatible");
        let (data, index) = super::get_vlen_bytes_and_offsets(
            &index_chunk_representation,
            &bytes,
            index_codecs,
            data_codecs,
            options,
        )?;
        extract_decoded_regions_vlen(&data, &index, decoded_regions, shape)
    } else {
        // Chunk is empty, all decoded regions are empty
        let mut output = Vec::with_capacity(decoded_regions.len());
        for decoded_region in decoded_regions {
            let array_size = ArraySize::Variable {
                num_elements: decoded_region.num_elements(),
            };
            output.push(ArrayBytes::new_fill_value(array_size, fill_value));
        }
        Ok(output)
    }
}

impl ArrayPartialDecoderTraits for VlenPartialDecoder<'_> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        // Get all of the input bytes (cached due to CodecTraits::partial_decoder_decodes_all() == true)
        let bytes = self.input_handle.decode(options)?;
        decode_vlen_bytes(
            self.index_codecs,
            self.data_codecs,
            self.index_data_type,
            bytes,
            decoded_regions,
            self.decoded_representation.fill_value(),
            &self.decoded_representation.shape_u64(),
            options,
        )
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `bytes` codec.
pub struct AsyncVlenPartialDecoder<'a> {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    index_codecs: &'a CodecChain,
    data_codecs: &'a CodecChain,
    index_data_type: VlenIndexDataType,
}

#[cfg(feature = "async")]
impl<'a> AsyncVlenPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        index_codecs: &'a CodecChain,
        data_codecs: &'a CodecChain,
        index_data_type: VlenIndexDataType,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            index_codecs,
            data_codecs,
            index_data_type,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncVlenPartialDecoder<'_> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        // Get all of the input bytes (cached due to CodecTraits::partial_decoder_decodes_all() == true)
        let bytes = self.input_handle.decode(options).await?;
        decode_vlen_bytes(
            self.index_codecs,
            self.data_codecs,
            self.index_data_type,
            bytes,
            decoded_regions,
            self.decoded_representation.fill_value(),
            &self.decoded_representation.shape_u64(),
            options,
        )
    }
}
