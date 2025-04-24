use std::sync::Arc;

use crate::{
    array::{ArrayBytes, ArraySize, ChunkRepresentation},
    array_subset::ArraySubset,
};

use super::{
    ArrayPartialDecoderTraits, ArrayToBytesCodecTraits, BytesPartialDecoderTraits, CodecError,
    CodecOptions,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

#[cfg_attr(feature = "async", async_generic::async_generic(
    async_signature(
    input_handle: &Arc<dyn AsyncBytesPartialDecoderTraits>,
    decoded_representation: &ChunkRepresentation,
    codec: &Arc<dyn ArrayToBytesCodecTraits>,
    array_subsets: &[ArraySubset],
    options: &CodecOptions,
)))]
fn partial_decode<'a>(
    input_handle: &Arc<dyn BytesPartialDecoderTraits>,
    decoded_representation: &ChunkRepresentation,
    codec: &Arc<dyn ArrayToBytesCodecTraits>,
    array_subsets: &[ArraySubset],
    options: &CodecOptions,
) -> Result<Vec<ArrayBytes<'a>>, CodecError> {
    // Read the entire chunk
    #[cfg(feature = "async")]
    let bytes_enc = if _async {
        input_handle.decode(options).await
    } else {
        input_handle.decode(options)
    }?;
    #[cfg(not(feature = "async"))]
    let bytes_enc = input_handle.decode(options)?;

    if let Some(bytes_enc) = bytes_enc {
        // Decode the entire chunk
        let bytes_dec = codec.decode(bytes_enc, decoded_representation, options)?;

        // Decode the subsets
        let chunk_shape = decoded_representation.shape_u64();
        array_subsets
            .iter()
            .map(|subset| {
                bytes_dec
                    .extract_array_subset(subset, &chunk_shape, decoded_representation.data_type())
                    .map(ArrayBytes::into_owned)
            })
            .collect()
    } else {
        Ok(array_subsets
            .iter()
            .map(|subset| {
                let array_size = ArraySize::new(
                    decoded_representation.data_type().size(),
                    subset.num_elements(),
                );
                ArrayBytes::new_fill_value(array_size, decoded_representation.fill_value())
            })
            .collect())
    }
}

/// The default array to bytes partial decoder. Decodes the entire chunk, and decodes the regions of interest.
pub struct ArrayToBytesPartialDecoderDefault {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
    decoded_representation: ChunkRepresentation,
    codec: Arc<dyn ArrayToBytesCodecTraits>,
}

impl ArrayToBytesPartialDecoderDefault {
    /// Create a new [`ArrayToBytesPartialDecoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: ChunkRepresentation,
        codec: Arc<dyn ArrayToBytesCodecTraits>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            codec,
        }
    }
}

impl ArrayPartialDecoderTraits for ArrayToBytesPartialDecoderDefault {
    fn data_type(&self) -> &super::DataType {
        self.decoded_representation.data_type()
    }

    fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
        options: &super::CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, super::CodecError> {
        partial_decode(
            &self.input_handle,
            &self.decoded_representation,
            &self.codec,
            array_subsets,
            options,
        )
    }
}

#[cfg(feature = "async")]
/// The default asynchronous array to bytes partial decoder. Decodes the entire chunk, and decodes the regions of interest.
pub struct AsyncArrayToBytesPartialDecoderDefault {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
    decoded_representation: ChunkRepresentation,
    codec: Arc<dyn ArrayToBytesCodecTraits>,
}

#[cfg(feature = "async")]
impl AsyncArrayToBytesPartialDecoderDefault {
    /// Create a new [`AsyncArrayToBytesPartialDecoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: ChunkRepresentation,
        codec: Arc<dyn ArrayToBytesCodecTraits>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            codec,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncArrayToBytesPartialDecoderDefault {
    fn data_type(&self) -> &super::DataType {
        self.decoded_representation.data_type()
    }

    async fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
        options: &super::CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, super::CodecError> {
        partial_decode_async(
            &self.input_handle,
            &self.decoded_representation,
            &self.codec,
            array_subsets,
            options,
        )
        .await
    }
}
