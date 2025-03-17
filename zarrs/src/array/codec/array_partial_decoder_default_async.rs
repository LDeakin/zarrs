use std::{num::NonZero, sync::Arc};

use zarrs_metadata::DataTypeSize;

use crate::{
    array::{ArrayBytes, ChunkRepresentation, RawBytesOffsets},
    array_subset::ArraySubset,
};

use super::{ArrayToArrayCodecTraits, AsyncArrayPartialDecoderTraits};

/// The default array to array partial decoder. Applies a codec to the regions of interest.
/// This cannot be applied on a codec reorganises elements (e.g. transpose).
pub struct AsyncArrayPartialDecoderDefault {
    input_handle: Arc<dyn AsyncArrayPartialDecoderTraits>,
    decoded_representation: ChunkRepresentation,
    codec: Arc<dyn ArrayToArrayCodecTraits>,
}

impl AsyncArrayPartialDecoderDefault {
    /// Create a new [`AsyncArrayPartialDecoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn AsyncArrayPartialDecoderTraits>,
        decoded_representation: ChunkRepresentation,
        codec: Arc<dyn ArrayToArrayCodecTraits>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            codec,
        }
    }
}

#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncArrayPartialDecoderDefault {
    fn data_type(&self) -> &super::DataType {
        self.decoded_representation.data_type()
    }

    async fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
        options: &super::CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, super::CodecError> {
        // Read the subsets
        let chunk_bytes = self
            .input_handle
            .partial_decode(array_subsets, options)
            .await?;

        // Decode the subsets
        chunk_bytes
            .into_iter()
            .zip(array_subsets)
            .map(|(bytes, subset)| {
                if let Ok(shape) = subset
                    .shape()
                    .iter()
                    .map(|f| NonZero::try_from(*f))
                    .collect()
                {
                    self.codec.decode(
                        bytes,
                        &ChunkRepresentation::new(
                            shape,
                            self.decoded_representation.data_type().clone(),
                            self.decoded_representation.fill_value().clone(),
                        )
                        .expect("data type and fill value are compatible"),
                        options,
                    )
                } else {
                    Ok(match self.decoded_representation.data_type().size() {
                        DataTypeSize::Fixed(_) => ArrayBytes::new_flen(vec![]),
                        DataTypeSize::Variable => {
                            ArrayBytes::new_vlen(vec![], RawBytesOffsets::new(vec![0]).unwrap())
                                .unwrap()
                        }
                    })
                }
            })
            .collect()
    }
}
