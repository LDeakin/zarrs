use zarrs_metadata::codec::SHARDING;

use super::{codec::ShardingCodecConfiguration, Array, ArrayShape, ChunkGrid, ChunkShape};

/// An [`Array`] extension trait to simplify working with arrays using the `sharding_indexed` codec.
pub trait ArrayShardedExt: private::Sealed {
    /// Returns true if the array to bytes codec of the array is `sharding_indexed`.
    fn is_sharded(&self) -> bool;

    /// Returns true if the array-to-bytes codec of the array is `sharding_indexed` and the array has no array-to-array or bytes-to-bytes codecs.
    fn is_exclusively_sharded(&self) -> bool;

    /// Return the inner chunk shape as defined in the `sharding_indexed` codec metadata.
    ///
    /// Returns [`None`] for an unsharded array.
    fn inner_chunk_shape(&self) -> Option<ChunkShape>;

    /// The effective inner chunk shape.
    ///
    /// The effective inner chunk shape is the "read granularity" of the sharded array that accounts for array-to-array codecs preceding the sharding codec.
    /// For example, the transpose codec changes the shape of an array subset that corresponds to a single inner chunk.
    /// The effective inner chunk shape is used when determining the inner chunk grid of a sharded array.
    fn effective_inner_chunk_shape(&self) -> Option<ChunkShape>;

    /// Retrieve the inner chunk grid.
    ///
    /// This uses the effective inner shape so that reading an inner chunk reads only one contiguous byte range.
    ///
    /// Returns the normal chunk grid for an unsharded array.
    fn inner_chunk_grid(&self) -> ChunkGrid;

    /// Return the shape of the inner chunk grid (i.e., the number of inner chunks).
    ///
    /// Returns the normal chunk grid shape for an unsharded array.
    fn inner_chunk_grid_shape(&self) -> Option<ArrayShape>;
}

impl<TStorage: ?Sized> ArrayShardedExt for Array<TStorage> {
    fn is_sharded(&self) -> bool {
        self.codecs.array_to_bytes_codec().name() == SHARDING
    }

    fn is_exclusively_sharded(&self) -> bool {
        self.is_sharded()
            && self.codecs.array_to_array_codecs().is_empty()
            && self.codecs.bytes_to_bytes_codecs().is_empty()
    }

    fn inner_chunk_shape(&self) -> Option<ChunkShape> {
        let configuration = self
            .codecs
            .array_to_bytes_codec()
            .configuration()
            .expect("the array to bytes codec should have metadata");
        if let Ok(ShardingCodecConfiguration::V1(sharding_configuration)) =
            ShardingCodecConfiguration::try_from(configuration)
        {
            Some(sharding_configuration.chunk_shape)
        } else {
            None
        }
    }

    fn effective_inner_chunk_shape(&self) -> Option<ChunkShape> {
        let inner_chunk_shape = self.inner_chunk_shape();
        if let Some(mut inner_chunk_shape) = inner_chunk_shape {
            for codec in self.codecs().array_to_array_codecs().iter().rev() {
                inner_chunk_shape = codec
                    .decoded_shape(&inner_chunk_shape)
                    .expect("the inner chunk shape is compatible");
            }
            Some(inner_chunk_shape)
        } else {
            None
        }
    }

    fn inner_chunk_grid(&self) -> ChunkGrid {
        if let Some(inner_chunk_shape) = self.effective_inner_chunk_shape() {
            ChunkGrid::new(crate::array::chunk_grid::RegularChunkGrid::new(
                inner_chunk_shape,
            ))
        } else {
            self.chunk_grid().clone()
        }
    }

    fn inner_chunk_grid_shape(&self) -> Option<ArrayShape> {
        unsafe {
            // SAFETY: The inner chunk grid dimensionality is validated against the array shape on creation
            self.inner_chunk_grid().grid_shape_unchecked(self.shape())
        }
    }
}

mod private {
    use super::Array;

    pub trait Sealed {}

    impl<TStorage: ?Sized> Sealed for Array<TStorage> {}
}
