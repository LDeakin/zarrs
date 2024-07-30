//! Zarr chunk grids. Includes a [regular grid](RegularChunkGrid) implementation.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#chunk-grids>.
//!
//! A [`ChunkGrid`] is a [`Box`] wrapped chunk grid which implements [`ChunkGridTraits`].
//! Chunk grids are Zarr extension points and they can be registered through [`inventory`] as a [`ChunkGridPlugin`].
//!
//! Includes a [`RegularChunkGrid`] and [`RectangularChunkGrid`] implementation.
//!
//! A regular chunk grid can be created from a [`ChunkShape`] and similar. See its [`from`/`try_from` implementations](./struct.ChunkGrid.html#trait-implementations).

pub mod rectangular;
pub mod regular;

use std::num::NonZeroU64;

pub use crate::metadata::v3::chunk_grid::rectangular::{
    RectangularChunkGridConfiguration, RectangularChunkGridDimensionConfiguration,
};
pub use crate::metadata::v3::chunk_grid::regular::RegularChunkGridConfiguration;

pub use rectangular::RectangularChunkGrid;
pub use regular::RegularChunkGrid;

use derive_more::{Deref, From};

use crate::{
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
    metadata::v3::MetadataV3,
    plugin::{Plugin, PluginCreateError},
};

use super::{ArrayIndices, ArrayShape, ChunkShape};

/// A chunk grid.
#[derive(Debug, Clone, Deref, From)]
pub struct ChunkGrid(Box<dyn ChunkGridTraits>);

/// A chunk grid plugin.
pub type ChunkGridPlugin = Plugin<ChunkGrid>;
inventory::collect!(ChunkGridPlugin);

impl ChunkGrid {
    /// Create a chunk grid.
    pub fn new<T: ChunkGridTraits + 'static>(chunk_grid: T) -> Self {
        let chunk_grid: Box<dyn ChunkGridTraits> = Box::new(chunk_grid);
        chunk_grid.into()
    }

    /// Create a chunk grid from metadata.
    ///
    /// # Errors
    ///
    /// Returns a [`PluginCreateError`] if the metadata is invalid or not associated with a registered chunk grid plugin.
    pub fn from_metadata(metadata: &MetadataV3) -> Result<Self, PluginCreateError> {
        for plugin in inventory::iter::<ChunkGridPlugin> {
            if plugin.match_name(metadata.name()) {
                return plugin.create(metadata);
            }
        }
        #[cfg(miri)]
        {
            // Inventory does not work in miri, so manually handle all known chunk grids
            match metadata.name() {
                regular::IDENTIFIER => {
                    return regular::create_chunk_grid_regular(metadata);
                }
                rectangular::IDENTIFIER => {
                    return rectangular::create_chunk_grid_rectangular(metadata);
                }
                _ => {}
            }
        }
        Err(PluginCreateError::Unsupported {
            name: metadata.name().to_string(),
            plugin_type: "chunk grid".to_string(),
        })
    }
}

macro_rules! from_chunkgrid_regular {
    ( $t:ty ) => {
        impl From<$t> for ChunkGrid {
            /// Create a regular chunk grid from a chunk shape.
            fn from(regular_chunk_shape: $t) -> Self {
                Self::new(RegularChunkGrid::new(regular_chunk_shape.into()))
            }
        }
    };
    ( $t:ty, $g:ident ) => {
        impl<const $g: usize> From<$t> for ChunkGrid {
            /// Create a regular chunk grid from a chunk shape.
            fn from(regular_chunk_shape: $t) -> Self {
                Self::new(RegularChunkGrid::new(regular_chunk_shape.into()))
            }
        }
    };
}

from_chunkgrid_regular!(&[NonZeroU64]);
from_chunkgrid_regular!(Vec<NonZeroU64>);
from_chunkgrid_regular!([NonZeroU64; N], N);
from_chunkgrid_regular!(&[NonZeroU64; N], N);

impl From<ChunkShape> for ChunkGrid {
    /// Create a regular chunk grid from a chunk shape.
    fn from(regular_chunk_shape: ChunkShape) -> Self {
        Self::new(RegularChunkGrid::new(regular_chunk_shape))
    }
}

impl TryFrom<ArrayShape> for ChunkGrid {
    type Error = PluginCreateError;
    /// Create a regular chunk grid from a chunk shape.
    fn try_from(regular_chunk_shape: ArrayShape) -> Result<Self, PluginCreateError> {
        let regular_chunk_shape = regular_chunk_shape
            .into_iter()
            .map(|i| {
                NonZeroU64::new(i)
                    .ok_or_else(|| PluginCreateError::from("chunk shape elements must be non-zero"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into();
        Ok(Self::new(RegularChunkGrid::new(regular_chunk_shape)))
    }
}

/// Chunk grid traits.
pub trait ChunkGridTraits: dyn_clone::DynClone + core::fmt::Debug + Send + Sync {
    /// Create metadata.
    fn create_metadata(&self) -> MetadataV3;

    /// The dimensionality of the grid.
    fn dimensionality(&self) -> usize;

    /// The grid shape (i.e. number of chunks).
    ///
    /// Zero sized array dimensions are considered "unlimited".
    /// The grid shape will be unlimited where the array shape is unlimited, if supported by the chunk grid.
    /// Returns [`None`] if the grid shape cannot be determined, likely due to an incompatibility with the `array_shape`.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if the length of `array_shape` does not match the dimensionality of the chunk grid.
    fn grid_shape(
        &self,
        array_shape: &[u64],
    ) -> Result<Option<ArrayShape>, IncompatibleDimensionalityError> {
        if array_shape.len() == self.dimensionality() {
            Ok(unsafe { self.grid_shape_unchecked(array_shape) })
        } else {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        }
    }

    /// The shape of the chunk at `chunk_indices`.
    ///
    /// Returns [`None`] if the shape of the chunk at `chunk_indices` cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if `chunk_indices` or `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_shape(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<Option<ChunkShape>, IncompatibleDimensionalityError> {
        if chunk_indices.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                chunk_indices.len(),
                self.dimensionality(),
            ))
        } else if array_shape.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        } else {
            Ok(unsafe { self.chunk_shape_unchecked(chunk_indices, array_shape) })
        }
    }

    /// The shape of the chunk at `chunk_indices` as an [`ArrayShape`] ([`Vec<u64>`]).
    ///
    /// Returns [`None`] if the shape of the chunk at `chunk_indices` cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if `chunk_indices` or `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_shape_u64(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<Option<ArrayShape>, IncompatibleDimensionalityError> {
        if chunk_indices.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                chunk_indices.len(),
                self.dimensionality(),
            ))
        } else if array_shape.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        } else {
            Ok(unsafe { self.chunk_shape_u64_unchecked(chunk_indices, array_shape) })
        }
    }

    /// The origin of the chunk at `chunk_indices`.
    ///
    /// Returns [`None`] if the chunk origin cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if the length of `chunk_indices` or `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_origin(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<Option<ArrayIndices>, IncompatibleDimensionalityError> {
        if chunk_indices.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                chunk_indices.len(),
                self.dimensionality(),
            ))
        } else if array_shape.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        } else {
            Ok(unsafe { self.chunk_origin_unchecked(chunk_indices, array_shape) })
        }
    }

    /// Return the [`ArraySubset`] of the chunk at `chunk_indices`.
    ///
    /// Returns [`None`] if the chunk subset cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if `chunk_indices` or `array_shape` do not match the dimensionality of the chunk grid.
    fn subset(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<Option<ArraySubset>, IncompatibleDimensionalityError> {
        if chunk_indices.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                chunk_indices.len(),
                self.dimensionality(),
            ))
        } else if array_shape.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        } else {
            Ok(unsafe { self.subset_unchecked(chunk_indices, array_shape) })
        }
    }

    /// Return the [`ArraySubset`] of the chunks in `chunks`.
    ///
    /// Returns [`None`] if the chunk subset cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if `chunks` or `array_shape` do not match the dimensionality of the chunk grid.
    fn chunks_subset(
        &self,
        chunks: &ArraySubset,
        array_shape: &[u64],
    ) -> Result<Option<ArraySubset>, IncompatibleDimensionalityError> {
        if chunks.dimensionality() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                chunks.dimensionality(),
                self.dimensionality(),
            ))
        } else if array_shape.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        } else if let Some(end) = chunks.end_inc() {
            let start = chunks.start();
            let chunk0 = self.subset(start, array_shape)?;
            let chunk1 = self.subset(&end, array_shape)?;
            if let (Some(chunk0), Some(chunk1)) = (chunk0, chunk1) {
                let start = chunk0.start();
                let end = chunk1.end_exc();
                Ok(Some(unsafe {
                    ArraySubset::new_with_start_end_exc_unchecked(start.to_vec(), end)
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(Some(ArraySubset::new_empty(chunks.dimensionality())))
        }
    }

    /// The indices of a chunk which has the element at `array_indices`.
    ///
    /// Returns [`None`] if the chunk indices cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if `array_indices` or `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_indices(
        &self,
        array_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<Option<ArrayIndices>, IncompatibleDimensionalityError> {
        if array_indices.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_indices.len(),
                self.dimensionality(),
            ))
        } else if array_shape.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        } else {
            Ok(unsafe { self.chunk_indices_unchecked(array_indices, array_shape) })
        }
    }

    /// The indices within the chunk of the element at `array_indices`.
    ///
    /// Returns [`None`] if the chunk element indices cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if `array_indices` or `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_element_indices(
        &self,
        array_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<Option<ArrayIndices>, IncompatibleDimensionalityError> {
        if array_indices.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_indices.len(),
                self.dimensionality(),
            ))
        } else if array_shape.len() != self.dimensionality() {
            Err(IncompatibleDimensionalityError::new(
                array_shape.len(),
                self.dimensionality(),
            ))
        } else {
            Ok(unsafe { self.chunk_element_indices_unchecked(array_indices, array_shape) })
        }
    }

    /// Check if array indices are in-bounds.
    ///
    /// Ensures array indices are within the array shape.
    /// Zero sized array dimensions are considered "unlimited" and always in-bounds.
    #[must_use]
    fn array_indices_inbounds(&self, array_indices: &[u64], array_shape: &[u64]) -> bool {
        array_indices.len() == self.dimensionality()
            && array_shape.len() == self.dimensionality()
            && std::iter::zip(array_indices, array_shape)
                .all(|(&index, &shape)| shape == 0 || index < shape)
    }

    /// Check if chunk indices are in-bounds.
    ///
    /// Ensures chunk grid indices are within the chunk grid shape.
    /// Zero sized array dimensions are considered "unlimited" and always in-bounds.
    #[must_use]
    fn chunk_indices_inbounds(&self, chunk_indices: &[u64], array_shape: &[u64]) -> bool {
        chunk_indices.len() == self.dimensionality()
            && array_shape.len() == self.dimensionality()
            && self.grid_shape(array_shape).is_ok_and(|chunk_grid_shape| {
                chunk_grid_shape.map_or(false, |chunk_grid_shape| {
                    std::iter::zip(chunk_indices, chunk_grid_shape)
                        .all(|(index, shape)| shape == 0 || *index < shape)
                })
            })
    }

    /// See [`ChunkGridTraits::grid_shape`].
    ///
    /// # Safety
    /// The length of `array_shape` must match the dimensionality of the chunk grid.
    unsafe fn grid_shape_unchecked(&self, array_shape: &[u64]) -> Option<ArrayShape>;

    /// See [`ChunkGridTraits::chunk_origin`].
    ///
    /// # Safety
    /// The length of `chunk_indices` must match the dimensionality of the chunk grid.
    unsafe fn chunk_origin_unchecked(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Option<ArrayIndices>;

    /// See [`ChunkGridTraits::chunk_shape`].
    ///
    /// # Safety
    /// The length of `chunk_indices` must match the dimensionality of the chunk grid.
    unsafe fn chunk_shape_unchecked(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Option<ChunkShape>;

    /// See [`ChunkGridTraits::chunk_shape_u64`].
    ///
    /// # Safety
    /// The length of `chunk_indices` must match the dimensionality of the chunk grid.
    unsafe fn chunk_shape_u64_unchecked(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Option<ArrayShape>;

    /// See [`ChunkGridTraits::chunk_indices`].
    ///
    /// # Safety
    /// The length of `array_indices` must match the dimensionality of the chunk grid.
    unsafe fn chunk_indices_unchecked(
        &self,
        array_indices: &[u64],
        array_shape: &[u64],
    ) -> Option<ArrayIndices>;

    /// See [`ChunkGridTraits::chunk_element_indices`].
    ///
    /// # Safety
    /// The length of `array_indices` must match the dimensionality of the chunk grid.
    unsafe fn chunk_element_indices_unchecked(
        &self,
        array_indices: &[u64],
        array_shape: &[u64],
    ) -> Option<ArrayIndices>;

    /// See [`ChunkGridTraits::subset`].
    ///
    /// # Safety
    /// The length of `chunk_indices` must match the dimensionality of the chunk grid.
    unsafe fn subset_unchecked(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Option<ArraySubset> {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        if let (Some(chunk_origin), Some(chunk_shape)) = (
            self.chunk_origin_unchecked(chunk_indices, array_shape),
            self.chunk_shape_u64_unchecked(chunk_indices, array_shape),
        ) {
            Some(ArraySubset::new_with_start_shape_unchecked(
                chunk_origin,
                chunk_shape,
            ))
        } else {
            None
        }
    }

    /// Return an array subset indicating the chunks intersecting `array_subset`.
    ///
    /// Returns [`None`] if the intersecting chunks cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if the array subset has an incorrect dimensionality.
    fn chunks_in_array_subset(
        &self,
        array_subset: &ArraySubset,
        array_shape: &[u64],
    ) -> Result<Option<ArraySubset>, IncompatibleDimensionalityError> {
        match array_subset.end_inc() {
            Some(end) => {
                let chunks_start = self.chunk_indices(array_subset.start(), array_shape)?;
                let chunks_end = self
                    .chunk_indices(&end, array_shape)?
                    .map_or_else(|| unsafe { self.grid_shape_unchecked(array_shape) }, Some);

                Ok(
                    if let (Some(chunks_start), Some(chunks_end)) = (chunks_start, chunks_end) {
                        Some(unsafe {
                            ArraySubset::new_with_start_end_inc_unchecked(chunks_start, chunks_end)
                        })
                    } else {
                        None
                    },
                )
            }
            None => Ok(Some(ArraySubset::new_empty(self.dimensionality()))),
        }
    }
}

dyn_clone::clone_trait_object!(ChunkGridTraits);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_grid_configuration_regular() {
        let json = r#"
    {
        "name": "regular",
        "configuration": {
            "chunk_shape": [5, 20, 400]
        }
    }"#;
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        ChunkGrid::from_metadata(&metadata).unwrap();
    }

    #[test]
    fn chunk_grid_configuration_rectangular() {
        let json = r#"
    {
        "name": "rectangular",
        "configuration": {
            "chunk_shape": [[5, 5, 5, 15, 15, 20, 35], 10]
        }
    }"#;
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        ChunkGrid::from_metadata(&metadata).unwrap();
    }
}
