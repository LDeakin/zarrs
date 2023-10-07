//! Zarr chunk grids. Includes a [regular grid](RegularChunkGrid) implementation.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#chunk-grids>.
//!
//! A [`ChunkGrid`] is a [`Box`] wrapped chunk grid which implements [`ChunkGridTraits`].
//! Chunk grids are zarr extension points and they can be registered through [`inventory`] as a [`ChunkGridPlugin`].
//!
//! Includes a [`RegularChunkGrid`] and [`RectangularChunkGrid`] implementation.
//!
//! A regular chunk grid can be created [from a `Vec<usize>` chunk shape](./type.ChunkGrid.html#method.from).

mod rectangular;
mod regular;

pub use rectangular::{RectangularChunkGrid, RectangularChunkGridConfiguration};
pub use regular::{RegularChunkGrid, RegularChunkGridConfiguration};

use derive_more::{Deref, From};
use thiserror::Error;

use crate::{
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
    metadata::Metadata,
    plugin::{Plugin, PluginCreateError},
};

use super::{ArrayIndices, ArrayShape};

/// A chunk grid.
#[derive(Debug, Clone, Deref, From)]
pub struct ChunkGrid(Box<dyn ChunkGridTraits>);

/// A chunk grid plugin.
pub type ChunkGridPlugin = Plugin<ChunkGrid>;
inventory::collect!(ChunkGridPlugin);

impl ChunkGrid {
    /// Create a chunk key encoding.
    pub fn new<T: ChunkGridTraits + 'static>(chunk_grid: T) -> Self {
        let chunk_grid: Box<dyn ChunkGridTraits> = Box::new(chunk_grid);
        chunk_grid.into()
    }

    /// Create a chunk grid from metadata.
    ///
    /// # Errors
    ///
    /// Returns a [`PluginCreateError`] if the metadata is invalid or not associated with a registered chunk grid plugin.
    pub fn from_metadata(metadata: &Metadata) -> Result<ChunkGrid, PluginCreateError> {
        for plugin in inventory::iter::<ChunkGridPlugin> {
            if plugin.match_name(metadata.name()) {
                return plugin.create(metadata);
            }
        }
        Err(PluginCreateError::Unsupported {
            name: metadata.name().to_string(),
        })
    }
}

impl From<ArrayShape> for ChunkGrid {
    /// Create a regular chunk grid from a chunk shape.
    fn from(regular_chunk_shape: ArrayShape) -> Self {
        ChunkGrid::new(RegularChunkGrid::new(regular_chunk_shape))
    }
}

/// Chunk grid traits.
pub trait ChunkGridTraits: dyn_clone::DynClone + core::fmt::Debug + Send + Sync {
    /// Create metadata.
    fn create_metadata(&self) -> Metadata;

    /// The dimensonality of the grid.
    fn dimensionality(&self) -> usize;

    /// The grid shape (i.e. number of chunks).
    ///
    /// # Errors
    ///
    /// Returns an error if the length of `array_shape` does not match the dimensionality of the chunk grid.
    /// An implementation may return an error if
    fn grid_shape(&self, array_shape: &[u64]) -> Result<ArrayShape, ChunkGridShapeError>;

    /// The shape of the chunk at `chunk_indices`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidChunkGridIndicesError`] if the either the length of `chunk_indices` or the `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_shape(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<ArrayShape, InvalidChunkGridIndicesError> {
        if self.validate_chunk_indices(chunk_indices, array_shape) {
            Ok(unsafe { self.chunk_shape_unchecked(chunk_indices) })
        } else {
            Err(InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
                array_shape.to_vec(),
            ))
        }
    }

    /// The shape of the chunk at `chunk_indices`.
    ///
    /// # Safety
    ///
    /// The length of `chunk_indices` must match the dimensionality of the chunk grid.
    #[doc(hidden)]
    unsafe fn chunk_shape_unchecked(&self, chunk_indices: &[u64]) -> ArrayShape;

    /// The origin of the chunk at `chunk_indices`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidChunkGridIndicesError`] if
    ///  - either the length of `chunk_indices` or the `array_shape` do not match the dimensionality of the chunk grid, or
    ///  - `chunk_indices` are out of boudns of the chunk grid.
    fn chunk_origin(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<ArrayIndices, InvalidChunkGridIndicesError> {
        if self.validate_chunk_indices(chunk_indices, array_shape) {
            Ok(unsafe { self.chunk_origin_unchecked(chunk_indices) })
        } else {
            Err(InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
                array_shape.to_vec(),
            ))
        }
    }

    /// The origin of the chunk at `chunk_indices`.
    ///
    /// # Safety
    ///
    /// The length of `chunk_indices` must match the dimensionality of the chunk grid.
    #[doc(hidden)]
    unsafe fn chunk_origin_unchecked(&self, chunk_indices: &[u64]) -> ArrayIndices;

    /// The indices of a chunk which has the element at `array_indices`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArrayIndicesError`] if the either the length of `array_indices` or the `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_indices(
        &self,
        array_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<ArrayIndices, InvalidArrayIndicesError> {
        if self.validate_array_indices(array_indices, array_shape) {
            Ok(unsafe { self.chunk_indices_unchecked(array_indices) })
        } else {
            Err(InvalidArrayIndicesError(
                array_indices.to_vec(),
                array_shape.to_vec(),
            ))
        }
    }

    /// The indices of a chunk which has the element at `array_indices`.
    ///
    /// # Safety
    ///
    /// The length of `array_indices` must match the dimensionality of the chunk grid.
    #[doc(hidden)]
    unsafe fn chunk_indices_unchecked(&self, array_indices: &[u64]) -> ArrayIndices;

    /// The indices within the chunk of the element at `array_indices`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArrayIndicesError`] if the either the length of `array_indices` or the `array_shape` do not match the dimensionality of the chunk grid.
    fn chunk_element_indices(
        &self,
        array_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<ArrayIndices, InvalidArrayIndicesError> {
        if self.validate_array_indices(array_indices, array_shape) {
            Ok(unsafe { self.element_indices_unchecked(array_indices) })
        } else {
            Err(InvalidArrayIndicesError(
                array_indices.to_vec(),
                array_shape.to_vec(),
            ))
        }
    }

    /// The indices within the chunk of the element at `array_indices`.
    ///
    /// # Safety
    ///
    /// The length of `array_indices` must match the dimensionality of the chunk grid.
    #[doc(hidden)]
    unsafe fn element_indices_unchecked(&self, array_indices: &[u64]) -> ArrayIndices;

    /// Check if array indices are valid.
    ///
    /// Ensures array indices are within the array shape.
    /// Zero sized dimensions are ignored in this test to enable writing array chunks without knowing the array shape on initialisation.
    #[must_use]
    fn validate_array_indices(&self, array_indices: &[u64], array_shape: &[u64]) -> bool {
        array_indices.len() == self.dimensionality()
            && array_shape.len() == self.dimensionality()
            && std::iter::zip(array_indices, array_shape)
                .all(|(&index, &shape)| shape == 0 || index < shape)
    }

    /// Check if array indices are valid.
    ///
    /// Ensures chunk grid indices are within the chunk grid shape.
    /// Zero sized dimensions are ignored in this test to enable writing array chunks without knowing the array shape on initialisation.
    #[must_use]
    fn validate_chunk_indices(&self, chunk_indices: &[u64], array_shape: &[u64]) -> bool {
        chunk_indices.len() == self.dimensionality()
            && array_shape.len() == self.dimensionality()
            && self.grid_shape(array_shape).is_ok_and(|chunk_grid_shape| {
                std::iter::zip(chunk_indices, chunk_grid_shape)
                    .all(|(index, shape)| shape == 0 || *index < shape)
            })
    }

    /// Return the [`ArraySubset`] of the chunk at `chunk_indices`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidChunkGridIndicesError`] if the either the length of `chunk_indices` or the `array_shape` do not match the dimensionality of the chunk grid.
    fn subset(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<ArraySubset, InvalidChunkGridIndicesError> {
        if self.validate_chunk_indices(chunk_indices, array_shape) {
            Ok(unsafe { self.subset_unchecked(chunk_indices) })
        } else {
            Err(InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
                array_shape.to_vec(),
            ))
        }
    }

    /// Return the [`ArraySubset`] of the chunk at `chunk_indices`.
    ///
    /// # Safety
    ///
    /// The length of `chunk_indices` must match the dimensionality of the chunk grid.
    #[doc(hidden)]
    unsafe fn subset_unchecked(&self, chunk_indices: &[u64]) -> ArraySubset {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        let chunk_origin = self.chunk_origin_unchecked(chunk_indices);
        let chunk_size = self.chunk_shape_unchecked(chunk_indices);
        ArraySubset::new_with_start_shape_unchecked(chunk_origin, chunk_size)
    }
}

dyn_clone::clone_trait_object!(ChunkGridTraits);

/// An invalid array indices error.
#[derive(Debug, Error)]
#[error("array indices {0:?} are incompatible with the array shape {1:?}")]
pub struct InvalidArrayIndicesError(ArrayIndices, ArrayShape);

/// An invalid chunk indices error.
#[derive(Debug, Error)]
#[error("chunk grid indices {0:?} are invalid for array with shape {1:?}")]
pub struct InvalidChunkGridIndicesError(ArrayIndices, ArrayShape);

/// A chunk grid shape error.
#[derive(Debug, Error)]
pub enum ChunkGridShapeError {
    /// An incompatible dimensionality.
    #[error(transparent)]
    IncompatibleDimensionality(#[from] IncompatibleDimensionalityError),
    /// An implementation error.
    #[error("{_0:?}")]
    Other(String),
}

impl From<&str> for ChunkGridShapeError {
    fn from(err: &str) -> Self {
        Self::Other(err.to_string())
    }
}

impl From<String> for ChunkGridShapeError {
    fn from(err: String) -> Self {
        Self::Other(err)
    }
}

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
        let metadata = serde_json::from_str::<Metadata>(json).unwrap();
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
        let metadata = serde_json::from_str::<Metadata>(json).unwrap();
        ChunkGrid::from_metadata(&metadata).unwrap();
    }
}
