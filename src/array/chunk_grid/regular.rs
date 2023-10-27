//! The regular chunk grid.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#regular-grids>.

use crate::{
    array::{chunk_grid::ChunkGridPlugin, ArrayIndices, ArrayShape},
    metadata::Metadata,
    plugin::PluginCreateError,
};

use derive_more::Display;
use serde::{Deserialize, Serialize};

use super::{ChunkGrid, ChunkGridTraits};

const IDENTIFIER: &str = "regular";

// Register the chunk grid.
inventory::submit! {
    ChunkGridPlugin::new(IDENTIFIER, is_name_regular, create_chunk_grid_regular)
}

fn is_name_regular(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_chunk_grid_regular(metadata: &Metadata) -> Result<ChunkGrid, PluginCreateError> {
    let configuration: RegularChunkGridConfiguration = metadata.to_configuration()?;
    let chunk_grid = RegularChunkGrid::new(configuration.chunk_shape);
    Ok(ChunkGrid::new(chunk_grid))
}

/// Configuration parameters for a `regular` chunk grid.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct RegularChunkGridConfiguration {
    /// The chunk shape.
    pub chunk_shape: ArrayShape,
}

/// A `regular` chunk grid.
#[derive(Debug, Clone)]
pub struct RegularChunkGrid {
    chunk_shape: ArrayShape,
}

impl RegularChunkGrid {
    /// Create a new regular chunk grid with chunk shape `chunk_shape`.
    #[must_use]
    pub fn new(chunk_shape: ArrayShape) -> Self {
        Self { chunk_shape }
    }

    /// Return the chunk shape.
    #[must_use]
    pub fn chunk_shape(&self) -> &[u64] {
        &self.chunk_shape
    }
}

impl ChunkGridTraits for RegularChunkGrid {
    fn create_metadata(&self) -> Metadata {
        let configuration = RegularChunkGridConfiguration {
            chunk_shape: self.chunk_shape.clone(),
        };
        Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap()
    }

    fn dimensionality(&self) -> usize {
        self.chunk_shape.len()
    }

    unsafe fn grid_shape_unchecked(&self, array_shape: &[u64]) -> Option<ArrayShape> {
        assert_eq!(array_shape.len(), self.dimensionality());
        Some(
            std::iter::zip(array_shape, &self.chunk_shape)
                .map(|(a, s)| if *s == 0 { 0 } else { (a + s - 1) / s })
                .collect(),
        )
    }

    /// The chunk shape. Fixed for a regular grid.
    unsafe fn chunk_shape_unchecked(
        &self,
        chunk_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayShape> {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        Some(self.chunk_shape.clone())
    }

    unsafe fn chunk_origin_unchecked(
        &self,
        chunk_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayIndices> {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        Some(
            std::iter::zip(chunk_indices, &self.chunk_shape)
                .map(|(i, s)| i * s)
                .collect(),
        )
    }

    unsafe fn chunk_indices_unchecked(
        &self,
        array_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayIndices> {
        debug_assert_eq!(self.dimensionality(), array_indices.len());
        Some(
            std::iter::zip(array_indices, &self.chunk_shape)
                .map(|(i, s)| i / s)
                .collect(),
        )
    }

    unsafe fn chunk_element_indices_unchecked(
        &self,
        array_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayIndices> {
        debug_assert_eq!(self.dimensionality(), array_indices.len());
        Some(
            std::iter::zip(array_indices, &self.chunk_shape)
                .map(|(i, s)| i % s)
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_grid_regular() {
        let array_shape: ArrayShape = vec![5, 7, 52];
        let chunk_shape: ArrayShape = vec![1, 2, 3];
        let chunk_grid = RegularChunkGrid::new(chunk_shape.clone());

        assert_eq!(chunk_grid.dimensionality(), 3);

        assert_eq!(
            chunk_grid.chunk_origin(&[1, 1, 1], &array_shape).unwrap(),
            Some(chunk_shape.clone())
        );

        assert_eq!(chunk_grid.chunk_shape(), chunk_shape);

        let chunk_grid_shape = chunk_grid.grid_shape(&array_shape).unwrap();
        assert_eq!(chunk_grid_shape, Some(vec![5, 4, 18]));

        let array_index: ArrayIndices = vec![3, 5, 50];
        assert_eq!(
            chunk_grid
                .chunk_indices(&array_index, &array_shape)
                .unwrap(),
            Some(vec![3, 2, 16])
        );
        assert_eq!(
            chunk_grid
                .chunk_element_indices(&array_index, &array_shape)
                .unwrap(),
            Some(vec![0, 1, 2])
        );
    }

    #[test]
    fn chunk_grid_regular_out_of_bounds() {
        let array_shape: ArrayShape = vec![5, 7, 52];
        let chunk_shape: ArrayShape = vec![1, 2, 3];
        let chunk_grid = RegularChunkGrid::new(chunk_shape);

        let array_indices: ArrayIndices = vec![3, 5, 53];
        assert_eq!(
            chunk_grid
                .chunk_indices(&array_indices, &array_shape)
                .unwrap(),
            Some(vec![3, 2, 17])
        );

        let chunk_indices: ArrayShape = vec![6, 1, 1];
        assert!(!chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
        assert_eq!(
            chunk_grid
                .chunk_origin(&chunk_indices, &array_shape)
                .unwrap(),
            Some(vec![6, 2, 3])
        );
    }

    #[test]
    fn chunk_grid_regular_unlimited() {
        let array_shape: ArrayShape = vec![5, 7, 0];
        let chunk_shape: ArrayShape = vec![1, 2, 3];
        let chunk_grid = RegularChunkGrid::new(chunk_shape);

        let array_indices: ArrayIndices = vec![3, 5, 1000];
        assert!(chunk_grid
            .chunk_indices(&array_indices, &array_shape)
            .unwrap()
            .is_some());

        assert_eq!(
            chunk_grid.grid_shape(&array_shape).unwrap(),
            Some(vec![5, 4, 0])
        );

        let chunk_indices: ArrayShape = vec![3, 1, 1000];
        assert!(chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
        assert!(chunk_grid
            .chunk_origin(&chunk_indices, &array_shape)
            .unwrap()
            .is_some());
    }
}
