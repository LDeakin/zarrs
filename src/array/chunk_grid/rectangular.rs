//! The `rectangular` chunk grid.
//!
//! See <https://zarr.dev/zeps/draft/ZEP0003.html>.

use std::num::NonZeroU64;

use crate::{
    array::{chunk_grid::ChunkGridPlugin, ArrayIndices, ArrayShape, ChunkShape},
    metadata::v3::{chunk_grid::rectangular, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

use derive_more::From;
use itertools::Itertools;

use super::{ChunkGrid, ChunkGridTraits};
pub use super::{RectangularChunkGridConfiguration, RectangularChunkGridDimensionConfiguration};

pub use rectangular::IDENTIFIER;

// Register the chunk grid.
inventory::submit! {
    ChunkGridPlugin::new(IDENTIFIER, is_name_rectangular, create_chunk_grid_rectangular)
}

fn is_name_rectangular(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

/// Create a `rectangular` chunk grid from metadata.
///
/// # Errors
/// Returns a [`PluginCreateError`] if the metadata is invalid for a regular chunk grid.
fn create_chunk_grid_rectangular(metadata: &MetadataV3) -> Result<ChunkGrid, PluginCreateError> {
    let configuration: RectangularChunkGridConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "chunk grid", metadata.clone()))?;
    let chunk_grid = RectangularChunkGrid::new(&configuration.chunk_shape);
    Ok(ChunkGrid::new(chunk_grid))
}

/// A `rectangular` chunk grid.
#[derive(Debug, Clone)]
pub struct RectangularChunkGrid {
    chunks: Vec<RectangularChunkGridDimension>,
}

#[derive(Debug, Clone)]
struct OffsetSize {
    offset: u64,
    size: NonZeroU64,
}

#[derive(Debug, Clone, From)]
enum RectangularChunkGridDimension {
    Fixed(NonZeroU64),
    Varying(Vec<OffsetSize>),
}

impl RectangularChunkGrid {
    /// Create a new `rectangular` chunk grid with chunk shapes `chunk_shapes`.
    #[must_use]
    pub fn new(chunk_shapes: &[RectangularChunkGridDimensionConfiguration]) -> Self {
        let chunks = chunk_shapes
            .iter()
            .map(|s| match s {
                RectangularChunkGridDimensionConfiguration::Fixed(f) => {
                    RectangularChunkGridDimension::Fixed(*f)
                }
                RectangularChunkGridDimensionConfiguration::Varying(chunk_sizes) => {
                    RectangularChunkGridDimension::Varying(
                        chunk_sizes
                            .as_slice()
                            .iter()
                            .scan(0, |offset, &size| {
                                let last_offset = *offset;
                                *offset += size.get();
                                Some(OffsetSize {
                                    offset: last_offset,
                                    size,
                                })
                            })
                            .collect(),
                    )
                }
            })
            .collect();
        Self { chunks }
    }
}

impl ChunkGridTraits for RectangularChunkGrid {
    fn create_metadata(&self) -> MetadataV3 {
        let chunk_shape = self
            .chunks
            .iter()
            .map(|chunk_dim| match chunk_dim {
                RectangularChunkGridDimension::Fixed(size) => {
                    RectangularChunkGridDimensionConfiguration::Fixed(*size)
                }
                RectangularChunkGridDimension::Varying(offsets_sizes) => {
                    RectangularChunkGridDimensionConfiguration::Varying(
                        offsets_sizes
                            .iter()
                            .map(|offset_size| offset_size.size)
                            .collect_vec()
                            .into(),
                    )
                }
            })
            .collect();
        let configuration = RectangularChunkGridConfiguration { chunk_shape };
        MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap()
    }

    fn dimensionality(&self) -> usize {
        self.chunks.len()
    }

    unsafe fn grid_shape_unchecked(&self, array_shape: &[u64]) -> Option<ArrayShape> {
        assert_eq!(array_shape.len(), self.dimensionality());
        std::iter::zip(array_shape, &self.chunks)
            .map(|(array_shape, chunks)| match chunks {
                RectangularChunkGridDimension::Fixed(s) => {
                    let s = s.get();
                    Some(array_shape.div_ceil(s))
                }
                RectangularChunkGridDimension::Varying(s) => {
                    let last_default = OffsetSize {
                        offset: 0,
                        size: NonZeroU64::new_unchecked(1),
                    };
                    let last = s.last().unwrap_or(&last_default);
                    if *array_shape == last.offset + last.size.get() {
                        Some(s.len() as u64)
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    unsafe fn chunk_shape_unchecked(
        &self,
        chunk_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ChunkShape> {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        std::iter::zip(chunk_indices, &self.chunks)
            .map(|(chunk_index, chunks)| match chunks {
                RectangularChunkGridDimension::Fixed(chunk_size) => Some(*chunk_size),
                RectangularChunkGridDimension::Varying(offsets_sizes) => {
                    let chunk_index = usize::try_from(*chunk_index).unwrap();
                    if chunk_index < offsets_sizes.len() {
                        Some(offsets_sizes[chunk_index].size)
                    } else {
                        None
                    }
                }
            })
            .collect::<Option<Vec<_>>>()
            .map(std::convert::Into::into)
    }

    unsafe fn chunk_shape_u64_unchecked(
        &self,
        chunk_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayShape> {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        std::iter::zip(chunk_indices, &self.chunks)
            .map(|(chunk_index, chunks)| match chunks {
                RectangularChunkGridDimension::Fixed(chunk_size) => Some(chunk_size.get()),
                RectangularChunkGridDimension::Varying(offsets_sizes) => {
                    let chunk_index = usize::try_from(*chunk_index).unwrap();
                    if chunk_index < offsets_sizes.len() {
                        Some(offsets_sizes[chunk_index].size.get())
                    } else {
                        None
                    }
                }
            })
            .collect::<Option<Vec<_>>>()
            .map(std::convert::Into::into)
    }

    unsafe fn chunk_origin_unchecked(
        &self,
        chunk_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayIndices> {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        std::iter::zip(chunk_indices, &self.chunks)
            .map(|(chunk_index, chunks)| match chunks {
                RectangularChunkGridDimension::Fixed(chunk_size) => {
                    Some(chunk_index * chunk_size.get())
                }
                RectangularChunkGridDimension::Varying(offsets_sizes) => {
                    let chunk_index = usize::try_from(*chunk_index).unwrap();
                    if chunk_index < offsets_sizes.len() {
                        Some(offsets_sizes[chunk_index].offset)
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    unsafe fn chunk_indices_unchecked(
        &self,
        array_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayIndices> {
        debug_assert_eq!(self.dimensionality(), array_indices.len());
        std::iter::zip(array_indices, &self.chunks)
            .map(|(index, chunks)| match chunks {
                RectangularChunkGridDimension::Fixed(size) => Some(index / size.get()),
                RectangularChunkGridDimension::Varying(offsets_sizes) => {
                    let last_default = OffsetSize {
                        offset: 0,
                        size: NonZeroU64::new_unchecked(1),
                    };
                    let last = offsets_sizes.last().unwrap_or(&last_default);
                    if *index < last.offset + last.size.get() {
                        let partition = offsets_sizes
                            .partition_point(|offset_size| *index >= offset_size.offset);
                        if partition <= offsets_sizes.len() {
                            let partition = partition as u64;
                            Some(std::cmp::max(partition, 1) - 1)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    unsafe fn chunk_element_indices_unchecked(
        &self,
        array_indices: &[u64],
        array_shape: &[u64],
    ) -> Option<ArrayIndices> {
        let chunk_indices = self.chunk_indices_unchecked(array_indices, array_shape);
        chunk_indices.and_then(|chunk_indices| {
            self.chunk_origin_unchecked(&chunk_indices, array_shape)
                .map(|chunk_start| {
                    std::iter::zip(array_indices, &chunk_start)
                        .map(|(i, s)| i - s)
                        .collect()
                })
        })
    }

    fn array_indices_inbounds(&self, array_indices: &[u64], array_shape: &[u64]) -> bool {
        array_indices.len() == self.dimensionality()
            && array_shape.len() == self.dimensionality()
            && itertools::izip!(array_indices, array_shape, &self.chunks).all(
                |(array_index, array_size, chunks)| {
                    (*array_size == 0 || array_index < array_size)
                        && match chunks {
                            RectangularChunkGridDimension::Fixed(_) => true,
                            RectangularChunkGridDimension::Varying(offsets_sizes) => offsets_sizes
                                .last()
                                .map_or(false, |last| *array_index < last.offset + last.size.get()),
                        }
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use crate::array_subset::ArraySubset;

    use super::*;

    #[test]
    fn chunk_grid_rectangular() {
        let array_shape: ArrayShape = vec![100, 100];
        let chunk_shapes: Vec<RectangularChunkGridDimensionConfiguration> = vec![
            [5, 5, 5, 15, 15, 20, 35].try_into().unwrap(),
            10.try_into().unwrap(),
        ];
        let chunk_grid = RectangularChunkGrid::new(&chunk_shapes);

        assert_eq!(chunk_grid.dimensionality(), 2);
        assert_eq!(
            chunk_grid.grid_shape(&array_shape).unwrap(),
            Some(vec![7, 10])
        );
        assert_eq!(
            chunk_grid.chunk_indices(&[17, 17], &array_shape).unwrap(),
            Some(vec![3, 1])
        );
        assert_eq!(
            chunk_grid
                .chunk_element_indices(&[17, 17], &array_shape)
                .unwrap(),
            Some(vec![2, 7])
        );

        assert_eq!(
            chunk_grid
                .chunks_subset(&ArraySubset::new_with_ranges(&[1..5, 2..6]), &array_shape)
                .unwrap(),
            Some(ArraySubset::new_with_ranges(&[5..45, 20..60]))
        );

        // assert_eq!(
        //     chunk_grid.chunk_indices(&array_index, &array_shape)?,
        //     &[3, 2, 16]
        // );
        // assert_eq!(
        //     chunk_grid.chunk_element_indices(&array_index, &array_shape)?,
        //     &[0, 1, 2]
        // );
    }

    #[test]
    fn chunk_grid_rectangular_out_of_bounds() {
        let array_shape: ArrayShape = vec![100, 100];
        let chunk_shapes: Vec<RectangularChunkGridDimensionConfiguration> = vec![
            [5, 5, 5, 15, 15, 20, 35].try_into().unwrap(),
            10.try_into().unwrap(),
        ];
        let chunk_grid = RectangularChunkGrid::new(&chunk_shapes);

        assert_eq!(
            chunk_grid.grid_shape(&array_shape).unwrap(),
            Some(vec![7, 10])
        );

        let array_indices: ArrayIndices = vec![99, 99];
        assert!(chunk_grid
            .chunk_indices(&array_indices, &array_shape)
            .unwrap()
            .is_some());

        let array_indices: ArrayIndices = vec![100, 100];
        assert!(chunk_grid
            .chunk_indices(&array_indices, &array_shape)
            .unwrap()
            .is_none());

        let chunk_indices: ArrayShape = vec![6, 9];
        assert!(chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
        assert!(chunk_grid
            .chunk_origin(&chunk_indices, &array_shape)
            .unwrap()
            .is_some());

        let chunk_indices: ArrayShape = vec![7, 9];
        assert!(!chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
        assert!(chunk_grid
            .chunk_origin(&chunk_indices, &array_shape)
            .unwrap()
            .is_none());

        let chunk_indices: ArrayShape = vec![6, 10];
        assert!(!chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
    }

    #[test]
    fn chunk_grid_rectangular_unlimited() {
        let array_shape: ArrayShape = vec![100, 0];
        let chunk_shapes: Vec<RectangularChunkGridDimensionConfiguration> = vec![
            [5, 5, 5, 15, 15, 20, 35].try_into().unwrap(),
            10.try_into().unwrap(),
        ];
        let chunk_grid = RectangularChunkGrid::new(&chunk_shapes);

        assert_eq!(
            chunk_grid.grid_shape(&array_shape).unwrap(),
            Some(vec![7, 0])
        );

        let array_indices: ArrayIndices = vec![101, 150];
        assert!(chunk_grid
            .chunk_indices(&array_indices, &array_shape)
            .unwrap()
            .is_none());

        let chunk_indices: ArrayShape = vec![6, 9];
        assert!(chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
        assert!(chunk_grid
            .chunk_origin(&chunk_indices, &array_shape)
            .unwrap()
            .is_some());

        let chunk_indices: ArrayShape = vec![7, 9];
        assert!(!chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
        assert!(chunk_grid
            .chunk_origin(&chunk_indices, &array_shape)
            .unwrap()
            .is_none());

        let chunk_indices: ArrayShape = vec![6, 123];
        assert!(chunk_grid.chunk_indices_inbounds(&chunk_indices, &array_shape));
    }
}
