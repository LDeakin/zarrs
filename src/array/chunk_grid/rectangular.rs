//! The rectangular chunk grid.
//!
//! See <https://zarr.dev/zeps/draft/ZEP0003.html>.

use crate::{
    array::{chunk_grid::ChunkGridPlugin, ArrayIndices, ArrayShape},
    metadata::Metadata,
    plugin::PluginCreateError,
};

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::{ChunkGrid, ChunkGridTraits};

const IDENTIFIER: &str = "rectangular";

// Register the chunk grid.
inventory::submit! {
    ChunkGridPlugin::new(IDENTIFIER, is_name_rectangular, create_chunk_grid_rectangular)
}

fn is_name_rectangular(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_chunk_grid_rectangular(metadata: &Metadata) -> Result<ChunkGrid, PluginCreateError> {
    let configuration: RectangularChunkGridConfiguration = metadata.to_configuration()?;
    let chunk_grid = RectangularChunkGrid::new(&configuration.chunk_shape);
    Ok(ChunkGrid::new(chunk_grid))
}

/// Configuration parameters for a `rectangular` chunk grid.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct RectangularChunkGridConfiguration {
    /// The chunk shape.
    pub chunk_shape: Vec<RectangularChunkGridDimensionConfiguration>,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, From)]
#[serde(untagged)]
pub enum RectangularChunkGridDimensionConfiguration {
    Fixed(u64),
    Varying(ArrayShape),
}

impl From<&[u64]> for RectangularChunkGridDimensionConfiguration {
    fn from(value: &[u64]) -> Self {
        Self::Varying(value.to_vec())
    }
}

impl<const N: usize> From<[u64; N]> for RectangularChunkGridDimensionConfiguration {
    fn from(value: [u64; N]) -> Self {
        Self::Varying(value.to_vec())
    }
}

/// A `rectangular` chunk grid.
#[derive(Debug, Clone)]
pub struct RectangularChunkGrid {
    chunks: Vec<RectangularChunkGridDimension>,
}

#[derive(Debug, Clone)]
struct OffsetSize {
    offset: u64,
    size: u64,
}

#[derive(Debug, Clone, From)]
enum RectangularChunkGridDimension {
    Fixed(u64),
    Varying(Vec<OffsetSize>),
}

impl RectangularChunkGrid {
    /// Create a new rectangular chunk grid with chunk shapes `chunk_shapes`.
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
                            .iter()
                            .scan(0, |offset, &size| {
                                let last_offset = *offset;
                                *offset += size;
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
    fn create_metadata(&self) -> Metadata {
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
                            .collect(),
                    )
                }
            })
            .collect();
        let configuration = RectangularChunkGridConfiguration { chunk_shape };
        Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap()
    }

    fn dimensionality(&self) -> usize {
        self.chunks.len()
    }

    unsafe fn grid_shape_unchecked(&self, array_shape: &[u64]) -> Option<ArrayShape> {
        assert_eq!(array_shape.len(), self.dimensionality());
        std::iter::zip(array_shape, &self.chunks)
            .map(|(array_shape, chunks)| match chunks {
                RectangularChunkGridDimension::Fixed(s) => Some((array_shape + s - 1) / s),
                RectangularChunkGridDimension::Varying(s) => {
                    let last = s.last().unwrap_or(&OffsetSize { offset: 0, size: 0 });
                    if *array_shape == last.offset + last.size {
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
    ) -> Option<ArrayShape> {
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
            .collect()
    }

    unsafe fn chunk_origin_unchecked(
        &self,
        chunk_indices: &[u64],
        _array_shape: &[u64],
    ) -> Option<ArrayIndices> {
        debug_assert_eq!(self.dimensionality(), chunk_indices.len());
        std::iter::zip(chunk_indices, &self.chunks)
            .map(|(chunk_index, chunks)| match chunks {
                RectangularChunkGridDimension::Fixed(chunk_size) => Some(chunk_index * chunk_size),
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
                RectangularChunkGridDimension::Fixed(size) => Some(index / size),
                RectangularChunkGridDimension::Varying(offsets_sizes) => {
                    let last = offsets_sizes
                        .last()
                        .unwrap_or(&OffsetSize { offset: 0, size: 0 });
                    if *index < last.offset + last.size {
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
                                .map_or(false, |last| *array_index < last.offset + last.size),
                        }
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_grid_rectangular() {
        let array_shape: ArrayShape = vec![100, 100];
        let chunk_shapes: Vec<RectangularChunkGridDimensionConfiguration> =
            vec![vec![5, 5, 5, 15, 15, 20, 35].into(), 10.into()];
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
        let chunk_shapes: Vec<RectangularChunkGridDimensionConfiguration> =
            vec![vec![5, 5, 5, 15, 15, 20, 35].into(), 10.into()];
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
        let chunk_shapes: Vec<RectangularChunkGridDimensionConfiguration> =
            vec![vec![5, 5, 5, 15, 15, 20, 35].into(), 10.into()];
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
