use std::num::{NonZeroU64, NonZeroUsize};

use serde::{Deserialize, Serialize};

use super::{ArrayShape, NonZeroError};

/// The shape of a chunk. All dimensions must be non-zero.
#[allow(clippy::unsafe_derive_deserialize)]
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct ChunkShape(Vec<NonZeroU64>);

impl ChunkShape {
    /// Return the number of elements.
    ///
    /// Equal to the product of the components of its shape.
    #[must_use]
    pub fn num_elements(&self) -> NonZeroU64 {
        unsafe {
            // Multiplying NonZeroU64 must result in NonZeroU64
            NonZeroU64::new_unchecked(self.num_elements_u64())
        }
    }

    /// Return the number of elements as a nonzero usize.
    ///
    /// Equal to the product of the components of its shape.
    ///
    /// # Panics
    /// Panics if the number of elements exceeds [`usize::MAX`].
    #[must_use]
    pub fn num_elements_nonzero_usize(&self) -> NonZeroUsize {
        unsafe {
            // Multiplying NonZeroU64 must result in NonZeroUsize
            NonZeroUsize::new_unchecked(usize::try_from(self.num_elements_u64()).unwrap())
        }
    }

    /// Return the number of elements as a u64.
    ///
    /// Equal to the product of the components of its shape.
    #[must_use]
    pub fn num_elements_u64(&self) -> u64 {
        self.0.iter().copied().map(NonZeroU64::get).product::<u64>()
    }

    /// Return the number of elements as a usize.
    ///
    /// Equal to the product of the components of its shape.
    ///
    /// # Panics
    /// Panics if the number of elements exceeds [`usize::MAX`].
    #[must_use]
    pub fn num_elements_usize(&self) -> usize {
        usize::try_from(self.num_elements_u64()).unwrap()
    }
}

impl From<ChunkShape> for Vec<NonZeroU64> {
    fn from(val: ChunkShape) -> Self {
        val.0
    }
}

impl std::ops::Deref for ChunkShape {
    type Target = Vec<NonZeroU64>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ChunkShape {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

macro_rules! from_chunkshape {
    ( $t:ty ) => {
        impl From<$t> for ChunkShape {
            fn from(value: $t) -> Self {
                ChunkShape(value.to_vec())
            }
        }
    };
    ( $t:ty, $g:ident ) => {
        impl<const $g: usize> From<$t> for ChunkShape {
            fn from(value: $t) -> Self {
                ChunkShape(value.to_vec())
            }
        }
    };
}

macro_rules! try_from_chunkshape {
    ( $t:ty ) => {
        impl TryFrom<$t> for ChunkShape {
            type Error = NonZeroError;
            fn try_from(value: $t) -> Result<Self, Self::Error> {
                Ok(ChunkShape(
                    value
                        .iter()
                        .map(|&i| NonZeroU64::new(i).ok_or(NonZeroError))
                        .collect::<Result<_, _>>()?,
                ))
            }
        }
    };
    ( $t:ty, $g:ident ) => {
        impl<const $g: usize> TryFrom<$t> for ChunkShape {
            type Error = NonZeroError;
            fn try_from(value: $t) -> Result<Self, Self::Error> {
                Ok(ChunkShape(
                    value
                        .iter()
                        .map(|&i| NonZeroU64::new(i).ok_or(NonZeroError))
                        .collect::<Result<_, _>>()?,
                ))
            }
        }
    };
}

from_chunkshape!(Vec<NonZeroU64>);
from_chunkshape!(&[NonZeroU64]);
from_chunkshape!([NonZeroU64; N], N);
from_chunkshape!(&[NonZeroU64; N], N);
try_from_chunkshape!(Vec<u64>);
try_from_chunkshape!(&[u64]);
try_from_chunkshape!([u64; N], N);
try_from_chunkshape!(&[u64; N], N);

/// Convert a [`ChunkShape`] to an [`ArrayShape`].
#[must_use]
pub fn chunk_shape_to_array_shape(chunk_shape: &[NonZeroU64]) -> ArrayShape {
    chunk_shape.iter().map(|i| i.get()).collect()
}
