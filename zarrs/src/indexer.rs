use derive_more::{
    derive::{Deref, Display},
    From,
};
use itertools::Itertools;
use thiserror::Error;
use zarrs_metadata::ArrayShape;
use std::iter::zip;

use crate::{array::ArrayIndices, array_subset::ArraySubset};

#[derive(Clone, Display, Debug, Deref)]
#[display("{_0:?}")]
pub struct VIndices(Vec<ArrayIndices>);

impl TryFrom<Vec<ArrayIndices>> for VIndices {
    type Error = Vec<ArrayIndices>; // FIXME: integer indexing InvalidVIndices

    fn try_from(value: Vec<ArrayIndices>) -> Result<Self, Self::Error> {
        // FIXME: integer indexing Validate
        Ok(Self(value))
    }
}

#[derive(Clone, Display, Debug, Deref)]
#[display("{_0:?}")]
pub struct OIndices(Vec<ArrayIndices>);

impl TryFrom<Vec<ArrayIndices>> for OIndices {
    type Error = Vec<ArrayIndices>; // FIXME: integer indexing InvalidOIndices

    fn try_from(value: Vec<ArrayIndices>) -> Result<Self, Self::Error> {
        // FIXME: integer indexing Validate
        Ok(Self(value))
    }
}

#[derive(Clone, Display, Debug, Deref)]
#[display("{_0:?}")]
pub struct MixedIndices(Vec<MixedIndex>);

impl TryFrom<Vec<MixedIndex>> for MixedIndices {
    type Error = Vec<MixedIndex>; // FIXME: integer indexing InvalidMixedIndices

    fn try_from(value: Vec<MixedIndex>) -> Result<Self, Self::Error> {
        // FIXME: Validate
        Ok(Self(value))
    }
}

/// The indices on a single dimension of [`MixedIndices`].
#[derive(Clone, Debug, From)]
pub enum MixedIndex {
    IntegerIndex(ArrayIndices),
    Range(std::ops::Range<u64>),
}

impl MixedIndex {
    // # Panics
    // Panics if the length of a range exceeds [`usize::MAX`].
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::IntegerIndex(oindex) => oindex.len(),
            Self::Range(range) => usize::try_from(range.end.saturating_sub(range.start)).unwrap(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// An incompatible indexer and array shape error.
#[derive(Clone, Debug, Error, From)]
#[error("incompatible indexer {0} with array shape {1:?}")]
pub struct IncompatibleIndexerAndShapeError(Indexer, ArrayShape);

impl IncompatibleIndexerAndShapeError {
    /// Create a new incompatible indexer and shape error.
    #[must_use]
    pub fn new(indexer: impl Into<Indexer>, array_shape: ArrayShape) -> Self {
        let indexer = indexer.into();
        Self(indexer, array_shape)
    }
}

/// The different kinds of array indexing methods.
///
/// See: <https://numpy.org/neps/nep-0021-advanced-indexing.html#existing-indexing-operations>
#[derive(Clone, Display, Debug, From)]
pub enum Indexer {
    /// Subset indexing.
    // Is this just basic?
    Subset(ArraySubset),
    /// Vectorized Indexing.
    VIndex(VIndices),
    /// Orthogonal Indexing.
    OIndex(OIndices),
    /// Mixed Indexing, a variant of vectorized where integer indices are treated as vectorized but with ranges too.
    Mixed(MixedIndices),
}

impl Indexer {
    #[must_use]
    pub fn new_subset(subset: ArraySubset) -> Self {
        Self::Subset(subset)
    }

    // TODO: integer indexing
    // pub fn new_mixed(vindices: Vec<ArrayIndices>) -> Result<Self, InvalidMixedIndices> {
    //     let vindinces: VIndices = vindices.try_into()
    // }

    // pub fn new_vindex(vindices: Vec<ArrayIndices>) -> Result<Self, InvalidVIndices> {
    //     let vindinces: VIndices = vindices.try_into()
    // }

    // pub fn new_oindex(vindices: Vec<ArrayIndices>) -> Result<Self, InvalidOIndices> {
    //     let vindinces: VIndices = vindices.try_into()
    // }

    /// Return the dimensionality of the indexer.
    #[must_use]
    pub fn dimensionality(&self) -> usize {
        match self {
            Indexer::Subset(subset) => subset.dimensionality(),
            Indexer::VIndex(vindices) => vindices.len(),
            Indexer::OIndex(oindices) => oindices.len(),
            Indexer::Mixed(mindices) => mindices.len(),
        }
    }

    /// Return the number of elements of the indexer.
    #[must_use]
    pub fn num_elements_usize(&self) -> usize {
        match self {
            Indexer::Subset(subset) => subset.num_elements_usize(),
            Indexer::VIndex(vindices) => vindices.first().map_or(0, Vec::len),
            Indexer::OIndex(oindices) => oindices.iter().map(Vec::len).product(),
            Indexer::Mixed(mindices) => mindices.iter().map(MixedIndex::len).product(),
        }
    }

    /// Check if the indexer is compatible with an array of shape `array_shape`.
    pub fn is_compatible(
        &self,
        array_shape: &[u64],
    ) -> Result<(), IncompatibleIndexerAndShapeError> {
        // TODO: integer indexing or bool?
        let compatible = match self {
            Indexer::Subset(subset) => {
                subset.dimensionality() == array_shape.len()
                    && std::iter::zip(subset.end_exc(), array_shape)
                        .all(|(end, shape)| end <= *shape)
            }
            Indexer::VIndex(vindices) => {
                let are_equal_length = vindices.0.iter().map(|x| x.len()).all_equal();
                let has_right_shape = vindices.0[0].len() != (array_shape[0] as usize) || array_shape.iter().skip(1).all_equal_value() != Ok(&1);
                are_equal_length && has_right_shape
            },
            Indexer::OIndex(oindices) => {
                let are_integer_indices_wrong_or_missing = zip(array_shape, oindices.0.iter()).any(|(sh, index)| index.len() as u64 != *sh);
                are_integer_indices_wrong_or_missing
            },
            Indexer::Mixed(mindices) => {
                let all_range_indices = mindices.0.iter().all(|x| matches!(x, MixedIndex::Range(_)));
                let some_range_indices = mindices.0.iter().any(|x| matches!(x, MixedIndex::Range(_)));
                !all_range_indices && some_range_indices
            },
        };
        if compatible {
            Ok(())
        } else {
            Err(IncompatibleIndexerAndShapeError(
                self.clone(),
                array_shape.to_vec(),
            ))
        }
    }
}
