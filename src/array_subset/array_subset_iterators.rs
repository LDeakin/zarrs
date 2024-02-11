mod chunks_iterator;
mod contiguous_indices_iterator;
mod contiguous_linearised_indices_iterator;
mod indices_iterator;
mod linearised_indices_iterator;

pub use chunks_iterator::ChunksIterator;
pub use contiguous_indices_iterator::ContiguousIndicesIterator;
pub use contiguous_linearised_indices_iterator::ContiguousLinearisedIndicesIterator;
pub use indices_iterator::IndicesIterator;
pub use linearised_indices_iterator::LinearisedIndicesIterator;

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::array_subset::ArraySubset;

    #[test]
    fn array_subset_iter_indices() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        let mut iter = subset.iter_indices();
        assert_eq!(iter.size_hint(), (4, Some(4)));
        assert_eq!(iter.next(), Some(vec![1, 1]));
        assert_eq!(iter.next(), Some(vec![1, 2]));
        assert_eq!(iter.next(), Some(vec![2, 1]));
        assert_eq!(iter.next(), Some(vec![2, 2]));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_linearised_indices() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        assert!(subset.iter_linearised_indices(&[4, 4, 4]).is_err());
        let mut iter = subset.iter_linearised_indices(&[4, 4]).unwrap();
        //  0  1  2  3
        //  4  5  6  7
        //  8  9 10 11
        // 12 13 14 15
        assert_eq!(iter.size_hint(), (4, Some(4)));
        assert_eq!(iter.next(), Some(5));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), Some(9));
        assert_eq!(iter.next(), Some(10));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices1() {
        let subset = ArraySubset::new_with_shape(vec![2, 2]);
        let mut iter = subset.iter_contiguous_indices(&[2, 2]).unwrap();
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next(), Some((vec![0, 0], 4)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices2() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        let mut iter = subset.iter_contiguous_indices(&[4, 4]).unwrap();
        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.next(), Some((vec![1, 1], 2)));
        assert_eq!(iter.next(), Some((vec![2, 1], 2)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices3() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 0..1, 0..2, 0..2]);
        let mut iter = subset.iter_contiguous_indices(&[3, 1, 2, 2]).unwrap();
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next(), Some((vec![1, 0, 0, 0], 8)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_continuous_linearised_indices() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        let mut iter = subset.iter_contiguous_linearised_indices(&[4, 4]).unwrap();
        //  0  1  2  3
        //  4  5  6  7
        //  8  9 10 11
        // 12 13 14 15
        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.next(), Some((5, 2)));
        assert_eq!(iter.next(), Some((9, 2)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    #[rustfmt::skip]
    fn array_subset_iter_chunks1() {
        let subset = ArraySubset::new_with_ranges(&[1..5, 1..5]);
        let chunk_shape_invalid = [NonZeroU64::new(2).unwrap()];
        assert!(subset.iter_chunks(&chunk_shape_invalid).is_err());
        let chunk_shape = [NonZeroU64::new(2).unwrap(), NonZeroU64::new(2).unwrap()];
        let mut iter = subset.iter_chunks(&chunk_shape).unwrap();
        assert_eq!(iter.size_hint(), (9, Some(9)));
        assert_eq!(iter.next(), Some((vec![0, 0], ArraySubset::new_with_ranges(&[0..2, 0..2]))));
        assert_eq!(iter.next(), Some((vec![0, 1], ArraySubset::new_with_ranges(&[0..2, 2..4]))));
        assert_eq!(iter.next(), Some((vec![0, 2], ArraySubset::new_with_ranges(&[0..2, 4..6]))));
        assert_eq!(iter.next(), Some((vec![1, 0], ArraySubset::new_with_ranges(&[2..4, 0..2]))));
        assert_eq!(iter.next(), Some((vec![1, 1], ArraySubset::new_with_ranges(&[2..4, 2..4]))));
        assert_eq!(iter.next(), Some((vec![1, 2], ArraySubset::new_with_ranges(&[2..4, 4..6]))));
        assert_eq!(iter.next(), Some((vec![2, 0], ArraySubset::new_with_ranges(&[4..6, 0..2]))));
        assert_eq!(iter.next(), Some((vec![2, 1], ArraySubset::new_with_ranges(&[4..6, 2..4]))));
        assert_eq!(iter.next(), Some((vec![2, 2], ArraySubset::new_with_ranges(&[4..6, 4..6]))));
        assert_eq!(iter.next(), None);
    }

    #[test]
    #[rustfmt::skip]
    fn array_subset_iter_chunks2() {
        let subset = ArraySubset::new_with_ranges(&[2..5, 2..6]);
        let chunk_shape = [NonZeroU64::new(2).unwrap(), NonZeroU64::new(3).unwrap()];
        let mut iter = subset.iter_chunks(&chunk_shape).unwrap();
        assert_eq!(iter.size_hint(), (4, Some(4)));
        assert_eq!(iter.next(), Some((vec![1, 0], ArraySubset::new_with_ranges(&[2..4, 0..3]))));
        assert_eq!(iter.next(), Some((vec![1, 1], ArraySubset::new_with_ranges(&[2..4, 3..6]))));
        assert_eq!(iter.next(), Some((vec![2, 0], ArraySubset::new_with_ranges(&[4..6, 0..3]))));
        assert_eq!(iter.next(), Some((vec![2, 1], ArraySubset::new_with_ranges(&[4..6, 3..6]))));
        assert_eq!(iter.next(), None);
    }
}
