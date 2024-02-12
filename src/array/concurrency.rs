//! Concurrency utilities for arrays and codecs.

use std::num::NonZeroUsize;

/// The recommended concurrency of a codec includes the most efficient and maximum recommended concurrency.
///
/// Consider a chain that does slow decoding first on a single thread, but subsequent codecs can run on multiple threads.
/// In this case, recommended concurrency is best expressed by two numbers:
///    - the efficient concurrency, equal to the minimum of codecs
///    - the maximum concurrency, equal to the maximum of codecs
// TODO: Compression codec example in docs?
#[derive(Debug, Copy, Clone)]
pub struct RecommendedConcurrency {
    efficient: NonZeroUsize,
    maximum: NonZeroUsize,
}

impl RecommendedConcurrency {
    /// Create a new recommended concurrency struct with an explicit efficient and maximum recommendation.
    #[must_use]
    pub fn new(efficient: NonZeroUsize, maximum: NonZeroUsize) -> Self {
        Self { efficient, maximum }
    }

    /// Create a new recommended concurrency struct with an efficient and maximum recommendation of one.
    #[must_use]
    pub fn one() -> Self {
        Self::new(unsafe { NonZeroUsize::new_unchecked(1) }, unsafe {
            NonZeroUsize::new_unchecked(1)
        })
    }

    /// Return the recommended efficient concurrency.
    #[must_use]
    pub fn efficient(&self) -> NonZeroUsize {
        self.efficient
    }

    /// Return the recommended maximum concurrency.
    #[must_use]
    pub fn maximum(&self) -> NonZeroUsize {
        self.maximum
    }

    /// Merge another concurrency, reducing the minimum concurrency or increasing the maximum concurrency to match `other`.
    pub fn merge(&mut self, other: &RecommendedConcurrency) {
        self.efficient = std::cmp::min(self.efficient, other.efficient);
        self.maximum = std::cmp::max(self.maximum, other.maximum);
    }
}

// FIXME: Better function name/improve docs
// FIXME: Can have multiple strategies: PreferInnerToMax PreferOuterToMax
/// Calculate concurrent limits
///
/// Return is (self, inner)
#[must_use]
pub fn calc_concurrent_limits(
    concurrency_target: NonZeroUsize,
    maximum_self_concurrent_limit: NonZeroUsize,
    recommended_concurrency_inner: &RecommendedConcurrency,
) -> (NonZeroUsize, NonZeroUsize) {
    let calc_concurrency = |target: NonZeroUsize, other: NonZeroUsize| {
        std::cmp::min(
            unsafe { NonZeroUsize::new_unchecked((target.get() + other.get() - 1) / other.get()) },
            concurrency_target,
        )
    };

    // Try using efficient inner
    let mut concurrency_limit_inner = std::cmp::min(
        recommended_concurrency_inner.efficient(),
        concurrency_target,
    );
    let mut concurrency_limit_self = std::cmp::min(
        calc_concurrency(concurrency_target, concurrency_limit_inner),
        maximum_self_concurrent_limit,
    );

    if (concurrency_limit_self.get() * concurrency_limit_inner.get()) < concurrency_target.get()
        && concurrency_limit_inner < recommended_concurrency_inner.maximum()
    {
        concurrency_limit_inner =
            std::cmp::min(recommended_concurrency_inner.maximum(), concurrency_target);
        concurrency_limit_self = std::cmp::min(
            calc_concurrency(concurrency_target, concurrency_limit_inner),
            maximum_self_concurrent_limit,
        );
    }

    (concurrency_limit_self, concurrency_limit_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_limits() {
        let target = NonZeroUsize::new(32).unwrap();
        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            NonZeroUsize::new(32).unwrap(),
            &RecommendedConcurrency::one(),
        );
        assert_eq!((self_limit.get(), inner_limit.get()), (32, 1));
        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            NonZeroUsize::new(24).unwrap(),
            &RecommendedConcurrency::one(),
        );
        assert_eq!((self_limit.get(), inner_limit.get()), (24, 1));

        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            NonZeroUsize::new(24).unwrap(),
            &RecommendedConcurrency::new(
                NonZeroUsize::new(4).unwrap(),
                NonZeroUsize::new(8).unwrap(),
            ),
        );
        assert_eq!((self_limit.get(), inner_limit.get()), (8, 4));

        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            NonZeroUsize::new(5).unwrap(),
            &RecommendedConcurrency::new(
                NonZeroUsize::new(7).unwrap(),
                NonZeroUsize::new(12).unwrap(),
            ),
        );
        assert_eq!((self_limit.get(), inner_limit.get()), (5, 7));

        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            NonZeroUsize::new(2).unwrap(),
            &RecommendedConcurrency::new(
                NonZeroUsize::new(7).unwrap(),
                NonZeroUsize::new(12).unwrap(),
            ),
        );
        assert_eq!((self_limit.get(), inner_limit.get()), (2, 12));
    }
}
