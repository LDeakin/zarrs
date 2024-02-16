//! Concurrency utilities for arrays and codecs.

/// The preferred concurrency in a [`RecommendedConcurrency`].
#[derive(Debug, Copy, Clone)]
pub enum PreferredConcurrency {
    /// Prefer the minimum concurrency.
    ///
    /// This is suggested in situations where memory scales with concurrency.
    Minimum,
    /// Prefer the maximum concurrency.
    ///
    /// This is suggested in situations where memory does not scale with concurrency (or does not scale much).
    Maximum,
}

/// The recommended concurrency of a codec includes the most efficient and maximum recommended concurrency.
///
/// Consider a chain that does slow decoding first on a single thread, but subsequent codecs can run on multiple threads.
/// In this case, recommended concurrency is best expressed by two numbers:
///    - the efficient concurrency, equal to the minimum of codecs
///    - the maximum concurrency, equal to the maximum of codecs
// TODO: Compression codec example in docs?
#[derive(Debug, Clone)]
pub struct RecommendedConcurrency {
    /// The range is just used for its constructor and start/end, no iteration
    range: std::ops::Range<usize>,
    preferred_concurrency: PreferredConcurrency,
}

impl RecommendedConcurrency {
    /// Create a new recommended concurrency struct with an explicit concurrency range and preferred concurrency.
    #[must_use]
    pub fn new(range: std::ops::Range<usize>, preferred_concurrency: PreferredConcurrency) -> Self {
        let range = std::cmp::max(1, range.start)..std::cmp::max(1, range.end);
        Self {
            range,
            preferred_concurrency,
        }
    }

    /// Create a new recommended concurrency struct with a minimum concurrency and unbounded maximum concurrency, preferring minimum.
    #[must_use]
    pub fn new_minimum(minimum: usize) -> Self {
        Self {
            range: std::cmp::max(1, minimum)..usize::MAX,
            preferred_concurrency: PreferredConcurrency::Minimum,
        }
    }

    /// Create a new recommended concurrency struct with a maximum concurrency and a bounded maximum concurrency, preferring maximum.
    #[must_use]
    pub fn new_maximum(maximum: usize) -> Self {
        Self {
            range: 1..maximum,
            preferred_concurrency: PreferredConcurrency::Maximum,
        }
    }

    /// Return the minimum concurrency.
    #[must_use]
    pub fn min(&self) -> usize {
        self.range.start
    }

    /// Return the maximum concurrency.
    #[must_use]
    pub fn max(&self) -> usize {
        self.range.end
    }

    /// Return the preferred concurrency.
    #[must_use]
    pub fn preferred(&self) -> usize {
        match self.preferred_concurrency {
            PreferredConcurrency::Minimum => self.range.start,
            PreferredConcurrency::Maximum => self.range.end,
        }
    }
}

// FIXME: Better function name/improve docs
// FIXME: Can have multiple strategies: PreferInnerToMax PreferOuterToMax
/// Calculate concurrent limits
///
/// Return is (self, inner)
#[must_use]
pub fn calc_concurrent_limits(
    concurrency_target: usize,
    recommended_concurrency_outer: &RecommendedConcurrency,
    recommended_concurrency_inner: &RecommendedConcurrency,
) -> (usize, usize) {
    let mut concurrency_inner = recommended_concurrency_inner.min();
    let mut concurrency_outer = recommended_concurrency_outer.min();

    if concurrency_inner * concurrency_outer < concurrency_target {
        // Try increasing inner
        concurrency_inner = std::cmp::min(
            (concurrency_target + concurrency_outer - 1) / concurrency_outer,
            recommended_concurrency_inner.max(),
        );
    }

    if concurrency_inner * concurrency_outer < concurrency_target {
        // Try increasing outer
        concurrency_outer = std::cmp::min(
            (concurrency_target + concurrency_inner - 1) / concurrency_inner,
            recommended_concurrency_outer.max(),
        );
    }

    (concurrency_outer, concurrency_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_limits() {
        let target = 32;

        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            &RecommendedConcurrency::new_minimum(24),
            &RecommendedConcurrency::new_maximum(1),
        );
        assert_eq!((self_limit, inner_limit), (32, 1));

        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            &RecommendedConcurrency::new_minimum(24),
            &RecommendedConcurrency::new(4..8, PreferredConcurrency::Minimum),
        );
        assert_eq!((self_limit, inner_limit), (24, 4));

        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            &RecommendedConcurrency::new_maximum(5),
            &RecommendedConcurrency::new(7..12, PreferredConcurrency::Maximum),
        );
        assert_eq!((self_limit, inner_limit), (3, 12));

        let (self_limit, inner_limit) = calc_concurrent_limits(
            target,
            &RecommendedConcurrency::new_maximum(2),
            &RecommendedConcurrency::new(7..14, PreferredConcurrency::Maximum),
        );
        assert_eq!((self_limit, inner_limit), (2, 14));
    }
}
