use crate::vec_spare_capacity_to_mut_slice;

/// An unsafe cell slice.
///
/// It can be used to acquire multiple mutable references to a slice enabling writing from multiple threads.
/// This is inherently unsafe and it is the responsibility of the caller to ensure safety.
#[derive(Copy, Clone)]
pub struct UnsafeCellSlice<'a, T>(&'a [std::cell::UnsafeCell<T>]);

unsafe impl<'a, T: Send + Sync> Send for UnsafeCellSlice<'a, T> {}
unsafe impl<'a, T: Send + Sync> Sync for UnsafeCellSlice<'a, T> {}

impl<'a, T: Copy> UnsafeCellSlice<'a, T> {
    /// Create a new [`UnsafeCellSlice`].
    #[must_use]
    pub fn new(slice: &'a mut [T]) -> Self {
        let ptr = slice as *mut [T] as *const [std::cell::UnsafeCell<T>];
        Self(unsafe { &*ptr })
    }

    /// Create a new [`UnsafeCellSlice`] from the spare capacity in a [`Vec`].
    #[must_use]
    pub fn new_from_vec_with_spare_capacity(vec: &'a mut Vec<T>) -> Self {
        Self::new(unsafe { vec_spare_capacity_to_mut_slice(vec) })
    }

    /// Get a mutable reference to the underlying slice.
    ///
    /// # Safety
    /// This returns a mutable reference to the underlying slice despite `self` being a non-mutable reference.
    /// This is unsafe because it can be called multiple times, thus creating multiple mutable references to the same data.
    /// It is the responsibility of the caller not to write to the same slice element from than one thread.
    #[must_use]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get(&self) -> &mut [T] {
        let ptr = self.0[0].get();
        std::slice::from_raw_parts_mut(ptr, self.0.len())
    }

    /// Get the length of the slice.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the slice has a length of 0.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
