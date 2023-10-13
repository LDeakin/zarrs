/// An unsafe cell slice.
///
/// This is used internally for parallel chunk decoding.
/// It is used to write to subsets of a slice from multiple threads without locking.
#[derive(Copy, Clone)]
pub struct UnsafeCellSlice<'a, T>(&'a [std::cell::UnsafeCell<T>]);

unsafe impl<'a, T: Send + Sync> Send for UnsafeCellSlice<'a, T> {}
unsafe impl<'a, T: Send + Sync> Sync for UnsafeCellSlice<'a, T> {}

impl<'a, T: Copy> UnsafeCellSlice<'a, T> {
    pub fn new(slice: &'a mut [T]) -> Self {
        let ptr = slice as *mut [T] as *const [std::cell::UnsafeCell<T>];
        Self(unsafe { &*ptr })
    }

    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get(&self) -> &mut [T] {
        let ptr = self.0[0].get();
        std::slice::from_raw_parts_mut(ptr, self.0.len())
    }
}
