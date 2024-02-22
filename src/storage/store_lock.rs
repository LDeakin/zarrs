//! Locks used by stores to acquire exclusive access to resources.
//!
//! [`StoreLocks`] implements [`StoreLocksTraits`] to request a locking primitive for a [`StoreKey`](crate::storage::StoreKey).
//! This is needed for some array operations, such as storing an array or chunk subset.
//!
//! Currently, the only abstract locking primitive is a [`StoreKeyMutex`] with a [`StoreKeyMutexGuard`] RAII guard.
//! The lock implementations include:
//!  - [`DefaultStoreLocks`] (with [`DefaultStoreMutex`]) implement [`parking_lot::Mutex`]-based locking in a single process.
//!    - Used by default in stores.
//!    - Async variants use [`async_lock::Mutex`].
//!  - [`DisabledStoreLocks`] (with [`DisabledStoreMutex`]) and their async variants disable locks for potentially improved performance.
//!    - **Requires careful usage of [`Array`](crate::array::Array) to maintain data integrity** (see [`Array`](crate::array::Array) for more information).
//!
//! Specialised locks are planned for distributed applications.

#[cfg(feature = "async")]
pub mod store_lock_async;
pub mod store_lock_sync;

pub use store_lock_sync::{
    StoreKeyMutex, StoreKeyMutexGuard, StoreKeyMutexGuardTraits, StoreKeyMutexTraits, StoreLocks,
    StoreLocksTraits,
};

#[cfg(feature = "async")]
pub use store_lock_async::{
    AsyncStoreKeyMutex, AsyncStoreKeyMutexGuard, AsyncStoreKeyMutexGuardTraits,
    AsyncStoreKeyMutexTraits, AsyncStoreLocks, AsyncStoreLocksTraits,
};

#[cfg(feature = "async")]
pub use store_lock_async::{
    default_async::{AsyncDefaultStoreLocks, AsyncDefaultStoreMutex, AsyncDefaultStoreMutexGuard},
    disabled_async::{
        AsyncDisabledStoreLocks, AsyncDisabledStoreMutex, AsyncDisabledStoreMutexGuard,
    },
};
pub use store_lock_sync::{
    default_sync::{DefaultStoreLocks, DefaultStoreMutex, DefaultStoreMutexGuard},
    disabled_sync::{DisabledStoreLocks, DisabledStoreMutex, DisabledStoreMutexGuard},
};
