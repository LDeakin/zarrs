# zarrs_zip

[![Latest Version](https://img.shields.io/crates/v/zarrs_zip.svg)](https://crates.io/crates/zarrs_zip)
[![zarrs_zip documentation](https://docs.rs/zarrs_zip/badge.svg)](https://docs.rs/zarrs_zip)
![msrv](https://img.shields.io/crates/msrv/zarrs_zip)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)

A storage adapter for `zip` files for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.

```rust
use zarrs_storage::StoreKey;
use zarrs_filesystem::FilesystemStore;
use zarrs_zip::ZipStorageAdapter;

let fs_root = PathBuf::from("/path/to/a/directory");
let fs_store = Arc::new(FilesystemStore::new(&fs_root)?);
let zip_key = StoreKey::new("zarr.zip")?;
let zip_store = Arc::new(ZipStorageAdapter::new(fs_store, zip_key)?);
```

## Licence
`zarrs_zip` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
