| Store/Storage Adapter                                                          | ZEP                                                    | Read     | Write    | List     | Sync    | Async   | Feature Flag |
| ------------------------------------------------------------------------------ | ------------------------------------------------------ | -------- | -------- | -------- | ------- | ------- | ------------ |
| [`FilesystemStore`](crate::storage::store::FilesystemStore)                    | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html) | &check;  | &check;  | &check;  | &check; |         |              |
| [`MemoryStore`](crate::storage::store::MemoryStore)                            |                                                        | &check;  | &check;  | &check;  | &check; |         |              |
| [`HTTPStore`](crate::storage::store::HTTPStore)                                |                                                        | &check;  |          |          | &check; |         | http         |
| [`OpendalStore`](crate::storage::store::OpendalStore)                          |                                                        | &check;* | &check;* | &check;* | &check; |         | opendal      |
| [`AsyncOpendalStore`](crate::storage::store::AsyncOpendalStore)                |                                                        | &check;* | &check;* | &check;* |         | &check; | opendal      |
| [`AsyncObjectStore`](crate::storage::store::AsyncObjectStore)                  |                                                        | &check;* | &check;* | &check;* |         | &check; | object_store |
| [`ZipStorageAdapter`](crate::storage::storage_adapter::zip::ZipStorageAdapter) |                                                        | &check;  |          | &check;  | &check; |         | zip          |

<sup>\* Support depends on the [`opendal`] [`BlockingOperator`](opendal::BlockingOperator)/[`Operator`](opendal::Operator) or [`object_store`] [store](https://docs.rs/object_store/latest/object_store/index.html#modules).</sup>
