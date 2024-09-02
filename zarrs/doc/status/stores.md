| Store/Storage Adapter                                                                                           | ZEP                                                    | Read     | Write    | List     | Sync    | Async   | Feature/Crate |
| --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | -------- | -------- | -------- | ------- | ------- | ------------- |
| [`FilesystemStore`](crate::storage::store::FilesystemStore)                                                     | [ZEP0001](https://zarr.dev/zeps/accepted/ZEP0001.html) | &check;  | &check;  | &check;  | &check; |         |               |
| [`MemoryStore`](crate::storage::store::MemoryStore)                                                             |                                                        | &check;  | &check;  | &check;  | &check; |         |               |
| [`HTTPStore`](crate::storage::store::HTTPStore)                                                                 |                                                        | &check;  |          |          | &check; |         | http          |
| [OpendalStore]                                                           |                                                        | &check;* | &check;* | &check;* | &check; |         | zarrs_opendal       |
| [AsyncOpendalStore]                                                 |                                                        | &check;* | &check;* | &check;* |         | &check; | zarrs_opendal       |
| [AsyncObjectStore]                                              |                                                        | &check;* | &check;* | &check;* |         | &check; | zarrs_object_store  |
| [`ZipStorageAdapter`](crate::storage::storage_adapter::zip::ZipStorageAdapter)                                  |                                                        | &check;  |          | &check;  | &check; |         | zip           |
| [`AsyncToSyncStorageAdapter`](crate::storage::storage_adapter::async_to_sync::AsyncToSyncStorageAdapter)        |                                                        | &check;  | &check;  | &check;  | &check; |         | async         |

<sup>\* Support depends on the [`opendal`] [`BlockingOperator`](https://docs.rs/opendal/latest/opendal/struct.BlockingOperator.html)/[`Operator`](https://docs.rs/opendal/latest/opendal/struct.Operator.html) or [`object_store`] [store](https://docs.rs/object_store/latest/object_store/index.html#modules).</sup>

[OpendalStore]:https://docs.rs/zarrs/latest/zarrs_opendal/array/struct.OpendalStore.html
[AsyncOpendalStore]:https://docs.rs/zarrs/latest/zarrs_opendal/array/struct.AsyncOpendalStore.html
[AsyncObjectStore]:https://docs.rs/zarrs/latest/zarrs_object_store/array/struct.AsyncObjectStore.html
