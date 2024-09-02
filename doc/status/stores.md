| Store/Storage Adapter       | ZEP       | Read     | Write    | List     | Sync    | Async   | Feature/Crate         |
| --------------------------- | --------  | -------- | -------- | -------- | ------- | ------- | --------------------- |
| [FilesystemStore]           | [ZEP0001] | &check;  | &check;  | &check;  | &check; |         |                       |
| [MemoryStore]               |           | &check;  | &check;  | &check;  | &check; |         |                       |
| [HTTPStore]                 |           | &check;  |          |          | &check; |         | http                  |
| [OpendalStore]              |           | &check;* | &check;* | &check;* | &check; |         | [zarrs_opendal]       |
| [AsyncOpendalStore]         |           | &check;* | &check;* | &check;* |         | &check; | [zarrs_opendal]       |
| [AsyncObjectStore]          |           | &check;* | &check;* | &check;* |         | &check; | [zarrs_object_store]  |
| [ZipStorageAdapter]         |           | &check;  |          | &check;  | &check; |         | zip                   |
| [AsyncToSyncStorageAdapter] |           | &check;  | &check;  | &check;  | &check; |         | async                 |

<sup>\* Support depends on the [`opendal`](https://docs.rs/opendal/latest/opendal/) [`BlockingOperator`](https://docs.rs/opendal/latest/opendal/struct.BlockingOperator.html)/[`Operator`](https://docs.rs/opendal/latest/opendal/struct.Operator.html) or [`object_store`](https://docs.rs/object_store/latest/object_store/) [store](https://docs.rs/object_store/latest/object_store/index.html#modules).</sup>

[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html

[FilesystemStore]: crate::storage::store::FilesystemStore
[MemoryStore]: crate::storage::store::MemoryStore
[HTTPStore]: crate::storage::store::HTTPStore
[ZipStorageAdapter]: crate::storage::storage_adapter::zip::ZipStorageAdapter
[AsyncToSyncStorageAdapter]: crate::storage::storage_adapter::async_to_sync::AsyncToSyncStorageAdapter

[zarrs_object_store]: https://docs.rs/zarrs_object_store/latest/zarrs_object_store/
[zarrs_opendal]: https://docs.rs/zarrs_opendal/latest/zarrs_opendal/
[OpendalStore]: https://docs.rs/zarrs_opendal/latest/zarrs_opendal/struct.OpendalStore.html
[AsyncOpendalStore]: https://docs.rs/zarrs_opendal/latest/zarrs_opendal/struct.AsyncOpendalStore.html
[AsyncObjectStore]: https://docs.rs/zarrs_object_store/latest/zarrs_object_store/struct.AsyncObjectStore.html
