| Store/Storage Adapter              | ZEP    | Read     | Write    | List     | Sync    | Async   | Crate                          |
| ---------------------------------- | ------ | -------- | -------- | -------- | ------- | ------- | ------------------------------ |
| [MemoryStore]                      |        | &check;  | &check;  | &check;  | &check; |         | [zarrs_storage]<sup>†</sup>    |
| [FilesystemStore]                  | [0001] | &check;  | &check;  | &check;  | &check; |         | [zarrs_filesystem]<sup>‡</sup> |
| [OpendalStore]                     |        | &check;* | &check;* | &check;* | &check; |         | [zarrs_opendal]                |
| [AsyncOpendalStore]                |        | &check;* | &check;* | &check;* |         | &check; | [zarrs_opendal]                |
| [AsyncObjectStore]                 |        | &check;* | &check;* | &check;* |         | &check; | [zarrs_object_store]           |
| [HTTPStore]                        |        | &check;  |          |          | &check; |         | [zarrs_http]                   |
| [AsyncToSyncStorageAdapter]        |        | &check;  | &check;  | &check;  | &check; | &check; | [zarrs_storage]<sup>†</sup>    |
| [UsageLogStorageAdapter]           |        | &check;  | &check;  | &check;  | &check; | &check; | [zarrs_storage]<sup>†</sup>    |
| [PerformanceMetricsStorageAdapter] |        | &check;  | &check;  | &check;  | &check; | &check; | [zarrs_storage]<sup>†</sup>    |
| [ZipStorageAdapter]                |        | &check;  |          | &check;  | &check; |         | [zarrs_zip]                    |

<sup>† Re-exported in the `zarrs::storage` module</sup>
<br>
<sup>‡ Re-exported as the `zarrs::filesystem` module</sup>
<br>
<sup>\* Support depends on the [`opendal`](https://docs.rs/opendal/latest/opendal/) [`BlockingOperator`](https://docs.rs/opendal/latest/opendal/struct.BlockingOperator.html)/[`Operator`](https://docs.rs/opendal/latest/opendal/struct.Operator.html) or [`object_store`](https://docs.rs/object_store/latest/object_store/) [store](https://docs.rs/object_store/latest/object_store/index.html#modules).</sup>

[0001]: https://zarr.dev/zeps/accepted/ZEP0001.html

[zarrs_storage]: https://docs.rs/zarrs_storage/latest/zarrs_storage/
[zarrs_filesystem]: https://docs.rs/zarrs_filesystem/latest/zarrs_filesystem/
[zarrs_object_store]: https://docs.rs/zarrs_object_store/latest/zarrs_object_store/
[zarrs_opendal]: https://docs.rs/zarrs_opendal/latest/zarrs_opendal/
[zarrs_http]: https://docs.rs/zarrs_http/latest/zarrs_http/
[zarrs_zip]: https://docs.rs/zarrs_zip/latest/zarrs_zip/

[MemoryStore]: https://docs.rs/zarrs_storage/latest/zarrs_storage/store/struct.MemoryStore.html
[FilesystemStore]: https://docs.rs/zarrs_filesystem/latest/zarrs_filesystem/struct.FilesystemStore.html
[OpendalStore]: https://docs.rs/zarrs_opendal/latest/zarrs_opendal/struct.OpendalStore.html
[AsyncOpendalStore]: https://docs.rs/zarrs_opendal/latest/zarrs_opendal/struct.AsyncOpendalStore.html
[AsyncObjectStore]: https://docs.rs/zarrs_object_store/latest/zarrs_object_store/struct.AsyncObjectStore.html
[HTTPStore]: https://docs.rs/zarrs_http/latest/zarrs_http/struct.HTTPStore.html

[AsyncToSyncStorageAdapter]: crate::storage::storage_adapter::async_to_sync::AsyncToSyncStorageAdapter
[UsageLogStorageAdapter]: crate::storage::storage_adapter::usage_log::UsageLogStorageAdapter
[PerformanceMetricsStorageAdapter]: crate::storage::storage_adapter::performance_metrics::PerformanceMetricsStorageAdapter
[ZipStorageAdapter]: https://docs.rs/zarrs_zip/latest/zarrs_zip/struct.ZipStorageAdapter.html
