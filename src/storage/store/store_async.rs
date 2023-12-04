pub mod filesystem_store;
pub mod memory_store;
pub mod object_store;

#[cfg(feature = "s3")]
pub mod amazon_s3_store;
#[cfg(feature = "gcp")]
pub mod google_cloud_store;
#[cfg(feature = "http")]
pub mod http_store;
#[cfg(feature = "azure")]
pub mod microsoft_azure_store;
