mod azure_storage;
mod cluster_page_id;
mod error;

pub use azure_storage::PagesClusterBlobRw;
pub use cluster_page_id::ClusterPageId;
pub use error::ReadCompressedPageError;
