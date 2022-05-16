use std::sync::Arc;

use crate::{
    message_pages::{CompressedCluster, CompressedClusterId},
    sub_page::{SubPage, SubPageId},
};

pub struct ReadFromCompressedClusters {
    compressed_cluster: Option<Arc<CompressedCluster>>,
    pub cluster_id: CompressedClusterId,
}

impl ReadFromCompressedClusters {
    pub fn new(
        compressed_cluster: Option<Arc<CompressedCluster>>,
        cluster_id: CompressedClusterId,
    ) -> Self {
        Self {
            compressed_cluster,
            cluster_id,
        }
    }

    pub fn get_sub_page(&self, sub_page_id: &SubPageId) -> Option<Arc<SubPage>> {
        let compressed_cluster = self.compressed_cluster.as_ref()?;

        let result = compressed_cluster.get_sub_page(sub_page_id)?;

        return Some(Arc::new(result));
    }
}
