use crate::compressed_page::*;
use crate::{
    app::Logs,
    sub_page::{SubPage, SubPageId},
};
use std::sync::Arc;

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

    pub async fn get_sub_page(
        &self,
        sub_page_id: &SubPageId,
        topic_id: &str,
        logs: &Logs,
    ) -> Option<SubPage> {
        let compressed_cluster = self.compressed_cluster.as_ref()?;

        match compressed_cluster.get_sub_page(sub_page_id).await {
            Ok(result) => result,
            Err(err) => {
                logs.add_error_str(
                    Some(topic_id),
                    "Reading Compressed subpage",
                    "Can not read compressed subpage".to_string(),
                    format!("{:?}", err),
                );
                None
            }
        }
    }
}
