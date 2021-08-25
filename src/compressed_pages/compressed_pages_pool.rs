use std::sync::Arc;

use my_azure_storage_sdk::AzureConnection;

use crate::{
    app::{AppError, Logs},
    message_pages::MessagesPage,
    utils::LazyObjectsHashMap,
};

use super::{ClusterPageId, CompressedPage, PagesClusterAzureBlob};

pub struct CompressedPagesPool {
    pub clusters_by_topic: LazyObjectsHashMap<String, PagesClusterAzureBlob>,
    connection: AzureConnection,
}

impl CompressedPagesPool {
    pub fn new(connection: AzureConnection) -> Self {
        Self {
            clusters_by_topic: LazyObjectsHashMap::new(),
            connection,
        }
    }

    pub async fn compress_page(
        &self,
        page: &MessagesPage,
        logs: Arc<Logs>,
    ) -> Result<(), AppError> {
        let compressed_page = CompressedPage::from_messages_page(page).await?;
        let cluster_page_id = ClusterPageId::from_page_id(&page.page_id);

        let blob = self
            .clusters_by_topic
            .get(&page.topic_id, || {
                PagesClusterAzureBlob::new(
                    self.connection.clone(),
                    page.topic_id.as_str(),
                    cluster_page_id,
                    logs.clone(),
                )
            })
            .await;

        if blob.cluster_page_id.value != cluster_page_id.value {
            return Ok(());
        }

        blob.write(page.page_id, &compressed_page.zip).await
    }
}
