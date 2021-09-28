use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    compressed_pages::{ClusterPageId, PagesClusterBlobRw, ReadCompressedPageError},
    message_pages::MessagePageId,
};

use super::AppContext;

pub struct CurrentPagesCluster {
    pub data: Mutex<Option<PagesClusterBlobRw>>,
    app: Arc<AppContext>,
    topic_id: String,
}

impl CurrentPagesCluster {
    pub fn new(app: Arc<AppContext>, topic_id: String) -> Self {
        Self {
            data: Mutex::new(None),
            app,
            topic_id,
        }
    }

    pub async fn write(&self, page_id: MessagePageId, payload: &[u8]) {
        self.update_current_cluster(page_id).await;

        let mut page_cluster = self.data.lock().await;

        if let Some(cluster) = &mut *page_cluster {
            cluster.write(page_id, payload).await;
        }
    }

    pub async fn read(
        &self,
        page_id: MessagePageId,
    ) -> Result<Option<Vec<u8>>, ReadCompressedPageError> {
        self.update_current_cluster(page_id).await;

        let mut page_cluster = self.data.lock().await;

        if let Some(cluster) = &mut *page_cluster {
            return cluster.read(page_id).await;
        }

        return Ok(None);
    }

    async fn update_current_cluster(&self, page_id: MessagePageId) {
        let cluster_page_id = ClusterPageId::from_page_id(&page_id);

        let mut write_access = self.data.lock().await;

        if let Some(blob) = &mut *write_access {
            if blob.cluster_page_id.value == cluster_page_id.value {
                return;
            }
        }

        let blob = PagesClusterBlobRw::new(
            self.app.messages_connection.clone(),
            self.topic_id.as_str(),
            cluster_page_id,
        );
        *write_access = Some(blob);
        return;
    }
}
