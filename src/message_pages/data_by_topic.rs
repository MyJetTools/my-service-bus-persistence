use std::{collections::HashMap, sync::Arc};

use my_azure_page_blob_append::PageBlobAppendCacheError;
use my_azure_storage_sdk::AzureStorageError;
use my_service_bus_shared::date_time::DateTimeAsMicroseconds;
use tokio::sync::{Mutex, RwLock};

use crate::{
    app::AppContext, azure_storage::messages_page_blob::MessagesPageBlob,
    message_pages::MessagePageId,
};

use super::{messages_page::MessagesPageStorageType, MessagesPage, PageWriterMetrics};

pub struct DataByTopic {
    pub topic_id: String,
    pub pages: Mutex<HashMap<i64, Arc<MessagesPage>>>,
    pub metrics: RwLock<PageWriterMetrics>,
    pub app: Arc<AppContext>,
}

impl DataByTopic {
    pub fn new(topic_id: &str, app: Arc<AppContext>) -> DataByTopic {
        Self {
            topic_id: topic_id.to_string(),
            pages: Mutex::new(HashMap::new()),
            metrics: RwLock::new(PageWriterMetrics::new()),
            app,
        }
    }

    async fn try_get(&self, page_id: &MessagePageId) -> Option<Arc<MessagesPage>> {
        let pages_access = self.pages.lock().await;
        let result = pages_access.get(&page_id.value)?;

        Some(result.clone())
    }

    pub async fn get(&self, page_id: MessagePageId) -> Arc<MessagesPage> {
        let result = self.try_get(&page_id).await;
        if result.is_some() {
            return result.unwrap();
        }

        let result = crate::operations::message_pages_loader::get_from_compressed_and_uncompressed(
            self.app.clone(),
            self.topic_id.as_str(),
            page_id,
        )
        .await;

        let result = Arc::new(result);

        let mut pages_access = self.pages.lock().await;

        pages_access.insert(page_id.value, result.clone());

        return result;
    }

    pub async fn get_restore_or_create_uncompressed_only(
        &self,
        page_id: MessagePageId,
    ) -> Result<Arc<MessagesPage>, PageBlobAppendCacheError> {
        let result = self.try_get(&page_id).await;
        if result.is_some() {
            return Ok(result.unwrap());
        }

        let mut messages_page_blob =
            MessagesPageBlob::new(self.topic_id.to_string(), page_id.clone(), self.app.clone());

        let load_result = messages_page_blob.load().await;

        let messages_page = match load_result {
            Ok(messages) => MessagesPage::new(
                self.topic_id.as_str(),
                page_id.clone(),
                Some(messages_page_blob),
                Some(MessagesPageStorageType::UncompressedPage),
                messages,
            ),
            Err(err) => {
                if let PageBlobAppendCacheError::AzureStorageError(err) = err {
                    if let AzureStorageError::BlobNotFound = err {
                        self.app
                            .logs
                            .add_info_string(
                                Some(self.topic_id.as_str()),
                                "Restoring or creating page ",
                                format!(
                                    "No Page {} to resore. Reason {:?}. Creating the new one",
                                    page_id.value, err
                                ),
                            )
                            .await;

                        MessagesPage::new_empty(
                            self.topic_id.as_str(),
                            page_id.clone(),
                            Some(messages_page_blob),
                            Some(MessagesPageStorageType::UncompressedPage),
                        )
                    } else {
                        return Err(PageBlobAppendCacheError::AzureStorageError(err));
                    }
                } else {
                    return Err(err);
                }
            }
        };

        let mut pages_access = self.pages.lock().await;
        let result = Arc::new(messages_page);
        pages_access.insert(page_id.value, result.clone());
        return Ok(result);
    }

    pub async fn gc_if_needed(
        &self,
        active_pages: &[MessagePageId],
    ) -> Option<Vec<Arc<MessagesPage>>> {
        let now = DateTimeAsMicroseconds::now();

        let mut pages_write_access = self.pages.lock().await;

        let mut pages_to_gc = Vec::new();

        for page in pages_write_access.values() {
            if active_pages
                .iter()
                .all(|itm| itm.value != page.page_id.value)
            {
                let page_read_access = page.data.read().await;

                let page_read_access = page_read_access.get(0).unwrap();

                if now.seconds_before(page_read_access.last_access) > 30 {
                    pages_to_gc.push(page.page_id.value);
                }
            }
        }

        if pages_to_gc.len() == 0 {
            return None;
        }

        let mut result = Vec::new();

        for page_to_gc in pages_to_gc.drain(..) {
            let removed_page = pages_write_access.remove(&page_to_gc);
            if let Some(page) = removed_page {
                result.push(page);
            }

            self.app
                .logs
                .add_info(
                    Some(self.topic_id.as_str()),
                    "Page GC",
                    "Page is garbage collected",
                )
                .await;
        }

        Some(result)
    }

    pub async fn has_messages_to_save(&self) -> bool {
        let pages_access = self.pages.lock().await;

        for page in pages_access.values() {
            if page.has_messages_to_save().await {
                return true;
            }
        }

        false
    }

    pub async fn get_pages_with_data_to_save(&self) -> Vec<Arc<MessagesPage>> {
        let mut result = Vec::new();

        let pages_access = self.pages.lock().await;

        for page in pages_access.values() {
            let page_data = page.data.read().await;
            let page_data = page_data.get(0).unwrap();

            if page_data.queue_to_save.len() > 0 {
                result.push(page.clone());
            }
        }

        return result;
    }

    pub async fn get_metrics(&self) -> PageWriterMetrics {
        let metrics_access = self.metrics.read().await;
        return metrics_access.clone();
    }

    pub async fn get_queue_size(&self) -> usize {
        let pages_access = self.pages.lock().await;

        let mut result = 0;
        for page in pages_access.values() {
            result += page.get_messages_to_save_amount().await;
        }

        result
    }
}
