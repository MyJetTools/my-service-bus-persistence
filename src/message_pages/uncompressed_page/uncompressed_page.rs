use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};
use tokio::sync::RwLock;

use crate::{message_pages::PageMetrics, uncompressed_page_storage::toc::UncompressedFileToc};

use super::UncompressedPageData;

pub struct UncompressedPage {
    pub pages: RwLock<UncompressedPageData>,
    pub page_id: PageId,
    pub metrics: PageMetrics,
}

impl UncompressedPage {
    pub fn new(page_id: PageId, toc: UncompressedFileToc) -> Self {
        Self {
            pages: RwLock::new(UncompressedPageData::new(page_id, toc)),
            page_id,
            metrics: PageMetrics::new(),
        }
    }

    pub fn has_messages_to_save(&self) -> bool {
        self.metrics.get_messages_amount_to_save() > 0
    }

    pub async fn get_write_position(&self) -> usize {
        self.pages.read().await.toc.get_write_position()
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let messages_to_save = {
            let mut write_access = self.pages.write().await;

            write_access.add(messages);

            write_access.queue_to_save.len() as usize
        };

        self.metrics
            .update_messages_amount_to_save(messages_to_save);

        self.metrics.update_last_access_to_now();
    }

    pub async fn get_message(&self, message_id: MessageId) -> Option<MessageProtobufModel> {
        let read_access = self.pages.read().await;
        let result = read_access.get(message_id)?;
        return result.clone().into();
    }

    pub async fn get_grpc_v0_snapshot(&self) -> Vec<MessageProtobufModel> {
        let read_access = self.pages.read().await;

        return read_access.get_grpc_v0_snapshot();
    }

    pub async fn get_messages_to_persist(&self, max_size: usize) -> Vec<MessageProtobufModel> {
        let read_access = self.pages.read().await;

        let mut result = Vec::new();

        let mut messages_size = 0;

        for msg_id in &read_access.queue_to_save {
            if let Some(msg) = read_access.get(msg_id) {
                if result.len() == 0 || messages_size + msg.data.len() <= max_size {
                    messages_size += msg.data.len();
                    result.push(msg.clone());
                }
            }
        }

        result
    }

    pub async fn confirm_persisted(&self, messages: &[MessageProtobufModel]) {
        let mut write_access = self.pages.write().await;

        for message in messages {
            write_access.queue_to_save.remove(message.message_id);
        }
    }
}
