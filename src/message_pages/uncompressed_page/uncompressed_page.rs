use std::{sync::Arc, time::Duration};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};
use rust_extensions::StopWatch;
use tokio::sync::Mutex;

use crate::{
    message_pages::PageMetrics,
    page_blob_random_access::RandomAccessData,
    uncompressed_page_storage::{PayloadsToUploadContainer, UncompressedPageStorage},
};

use super::{NewMessages, UncompressedPageData};

pub struct UncompressedPage {
    page_data: Mutex<UncompressedPageData>,
    pub page_id: PageId,
    pub metrics: PageMetrics,
    pub new_messages: NewMessages,
}

impl UncompressedPage {
    pub async fn new(page_id: PageId, storage: UncompressedPageStorage) -> Self {
        Self {
            page_data: Mutex::new(UncompressedPageData::new(page_id, storage).await),
            page_id,
            metrics: PageMetrics::new(),
            new_messages: NewMessages::new(),
        }
    }

    pub async fn restore_message(&self, message: MessageProtobufModel) -> MessageProtobufModel {
        let result = message.clone();
        let mut write_access = self.page_data.lock().await;
        write_access.add_message(message);
        result
    }

    pub fn has_messages_to_save(&self) -> bool {
        self.metrics.get_messages_amount_to_save() > 0
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let messages_to_save = self.new_messages.add_messages(messages).await;

        self.metrics
            .update_messages_amount_to_save(messages_to_save);

        self.metrics.update_last_access_to_now();
    }

    pub async fn get_message(&self, message_id: MessageId) -> Option<Arc<MessageProtobufModel>> {
        let read_access = self.page_data.lock().await;
        read_access.get(message_id)
    }

    pub async fn get_grpc_v0_snapshot(&self) -> Vec<Arc<MessageProtobufModel>> {
        let read_access = self.page_data.lock().await;
        return read_access.get_grpc_v0_snapshot();
    }

    pub async fn read_content(&self, message_id: MessageId) -> Option<RandomAccessData> {
        let mut write_access = self.page_data.lock().await;

        let offset = write_access.get_message_offset(message_id);

        if !offset.has_data() {
            return None;
        }

        let content = write_access.storage.read_content(&offset).await;

        Some(content)
    }

    pub async fn flush_to_storage(&self, max_persist_size: usize) -> Duration {
        let mut sw = StopWatch::new();
        sw.start();

        let messages_to_persist = self
            .new_messages
            .get_messages_to_persist(max_persist_size)
            .await;

        if messages_to_persist.is_none() {
            sw.pause();
            return sw.duration();
        }

        let messages_to_persist = messages_to_persist.as_ref().unwrap().as_slice();

        {
            let mut write_access = self.page_data.lock().await;

            let write_position = write_access.toc.get_write_position();

            let mut upload_container = PayloadsToUploadContainer::new(write_position);

            for msg_to_persist in messages_to_persist {
                upload_container.append(self.page_id, msg_to_persist);
            }

            write_access
                .storage
                .append_payload(upload_container)
                .await
                .unwrap();
        }

        self.new_messages
            .confirm_persisted(messages_to_persist)
            .await;

        sw.pause();

        sw.duration()
    }
}
