use std::{sync::Arc, time::Duration};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};
use rust_extensions::StopWatch;
use tokio::sync::Mutex;

use crate::{message_pages::PageMetrics, page_blob_random_access::PageBlobRandomAccess};

use super::{NewMessages, UncompressedPageData};

pub struct UncompressedPage {
    page_data: Mutex<UncompressedPageData>,
    pub page_id: PageId,
    pub metrics: PageMetrics,
    pub new_messages: NewMessages,
}

impl UncompressedPage {
    pub async fn new(page_id: PageId, page_blob: PageBlobRandomAccess) -> Self {
        Self {
            page_data: Mutex::new(UncompressedPageData::new(page_id, page_blob).await),
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
        let mut write_access = self.page_data.lock().await;
        write_access.get(message_id).await
    }

    pub async fn get_all(&self) -> Vec<Arc<MessageProtobufModel>> {
        let read_access = self.page_data.lock().await;
        return read_access.get_all();
    }
    pub async fn get_range(
        &self,
        from_id: MessageId,
        to_id: MessageId,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let read_access = self.page_data.lock().await;
        return read_access.get_range(from_id, to_id);
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

        let messages_to_persist = messages_to_persist.unwrap();

        {
            let mut write_access = self.page_data.lock().await;
            write_access
                .persist_messages(messages_to_persist.as_slice())
                .await;
        }

        self.new_messages
            .confirm_persisted(messages_to_persist.as_slice())
            .await;

        sw.pause();

        sw.duration()
    }
}
