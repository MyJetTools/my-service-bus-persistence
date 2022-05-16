use std::{sync::Arc, time::Duration};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};
use rust_extensions::StopWatch;
use tokio::sync::Mutex;

use crate::{
    page_blob_random_access::PageBlobRandomAccess,
    sub_page::{SubPage, SubPageId},
};

use super::{NewMessages, PageMetrics, UncompressedPageData, UncompressedPageId};

pub struct FlushToStorageResult {
    pub duration: Duration,
    pub messages_to_save: usize,
    pub last_saved_chunk: usize,
    pub last_saved_message_id: MessageId,
}

pub struct UncompressedPage {
    page_data: Mutex<UncompressedPageData>,
    pub page_id: UncompressedPageId,
    pub new_messages: NewMessages,

    pub metrics: PageMetrics,
}

impl UncompressedPage {
    pub async fn new(
        page_id: PageId,
        page_blob: PageBlobRandomAccess,
        max_message_size_protection: usize,
    ) -> Self {
        let uncompressed_page =
            UncompressedPageData::new(page_id, page_blob, max_message_size_protection).await;

        let messages_count = uncompressed_page.toc.get_messages_count();
        let write_position = uncompressed_page.toc.get_write_position();

        Self {
            page_data: Mutex::new(uncompressed_page),
            page_id: UncompressedPageId::new(page_id),
            new_messages: NewMessages::new(),
            metrics: PageMetrics::new(messages_count, write_position),
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let amount = self.new_messages.add_messages(messages.as_slice()).await;
        self.metrics.update_messages_amount_to_save(amount);
        self.metrics.update_last_access_to_now();

        let mut page_data = self.page_data.lock().await;
        page_data.add_messages(messages).await;
    }

    pub async fn get_sub_page(&self, sub_page_id: &SubPageId) -> Option<Arc<SubPage>> {
        let page_data = self.page_data.lock().await;
        page_data.get_sub_page(sub_page_id)
    }

    pub async fn get_or_restore_sub_page(&self, sub_page_id: &SubPageId) -> Option<Arc<SubPage>> {
        let mut page_data = self.page_data.lock().await;
        page_data.get_or_restore_sub_page(sub_page_id).await
    }

    pub async fn flush_to_storage(&self, max_persist_size: usize) -> Option<FlushToStorageResult> {
        let mut sw = StopWatch::new();
        sw.start();

        let messages_to_persist = self
            .new_messages
            .get_messages_to_persist(max_persist_size)
            .await;

        if messages_to_persist.is_none() {
            sw.pause();
            return None;
        }

        let messages_to_persist = messages_to_persist.unwrap();

        let (persisted_size, last_saved_message_id) = {
            let mut write_access = self.page_data.lock().await;
            let result = write_access
                .persist_messages(messages_to_persist.as_slice())
                .await;

            self.metrics
                .update_messages_count(write_access.toc.get_messages_count());

            let write_pos = write_access.toc.get_write_position();
            println!(
                "Write pos {}. Messages: {}",
                write_pos,
                messages_to_persist.len()
            );

            self.metrics.update_write_position(write_pos);

            result
        };

        let amount = self
            .new_messages
            .confirm_persisted(messages_to_persist.as_slice())
            .await;

        self.metrics.update_messages_amount_to_save(amount);

        sw.pause();

        FlushToStorageResult {
            duration: sw.duration(),
            messages_to_save: self.new_messages.get_count().await,
            last_saved_chunk: persisted_size,
            last_saved_message_id,
        }
        .into()
    }
}
