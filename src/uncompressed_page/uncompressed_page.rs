use std::{sync::Arc, time::Duration};

use my_service_bus_shared::{
    protobuf_models::MessageProtobufModel,
    sub_page::{SubPage, SubPageId},
    MessageId,
};
use rust_extensions::{lazy::LazyVec, StopWatch};
use tokio::sync::Mutex;

use crate::page_blob_random_access::PageBlobRandomAccess;

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
        page_id: UncompressedPageId,
        page_blob: PageBlobRandomAccess,
        max_message_size_protection: usize,
    ) -> Self {
        let uncompressed_page =
            UncompressedPageData::new(page_id.clone(), page_blob, max_message_size_protection)
                .await;

        let messages_count = uncompressed_page.toc.get_messages_count();
        let write_position = uncompressed_page.toc.get_write_position();

        Self {
            page_data: Mutex::new(uncompressed_page),
            page_id,
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

    pub async fn get_or_restore_sub_page(&self, sub_page_id: &SubPageId) -> Option<Arc<SubPage>> {
        let mut page_data = self.page_data.lock().await;
        page_data.get_or_restore_sub_page(sub_page_id).await
    }

    pub async fn sub_get_page_ids(&self) -> Vec<i16> {
        let mut result = Vec::new();

        let page_data = self.page_data.lock().await;

        let first_sub_page_id = self.page_id.get_first_sub_page_id();

        for sub_page_no in page_data.sub_pages.keys() {
            let id = sub_page_no - first_sub_page_id.value;
            result.push(id as i16);
        }

        result
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

    pub async fn gc_sub_pages(
        &self,
        current_page_id: &SubPageId,
        except_this_page: Option<&SubPageId>,
    ) {
        let mut write_access = self.page_data.lock().await;

        let mut pages_to_gc = LazyVec::new();

        for key in write_access.sub_pages.keys() {
            if *key == current_page_id.value {
                continue;
            }

            if let Some(except_this_page) = except_this_page {
                if *key == except_this_page.value {
                    continue;
                }
            }
            pages_to_gc.add(*key);
        }

        if let Some(pages_to_gc) = pages_to_gc.get_result() {
            for page_to_gc in &pages_to_gc {
                write_access.sub_pages.remove(page_to_gc);
            }
        }
    }
}
