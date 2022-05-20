use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};
use rust_extensions::StopWatch;
use tokio::sync::Mutex;

use crate::page_blob_random_access::PageBlobRandomAccess;

use super::{NewMessages, UncompressedPageData};

pub struct FlushToStorageResult {
    pub duration: Duration,
    pub messages_to_save: usize,
    pub last_saved_chunk: usize,
    pub last_saved_message_id: MessageId,
}

pub struct UncompressedPage {
    page_data: Mutex<UncompressedPageData>,
    pub page_id: PageId,
    pub new_messages: NewMessages,
    pub messages_count: AtomicUsize,
    pub write_position: AtomicUsize,
    pub has_skipped_messages: AtomicBool, //TODO Plug has skipped messages
    pub messages_amount_to_save: AtomicUsize,
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
            page_id,

            new_messages: NewMessages::new(),
            messages_count: AtomicUsize::new(messages_count),
            has_skipped_messages: AtomicBool::new(false),
            write_position: AtomicUsize::new(write_position),
            messages_amount_to_save: AtomicUsize::new(0),
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let amount = self.new_messages.add_messages(messages).await;
        self.messages_amount_to_save.store(amount, Ordering::SeqCst);
    }

    pub async fn get_message(&self, message_id: MessageId) -> Option<Arc<MessageProtobufModel>> {
        let mut page_data_access = self.page_data.lock().await;
        page_data_access.get(message_id).await
    }

    pub async fn get_all(
        &self,
        current_message_id: Option<MessageId>,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let mut page_data_access = self.page_data.lock().await;
        return page_data_access.get_all(current_message_id).await;
    }
    pub async fn get_range(
        &self,
        from_id: MessageId,
        to_id: MessageId,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let mut page_data_access = self.page_data.lock().await;
        return page_data_access.get_range(from_id, to_id, None).await;
    }

    pub async fn read_from_message_id(
        &self,
        from_message_id: MessageId,
        max_amount: usize,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let mut page_data_access = self.page_data.lock().await;
        return page_data_access
            .read_from_message_id(from_message_id, max_amount)
            .await;
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

            self.messages_count
                .store(write_access.toc.get_messages_count(), Ordering::SeqCst);

            let write_pos = write_access.toc.get_write_position();

            self.write_position.store(write_pos, Ordering::SeqCst);

            result
        };

        let amount = self
            .new_messages
            .confirm_persisted(messages_to_persist.as_slice())
            .await;

        self.messages_amount_to_save.store(amount, Ordering::SeqCst);

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
