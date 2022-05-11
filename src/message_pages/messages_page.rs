use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use crate::page_blob_random_access::PageBlobRandomAccess;

use super::{uncompressed_page::FlushToStorageResult, BlankPage, PageMetrics, UncompressedPage};

pub enum MessagesPageType {
    Uncompressed(UncompressedPage),
    Empty(BlankPage),
}

pub struct MessagesPage {
    pub page_type: MessagesPageType,
    pub metrics: PageMetrics,
}

impl MessagesPage {
    pub fn create_as_empty(page_id: PageId) -> Self {
        Self {
            page_type: MessagesPageType::Empty(BlankPage::new(page_id)),
            metrics: PageMetrics::new(),
        }
    }

    pub async fn create_uncompressed(
        page_id: PageId,
        page_blob: PageBlobRandomAccess,
        max_message_size_protection: usize,
    ) -> Self {
        let result = Self {
            page_type: MessagesPageType::Uncompressed(
                UncompressedPage::new(page_id, page_blob, max_message_size_protection).await,
            ),
            metrics: PageMetrics::new(),
        };

        result
    }

    pub fn has_messages_to_save(&self) -> bool {
        self.get_messages_amount_to_save() > 0
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => {
                page.messages_amount_to_save.load(Ordering::SeqCst)
            }
            MessagesPageType::Empty(_) => 0,
        }
    }

    pub fn is_uncompressed(&self) -> bool {
        match &self.page_type {
            MessagesPageType::Uncompressed(_) => true,
            _ => false,
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let uncompressed_messages = self.unwrap_as_uncompressed_page();
        uncompressed_messages.new_messages(messages).await;

        self.metrics.update_last_access_to_now();
    }

    pub fn unwrap_as_uncompressed_page(&self) -> &UncompressedPage {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => page,
            _ => panic!("Can not get message as uncompressed. It's empty"),
        }
    }

    pub fn get_page_id(&self) -> PageId {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => page.page_id.value,
            MessagesPageType::Empty(page) => page.page_id,
        }
    }

    pub async fn get_message(&self, message_id: MessageId) -> Option<Arc<MessageProtobufModel>> {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => page.get_message(message_id).await,
            MessagesPageType::Empty(_) => None,
        }
    }

    pub async fn get_all(
        &self,
        current_message_id: Option<MessageId>,
    ) -> Vec<Arc<MessageProtobufModel>> {
        self.metrics.update_last_access_to_now();
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => page.get_all(current_message_id).await,
            MessagesPageType::Empty(_) => vec![],
        }
    }

    pub async fn get_range(
        &self,
        from_message_id: MessageId,
        to_message_id: MessageId,
    ) -> Vec<Arc<MessageProtobufModel>> {
        self.metrics.update_last_access_to_now();

        match &self.page_type {
            MessagesPageType::Uncompressed(page) => {
                page.get_range_incl_to_id(from_message_id, to_message_id)
                    .await
            }
            MessagesPageType::Empty(_) => vec![],
        }
    }

    pub async fn read_range(
        &self,
        from_message_id: MessageId,
        to_message_id: MessageId,
    ) -> BTreeMap<MessageId, MessageProtobufModel> {
        todo!("Implement");
    }

    pub async fn read_from_message_id(
        &self,
        from_message_id: MessageId,
        max_amount: usize,
    ) -> Vec<Arc<MessageProtobufModel>> {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => {
                page.read_from_message_id(from_message_id, max_amount).await
            }
            MessagesPageType::Empty(_) => vec![],
        }
    }

    pub fn get_messages_count(&self) -> usize {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => page.messages_count.load(Ordering::SeqCst),
            MessagesPageType::Empty(_) => 0,
        }
    }

    pub fn get_write_position(&self) -> usize {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => page.write_position.load(Ordering::SeqCst),
            MessagesPageType::Empty(_) => 0,
        }
    }

    pub fn has_skipped_messages(&self) -> bool {
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => {
                page.has_skipped_messages.load(Ordering::SeqCst)
            }
            MessagesPageType::Empty(_) => true,
        }
    }

    pub async fn flush_to_storage(&self, max_persist_size: usize) -> Option<FlushToStorageResult> {
        if let MessagesPageType::Uncompressed(page) = &self.page_type {
            if let Some(write_result) = page.flush_to_storage(max_persist_size).await {
                self.metrics.update_last_access_to_now();

                return Some(write_result);
            }
        }

        None
    }
}
