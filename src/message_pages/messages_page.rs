use std::sync::Arc;

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use crate::page_blob_random_access::PageBlobRandomAccess;

use super::{BlankPage, PageMetrics, UncompressedPage};

pub enum MessagesPageType {
    Uncompressed(UncompressedPage),
    Empty(BlankPage),
}

pub struct MessagesPage {
    page_type: MessagesPageType,
    pub metrics: PageMetrics,
}

impl MessagesPage {
    pub fn create_as_empty(page_id: PageId) -> Self {
        Self {
            page_type: MessagesPageType::Empty(BlankPage::new(page_id)),
            metrics: PageMetrics::new(),
        }
    }

    pub async fn create_uncompressed(page_id: PageId, page_blob: PageBlobRandomAccess) -> Self {
        Self {
            page_type: MessagesPageType::Uncompressed(
                UncompressedPage::new(page_id, page_blob).await,
            ),
            metrics: PageMetrics::new(),
        }
    }

    pub fn has_messages_to_save(&self) -> bool {
        self.get_messages_amount_to_save() > 0
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        self.metrics.get_messages_amount_to_save()
    }

    pub fn is_uncompressed(&self) -> bool {
        match &self.page_type {
            MessagesPageType::Uncompressed(_) => true,
            _ => false,
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let messages_to_save = {
            let uncompressed_messages = self.unwrap_as_uncompressed_page();
            uncompressed_messages.new_messages(messages).await
        };

        self.metrics
            .update_messages_amount_to_save(messages_to_save);

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
            MessagesPageType::Uncompressed(page) => page.page_id,
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
        match &self.page_type {
            MessagesPageType::Uncompressed(page) => {
                page.get_range(from_message_id, to_message_id).await
            }
            MessagesPageType::Empty(_) => vec![],
        }
    }
}
