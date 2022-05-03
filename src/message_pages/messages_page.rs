use std::sync::Arc;

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use crate::page_blob_random_access::PageBlobRandomAccess;

use super::{BlankPage, UncompressedPage};

pub enum MessagesPage {
    Uncompressed(UncompressedPage),
    Empty(BlankPage),
}

impl MessagesPage {
    pub fn create_as_empty(page_id: PageId) -> Self {
        Self::Empty(BlankPage::new(page_id))
    }

    pub fn get_metrics_percent(&self) -> usize {
        match self {
            MessagesPage::Uncompressed(page) => page.metrics.get_precent(),
            MessagesPage::Empty(_) => 0,
        }
    }

    pub fn get_messages_count(&self) -> usize {
        match self {
            MessagesPage::Uncompressed(page) => page.metrics.get_messages_count(),
            MessagesPage::Empty(_) => 0,
        }
    }

    pub fn get_metrics_skipped_messages(&self) -> bool {
        match self {
            MessagesPage::Uncompressed(page) => page.metrics.get_has_skipped_messages(),
            MessagesPage::Empty(_) => true,
        }
    }

    pub fn get_metrics_blob_position(&self) -> usize {
        match self {
            MessagesPage::Uncompressed(page) => page.metrics.get_blob_position(),
            MessagesPage::Empty(_) => 0,
        }
    }

    pub async fn create_uncompressed(page_id: PageId, page_blob: PageBlobRandomAccess) -> Self {
        Self::Uncompressed(UncompressedPage::new(page_id, page_blob).await)
    }

    pub fn has_messages_to_save(&self) -> bool {
        match self {
            MessagesPage::Uncompressed(page) => page.has_messages_to_save(),
            MessagesPage::Empty(_) => false,
        }
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        match self {
            MessagesPage::Uncompressed(page) => page.metrics.get_messages_amount_to_save(),
            MessagesPage::Empty(_) => 0,
        }
    }

    pub fn is_uncompressed(&self) -> bool {
        match self {
            MessagesPage::Uncompressed(_) => true,
            _ => false,
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let uncompressed_messages = self.unwrap_as_uncompressed_page();
        uncompressed_messages.new_messages(messages).await;
    }

    pub fn unwrap_as_uncompressed_page(&self) -> &UncompressedPage {
        match self {
            MessagesPage::Uncompressed(result) => result,
            MessagesPage::Empty(_) => {
                panic!("Can not get message as uncompressed. It's empty");
            }
        }
    }

    pub fn get_page_id(&self) -> PageId {
        match self {
            MessagesPage::Uncompressed(page) => page.page_id,
            MessagesPage::Empty(page) => page.page_id,
        }
    }

    pub async fn get_message(&self, message_id: MessageId) -> Option<Arc<MessageProtobufModel>> {
        match self {
            MessagesPage::Uncompressed(page) => page.get_message(message_id).await,
            MessagesPage::Empty(_) => None,
        }
    }

    pub async fn get_all(&self) -> Vec<Arc<MessageProtobufModel>> {
        match self {
            MessagesPage::Uncompressed(page) => page.get_all().await,
            MessagesPage::Empty(_) => vec![],
        }
    }

    pub async fn get_range(
        &self,
        from_message_id: MessageId,
        to_message_id: MessageId,
    ) -> Vec<Arc<MessageProtobufModel>> {
        match self {
            MessagesPage::Uncompressed(page) => {
                page.get_range(from_message_id, to_message_id).await
            }
            MessagesPage::Empty(_) => vec![],
        }
    }
}
