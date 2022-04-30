use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel};

use crate::uncompressed_page_storage::UncompressedPageStorage;

use super::{BlankPage, UncompressedPage};

pub enum MessagesPage {
    Uncompressed(UncompressedPage),
    Empty(BlankPage),
}

impl MessagesPage {
    pub fn create_as_empty(page_id: PageId) -> Self {
        Self::Empty(BlankPage::new(page_id))
    }
    pub async fn create_uncompressed(page_id: PageId, storage: UncompressedPageStorage) -> Self {
        Self::Uncompressed(UncompressedPage::new(page_id, storage).await)
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

        uncompressed_messages.new_messages(messages);
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
}
