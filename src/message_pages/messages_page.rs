use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel};

use super::UncompressedMessagesPage;

pub enum MessagesPage {
    Uncompressed(UncompressedMessagesPage),
    Compressed { page_id: PageId, content: Vec<u8> },
    Empty(PageId),
}

impl MessagesPage {
    pub fn create_as_empty(page_id: PageId) -> Self {
        Self::Empty(page_id)
    }
    pub fn create_uncompressed(page_id: PageId) -> Self {
        Self::Uncompressed(UncompressedMessagesPage::brand_new(page_id))
    }

    pub fn has_messages_to_save(&self) -> bool {
        match self {
            MessagesPage::Uncompressed(page) => page.has_messages_to_save(),
            MessagesPage::Compressed {
                page_id: _,
                content: _,
            } => false,
            MessagesPage::Empty(_) => false,
        }
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        match self {
            MessagesPage::Uncompressed(page) => page.metrics.get_messages_amount_to_save(),
            MessagesPage::Compressed {
                page_id: _,
                content: _,
            } => 0,
            MessagesPage::Empty(_) => 0,
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let uncompressed_messages = self.unwrap_as_uncompressed_page();

        let mut write_access = uncompressed_messages.pages.write().await;

        write_access.add(messages);
    }

    pub fn unwrap_as_uncompressed_page(&self) -> &UncompressedMessagesPage {
        match self {
            MessagesPage::Uncompressed(result) => result,
            MessagesPage::Compressed {
                page_id: _,
                content: _,
            } => {
                panic!("Can not get message as uncompressed. It's compressed");
            }
            MessagesPage::Empty(_) => {
                panic!("Can not get message as uncompressed. It's empty");
            }
        }
    }

    pub fn is_initialized(&self) -> bool {
        todo!("Implement")
    }

    pub fn get_page_id(&self) -> PageId {
        match self {
            MessagesPage::Uncompressed(page) => page.page_id,
            MessagesPage::Compressed {
                page_id,
                content: _,
            } => *page_id,
            MessagesPage::Empty(page_id) => *page_id,
        }
    }
}
