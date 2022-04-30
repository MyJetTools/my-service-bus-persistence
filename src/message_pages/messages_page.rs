use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel};

use crate::uncompressed_page_storage::toc::UncompressedFileToc;

use super::{BlankPage, CompressedPage, UncompressedPage};

pub enum MessagesPage {
    Uncompressed(UncompressedPage),
    Compressed(CompressedPage),
    Empty(BlankPage),
}

impl MessagesPage {
    pub fn create_as_empty(page_id: PageId) -> Self {
        Self::Empty(BlankPage::new(page_id))
    }
    pub fn create_uncompressed(page_id: PageId, toc: UncompressedFileToc) -> Self {
        Self::Uncompressed(UncompressedPage::new(page_id, toc))
    }

    pub fn has_messages_to_save(&self) -> bool {
        match self {
            MessagesPage::Uncompressed(page) => page.has_messages_to_save(),
            MessagesPage::Compressed(_) => false,
            MessagesPage::Empty(_) => false,
        }
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        match self {
            MessagesPage::Uncompressed(page) => page.metrics.get_messages_amount_to_save(),
            MessagesPage::Compressed(_) => 0,
            MessagesPage::Empty(_) => 0,
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        let uncompressed_messages = self.unwrap_as_uncompressed_page();

        let mut write_access = uncompressed_messages.pages.write().await;

        write_access.add(messages);
    }

    pub fn unwrap_as_uncompressed_page(&self) -> &UncompressedPage {
        match self {
            MessagesPage::Uncompressed(result) => result,
            MessagesPage::Compressed(_) => {
                panic!("Can not get message as uncompressed. It's compressed");
            }
            MessagesPage::Empty(_) => {
                panic!("Can not get message as uncompressed. It's empty");
            }
        }
    }

    pub fn get_page_id(&self) -> PageId {
        match self {
            MessagesPage::Uncompressed(page) => page.page_id,
            MessagesPage::Compressed(page) => page.page_id,
            MessagesPage::Empty(page) => page.page_id,
        }
    }
}
