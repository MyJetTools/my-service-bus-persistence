use my_service_bus_shared::{page_id::PageId, MessageId};

use crate::sub_page::SubPageId;

use super::utils::MESSAGES_PER_PAGE;

#[derive(Clone, Copy)]
pub struct UncompressedPageId {
    pub value: PageId,
}

impl UncompressedPageId {
    pub fn new(page_id: PageId) -> Self {
        Self { value: page_id }
    }
    pub fn from_message_id(message_id: i64) -> Self {
        Self {
            value: super::utils::get_message_page_id(message_id),
        }
    }

    pub fn get_first_message_id(&self) -> MessageId {
        self.value * MESSAGES_PER_PAGE
    }

    pub fn from_sub_page_id(sub_page_id: &SubPageId) -> Self {
        todo!("Implement");
    }
}
