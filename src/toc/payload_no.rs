use my_service_bus_shared::{page_id::PageId, MessageId};

use crate::uncompressed_page::MESSAGES_PER_PAGE;

pub struct PayloadNo {
    pub value: usize,
}

impl PayloadNo {
    pub fn new(value: usize) -> Self {
        Self { value }
    }

    pub fn from_uncompressed_message_id(message_id: MessageId, page_id: PageId) -> Self {
        let value = message_id - page_id * MESSAGES_PER_PAGE;

        Self {
            value: value as usize,
        }
    }
    pub fn get_toc_offset(&self) -> usize {
        self.value * 8
    }
}
