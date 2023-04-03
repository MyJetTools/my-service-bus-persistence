use my_service_bus_abstractions::MessageId;
use my_service_bus_shared::page_id::PageId;

use crate::message_pages::MESSAGES_PER_PAGE;

pub struct FileNo(usize);

impl FileNo {
    pub fn new(value: usize) -> Self {
        Self(value)
    }

    pub fn from_message_id(message_id: MessageId, page_id: PageId) -> Self {
        let result = message_id.get_value() - page_id.get_value() * MESSAGES_PER_PAGE;
        Self(result as usize)
    }

    pub fn get_toc_pos(&self) -> usize {
        self.0 * 8
    }

    pub fn get_toc_range<'s>(&self, toc_data: &'s [u8]) -> &'s [u8] {
        let toc_pos = self.get_toc_pos();
        &toc_data[toc_pos..toc_pos + 8]
    }
}
