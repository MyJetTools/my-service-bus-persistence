use my_service_bus_shared::{
    page_id::PageId, protobuf_models::MessageProtobufModel,
    queue_with_intervals::QueueWithIntervals,
};

use crate::toc::{ContentOffset, FileToc};

pub struct PayloadsToUploadContainer<'s> {
    pub write_position: usize,
    toc: &'s mut FileToc,
    pub toc_pages: QueueWithIntervals,
    pub payload: Vec<u8>,
    pub page_id: PageId,
}

impl<'s> PayloadsToUploadContainer<'s> {
    pub fn new(page_id: PageId, write_position: usize, toc: &'s mut FileToc) -> Self {
        Self {
            write_position,
            toc_pages: QueueWithIntervals::new(),
            payload: Vec::new(),
            page_id,
            toc,
        }
    }

    pub fn append(&mut self, contract: &MessageProtobufModel) {
        let current_pos = self.payload.len();

        prost::Message::encode(contract, &mut self.payload).unwrap();

        let size = self.payload.len() - current_pos;

        let offset = ContentOffset {
            offset: self.write_position + current_pos,
            size,
        };

        let file_no =
            super::utils::get_file_no_inside_uncompressed_file(self.page_id, contract.message_id);

        if let Some(toc_page) = self.toc.update_file_position(file_no, &offset) {
            self.toc_pages.enqueue(toc_page as i64);
        }
    }
}
