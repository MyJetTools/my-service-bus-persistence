use my_service_bus_shared::{
    protobuf_models::MessageProtobufModel, queue_with_intervals::QueueWithIntervals,
};

use crate::toc::{ContentOffset, FileToc};

use super::UncompressedPageId;

pub struct PayloadsToUploadContainer<'s> {
    pub write_position: usize,
    toc: &'s mut FileToc,
    pub toc_pages: QueueWithIntervals,
    pub payload: Vec<u8>,
    pub page_id: UncompressedPageId,
}

impl<'s> PayloadsToUploadContainer<'s> {
    pub fn new(page_id: UncompressedPageId, write_position: usize, toc: &'s mut FileToc) -> Self {
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

        let file_no = self
            .page_id
            .get_payload_no_inside_uncompressed_file(contract.message_id);

        if let Some(toc_page) = self.toc.update_file_position(&file_no, &offset) {
            self.toc_pages.enqueue(toc_page as i64);
        }
    }
}
