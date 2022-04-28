use std::collections::BTreeMap;

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use super::toc::MessageContentOffset;

pub struct PayloadsToUploadContainer {
    pub write_position: usize,
    pub tocs: BTreeMap<usize, MessageContentOffset>,
    pub payload: Vec<u8>,
}

impl PayloadsToUploadContainer {
    pub fn new(write_position: usize) -> Self {
        Self {
            write_position,
            tocs: BTreeMap::new(),
            payload: Vec::new(),
        }
    }

    pub fn append(&mut self, page_id: PageId, contract: &MessageProtobufModel) {
        let current_pos = self.payload.len();

        prost::Message::encode(contract, &mut self.payload).unwrap();

        let size = self.payload.len() - current_pos;

        let offset = MessageContentOffset {
            offset: self.write_position + current_pos,
            size,
        };

        let file_no =
            super::utils::get_file_no_inside_uncompressed_file(page_id, contract.message_id);

        self.tocs.insert(file_no, offset);
    }
}
