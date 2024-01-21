use std::sync::Arc;

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::{protobuf_models::MessageProtobufModel, sub_page::SubPageId};
use rust_extensions::sorted_vec::SortedVecOfArc;

pub struct SubPageReadCopy {
    pub sub_page_id: SubPageId,
    messages: SortedVecOfArc<i64, MessageProtobufModel>,
}

impl SubPageReadCopy {
    pub fn new(
        sub_page_id: SubPageId,
        messages: SortedVecOfArc<i64, MessageProtobufModel>,
    ) -> Self {
        Self {
            sub_page_id,
            messages,
        }
    }

    pub fn get(&self, message_id: MessageId) -> Option<&Arc<MessageProtobufModel>> {
        self.messages.get(message_id.as_ref())
    }
}
