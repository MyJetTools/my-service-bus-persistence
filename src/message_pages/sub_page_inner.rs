use std::{collections::BTreeMap, sync::Arc};

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::{
    page_compressor::CompressedPageReaderError,
    protobuf_models::MessageProtobufModel,
    sub_page::{SizeAndAmount, SubPageId},
};
use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct SubPageInner {
    pub sub_page_id: SubPageId,
    pub messages: BTreeMap<i64, Arc<MessageProtobufModel>>,
    pub created: DateTimeAsMicroseconds,

    size_and_amount: SizeAndAmount,
}

impl SubPageInner {
    pub fn new(sub_page_id: SubPageId) -> Self {
        Self {
            sub_page_id,
            messages: BTreeMap::new(),
            created: DateTimeAsMicroseconds::now(),
            size_and_amount: SizeAndAmount::new(),
        }
    }

    pub fn restore(
        sub_page_id: SubPageId,
        messages: BTreeMap<i64, Arc<MessageProtobufModel>>,
    ) -> Self {
        let mut size_and_amount = SizeAndAmount::new();

        for msg in messages.values() {
            size_and_amount.added(msg.data.len());
        }

        Self {
            sub_page_id,
            messages,
            created: DateTimeAsMicroseconds::now(),
            size_and_amount,
        }
    }

    pub fn add_message(
        &mut self,
        message: Arc<MessageProtobufModel>,
    ) -> Option<Arc<MessageProtobufModel>> {
        let message_id = message.get_message_id();

        if !self.sub_page_id.is_my_message_id(message_id) {
            println!(
                "Somehow we are uploading message_id {} to sub_page {}. Skipping message...",
                message_id.get_value(),
                self.sub_page_id.get_value()
            );
            return None;
        }

        self.size_and_amount.added(message.data.len());

        if let Some(old_message) = self.messages.insert(message_id.get_value(), message) {
            self.size_and_amount.removed(old_message.data.len());
            return Some(old_message);
        }

        None
    }

    pub fn get_message(&self, msg_id: MessageId) -> Option<&Arc<MessageProtobufModel>> {
        self.messages.get(msg_id.as_ref())
    }

    pub fn get_all_messages(&self) -> BTreeMap<i64, Arc<MessageProtobufModel>> {
        self.messages.clone()
    }

    pub fn get_size_and_amount(&self) -> &SizeAndAmount {
        &self.size_and_amount
    }

    pub fn from_compressed_payload(
        sub_page_id: SubPageId,
        compressed_payload: &[u8],
    ) -> Result<SubPageInner, CompressedPageReaderError> {
        let mut compressed_payload =
            my_service_bus::shared::page_compressor::CompressedPageReader::new(compressed_payload)?;

        let mut messages: BTreeMap<i64, Arc<MessageProtobufModel>> = BTreeMap::new();

        while let Some(msg) = compressed_payload.get_next_message().unwrap() {
            messages.insert(msg.get_message_id().get_value(), Arc::new(msg));
        }

        let sub_page_inner = SubPageInner::restore(sub_page_id, messages);

        Ok(sub_page_inner)
    }
}
