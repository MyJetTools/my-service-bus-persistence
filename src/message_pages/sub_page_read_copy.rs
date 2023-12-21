use std::{collections::BTreeMap, sync::Arc};

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::{protobuf_models::MessageProtobufModel, sub_page::SubPageId};

pub struct SubPageReadCopy {
    pub sub_page_id: SubPageId,
    messages: Option<BTreeMap<i64, Arc<MessageProtobufModel>>>,
}

impl SubPageReadCopy {
    pub fn new(
        sub_page_id: SubPageId,
        messages: Option<BTreeMap<i64, Arc<MessageProtobufModel>>>,
    ) -> Self {
        Self {
            sub_page_id,
            messages,
        }
    }

    pub fn get(&self, message_id: MessageId) -> Option<&MessageProtobufModel> {
        let messages = self.messages.as_ref()?;
        messages.get(&message_id.get_value()).map(|x| x.as_ref())
    }
}
