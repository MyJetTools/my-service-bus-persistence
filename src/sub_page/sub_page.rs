use std::collections::BTreeMap;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};
use rust_extensions::{date_time::DateTimeAsMicroseconds, lazy::LazyVec};
use tokio::sync::Mutex;

use crate::toc::PayloadNo;

use super::SubPageMessagesIterator;

pub struct SubPage {
    pub sub_page_id: SubPageId,
    pub messages: Mutex<BTreeMap<i64, MessageProtobufModel>>,
    pub created: DateTimeAsMicroseconds,
}

impl SubPage {
    pub fn new(sub_page_id: SubPageId) -> Self {
        Self {
            sub_page_id,
            messages: Mutex::new(BTreeMap::new()),
            created: DateTimeAsMicroseconds::now(),
        }
    }

    pub fn restored(sub_page_id: SubPageId, messages: BTreeMap<i64, MessageProtobufModel>) -> Self {
        Self {
            sub_page_id,
            messages: Mutex::new(messages),
            created: DateTimeAsMicroseconds::now(),
        }
    }

    pub async fn add_messages(&self, new_messages: Vec<MessageProtobufModel>) {
        let mut messages = self.messages.lock().await;

        for message in new_messages {
            messages.insert(message.message_id, message);
        }
    }

    pub async fn get_message(&self, message_id: MessageId) -> Option<MessageProtobufModel> {
        let messages = self.messages.lock().await;
        let result = messages.get(&message_id)?;
        Some(result.clone())
    }
    pub async fn get_messages(
        &self,
        from_id: MessageId,
        to_id: MessageId,
    ) -> Option<Vec<MessageProtobufModel>> {
        let mut result = LazyVec::new();
        let messages = self.messages.lock().await;

        for message_id in from_id..=to_id {
            if let Some(msg) = messages.get(&message_id) {
                result.add(msg.clone());
            }
        }

        result.get_result()
    }
}

pub const SUB_PAGE_MESSAGES_AMOUNT: usize = 1000;

#[derive(Debug, Clone, Copy)]
pub struct SubPageId {
    pub value: usize,
}

impl SubPageId {
    pub fn new(value: usize) -> Self {
        Self { value }
    }
    pub fn from_message_id(message_id: MessageId) -> Self {
        Self {
            value: message_id as usize / SUB_PAGE_MESSAGES_AMOUNT,
        }
    }

    pub fn get_first_message_id(&self) -> MessageId {
        let result = self.value * SUB_PAGE_MESSAGES_AMOUNT;
        result as MessageId
    }

    pub fn get_first_message_id_of_next_page(&self) -> MessageId {
        self.get_first_message_id() + SUB_PAGE_MESSAGES_AMOUNT as MessageId
    }

    //Get Payload no inside Compressed cluster or noncompressed page
    pub fn get_payload_no(&self, subpages_per_file: usize) -> PayloadNo {
        let cluster_id = self.value / subpages_per_file;
        PayloadNo::new(self.value - cluster_id * subpages_per_file)
    }

    pub fn iterate_through_messages(&self) -> SubPageMessagesIterator {
        SubPageMessagesIterator::new(self)
    }
}

#[cfg(test)]
mod test {
    use crate::sub_page::*;

    #[test]
    fn test_first_message_id() {
        assert_eq!(0, SubPageId::new(0).get_first_message_id());
        assert_eq!(1000, SubPageId::new(1).get_first_message_id());
        assert_eq!(2000, SubPageId::new(2).get_first_message_id());
    }

    #[test]
    fn test_first_message_id_of_next_page() {
        assert_eq!(1000, SubPageId::new(0).get_first_message_id_of_next_page());
        assert_eq!(2000, SubPageId::new(1).get_first_message_id_of_next_page());
        assert_eq!(3000, SubPageId::new(2).get_first_message_id_of_next_page());
    }
}
