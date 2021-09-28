/*
use std::collections::BTreeMap;

use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds, protobuf_models::MessageProtobufModel,
};

use crate::message_pages::MessagePageId;

pub struct MessagesPageData {
    pub page_id: MessagePageId,
    pub messages: BTreeMap<i64, MessageProtobufModel>,
    pub queue_to_save: Vec<i64>,
    pub last_access: DateTimeAsMicroseconds,
    pub min_message_id: Option<i64>,
    pub max_message_id: Option<i64>,
}

impl MessagesPageData {
    pub fn new_empty(page_id: MessagePageId) -> Self {
        Self {
            page_id,
            messages: BTreeMap::new(),
            queue_to_save: Vec::new(),
            last_access: DateTimeAsMicroseconds::now(),
            max_message_id: None,
            min_message_id: None,
        }
    }

    pub fn new(page_id: MessagePageId, messages: BTreeMap<i64, MessageProtobufModel>) -> Self {
        let min_max = get_min_max(&&messages);
        Self {
            page_id,
            messages: messages,
            queue_to_save: Vec::new(),
            last_access: DateTimeAsMicroseconds::now(),
            max_message_id: min_max.0,
            min_message_id: min_max.1,
        }
    }

    pub fn percent(&self) -> usize {
        self.messages.len() / 1000
    }

    fn should_have_amount(&self) -> usize {
        if self.max_message_id.is_none() {
            return 0;
        }
        let max_message_id = self.max_message_id.unwrap();

        let first_message_id = self.page_id.value * super::super::MESSAGES_PER_PAGE;
        let result = max_message_id - first_message_id + 1;

        result as usize
    }

    pub fn has_skipped_messages(&self) -> bool {
        self.messages.len() != self.should_have_amount()
    }

    pub fn update_min_max(&mut self, id: i64) {
        match self.min_message_id {
            Some(value) => {
                if value > id {
                    self.min_message_id = Some(id)
                }
            }
            None => self.min_message_id = Some(id),
        }

        match self.max_message_id {
            Some(value) => {
                if value < id {
                    self.max_message_id = Some(id)
                }
            }
            None => self.max_message_id = Some(id),
        }
    }
}

pub fn get_min_max(messages: &BTreeMap<i64, MessageProtobufModel>) -> (Option<i64>, Option<i64>) {
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;

    for id in messages.keys() {
        match min {
            Some(value) => {
                if value < *id {
                    min = Some(*id)
                }
            }
            None => min = Some(*id),
        }

        match max {
            Some(value) => {
                if value > *id {
                    max = Some(*id)
                }
            }
            None => max = Some(*id),
        }
    }

    (min, max)
}

#[cfg(test)]
mod tests {

    use my_service_bus_shared::bcl::BclDateTime;

    use super::*;

    fn create_message(id: i64) -> MessageProtobufModel {
        MessageProtobufModel {
            created: Some(BclDateTime {
                kind: 0,
                scale: 0,
                value: 0,
            }),
            data: Vec::new(),
            message_id: id,
            metadata: Vec::new(),
        }
    }

    #[test]
    pub fn test_has_no_skipped_messages() {
        let page_id = MessagePageId::new(0);

        let mut data = BTreeMap::new();

        data.insert(0, create_message(0));

        let page_data = MessagesPageData::new(page_id, data);

        let has_skipped_messages = page_data.has_skipped_messages();

        assert_eq!(false, has_skipped_messages);
    }

    #[test]
    pub fn test_has_skipped_messages() {
        let page_id = MessagePageId::new(0);

        let mut data = BTreeMap::new();

        data.insert(1, create_message(1));

        let page_data = MessagesPageData::new(page_id, data);

        let has_skipped_messages = page_data.has_skipped_messages();
        assert_eq!(true, has_skipped_messages);
    }

    #[test]
    pub fn test_has_no_skipped_messages_2() {
        let mut message_id = 200_000;
        let page_id = MessagePageId::from_message_id(message_id);

        let mut data = BTreeMap::new();

        data.insert(message_id, create_message(message_id));
        message_id += 1;
        data.insert(message_id, create_message(message_id));

        let page_data = MessagesPageData::new(page_id, data);

        let has_skipped_messages = page_data.has_skipped_messages();
        assert_eq!(false, has_skipped_messages);
    }

    #[test]
    pub fn test_has_skipped_messages_2() {
        let mut message_id = 200_000;
        let page_id = MessagePageId::from_message_id(message_id);

        let mut data = BTreeMap::new();

        data.insert(message_id, create_message(message_id));
        message_id += 2;
        data.insert(message_id, create_message(message_id));

        let page_data = MessagesPageData::new(page_id, data);

        let has_skipped_messages = page_data.has_skipped_messages();
        assert_eq!(true, has_skipped_messages);
    }
}
 */
