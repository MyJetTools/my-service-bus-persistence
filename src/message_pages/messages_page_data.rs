use std::collections::BTreeMap;

use my_service_bus_shared::{
    page_id::PageId, protobuf_models::MessageProtobufModel,
    queue_with_intervals::QueueWithIntervals, MessageId,
};

use super::MESSAGES_PER_PAGE;

pub struct MessagesPageData {
    pub page_id: PageId,
    pub messages: BTreeMap<i64, MessageProtobufModel>,
    pub queue_to_save: QueueWithIntervals,
    pub min_message_id: Option<i64>,
    pub max_message_id: Option<i64>,
}

impl MessagesPageData {
    pub fn new(page_id: PageId, messages: BTreeMap<i64, MessageProtobufModel>) -> Self {
        let min_max = get_min_max(&&messages);
        Self {
            page_id,
            messages: messages,
            queue_to_save: QueueWithIntervals::new(),
            max_message_id: min_max.0,
            min_message_id: min_max.1,
        }
    }

    pub fn add(&mut self, messages: Vec<MessageProtobufModel>) {
        for msg in messages {
            self.queue_to_save.enqueue(msg.message_id);
            self.update_min_max(msg.message_id);
            self.messages.insert(msg.message_id, msg);
        }
    }

    pub fn restore(&mut self, messages: Vec<MessageProtobufModel>) {
        for msg in messages {
            self.update_min_max(msg.message_id);
            self.messages.insert(msg.message_id, msg);
        }
    }

    pub fn get(&self, message_id: MessageId) -> Option<&MessageProtobufModel> {
        let result = self.messages.get(&message_id)?;
        Some(result)
    }

    pub fn commit_saved(&mut self, messages: &[MessageProtobufModel]) {
        for msg in messages {
            self.queue_to_save.remove(msg.message_id);
        }
    }

    pub fn get_messages_to_save(&self) -> Vec<MessageProtobufModel> {
        let mut result = Vec::new();

        for msg_id in &self.queue_to_save {
            let msg = self.messages.get(&msg_id);

            if let Some(msg) = msg {
                result.push(msg.clone());
            }
        }

        result
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

    pub fn get_grpc_v0_snapshot(&self) -> Vec<MessageProtobufModel> {
        let mut result = Vec::new();

        for msg in self.messages.values() {
            result.push(msg.clone());
        }

        result
    }

    pub fn has_skipped_messages(&self) -> bool {
        self.messages.len() != should_have_amount(self.page_id, self.max_message_id)
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

fn should_have_amount(page_id: PageId, max_message_id: Option<MessageId>) -> usize {
    if max_message_id.is_none() {
        return 0;
    }
    let max_message_id = max_message_id.unwrap();

    let first_message_id = page_id * MESSAGES_PER_PAGE;
    let result = max_message_id - first_message_id + 1;

    result as usize
}

pub fn has_skipped_messages(
    page_id: PageId,
    amount: usize,
    max_message_id: Option<MessageId>,
) -> bool {
    amount != should_have_amount(page_id, max_message_id)
}
