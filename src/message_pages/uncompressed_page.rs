use std::collections::BTreeMap;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::{azure_storage::messages_page_blob::MessagesPageBlob, message_pages::MessagePageId};

pub struct UncompressedPage {
    pub page_id: MessagePageId,
    pub messages: BTreeMap<i64, MessageProtobufModel>,
    pub queue_to_save: Vec<i64>,
    pub min_message_id: Option<i64>,
    pub max_message_id: Option<i64>,
    pub blob: MessagesPageBlob,
}

impl UncompressedPage {
    pub fn new(
        page_id: MessagePageId,
        messages: BTreeMap<i64, MessageProtobufModel>,
        blob: MessagesPageBlob,
    ) -> Self {
        let min_max = get_min_max(&&messages);
        Self {
            page_id,
            messages: messages,
            queue_to_save: Vec::new(),
            max_message_id: min_max.0,
            min_message_id: min_max.1,
            blob,
        }
    }

    pub fn add(&mut self, messages: Vec<MessageProtobufModel>) {
        for msg in messages {
            self.queue_to_save.push(msg.message_id);
            self.update_min_max(msg.message_id);
            self.messages.insert(msg.message_id, msg);
        }
    }

    pub fn get_and_clone_message(&self, message_id: MessageId) -> Option<MessageProtobufModel> {
        let result = self.messages.get(&message_id)?;
        Some(result.clone())
    }

    pub fn get_messages_to_save(&self) -> Vec<MessageProtobufModel> {
        let mut result = Vec::new();

        for msg_id in &self.queue_to_save {
            let msg = self.messages.get(msg_id);

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
