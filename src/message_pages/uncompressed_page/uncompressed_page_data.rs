use std::{collections::BTreeMap, sync::Arc};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use crate::{
    message_pages::MESSAGES_PER_PAGE,
    uncompressed_page_storage::{
        toc::{MessageContentOffset, UncompressedFileToc},
        UncompressedPageStorage,
    },
};

pub struct MinMax {
    pub min: MessageId,
    pub max: MessageId,
}

impl MinMax {
    pub fn new(message_id: MessageId) -> Self {
        MinMax {
            min: message_id,
            max: message_id,
        }
    }
    pub fn update(&mut self, message_id: MessageId) {
        if self.min > message_id {
            self.min = message_id;
        }

        if self.max < message_id {
            self.max = message_id;
        }
    }
}

pub enum MessageStatus {
    Loaded(Arc<MessageProtobufModel>),
    Missing,
}

impl MessageStatus {
    pub fn get_message_content(&self) -> Option<Arc<MessageProtobufModel>> {
        match self {
            MessageStatus::Loaded(message) => Some(message.clone()),
            MessageStatus::Missing => None,
        }
    }
}

pub struct UncompressedPageData {
    pub page_id: PageId,
    pub toc: UncompressedFileToc,
    pub messages: BTreeMap<i64, MessageStatus>,
    pub min_max: Option<MinMax>,
    pub storage: UncompressedPageStorage,
}

impl UncompressedPageData {
    pub async fn new(page_id: PageId, mut storage: UncompressedPageStorage) -> Self {
        let toc = storage.read_toc().await;
        let min_max = get_min_max_from_toc(page_id, &toc);

        Self {
            page_id,
            messages: BTreeMap::new(),
            min_max,
            toc,
            storage,
        }
    }

    pub fn get_message_offset(&self, message_id: MessageId) -> MessageContentOffset {
        let file_no = message_id - self.page_id * MESSAGES_PER_PAGE;

        self.toc.get_position(file_no as usize)
    }

    fn update_min_max(&mut self, message_id: MessageId) {
        if let Some(ref mut min_max) = self.min_max {
            min_max.update(message_id);
        } else {
            self.min_max = Some(MinMax::new(message_id));
        }
    }

    pub fn add(&mut self, messages: Vec<MessageProtobufModel>) {
        for msg in messages {
            self.add_message(msg);
        }
    }

    pub fn add_message(&mut self, msg: MessageProtobufModel) {
        self.update_min_max(msg.message_id);
        self.messages
            .insert(msg.message_id, MessageStatus::Loaded(Arc::new(msg)));
    }

    pub fn get(&self, message_id: MessageId) -> Option<Arc<MessageProtobufModel>> {
        let result = self.messages.get(&message_id)?;
        result.get_message_content()
    }

    pub fn get_grpc_v0_snapshot(&self) -> Vec<Arc<MessageProtobufModel>> {
        let mut result = Vec::new();

        for msg in self.messages.values() {
            if let Some(msg) = msg.get_message_content() {
                result.push(msg);
            }
        }

        result
    }

    pub fn has_skipped_messages(&self) -> bool {
        self.messages.len() != should_have_amount(self.page_id, self.min_max.as_ref())
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

fn should_have_amount(page_id: PageId, min_max: Option<&MinMax>) -> usize {
    if min_max.is_none() {
        return 0;
    }
    let min_max = min_max.unwrap();

    let first_message_id = page_id * MESSAGES_PER_PAGE;
    let result = min_max.max - first_message_id + 1;

    result as usize
}

pub fn has_skipped_messages(page_id: PageId, amount: usize, min_max: Option<&MinMax>) -> bool {
    amount != should_have_amount(page_id, min_max)
}

pub fn get_min_max_from_toc(page_id: PageId, toc: &UncompressedFileToc) -> Option<MinMax> {
    let mut result: Option<MinMax> = None;

    for file_no in 0..100_000 {
        if toc.has_content(file_no) {
            let message_id: MessageId = file_no as MessageId + page_id * MESSAGES_PER_PAGE;

            if let Some(ref mut min_max) = result {
                min_max.update(message_id);
            } else {
                result = Some(MinMax::new(message_id));
            }
        }
    }

    result
}
