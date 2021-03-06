use std::collections::HashMap;

use my_service_bus_shared::{page_id::PageId, protobuf_models::TopicSnapshotProtobufModel};

use crate::message_pages::MessagePageId;

pub const MESSAGES_PER_PAGE: i64 = 100_000;

pub fn get_active_pages(snapshot: &TopicSnapshotProtobufModel) -> HashMap<i64, PageId> {
    let mut result: HashMap<i64, PageId> = HashMap::new();

    let page_id = MessagePageId::from_message_id(snapshot.message_id);
    result.insert(page_id.value, page_id.value);

    for topic_queue in &snapshot.queues {
        for range in &topic_queue.ranges {
            let page_id = MessagePageId::from_message_id(range.from_id);
            result.insert(page_id.value, page_id.value);
        }
    }

    result
}

pub fn get_message_page_id(message_id: i64) -> i64 {
    message_id / MESSAGES_PER_PAGE
}
