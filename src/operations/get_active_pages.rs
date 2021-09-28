use my_service_bus_shared::protobuf_models::TopicSnapshotProtobufModel;

use crate::message_pages::MessagePageId;

pub fn get_active_pages(topic: &TopicSnapshotProtobufModel) -> Vec<MessagePageId> {
    let mut pages = Vec::new();

    let page = MessagePageId::from_message_id(topic.message_id);

    pages.push(page);

    for queue in &topic.queues {
        for range in &queue.ranges {
            let from_id = MessagePageId::from_message_id(range.from_id);

            let to_id = MessagePageId::from_message_id(range.from_id);

            if pages.iter().all(|itm| itm.value != from_id.value) {
                pages.push(from_id);
            }

            if pages.iter().all(|itm| itm.value != to_id.value) {
                pages.push(to_id);
            }
        }
    }

    pages
}
