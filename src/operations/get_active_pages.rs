use my_service_bus_shared::{page_id::PageId, protobuf_models::TopicSnapshotProtobufModel};

pub fn get_active_pages(topic: &TopicSnapshotProtobufModel) -> Vec<PageId> {
    let mut pages = Vec::new();

    let page_id: PageId = topic.get_message_id().into();

    pages.push(page_id);

    for queue in &topic.queues {
        for range in &queue.ranges {
            let from_page_id: PageId = range.get_from_id().into();

            let to_page_id: PageId = range.get_to_id().into();

            if pages
                .iter()
                .all(|itm| itm.get_value() != from_page_id.get_value())
            {
                pages.push(from_page_id);
            }

            if pages
                .iter()
                .all(|itm| itm.get_value() != to_page_id.get_value())
            {
                pages.push(to_page_id);
            }
        }
    }

    pages
}
