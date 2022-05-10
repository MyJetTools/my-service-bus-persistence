use my_service_bus_shared::protobuf_models::TopicSnapshotProtobufModel;

use crate::{
    app::AppContext,
    message_pages::{CompressedPageId, UncompressedPage},
    topic_data::TopicData,
};

pub async fn compress_previous_page(
    app: &AppContext,
    topic_data: &TopicData,
    topic_snapshot: &TopicSnapshotProtobufModel,
    uncompressed_page: &UncompressedPage,
) {
    let mut compressed_page_id = CompressedPageId::from_message_id(topic_snapshot.message_id);
    if compressed_page_id.value == 0 {
        return;
    }

    compressed_page_id.value -= 1; // Trying to compressed prev page

    super::compress_page_if_needed(app, topic_data, &compressed_page_id, uncompressed_page).await;
}
