use my_service_bus_shared::protobuf_models::TopicSnapshotProtobufModel;

use crate::{app::AppContext, sub_page::SubPageId, topic_data::TopicData, uncompressed_page::*};

//TODO - Unit test it
pub async fn compress_previous_page(
    app: &AppContext,
    topic_data: &TopicData,
    topic_snapshot: &TopicSnapshotProtobufModel,
    uncompressed_page: &UncompressedPage,
) {
    let mut sub_page_id = SubPageId::from_message_id(topic_snapshot.message_id);
    if sub_page_id.value == 0 {
        return;
    }

    sub_page_id.value -= 1; // Trying to compressed prev page

    super::compress_page_if_needed(
        app,
        topic_data,
        uncompressed_page,
        &sub_page_id,
        topic_snapshot.message_id,
    )
    .await;
}
