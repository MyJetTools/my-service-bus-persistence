use my_service_bus_shared::MessageId;

use crate::{
    app::AppContext,
    message_pages::{
        CompressedClusterId, CompressedPageId, UncompressedPage, MESSAGES_PER_COMPRESSED_PAGE,
    },
    topic_data::TopicData,
};

pub async fn compress_page_if_needed(
    app: &AppContext,
    topic_data: &TopicData,

    compressed_page_id: &CompressedPageId,
    uncompressed_page: &UncompressedPage,
) {
    let cluster_id = CompressedClusterId::from_compressed_page_id(&compressed_page_id);

    let cluset = super::get_compressed_cluster_to_write(app, topic_data, &cluster_id).await;

    if cluset.has_compressed_page(compressed_page_id).await {
        return;
    }

    let from_id = compressed_page_id.get_first_message_id();

    let to_id = from_id + (MESSAGES_PER_COMPRESSED_PAGE as MessageId);

    //TODO - Check if we include last message_id
    let messages_to_compress = uncompressed_page.get_range(from_id, to_id).await;

    let mut clusters = topic_data.compressed_clusters.lock().await;

    let cluster = clusters.get_mut(&cluster_id.value).unwrap();

    cluster
        .save_cluser_page(messages_to_compress.as_slice())
        .await;
}
