use my_service_bus_shared::MessageId;

use crate::{
    app::AppContext, message_pages::CompressedClusterId, sub_page::SubPageId,
    topic_data::TopicData, uncompressed_page::*,
};

//TODO - Somehow unit test it
pub async fn compress_page_if_needed(
    app: &AppContext,
    topic_data: &TopicData,
    uncompressed_page: &UncompressedPage,
    sub_page_id: &SubPageId,
    topic_message_id: MessageId,
) {
    let compressed_cluster_id = CompressedClusterId::from_sub_page_id(sub_page_id);

    let cluster =
        super::get_compressed_cluster_to_write(app, topic_data, &compressed_cluster_id).await;

    if cluster.has_compressed_page(sub_page_id).await {
        return;
    }

    let first_message_id_of_next_page = sub_page_id.get_first_message_id_of_next_page();

    if topic_message_id <= first_message_id_of_next_page {
        return;
    }

    if let Some(sub_page) = uncompressed_page.get_sub_page(sub_page_id).await {
        cluster.save_cluser_page(sub_page.as_ref()).await;
    }
}
