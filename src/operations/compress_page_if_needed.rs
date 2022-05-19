use my_service_bus_shared::MessageId;

use crate::compressed_page::*;

use crate::{app::AppContext, sub_page::SubPageId, topic_data::TopicData, uncompressed_page::*};

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

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use my_service_bus_shared::{bcl::BclDateTime, protobuf_models::MessageProtobufModel};
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::{app::AppContext, settings::SettingsModel};

    #[tokio::test]
    async fn test_we_do_not_compress_yet() {
        const TOPIC_ID: &str = "test";
        let settings_model = SettingsModel::create_for_test_environment();
        let app = Arc::new(AppContext::new(settings_model).await);

        let topic_data =
            crate::operations::get_topic_data_to_publish_messages(app.as_ref(), TOPIC_ID, 1).await;

        let new_messages = vec![MessageProtobufModel {
            message_id: 1,
            created: Some(BclDateTime::from(DateTimeAsMicroseconds::now())),
            data: vec![0u8, 1u8, 2u8],
            headers: vec![],
        }];

        crate::operations::new_messages(app.as_ref(), topic_data.as_ref(), 0, new_messages).await;
    }
}
