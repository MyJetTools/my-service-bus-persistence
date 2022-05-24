use std::sync::Arc;

use my_service_bus_shared::MessageId;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{app::AppContext, topic_data::TopicData};
const MAX_PERSIST_SIZE: usize = 1024 * 1024 * 4;

pub async fn persist_topic_pages(
    app: &AppContext,
    topic_id: &str,
    topic_message_id: MessageId,
    topic_data: Option<Arc<TopicData>>,
) -> Arc<TopicData> {
    let topic_data = if topic_data.is_none() {
        app.logs.add_info(
            Some(topic_id),
            "persist_topic_pages",
            format!("Topic data {} is not found. Creating it", topic_id),
        );

        crate::operations::init_new_topic(app, topic_id, topic_message_id).await
    } else {
        topic_data.unwrap()
    };

    flush_minute_index_data(topic_data.as_ref()).await;

    let pages_with_data_to_save = topic_data
        .uncompressed_pages_list
        .get_pages_with_data_to_save()
        .await;

    for page in pages_with_data_to_save {
        if let Some(result) = page.flush_to_storage(MAX_PERSIST_SIZE).await {
            topic_data
                .metrics
                .update_last_saved_duration(result.duration);

            topic_data
                .metrics
                .update_last_saved_moment(DateTimeAsMicroseconds::now());

            topic_data
                .metrics
                .update_last_saved_chunk(result.last_saved_chunk);

            topic_data
                .metrics
                .update_last_saved_message_id(result.last_saved_message_id);
        }
    }

    topic_data
}

async fn flush_minute_index_data(topic_data: &TopicData) {
    let mut index_by_minute = topic_data.yearly_index_by_minute.lock().await;

    for item in index_by_minute.values_mut() {
        item.flush_to_storage().await;
    }
}
