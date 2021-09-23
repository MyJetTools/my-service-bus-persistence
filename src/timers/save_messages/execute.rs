use std::sync::Arc;

use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

use crate::{app::AppContext, message_pages::MessagePageId};

pub async fn execute(app: Arc<AppContext>, topics: Arc<TopicsSnapshotProtobufModel>) {
    let timer_result = tokio::spawn(timer_tick(app.clone(), topics)).await;

    if let Err(err) = timer_result {
        app.logs.add_fatal_error("save_messages_timer", err).await;
    }
}

async fn timer_tick(app: Arc<AppContext>, topics: Arc<TopicsSnapshotProtobufModel>) {
    for topic in &topics.data {
        let data_by_topic = app.get_data_by_topic(&topic.topic_id).await;

        if data_by_topic.is_none() {
            app.logs
                .add_info(
                    Some(topic.topic_id.as_str()),
                    "Saving messages",
                    "Topic data is not found",
                )
                .await;

            continue;
        }

        let data_by_topic = data_by_topic.unwrap();

        let pages_with_data_to_save = data_by_topic.get_pages_with_data_to_save().await;

        for page in pages_with_data_to_save {
            let mut storage = page.storage.lock().await;

            loop {
                if let Some(page_blob) = &mut storage.blob {
                    super::save_to_blob(page_blob, page.as_ref(), data_by_topic.as_ref()).await;
                    break;
                } else {
                    let current_page_id = MessagePageId::from_message_id(topic.message_id);

                    if current_page_id.value == page.page_id.value {
                        app.logs
                            .add_info_string(
                                Some(topic.topic_id.as_str()),
                                "Saving messages",
                                format!(
                                    "Found {} messages to save by page has no pageBlob initialized. Since this is a current page - we initialize blob",
                                    page.get_messages_to_save_amount().await,
                                ),
                            )
                            .await;

                        super::initialize_uncompressed_blob(
                            &mut storage,
                            page.as_ref(),
                            app.clone(),
                        )
                        .await;
                    } else {
                        app.logs
                            .add_info_string(
                                Some(topic.topic_id.as_str()),
                                "Saving messages",
                                format!(
                                    "Found {} messages to save by page has no pageBlob initialized. Page is #{} There is a bug.... Skipping...",
                                    page.get_messages_to_save_amount().await,
                                    MessagePageId::from_message_id(topic.message_id).value
                                ),
                            )
                            .await;
                        break;
                    }
                }
            }
        }
    }
}
