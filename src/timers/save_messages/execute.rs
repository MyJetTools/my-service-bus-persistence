use std::sync::Arc;

use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

use crate::{app::AppContext, utils::StopWatch};

pub async fn execute(app: Arc<AppContext>, topics: Arc<TopicsSnapshotProtobufModel>) {
    let timer_result = tokio::spawn(timer_tick(app.clone(), topics)).await;

    if let Err(err) = timer_result {
        app.logs.add_fatal_error("save_messages_timer", err).await;
    }
}

async fn timer_tick(app: Arc<AppContext>, topics: Arc<TopicsSnapshotProtobufModel>) {
    for topic in &topics.data {
        let data_by_topic = app.topics_data_list.get(&topic.topic_id).await;

        if data_by_topic.is_none() {
            app.logs
                .add_info(
                    Some(topic.topic_id.as_str()),
                    "Saving messages",
                    format!("Topic data {} is not found", topic.topic_id),
                )
                .await;

            continue;
        }

        let data_by_topic = data_by_topic.unwrap();

        let pages_with_data_to_save = data_by_topic.get_pages_with_data_to_save().await;

        for page in pages_with_data_to_save {
            let mut write_access = page.data.lock().await;

            match &mut *write_access {
                crate::message_pages::MessagesPageData::Uncompressed(uncompressed_page) => {
                    let messages_to_save = uncompressed_page.get_messages_to_save();

                    let mut sw = StopWatch::new();
                    sw.start();
                    super::save_to_blob(
                        &mut uncompressed_page.blob,
                        data_by_topic.as_ref(),
                        messages_to_save.as_ref(),
                        page.page_id,
                    )
                    .await;
                    sw.pause();
                    uncompressed_page.commit_saved(&messages_to_save);
                    write_access.update_metrics(&page.metrics);
                    data_by_topic
                        .metrics
                        .update_last_saved_duration(sw.duration());
                }
                _ => {
                    app.logs
                        .add_error_str(
                            Some(data_by_topic.topic_id.as_str()),
                            "save_data_to_blob",
                            format!("Somehow we get to the point we have to Save messages but or Page is {}", write_access.get_page_type()),
                            format!("N/A")
                        )
                        .await;
                }
            }
        }
    }
}
