use std::{sync::Arc, time::Duration};

use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds,
    protobuf_models::{MessageProtobufModel, TopicsSnapshotProtobufModel},
};

use crate::{
    app::{AppContext, AppError},
    azure_storage::messages_page_blob::MessagesPageBlob,
    message_pages::{data_by_topic::DataByTopic, MessagePageId, MessagesPage, MessagesPageStorage},
    utils::StopWatch,
};

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
                    save_messages(page_blob, page.as_ref(), data_by_topic.as_ref()).await;
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

                        initialize_uncompressed_blob(&mut storage, page.as_ref(), app.clone())
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

async fn initialize_uncompressed_blob(
    storage: &mut MessagesPageStorage,
    page: &MessagesPage,
    app: Arc<AppContext>,
) {
    let mut attempt_no: usize = 1;
    while attempt_no < 5 {
        let result = storage
            .initialize_uncompressed_blob(page.topic_id.to_string(), page.page_id, app.clone())
            .await;

        if result.is_ok() {
            return;
        }

        let err = result.err().unwrap();

        app.logs
            .add_error_str(
                Some(page.topic_id.as_str()),
                "Initializing uncompressed page blob",
                format!("Attemped #{} failed", attempt_no),
                format!("{:?}", err),
            )
            .await;

        attempt_no += 1;

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    app.logs
        .add_error_str(
            Some(page.topic_id.as_str()),
            "Initializing uncompressed page blob",
            format!("Attemped #{} failed", attempt_no),
            "Could not initialize page blob. Skipping save iteration".to_string(),
        )
        .await;
}

async fn save_messages(
    page_blob: &mut MessagesPageBlob,
    page: &MessagesPage,
    data_by_topic: &DataByTopic,
) {
    let messages_to_save = page.get_messages_to_save().await;

    let max_message_id = get_max_message_id(&messages_to_save);
    let mut sw = StopWatch::new();
    sw.start();
    let save_result = page_blob.save_messages(&messages_to_save[..]).await;
    sw.pause();
    match save_result {
        Ok(()) => {
            let mut metrics = data_by_topic.metrics.write().await;
            metrics.last_saved_duration = sw.duration();
            metrics.last_saved_chunk = messages_to_save.len();
            metrics.last_saved_moment = DateTimeAsMicroseconds::now();
            metrics.last_saved_message_id = max_message_id;
        }
        Err(error) => handle_save_error(error, data_by_topic, &page.page_id).await,
    }
}

fn get_max_message_id(msgs: &[MessageProtobufModel]) -> i64 {
    let mut max = msgs[0].message_id;

    for msg in msgs {
        if max < msg.message_id {
            max = msg.message_id;
        }
    }

    max
}

async fn handle_save_error(error: AppError, data_by_topic: &DataByTopic, page_id: &MessagePageId) {
    data_by_topic
        .app
        .logs
        .add_info_string(
            Some(data_by_topic.topic_id.as_str()),
            "Saving messages",
            format!(
                "Can no save messages {}/#{} messages to save by there is no loader. Reason: {:?}",
                data_by_topic.topic_id, page_id.value, error
            ),
        )
        .await
}
