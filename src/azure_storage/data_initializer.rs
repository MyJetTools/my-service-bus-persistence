use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::{
    app::AppContext,
    azure_storage::consts::SYSTEM_BLOB_NAME,
    message_pages::{utils::get_active_pages, MessagePageId},
    utils::StopWatch,
};

pub struct LoadingTopicHandle {
    pub join_handle: JoinHandle<()>,
    pub topic_id: String,
}

pub async fn init(app: Arc<AppContext>) {
    let topics_snapshots = app.topics_snapshot.get().await;

    let mut result = Vec::new();
    let mut sw = StopWatch::new();

    sw.start();

    for topic_snapshot in &topics_snapshots.snapshot.data {
        if topic_snapshot.topic_id == SYSTEM_BLOB_NAME {
            continue;
        }

        let current_page_id = MessagePageId::from_message_id(topic_snapshot.message_id);

        let active_pages = get_active_pages(topic_snapshot);

        for page_id in active_pages.values() {
            let restore_page_future = restore_page(
                app.clone(),
                page_id.clone(),
                page_id.value >= current_page_id.value,
                topic_snapshot.topic_id.to_string(),
            );

            let handler = tokio::spawn(restore_page_future);

            let handler = LoadingTopicHandle {
                join_handle: handler,
                topic_id: topic_snapshot.topic_id.clone(),
            };

            let handler = tokio::spawn(handle_init_result(handler));

            result.push(handler)
        }
    }

    for itm in result {
        itm.await.unwrap();
    }

    sw.pause();

    app.logs
        .add_info_string(
            None,
            "Initialization",
            format!("Application is initialized in {:?}", sw.duration()),
        )
        .await;

    app.set_initialized();
}

async fn handle_init_result(handle: LoadingTopicHandle) {
    let result = handle.join_handle.await;

    if let Err(err) = result {
        panic!(
            "Error loading topic {} data. Err {:?} ",
            handle.topic_id, err
        )
    }
}

async fn restore_page(
    app: Arc<AppContext>,
    page_id: MessagePageId,
    is_page_current: bool,
    topic_id: String,
) {
    let mut sw = StopWatch::new();
    sw.start();

    app.logs
        .add_info_string(
            Some(topic_id.as_str()),
            "Initialization",
            format!("Loading messages #{}", page_id.value),
        )
        .await;

    let topic_data = app
        .topics_data_list
        .get_or_create_data_by_topic(topic_id.as_str(), app.clone())
        .await;

    crate::operations::pages::get_or_restore(app.clone(), topic_data, page_id, is_page_current)
        .await;

    sw.pause();

    app.logs
        .add_info_string(
            Some(topic_id.as_str()),
            "Initialization",
            format!("Loaded messages #{} in {:?}", page_id.value, sw.duration()),
        )
        .await;
}
