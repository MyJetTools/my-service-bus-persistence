use std::sync::Arc;

use my_service_bus_shared::MessageId;
use rust_extensions::StopWatch;

use crate::{
    app::{file_name_generators::SYSTEM_FILE_NAME, AppContext},
    uncompressed_page::UncompressedPageId,
};

pub async fn init(app: Arc<AppContext>) {
    let topics_snapshots = app.topics_snapshot.get().await;

    let mut sw = StopWatch::new();

    sw.start();

    for topic_snapshot in &topics_snapshots.snapshot.data {
        if topic_snapshot.topic_id == SYSTEM_FILE_NAME {
            continue;
        }

        let current_page_id = UncompressedPageId::from_message_id(topic_snapshot.message_id);

        restore_page(
            app.clone(),
            current_page_id,
            topic_snapshot.topic_id.to_string(),
            topic_snapshot.message_id,
        )
        .await;
    }

    sw.pause();

    app.logs.add_info_string(
        None,
        "Initialization",
        format!("Application is initialized in {:?}", sw.duration()),
    );

    app.set_initialized();
}

async fn restore_page(
    app: Arc<AppContext>,
    page_id: UncompressedPageId,
    topic_id: String,
    current_message_id: MessageId,
) {
    let mut sw = StopWatch::new();
    sw.start();

    app.logs.add_info_string(
        Some(topic_id.as_str()),
        "Initialization",
        format!("Loading messages for page #{}", page_id.value),
    );

    let topic_data = app
        .topics_list
        .init_topic_data(topic_id.as_str(), current_message_id)
        .await;

    crate::operations::restore_uncompressed_page::open_if_exists(
        app.as_ref(),
        topic_data.as_ref(),
        &page_id,
    )
    .await;

    sw.pause();

    app.logs.add_info_string(
        Some(topic_id.as_str()),
        "Initialization",
        format!("Loaded messages #{} in {:?}", page_id.value, sw.duration()),
    );
}
