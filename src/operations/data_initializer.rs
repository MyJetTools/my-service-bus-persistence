use std::sync::Arc;

use my_service_bus_shared::MessageId;
use rust_extensions::StopWatch;

use crate::app::{file_name_generators::SYSTEM_FILE_NAME, AppContext};

pub async fn init(app: Arc<AppContext>) {
    let topics_snapshots = app.topics_snapshot.get().await;

    let mut sw = StopWatch::new();

    sw.start();

    for topic_snapshot in &topics_snapshots.snapshot.data {
        if topic_snapshot.topic_id == SYSTEM_FILE_NAME {
            continue;
        }

        restore_page(
            app.clone(),
            topic_snapshot.topic_id.as_str(),
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

async fn restore_page(app: Arc<AppContext>, topic_id: &str, message_id: MessageId) {
    let mut sw = StopWatch::new();
    sw.start();

    app.logs.add_info_string(
        Some(topic_id),
        "Initialization",
        format!("Restoring topic {}", topic_id),
    );

    let topic_data = app.topics_list.init_topic_data(topic_id, message_id).await;

    sw.pause();

    app.logs.add_info_string(
        Some(topic_id),
        "Initialization",
        format!("Topic {} is restored  in {:?}", topic_id, sw.duration()),
    );
}
