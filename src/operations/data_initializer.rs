use std::sync::Arc;

use my_service_bus_shared::page_id::PageId;
use rust_extensions::StopWatch;

use crate::{
    app::{file_name_generators::SYSTEM_FILE_NAME, AppContext},
    message_pages::{utils::get_active_pages, MessagePageId},
};

pub async fn init(app: Arc<AppContext>) {
    let topics_snapshots = app.topics_snapshot.get().await;

    let mut sw = StopWatch::new();

    sw.start();

    for topic_snapshot in &topics_snapshots.snapshot.data {
        if topic_snapshot.topic_id == SYSTEM_FILE_NAME {
            continue;
        }

        let current_page_id = MessagePageId::from_message_id(topic_snapshot.message_id);

        let active_pages = get_active_pages(topic_snapshot);

        for page_id in active_pages.values() {
            println!("Restoring {}/{}", topic_snapshot.topic_id, page_id);

            restore_page(
                app.clone(),
                page_id.clone(),
                page_id >= &current_page_id.value,
                topic_snapshot.topic_id.to_string(),
            )
            .await;
        }
    }

    sw.pause();

    app.logs.add_info_string(
        None,
        "Initialization",
        format!("Application is initialized in {:?}", sw.duration()),
    );

    app.app_states.set_initialized();
}

async fn restore_page(
    app: Arc<AppContext>,
    page_id: PageId,
    is_page_current: bool,
    topic_id: String,
) {
    let mut sw = StopWatch::new();
    sw.start();

    app.logs.add_info_string(
        Some(topic_id.as_str()),
        "Initialization",
        format!("Loading messages #{}", page_id),
    );

    let topic_data = app.topics_list.init_topic_data(topic_id.as_str()).await;

    if is_page_current {
        crate::operations::restore_page::open_or_create(app.as_ref(), topic_data.as_ref(), page_id)
            .await;
    } else {
        crate::operations::restore_page::open_uncompressed_or_empty(
            app.as_ref(),
            topic_data.as_ref(),
            page_id,
        )
        .await;
    }

    sw.pause();

    app.logs.add_info_string(
        Some(topic_id.as_str()),
        "Initialization",
        format!("Loaded messages #{} in {:?}", page_id, sw.duration()),
    );
}
