use std::sync::Arc;

use crate::{
    app::{AppContext, TopicData},
    message_pages::MessagePageId,
    operations::OperationError,
};

use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

pub async fn execute(app: Arc<AppContext>, topics: Arc<TopicsSnapshotProtobufModel>) {
    let timer_result = tokio::spawn(timer_tick(app.clone(), topics)).await;

    if let Err(err) = timer_result {
        app.logs.add_fatal_error("pages_gc_timer", err).await;
    }
}

async fn timer_tick(
    app: Arc<AppContext>,
    topics: Arc<TopicsSnapshotProtobufModel>,
) -> Result<(), OperationError> {
    for topic_snapshot in &topics.data {
        let active_pages = crate::operations::get_active_pages(topic_snapshot);

        let topic_data = app
            .as_ref()
            .topics_data_list
            .get(topic_snapshot.topic_id.as_str())
            .await;

        if topic_data.is_none() {
            continue;
        }

        let topic_data = topic_data.unwrap();

        let current_page_id = MessagePageId::from_message_id(topic_snapshot.message_id);

        warm_up_pages(
            app.clone(),
            topic_data.clone(),
            active_pages.as_ref(),
            current_page_id,
        )
        .await;

        crate::operations::gc::gc_if_needed(
            app.as_ref(),
            topic_data.clone(),
            active_pages.as_slice(),
        )
        .await?;
    }

    Ok(())
}

async fn warm_up_pages(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    active_pages: &[MessagePageId],
    current_page_id: MessagePageId,
) {
    for page_id in active_pages {
        crate::operations::pages::get_or_restore(
            app.clone(),
            topic_data.clone(),
            *page_id,
            page_id.value >= current_page_id.value,
        )
        .await;
    }
}
