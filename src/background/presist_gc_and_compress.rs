use std::sync::Arc;

use crate::{
    app::AppContext, operations::OperationError, sub_page::SubPageId, topic_data::TopicData,
    uncompressed_page::UncompressedPageId,
};

use my_service_bus_shared::protobuf_models::{
    TopicSnapshotProtobufModel, TopicsSnapshotProtobufModel,
};
use rust_extensions::{date_time::DateTimeAsMicroseconds, MyTimerTick};

const MAX_PERSIST_SIZE: usize = 1024 * 1024 * 4;

pub struct PersitGcAndCompressTimer {
    app: Arc<AppContext>,
}

impl PersitGcAndCompressTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for PersitGcAndCompressTimer {
    async fn tick(&self) {
        let topics_snapshot = self.app.topics_snapshot.get().await;

        process(self.app.clone(), topics_snapshot.snapshot)
            .await
            .unwrap();
    }
}

async fn process(
    app: Arc<AppContext>,
    topics: TopicsSnapshotProtobufModel,
) -> Result<(), OperationError> {
    for topic_snapshot in &topics.data {
        let mut topic_data = app.topics_list.get(topic_snapshot.topic_id.as_str()).await;

        if topic_data.is_none() {
            app.logs.add_info(
                Some(topic_snapshot.topic_id.as_str()),
                "Persit Gc And Compress Timer",
                format!(
                    "Topic data {} is not found. Creating it",
                    topic_snapshot.topic_id
                ),
            );
            topic_data = crate::operations::init_new_topic(
                app.as_ref(),
                topic_snapshot.topic_id.as_str(),
                topic_snapshot.message_id,
            )
            .await;
        }

        let topic_data = topic_data.unwrap();

        flush_minute_index_data(topic_data.as_ref()).await;
        persist_messages(&topic_data).await;

        let mut sub_page_id = SubPageId::from_message_id(topic_data.get_max_message_id());

        if sub_page_id.value > 0 {
            sub_page_id.value = sub_page_id.value - 1;
            compress_page_if_needed(&app, &topic_data, sub_page_id).await;
        }

        gc_uncompressed_page(&app, topic_snapshot, &topic_data).await;

        crate::operations::gc_yearly_index(app.as_ref(), topic_data.as_ref()).await;
    }

    Ok(())
}

async fn flush_minute_index_data(topic_data: &TopicData) {
    let mut index_by_minute = topic_data.yearly_index_by_minute.lock().await;

    for item in index_by_minute.values_mut() {
        item.flush_to_storage().await;
    }
}

async fn compress_page_if_needed(
    app: &Arc<AppContext>,
    topic_data: &Arc<TopicData>,
    sub_page_id: &SubPageId,
) {
    let page_id = UncompressedPageId::from_sub_page_id(sub_page_id);
    if let Some(uncompressed_page) = topic_data.uncompressed_pages_list.get(page_id.value).await {
        crate::operations::compress_page_if_needed(
            app.as_ref(),
            topic_data,
            uncompressed_page.as_ref(),
            sub_page_id,
        )
        .await;
    }
}

async fn gc_uncompressed_page(
    app: &Arc<AppContext>,
    topic_snapshot: &TopicSnapshotProtobufModel,
    topic_data: &Arc<TopicData>,
) {
    let active_pages = crate::operations::get_active_pages(topic_snapshot);

    let result = crate::operations::gc_uncompressed_pages(
        app.as_ref(),
        topic_data,
        active_pages.as_slice(),
        topic_snapshot.message_id,
    )
    .await;

    if let Err(err) = result {
        app.logs.add_error(
            Some(topic_snapshot.topic_id.as_str()),
            "compresss_page_if_needed",
            format!("Error during gc_uncompressed_pages"),
            format!("{:?}", err),
        );
    }
}

async fn persist_messages(topic_data: &Arc<TopicData>) {
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
}
