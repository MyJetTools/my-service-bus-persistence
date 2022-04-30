use std::sync::Arc;

use crate::{
    app::AppContext, message_pages::MessagePageId, operations::OperationError,
    topic_data::TopicData,
};

use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;
use rust_extensions::MyTimerTick;

pub struct PagesGcTimer {
    app: Arc<AppContext>,
}

impl PagesGcTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for PagesGcTimer {
    async fn tick(&self) {
        let topics_snapshot = self.app.topics_snapshot.get().await;
        gc_pages(self.app.clone(), topics_snapshot.snapshot)
            .await
            .unwrap();
    }
}

async fn gc_pages(
    app: Arc<AppContext>,
    topics: TopicsSnapshotProtobufModel,
) -> Result<(), OperationError> {
    for topic_snapshot in &topics.data {
        let active_pages = crate::operations::get_active_pages(topic_snapshot);

        let topic_data = app.topics_list.get(topic_snapshot.topic_id.as_str()).await;

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
    todo!("Implement");
    /*
    for page_id in active_pages {
        crate::operations::pages::get_or_restore(
            app.clone(),
            topic_data.clone(),
            *page_id,
            page_id.value >= current_page_id.value,
        )
        .await;
    }
     */
}
