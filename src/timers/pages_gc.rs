use std::sync::Arc;

use crate::{app::AppContext, operations::OperationError};

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

        crate::operations::gc::gc_if_needed(
            app.as_ref(),
            topic_data.clone(),
            active_pages.as_slice(),
        )
        .await?;

        crate::operations::gc_yearly_index(app.as_ref(), topic_data.as_ref()).await;
    }

    Ok(())
}
