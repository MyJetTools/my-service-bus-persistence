use std::sync::Arc;

use crate::{
    app::AppContext, operations::OperationError, topics_snapshot::TopicSnapshotProtobufModel,
};

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

        gc_pages(self.app.clone(), &topics_snapshot.snapshot.data)
            .await
            .unwrap();
    }
}

async fn gc_pages(
    app: Arc<AppContext>,
    topics: &Vec<TopicSnapshotProtobufModel>,
) -> Result<(), OperationError> {
    for topic_snapshot in topics {
        let topic_data = app.topics_list.get(topic_snapshot.topic_id.as_str()).await;

        if topic_data.is_none() {
            continue;
        }

        let topic_data = topic_data.unwrap();

        if let Some(persist) = topic_snapshot.persist {
            if !persist {
                app.topics_list
                    .remove(topic_snapshot.topic_id.as_str())
                    .await;
                continue;
            }
        }

        crate::operations::gc_pages(app.as_ref(), topic_data.clone()).await?;

        topic_data.yearly_index_by_minute.gc().await;
    }

    Ok(())
}
